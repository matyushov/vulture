//! A local stand-in for the public Binance USDT-M endpoints, for trying `vulture-server` without any exchange
//! traffic. It serves, on loopback only:
//!
//! - HTTP: `GET /fapi/v1/exchangeInfo` (the repository fixture) and `GET /fapi/v1/openInterest?symbol=...`
//!   (synthetic values that drift upwards);
//! - WebSocket: acknowledges `SUBSCRIBE`, then sends a mark price frame every second and a liquidation every
//!   700 ms for BTCUSDT and ETHUSDT.
//!
//! ```sh
//! cargo run -p vulture-server --example mock_binance -- --http 127.0.0.1:18080 --ws 127.0.0.1:18081
//! ```
//!
//! and point the connector at it in the server configuration:
//!
//! ```toml
//! [[exchanges]]
//! name = "binance"
//! ws_url = "ws://127.0.0.1:18081"
//! rest_url = "http://127.0.0.1:18080"
//! oi_poll_interval_secs = 2
//! ```

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message;

const EXCHANGE_INFO: &str = include_str!("../../../fixtures/binance/exchange_info_sample.json");

#[derive(Parser)]
#[command(about = "Local mock of the Binance USDT-M public endpoints (loopback only)")]
struct Args {
    /// HTTP listen address (must be loopback).
    #[arg(long, default_value = "127.0.0.1:18080")]
    http: SocketAddr,
    /// WebSocket listen address (must be loopback).
    #[arg(long, default_value = "127.0.0.1:18081")]
    ws: SocketAddr,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    anyhow::ensure!(
        args.http.ip().is_loopback() && args.ws.ip().is_loopback(),
        "the mock only listens on loopback"
    );
    let http = TcpListener::bind(args.http).await?;
    let ws = TcpListener::bind(args.ws).await?;
    eprintln!("mock binance: http://{}  ws://{}", args.http, args.ws);
    let counters = Arc::new(Mutex::new(HashMap::<String, u64>::new()));
    tokio::spawn(async move {
        loop {
            if let Ok((socket, _)) = ws.accept().await {
                tokio::spawn(serve_ws(socket));
            }
        }
    });
    loop {
        let (socket, _) = http.accept().await?;
        tokio::spawn(serve_http(socket, counters.clone()));
    }
}

async fn serve_http(mut socket: TcpStream, counters: Arc<Mutex<HashMap<String, u64>>>) {
    let mut buf = vec![0u8; 4096];
    let Ok(n) = socket.read(&mut buf).await else {
        return;
    };
    let request = String::from_utf8_lossy(&buf[..n]).to_string();
    let target = request.split_whitespace().nth(1).unwrap_or("/");
    let body = if target.starts_with("/fapi/v1/exchangeInfo") {
        EXCHANGE_INFO.to_string()
    } else if let Some(query) = target.strip_prefix("/fapi/v1/openInterest?") {
        let symbol = query
            .split('&')
            .find_map(|kv| kv.strip_prefix("symbol="))
            .unwrap_or("BTCUSDT")
            .to_string();
        let step = {
            let mut map = counters.lock().unwrap();
            let c = map.entry(symbol.clone()).or_insert(0);
            *c += 1;
            *c
        };
        format!(
            r#"{{"symbol":"{symbol}","openInterest":"{:.3}","time":{}}}"#,
            90_000.0 + step as f64 * 3.5,
            now_ms()
        )
    } else {
        let _ = socket
            .write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .await;
        return;
    };
    let reply = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = socket.write_all(reply.as_bytes()).await;
}

async fn serve_ws(socket: TcpStream) {
    let Ok(mut ws) = tokio_tungstenite::accept_async(socket).await else {
        return;
    };
    // Wait for the SUBSCRIBE request and acknowledge it.
    loop {
        match ws.next().await {
            Some(Ok(Message::Text(text))) if text.contains("SUBSCRIBE") => {
                if ws
                    .send(Message::text(r#"{"result":null,"id":1}"#))
                    .await
                    .is_err()
                {
                    return;
                }
                break;
            }
            Some(Ok(_)) => {}
            _ => return,
        }
    }
    let mut marks = tokio::time::interval(Duration::from_secs(1));
    let mut liqs = tokio::time::interval(Duration::from_millis(700));
    let mut n: u64 = 0;
    loop {
        let frame = tokio::select! {
            _ = marks.tick() => {
                let t = now_ms();
                format!(
                    r#"[{{"e":"markPriceUpdate","E":{t},"s":"BTCUSDT","p":"64000.10","P":"64000.10","i":"64000.10","r":"0.0001","T":{t}}},{{"e":"markPriceUpdate","E":{t},"s":"ETHUSDT","p":"3100.50","P":"3100.50","i":"3100.50","r":"0.0001","T":{t}}}]"#
                )
            }
            _ = liqs.tick() => {
                n += 1;
                let t = now_ms();
                let (symbol, side, price) = if n.is_multiple_of(2) { ("BTCUSDT", "SELL", "63950.00") } else { ("ETHUSDT", "BUY", "3105.00") };
                format!(
                    r#"{{"e":"forceOrder","E":{},"o":{{"s":"{symbol}","S":"{side}","o":"LIMIT","f":"IOC","q":"0.5","p":"{price}","ap":"{price}","X":"FILLED","l":"0.5","z":"0.5","T":{t},"ps":"{symbol}","st":1}}}}"#,
                    t + 5
                )
            }
            msg = ws.next() => match msg {
                Some(Ok(_)) => continue,
                _ => return,
            },
        };
        if ws.send(Message::text(frame)).await.is_err() {
            return;
        }
    }
}
