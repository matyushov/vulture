//! A local stand-in for the Telegram Bot API and Discord webhooks, to try the alert delivery without touching a
//! real service. It listens on loopback only, answers every POST with success and prints what it received (path
//! with the token masked, and the body).
//!
//! ```sh
//! cargo run -p vulture-server --example mock_chat -- --listen 127.0.0.1:18090
//! ```
//!
//! Point a channel at it: `[alerts.delivery] telegram_base_url = "http://127.0.0.1:18090"` for Telegram, and for
//! Discord a webhook URL `http://127.0.0.1:18090/api/webhooks/1/token` together with
//! `[alerts.delivery] allow_custom_endpoints = true`.

use std::net::SocketAddr;

use clap::Parser;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[derive(Parser)]
#[command(about = "Local mock of Telegram and Discord (loopback only)")]
struct Args {
    /// Listen address (must be loopback).
    #[arg(long, default_value = "127.0.0.1:18090")]
    listen: SocketAddr,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    anyhow::ensure!(
        args.listen.ip().is_loopback(),
        "the mock only listens on loopback"
    );
    let listener = TcpListener::bind(args.listen).await?;
    eprintln!("mock chat listening on {}", listener.local_addr()?);
    loop {
        let (mut sock, _) = listener.accept().await?;
        tokio::spawn(async move {
            let mut buf = Vec::new();
            let mut chunk = [0u8; 8192];
            loop {
                let Ok(n) = sock.read(&mut chunk).await else {
                    return;
                };
                if n == 0 {
                    return;
                }
                buf.extend_from_slice(&chunk[..n]);
                let Some(head_end) = buf.windows(4).position(|w| w == b"\r\n\r\n").map(|i| i + 4)
                else {
                    continue;
                };
                let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
                let len: usize = head
                    .lines()
                    .find_map(|l| {
                        l.to_lowercase()
                            .strip_prefix("content-length:")
                            .map(|v| v.trim().parse().unwrap_or(0))
                    })
                    .unwrap_or(0);
                if buf.len() < head_end + len {
                    continue;
                }
                let line = head.lines().next().unwrap_or("");
                let path = line.split(' ').nth(1).unwrap_or("");
                // Telegram puts the token in the path (/bot<token>/sendMessage): never print it.
                let shown = match path.strip_prefix("/bot") {
                    Some(rest) => format!("/bot****/{}", rest.rsplit('/').next().unwrap_or("")),
                    None => path.to_string(),
                };
                println!(
                    "POST {shown}\n{}\n",
                    String::from_utf8_lossy(&buf[head_end..head_end + len])
                );
                let body = r#"{"ok":true}"#;
                let reply = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = sock.write_all(reply.as_bytes()).await;
                let _ = sock.shutdown().await;
                return;
            }
        });
    }
}
