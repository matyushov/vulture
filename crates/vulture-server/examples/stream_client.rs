//! A small client for `Stream.Subscribe`, to try the server by hand.
//!
//! ```sh
//! TOKEN=$(vulture-server token show --config config.toml)
//! cargo run -p vulture-server --example stream_client -- --endpoint http://127.0.0.1:50051 \
//!     --token "$TOKEN" --resume-from 0 --exchange binance --max-events 20
//! ```
//!
//! The token can also come from the `VULTURE_TOKEN` environment variable (preferred: it stays out of the
//! process list). Every event is printed as one line; the last printed id is what to pass to `--resume-from`
//! after a disconnect. With `--reconnect` the client reconnects by itself with the last id it received.

use std::time::Duration;

use clap::Parser;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use vulture_proto::v1::event::Payload;
use vulture_proto::v1::stream_client::StreamClient;
use vulture_proto::v1::{Event, EventType, SubscribeRequest};

#[derive(Parser)]
#[command(about = "Prints the events of vulture Stream.Subscribe")]
struct Args {
    /// Server endpoint, for example http://127.0.0.1:50051.
    #[arg(long, default_value = "http://127.0.0.1:50051")]
    endpoint: String,
    /// Bearer token (or set VULTURE_TOKEN).
    #[arg(long)]
    token: Option<String>,
    /// Empty = live only, "0" = replay the hot window, otherwise an Event.id (exclusive).
    #[arg(long, default_value = "")]
    resume_from: String,
    /// Exchange filter (repeatable).
    #[arg(long = "exchange")]
    exchanges: Vec<String>,
    /// Normalized symbol filter, for example BTC-USDT-PERP (repeatable).
    #[arg(long = "symbol")]
    symbols: Vec<String>,
    /// Event type filter: liquidation, oi, gap, signal (repeatable).
    #[arg(long = "type")]
    types: Vec<String>,
    /// Stop after this many events (0 = run until interrupted).
    #[arg(long, default_value_t = 0)]
    max_events: u64,
    /// Reconnect after an error, resuming from the last received id.
    #[arg(long)]
    reconnect: bool,
}

fn event_type(name: &str) -> anyhow::Result<EventType> {
    Ok(match name {
        "liquidation" | "liq" => EventType::Liquidation,
        "oi" | "open_interest" => EventType::OpenInterest,
        "gap" => EventType::Gap,
        "signal" => EventType::Signal,
        other => anyhow::bail!("unknown event type {other:?} (liquidation, oi, gap, signal)"),
    })
}

fn describe(event: &Event) -> String {
    let body = match &event.payload {
        Some(Payload::Liquidation(l)) => format!(
            "liq {} {} side={} price={} qty={} usd={:.2}",
            l.exchange, l.symbol, l.side, l.price, l.qty, l.notional_usd
        ),
        Some(Payload::OpenInterest(o)) => format!(
            "oi {} {} oi_base={} oi_usd={:?}",
            o.exchange, o.symbol, o.oi_base, o.oi_usd
        ),
        Some(Payload::Gap(g)) => {
            format!("gap {} {} {}..{}", g.exchange, g.stream, g.from_ts, g.to_ts)
        }
        Some(Payload::Signal(s)) => format!(
            "signal type={} {} {} window={}s change={:.2}%",
            s.r#type, s.exchange, s.symbol, s.window_secs, s.change_pct
        ),
        None => "empty".to_string(),
    };
    format!("{} {body}", event.id)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let types = args
        .types
        .iter()
        .map(|t| event_type(t).map(|t| t as i32))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let token = match (&args.token, std::env::var("VULTURE_TOKEN")) {
        (Some(token), _) => token.clone(),
        (None, Ok(token)) => token,
        (None, Err(_)) => anyhow::bail!("pass --token or set VULTURE_TOKEN"),
    };
    let header: MetadataValue<_> = format!("Bearer {token}").parse()?;
    let mut cursor = args.resume_from.clone();
    let mut received = 0u64;
    loop {
        let channel = Channel::from_shared(args.endpoint.clone())?.connect().await;
        let channel = match channel {
            Ok(c) => c,
            Err(e) if args.reconnect => {
                eprintln!("cannot connect ({e}); retrying");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
            Err(e) => return Err(e.into()),
        };
        let header = header.clone();
        let mut client =
            StreamClient::with_interceptor(channel, move |mut req: tonic::Request<()>| {
                req.metadata_mut().insert("authorization", header.clone());
                Ok(req)
            });
        let request = SubscribeRequest {
            exchanges: args.exchanges.clone(),
            symbols: args.symbols.clone(),
            types: types.clone(),
            resume_from: cursor.clone(),
        };
        eprintln!("subscribing (resume_from={:?})", cursor);
        let outcome = match client.subscribe(request).await {
            Ok(response) => {
                let mut stream = response.into_inner();
                loop {
                    match stream.message().await {
                        Ok(Some(event)) => {
                            println!("{}", describe(&event));
                            cursor = event.id;
                            received += 1;
                            if args.max_events > 0 && received >= args.max_events {
                                return Ok(());
                            }
                        }
                        Ok(None) => break Ok(()),
                        Err(status) => break Err(status),
                    }
                }
            }
            Err(status) => Err(status),
        };
        match outcome {
            Err(status)
                if args.reconnect
                    && status.code() != tonic::Code::OutOfRange
                    && status.code() != tonic::Code::Unauthenticated =>
            {
                eprintln!("stream failed: {status}; reconnecting from {cursor:?}");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(status) => anyhow::bail!("stream failed: {status}"),
            Ok(()) if args.reconnect => tokio::time::sleep(Duration::from_secs(1)).await,
            Ok(()) => return Ok(()),
        }
    }
}
