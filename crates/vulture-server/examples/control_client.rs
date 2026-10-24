//! A small client for the symbol RPCs of `Control`, to manage tracked symbols by hand. The proper CLI comes later.
//!
//! ```sh
//! export VULTURE_TOKEN=$(vulture-server token show --config config.toml)
//! cargo run -p vulture-server --example control_client -- list
//! cargo run -p vulture-server --example control_client -- set --exchange binance --mode blacklist \
//!     --symbol ETH-USDT-PERP
//! cargo run -p vulture-server --example control_client -- set --exchange binance --mode all
//! ```
//!
//! Modes: `all`, `whitelist`, `blacklist` (repeat `--symbol`). Connection flags are the same as in
//! `stream_client`: `https://` endpoints use TLS (`--ca-cert`, `--domain`, `--client-cert`, `--client-key`), the
//! token comes from `--token` or `VULTURE_TOKEN`.

use clap::{Parser, Subcommand};
use tonic::metadata::MetadataValue;
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity};
use vulture_proto::v1::control_client::ControlClient;
use vulture_proto::v1::{
    ListSymbolPoliciesRequest, SetSymbolPolicyRequest, SymbolMode, SymbolPolicy,
};

#[derive(Parser)]
#[command(about = "Lists and sets the tracked symbols of vulture Control")]
struct Args {
    /// Server endpoint, for example http://127.0.0.1:50051.
    #[arg(long, default_value = "http://127.0.0.1:50051")]
    endpoint: String,
    /// Bearer token (or set VULTURE_TOKEN).
    #[arg(long)]
    token: Option<String>,
    /// PEM file with the CA that signed the server certificate (https endpoints; default: public web roots).
    #[arg(long)]
    ca_cert: Option<std::path::PathBuf>,
    /// Name to verify in the server certificate when it differs from the host of the endpoint.
    #[arg(long)]
    domain: Option<String>,
    /// PEM client certificate for mTLS (with --client-key).
    #[arg(long, requires = "client_key")]
    client_cert: Option<std::path::PathBuf>,
    /// PEM client private key for mTLS (with --client-cert).
    #[arg(long, requires = "client_cert")]
    client_key: Option<std::path::PathBuf>,
    #[command(subcommand)]
    command: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Show the policy and the counts of the exchanges (all when none is named).
    List {
        /// Exchange to show (repeatable).
        #[arg(long = "exchange")]
        exchanges: Vec<String>,
    },
    /// Replace the policy of one exchange.
    Set {
        /// Exchange name as in the server configuration.
        #[arg(long)]
        exchange: String,
        /// all, whitelist or blacklist.
        #[arg(long)]
        mode: String,
        /// Normalized symbol, for example BTC-USDT-PERP (repeatable).
        #[arg(long = "symbol")]
        symbols: Vec<String>,
    },
}

fn endpoint(args: &Args) -> anyhow::Result<Endpoint> {
    let mut endpoint = Endpoint::from_shared(args.endpoint.clone())?;
    if args.endpoint.starts_with("https://") {
        let mut tls = ClientTlsConfig::new();
        tls = match &args.ca_cert {
            Some(path) => tls.ca_certificate(Certificate::from_pem(std::fs::read(path)?)),
            None => tls.with_webpki_roots(),
        };
        if let Some(domain) = &args.domain {
            tls = tls.domain_name(domain.clone());
        }
        if let (Some(cert), Some(key)) = (&args.client_cert, &args.client_key) {
            tls = tls.identity(Identity::from_pem(
                std::fs::read(cert)?,
                std::fs::read(key)?,
            ));
        }
        endpoint = endpoint.tls_config(tls)?;
    }
    Ok(endpoint)
}

fn mode_name(mode: i32) -> &'static str {
    match SymbolMode::try_from(mode) {
        Ok(SymbolMode::All) => "all",
        Ok(SymbolMode::Whitelist) => "whitelist",
        Ok(SymbolMode::Blacklist) => "blacklist",
        _ => "unspecified",
    }
}

fn show(policy: &SymbolPolicy) {
    let mut list = policy
        .symbols
        .iter()
        .take(10)
        .cloned()
        .collect::<Vec<_>>()
        .join(",");
    if policy.symbols.len() > 10 {
        list.push_str(&format!(",...(+{})", policy.symbols.len() - 10));
    }
    println!(
        "{} mode={} symbols=[{list}]",
        policy.exchange,
        mode_name(policy.mode)
    );
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let token = match (&args.token, std::env::var("VULTURE_TOKEN")) {
        (Some(token), _) => token.clone(),
        (None, Ok(token)) => token,
        (None, Err(_)) => anyhow::bail!("pass --token or set VULTURE_TOKEN"),
    };
    let header: MetadataValue<_> = format!("Bearer {token}").parse()?;
    let channel = endpoint(&args)?.connect().await?;
    let mut client = ControlClient::with_interceptor(channel, move |mut req: tonic::Request<()>| {
        req.metadata_mut().insert("authorization", header.clone());
        Ok(req)
    });
    match args.command {
        Cmd::List { exchanges } => {
            let resp = client
                .list_symbol_policies(ListSymbolPoliciesRequest { exchanges })
                .await?
                .into_inner();
            for (policy, status) in resp.policies.iter().zip(&resp.statuses) {
                show(policy);
                println!(
                    "  tracked={} universe={} universe_loaded={} overridden={}",
                    status.tracked_count,
                    status.universe_size,
                    status.universe_loaded,
                    status.overridden
                );
            }
        }
        Cmd::Set {
            exchange,
            mode,
            symbols,
        } => {
            let mode = match mode.as_str() {
                "all" => SymbolMode::All,
                "whitelist" => SymbolMode::Whitelist,
                "blacklist" => SymbolMode::Blacklist,
                other => anyhow::bail!("unknown mode {other:?} (all, whitelist, blacklist)"),
            };
            let resp = client
                .set_symbol_policy(SetSymbolPolicyRequest {
                    policy: Some(SymbolPolicy {
                        exchange,
                        mode: mode as i32,
                        symbols,
                        oi_intervals: vec![],
                    }),
                })
                .await?
                .into_inner();
            if let Some(p) = &resp.policy {
                show(p);
            }
            println!(
                "  changed={} tracked={} universe={} added={:?} removed={:?}",
                resp.changed, resp.tracked_count, resp.universe_size, resp.added, resp.removed
            );
        }
    }
    Ok(())
}
