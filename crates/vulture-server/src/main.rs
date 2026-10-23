//! `vulture-server` command line.

use std::path::PathBuf;

use anyhow::Context;
use clap::{Parser, Subcommand};
use tokio_util::sync::CancellationToken;
use vulture_server::app::{self, AppContext};
use vulture_server::config::Config;
use vulture_server::logging;

#[derive(Parser)]
#[command(name = "vulture-server", version, about = "vulture server")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the server.
    Run {
        /// Path to the TOML configuration file.
        #[arg(long)]
        config: PathBuf,
    },
    /// Configuration utilities.
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
    },
    /// Access token management (the token file lives in `server.data_dir`).
    Token {
        #[command(subcommand)]
        command: TokenCommand,
    },
    /// TLS utilities.
    Tls {
        #[command(subcommand)]
        command: TlsCommand,
    },
    /// Ask the running server's metrics endpoint whether it is alive (`/healthz`) or ready (`/readyz`); exit
    /// code 0 means yes. Meant for the container health check.
    Healthcheck {
        /// Path to the TOML configuration file (for `metrics.listen_addr`).
        #[arg(long)]
        config: PathBuf,
        /// Check `/readyz` (Redis reachable and the gRPC listener running) instead of `/healthz`.
        #[arg(long)]
        ready: bool,
    },
}

#[derive(Subcommand)]
enum TlsCommand {
    /// Validate the certificate, the key and the client CA named in `[tls]` without starting a server: files
    /// readable, PEM valid, key matches the certificate, certificate valid now. Prints the expiry.
    Check {
        /// Path to the TOML configuration file.
        #[arg(long)]
        config: PathBuf,
    },
}

#[derive(Subcommand)]
enum TokenCommand {
    /// Print the token to stdout (nothing else is printed there).
    Show {
        /// Path to the TOML configuration file (for `server.data_dir`).
        #[arg(long)]
        config: PathBuf,
    },
    /// Create a new token and print it to stdout. The old one stops working; a running server notices within
    /// `server.token_check_interval_ms` and ends the streams that used it.
    Rotate {
        /// Path to the TOML configuration file (for `server.data_dir`).
        #[arg(long)]
        config: PathBuf,
    },
    /// Create the token if it does not exist yet (and print it once, to stderr). Does nothing if it exists.
    Create {
        /// Path to the TOML configuration file (for `server.data_dir`).
        #[arg(long)]
        config: PathBuf,
    },
}

#[derive(Subcommand)]
enum ConfigCommand {
    /// Validate a configuration file (with environment overrides applied) and exit.
    Check {
        /// Path to the TOML configuration file.
        #[arg(long)]
        config: PathBuf,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    match Cli::parse().command {
        Command::Run { config } => run(config).await,
        Command::Config {
            command: ConfigCommand::Check { config },
        } => check(config),
        Command::Token { command } => token(command),
        Command::Tls {
            command: TlsCommand::Check { config },
        } => tls_check(config),
        Command::Healthcheck { config, ready } => healthcheck(config, ready).await,
    }
}

fn token(command: TokenCommand) -> anyhow::Result<()> {
    use vulture_server::auth::{Created, TokenStore, print_created_banner};
    let (TokenCommand::Show { config }
    | TokenCommand::Rotate { config }
    | TokenCommand::Create { config }) = &command;
    let config = Config::load(config)?;
    let store = TokenStore::new(&config.server.data_dir);
    match command {
        TokenCommand::Show { .. } => {
            // Only the token goes to stdout, so `$(vulture-server token show ...)` is safe to use.
            println!("{}", store.read_required()?.expose());
        }
        TokenCommand::Rotate { .. } => {
            println!("{}", store.rotate()?.expose());
            eprintln!("token rotated; the previous token no longer works");
        }
        TokenCommand::Create { .. } => match store.create_if_missing()? {
            Created::New(token) => print_created_banner(&token, store.path()),
            Created::Existing(_) => eprintln!(
                "a token already exists at {}; use `token show` to read it or `token rotate` to replace it",
                store.path().display()
            ),
        },
    }
    Ok(())
}

fn check(path: PathBuf) -> anyhow::Result<()> {
    let config = Config::load(&path)?;
    // Secrets are only checked for being readable, never printed.
    if let Some(url) = config.redis.resolve_url()? {
        vulture_server::store::validate_url(url.expose())
            .map_err(|e| anyhow::anyhow!("redis URL: {e}"))?;
    }
    if let Some(file) = &config.normalize.base_aliases_file {
        vulture_server::normalize::BaseAliases::load(file)?;
    }
    if config.tls.is_some() {
        tls_report(&config)?;
    }
    println!(
        "config OK: {} exchange(s), {} enabled",
        config.exchanges.len(),
        config.enabled_exchanges().count()
    );
    Ok(())
}

/// Checks the files of `[tls]` and prints what is known about the certificate.
fn tls_report(config: &Config) -> anyhow::Result<()> {
    let tls = config
        .tls
        .as_ref()
        .context("no [tls] section in the configuration")?;
    let info = vulture_server::tls::check_files(tls)?;
    let days = info.expires_in_secs() / 86_400;
    println!(
        "tls OK: certificate for {} expires {} (in {days} days){}",
        info.subject,
        info.not_after,
        if tls.client_ca.is_some() {
            "; client certificates required (mTLS)"
        } else {
            ""
        }
    );
    if info.expires_in_secs() < vulture_server::tls::EXPIRY_WARNING.as_secs() as i64 {
        eprintln!("warning: the certificate expires in less than 14 days");
    }
    Ok(())
}

fn tls_check(path: PathBuf) -> anyhow::Result<()> {
    tls_report(&Config::load(&path)?)
}

async fn healthcheck(path: PathBuf, ready: bool) -> anyhow::Result<()> {
    let config = Config::load(&path)?;
    let mut addr: std::net::SocketAddr = config.metrics.listen_addr.parse()?;
    if addr.ip().is_unspecified() {
        addr.set_ip(std::net::Ipv4Addr::LOCALHOST.into());
    }
    let route = if ready { "/readyz" } else { "/healthz" };
    match vulture_server::metrics::healthcheck(addr, route).await {
        Ok((200, _)) => Ok(()),
        Ok((status, body)) => anyhow::bail!("{route} answered {status}: {}", body.trim()),
        Err(e) => anyhow::bail!("{e}"),
    }
}

async fn run(path: PathBuf) -> anyhow::Result<()> {
    let config = Config::load(&path)?;
    logging::init(&config.logging)?;
    tracing::info!(
        version = env!("CARGO_PKG_VERSION"),
        config = %path.display(),
        "vulture-server starting"
    );
    let shutdown = CancellationToken::new();
    app::cancel_on_signals(shutdown.clone()).context("cannot install signal handlers")?;
    let ctx = AppContext::from_config(config, shutdown)?;
    app::run(ctx).await?;
    Ok(())
}
