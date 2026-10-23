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
    println!(
        "config OK: {} exchange(s), {} enabled",
        config.exchanges.len(),
        config.enabled_exchanges().count()
    );
    Ok(())
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
