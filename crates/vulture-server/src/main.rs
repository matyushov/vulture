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
    }
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
