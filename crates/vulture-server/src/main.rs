//! `vulture-server` command line.

use std::path::PathBuf;

use std::io::Read as _;

use anyhow::Context;
use clap::{Args, Parser, Subcommand};
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
#[allow(clippy::large_enum_variant)]
enum Command {
    /// Run the server.
    Run {
        /// Path to the TOML configuration file.
        #[arg(long)]
        config: PathBuf,
    },
    /// Apply the ClickHouse schema migrations (idempotent, additive, never destructive) and exit. The
    /// `[clickhouse]` section is required; `run` applies the same migrations at start.
    Migrate {
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
    /// Offline analysis of the stored history (read-only against ClickHouse).
    Analyze {
        #[command(subcommand)]
        command: AnalyzeCommand,
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

/// Output of the analysis commands.
#[derive(Args)]
struct OutputArgs {
    /// `md` (default), `json`, or `both` (Markdown first, then JSON, separated by a line).
    #[arg(long, default_value = "md")]
    format: String,
    /// Also write `report.md` and `report.json` into this directory.
    #[arg(long)]
    out_dir: Option<PathBuf>,
}

/// Settings of the event study shared by both commands.
#[derive(Args)]
struct StudyArgs {
    /// Split the groups by signal strength (info, warning, critical).
    #[arg(long)]
    by_strength: bool,
    /// Return thresholds of the hit rates, percent, comma separated.
    #[arg(long, default_value = "0.1,0.25,0.5,1")]
    thresholds: String,
    /// Market proxy for liquidation cascades: `exchange:symbol`.
    #[arg(long, default_value = "binance:BTC-USDT-PERP")]
    market_proxy: String,
}

#[derive(Subcommand)]
#[allow(clippy::large_enum_variant)]
enum AnalyzeCommand {
    /// Event study of the stored signals: forward mark-price returns at +5, +15 and +60 minutes, hit rates and
    /// the liquidations of the next hour, per signal type, window and exchange. Only `SELECT`s are issued.
    Signals {
        /// Path to the TOML configuration file (for `[clickhouse]`).
        #[arg(long)]
        config: PathBuf,
        /// Start: epoch milliseconds, `YYYY-MM-DD` or `YYYY-MM-DDTHH:MM[:SS]Z` (UTC).
        #[arg(long)]
        from: String,
        /// End (exclusive), same formats.
        #[arg(long)]
        to: String,
        /// Only signals of these exchanges (repeatable).
        #[arg(long = "exchange")]
        exchanges: Vec<String>,
        #[command(flatten)]
        study: StudyArgs,
        #[command(flatten)]
        output: OutputArgs,
    },
    /// Replays a detector (the engine's own core) over stored history with other thresholds and reports the
    /// signals it would have produced with the same statistics. No Redis, delivery or rule changes are involved.
    /// `--params` may be repeated to compare threshold sets. oi_spike keys: z_min, min_oi_usd, cooldown_secs,
    /// extension_factor, min_baseline_points, require_baseline, windows=300+900, threshold_pct.<secs>,
    /// min_change_usd.<secs>. liq_cascade keys: k_mad, floor_usd, floor_usd_exchange, min_count, quiet_secs,
    /// end_ratio, min_baseline_minutes, baseline_secs, top_symbols, per_exchange.
    Replay {
        /// Path to the TOML configuration file (for `[clickhouse]` and the `[alerts]` defaults).
        #[arg(long)]
        config: PathBuf,
        /// `oi_spike` or `liq_cascade`.
        #[arg(long)]
        detector: String,
        /// Parameters as `key=value,key=value` (repeatable; none = the configured defaults).
        #[arg(long = "params")]
        params: Vec<String>,
        /// Start of the period (signals before it are not reported).
        #[arg(long)]
        from: String,
        /// End of the period (exclusive).
        #[arg(long)]
        to: String,
        /// State-only run before `--from`, seconds (the live engine warms up with 4 hours).
        #[arg(long, default_value_t = 14_400)]
        warmup_secs: u64,
        /// Length of one read from ClickHouse, seconds.
        #[arg(long, default_value_t = 1_200)]
        chunk_secs: u64,
        /// Only these exchanges (repeatable).
        #[arg(long = "exchange")]
        exchanges: Vec<String>,
        /// Only these normalized symbols (repeatable).
        #[arg(long = "symbol")]
        symbols: Vec<String>,
        /// Signals listed per case in the Markdown.
        #[arg(long, default_value_t = 50)]
        list_max: usize,
        #[command(flatten)]
        study: StudyArgs,
        #[command(flatten)]
        output: OutputArgs,
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
    /// Replace the token with one read from stdin (never from the command line, so it stays out of the shell
    /// history and the process list). Validated like a hand edit; written atomically with mode 0600. A running
    /// server notices within `server.token_check_interval_ms`.
    Set {
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
        Command::Migrate { config } => migrate(config).await,
        Command::Config {
            command: ConfigCommand::Check { config },
        } => check(config),
        Command::Token { command } => token(command),
        Command::Tls {
            command: TlsCommand::Check { config },
        } => tls_check(config),
        Command::Healthcheck { config, ready } => healthcheck(config, ready).await,
        Command::Analyze { command } => analyze(command).await,
    }
}

fn token(command: TokenCommand) -> anyhow::Result<()> {
    use vulture_server::auth::{Created, TokenStore, announce_created};
    let (TokenCommand::Show { config }
    | TokenCommand::Rotate { config }
    | TokenCommand::Set { config }
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
        TokenCommand::Set { .. } => {
            let token = read_token_from(&mut std::io::stdin().lock())?;
            store.set(&token)?;
            eprintln!(
                "token stored in {}; a running server switches to it within its check interval",
                store.path().display()
            );
        }
        TokenCommand::Create { .. } => match store.create_if_missing()? {
            Created::New(token) => {
                let tty = std::io::IsTerminal::is_terminal(&std::io::stderr());
                if tty {
                    announce_created(&token, store.path(), true, &mut std::io::stderr());
                } else {
                    // No terminal: never print the value into a log; say where it is.
                    eprintln!(
                        "token file created at {}; read it with `token show` or `cat` on the host",
                        store.path().display()
                    );
                }
            }
            Created::Existing(_) => eprintln!(
                "a token already exists at {}; use `token show` to read it or `token rotate` to replace it",
                store.path().display()
            ),
        },
    }
    Ok(())
}

/// Reads a new token from `input` (at most 4 KiB; surrounding whitespace is dropped) and validates it. The
/// error never contains what was read.
fn read_token_from(
    input: &mut dyn std::io::Read,
) -> anyhow::Result<vulture_server::config::Secret> {
    use vulture_server::auth::{MIN_TOKEN_LEN, valid_token};
    let mut text = String::new();
    input
        .take(4096)
        .read_to_string(&mut text)
        .context("cannot read the token from stdin")?;
    let token = text.trim();
    anyhow::ensure!(
        valid_token(token),
        "the token read from stdin is not valid (expected at least {MIN_TOKEN_LEN} characters from A-Z a-z 0-9 - _); the token file was not changed"
    );
    Ok(vulture_server::config::Secret::new(token))
}

fn check(path: PathBuf) -> anyhow::Result<()> {
    let config = Config::load(&path)?;
    // Secrets are only checked for being readable, never printed.
    if let Some(url) = config.redis.resolve_url()? {
        vulture_server::store::validate_url(url.expose())
            .map_err(|e| anyhow::anyhow!("redis URL: {e}"))?;
    }
    if let Some(ch) = &config.clickhouse {
        ch.resolve_password()?;
    }
    if let Some(file) = &config.normalize.base_aliases_file {
        vulture_server::normalize::BaseAliases::load(file)?;
    }
    if config.tls.is_some() {
        tls_report(&config)?;
    }
    data_dir_report(&config)?;
    println!(
        "config OK: {} exchange(s), {} enabled",
        config.exchanges.len(),
        config.enabled_exchanges().count()
    );
    Ok(())
}

/// Checks the files in `server.data_dir` that already exist: `state.json` must be readable, of a known version
/// and hold valid policies; `alerts.json` must be valid (it holds secrets, which are never printed); the token file must hold a valid token and should not be readable by others. A
/// missing file is fine (the server creates it). Nothing secret is printed: only paths and reasons.
fn data_dir_report(config: &Config) -> anyhow::Result<()> {
    let dir = &config.server.data_dir;
    let state = vulture_server::symbols::StateStore::new(dir);
    let policies = state.load()?;
    if state.path().exists() {
        println!(
            "state OK: {} with {} exchange polic(ies)",
            state.path().display(),
            policies.policies.len()
        );
    }
    let alerts = vulture_server::alerts::AlertsFile::new(dir);
    let allow_custom = config.alerts.delivery.allow_custom_endpoints;
    match alerts.load(allow_custom)? {
        Some(doc) => println!(
            "alerts OK: {} with {} rule(s), {} channel(s)",
            alerts.path().display(),
            doc.rules.len(),
            doc.channels.len()
        ),
        None => {
            // No file yet: the defaults of [[alerts.rules]] and [[alerts.channels]] are in force.
            let mut doc = vulture_server::alerts::file::AlertsDoc {
                version: vulture_server::alerts::file::ALERTS_VERSION,
                rules: config.alerts.rules.clone(),
                channels: config.alerts.channels.clone(),
            };
            vulture_server::alerts::file::validate_doc(&mut doc, allow_custom)
                .map_err(|m| anyhow::anyhow!("alerts defaults in the configuration: {m}"))?;
        }
    }
    let token = vulture_server::auth::TokenStore::new(dir);
    if token.read()?.is_some() {
        println!("token OK: {}", token.path().display());
        if let Some(problem) = token.permission_problem() {
            eprintln!("warning: {problem}");
        }
    }
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

async fn migrate(path: PathBuf) -> anyhow::Result<()> {
    use vulture_server::history::{HistorySink, MIGRATIONS, build_client};
    let config = Config::load(&path)?;
    let ch = config
        .clickhouse
        .as_ref()
        .context("no [clickhouse] section in the configuration")?;
    let client = build_client(ch)?;
    let applied = client.migrate().await.map_err(|e| anyhow::anyhow!("{e}"))?;
    if applied.is_empty() {
        println!(
            "schema is current: {} migration(s) known, none applied now",
            MIGRATIONS.len()
        );
    } else {
        println!("applied migration(s): {applied:?}");
    }
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

fn study_config(a: &StudyArgs) -> anyhow::Result<vulture_server::analyze::study::StudyConfig> {
    let thresholds = a
        .thresholds
        .split(',')
        .map(|t| t.trim().parse::<f64>())
        .collect::<Result<Vec<_>, _>>()
        .context("--thresholds must be numbers separated by commas")?;
    anyhow::ensure!(
        !thresholds.is_empty() && thresholds.iter().all(|t| t.is_finite() && *t >= 0.0),
        "--thresholds must be non-negative numbers"
    );
    let (exchange, symbol) = a
        .market_proxy
        .split_once(':')
        .context("--market-proxy must be exchange:symbol")?;
    Ok(vulture_server::analyze::study::StudyConfig {
        thresholds_pct: thresholds,
        by_strength: a.by_strength,
        proxy: (exchange.to_string(), symbol.to_string()),
        ..Default::default()
    })
}

fn emit(rendered: &vulture_server::analyze::cli::Rendered, out: &OutputArgs) -> anyhow::Result<()> {
    if let Some(dir) = &out.out_dir {
        std::fs::create_dir_all(dir).with_context(|| format!("cannot create {}", dir.display()))?;
        std::fs::write(dir.join("report.md"), &rendered.markdown)?;
        std::fs::write(dir.join("report.json"), &rendered.json)?;
    }
    match out.format.as_str() {
        "md" => println!("{}", rendered.markdown),
        "json" => println!("{}", rendered.json),
        "both" => println!("{}\n---\n{}", rendered.markdown, rendered.json),
        other => anyhow::bail!("--format must be md, json or both, not {other:?}"),
    }
    Ok(())
}

async fn analyze(command: AnalyzeCommand) -> anyhow::Result<()> {
    use vulture_server::analyze::{cli, params::Detector, parse_time};
    use vulture_server::history::build_client;
    use vulture_server::query::cold::Scope;
    let load = |path: &PathBuf| -> anyhow::Result<(Config, std::sync::Arc<vulture_server::history::ChClient>)> {
        let config = Config::load(path)?;
        let ch = config
            .clickhouse
            .as_ref()
            .context("no [clickhouse] section in the configuration")?;
        let client = std::sync::Arc::new(build_client(ch)?);
        Ok((config, client))
    };
    match command {
        AnalyzeCommand::Signals {
            config,
            from,
            to,
            exchanges,
            study,
            output,
        } => {
            let (_, client) = load(&config)?;
            let (from, to) = (
                parse_time(&from).map_err(anyhow::Error::msg)?,
                parse_time(&to).map_err(anyhow::Error::msg)?,
            );
            anyhow::ensure!(from < to, "--from must be before --to");
            let opts = cli::SignalsOpts {
                from,
                to,
                exchanges,
                study: study_config(&study)?,
            };
            let rendered = cli::signals_on_clickhouse(client, &opts)
                .await
                .map_err(anyhow::Error::msg)?;
            emit(&rendered, &output)
        }
        AnalyzeCommand::Replay {
            config,
            detector,
            params,
            from,
            to,
            warmup_secs,
            chunk_secs,
            exchanges,
            symbols,
            list_max,
            study,
            output,
        } => {
            let (cfg, client) = load(&config)?;
            let (from, to) = (
                parse_time(&from).map_err(anyhow::Error::msg)?,
                parse_time(&to).map_err(anyhow::Error::msg)?,
            );
            anyhow::ensure!(from < to, "--from must be before --to");
            let opts = cli::ReplayOpts {
                detector: Detector::parse(&detector).map_err(anyhow::Error::msg)?,
                params,
                from,
                to,
                warmup_secs,
                chunk_secs,
                scope: Scope {
                    exchanges,
                    symbols,
                    base_assets: vec![],
                },
                list_max,
                study: study_config(&study)?,
            };
            let result = cli::replay_on_clickhouse(client, &cfg.alerts, &opts)
                .await
                .map_err(anyhow::Error::msg)?;
            emit(&result.rendered, &output)
        }
    }
}
