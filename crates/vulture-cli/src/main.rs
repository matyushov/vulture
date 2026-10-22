use clap::Parser;

/// Консольный клиент и администрирование vulture.
#[derive(Parser)]
#[command(name = "vulture-cli", version, about)]
struct Cli {}

fn main() -> anyhow::Result<()> {
    let _cli = Cli::parse();
    println!("vulture-cli {}", env!("CARGO_PKG_VERSION"));
    Ok(())
}
