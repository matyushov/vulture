use clap::Parser;

/// Command-line client and administration for vulture.
#[derive(Parser)]
#[command(name = "vulture-cli", version, about)]
struct Cli {}

fn main() -> anyhow::Result<()> {
    let _cli = Cli::parse();
    println!("vulture-cli {}", env!("CARGO_PKG_VERSION"));
    Ok(())
}
