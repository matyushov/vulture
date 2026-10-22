use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../proto");
    let protos = [proto_root.join("vulture/v1/vulture.proto")];

    // protoc берём из вендорного крейта: системный не требуется.
    let mut config = prost_build::Config::new();
    config.protoc_executable(protoc_bin_vendored::protoc_bin_path()?);

    tonic_prost_build::configure().compile_with_config(config, &protos, &[proto_root])?;

    // Пока в пакете нет сообщений, prost файл не создаёт; include! в lib.rs его ждёт.
    let generated = PathBuf::from(std::env::var("OUT_DIR")?).join("vulture.v1.rs");
    if !generated.exists() {
        std::fs::write(generated, "")?;
    }

    println!("cargo:rerun-if-changed={}", protos[0].display());
    Ok(())
}
