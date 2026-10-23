use std::path::PathBuf;

const PROTOS: [&str; 4] = [
    "vulture/v1/common.proto",
    "vulture/v1/stream.proto",
    "vulture/v1/history.proto",
    "vulture/v1/control.proto",
];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../proto");
    let protos: Vec<PathBuf> = PROTOS.iter().map(|p| proto_root.join(p)).collect();

    // protoc is taken from the vendored crate: a system one is not required.
    let mut config = prost_build::Config::new();
    config.protoc_executable(protoc_bin_vendored::protoc_bin_path()?);

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_with_config(config, &protos, &[proto_root])?;

    for proto in &protos {
        println!("cargo:rerun-if-changed={}", proto.display());
    }
    Ok(())
}
