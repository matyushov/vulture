# vulture

Open source service that collects **liquidations** and **open interest** from crypto exchanges: a Rust server,
gRPC API, Redis Streams hot storage and ClickHouse history, plus a Qt6 client.

Status: scaffold, proto v0 contract and CI; no business logic yet.

## Components

| Part | Language | Purpose | License |
|---|---|---|---|
| `proto/`, `crates/vulture-proto` | protobuf / Rust | `vulture.v1` contract | Apache-2.0 |
| `crates/vulture-server` | Rust | connectors, storage, gRPC | Apache-2.0 |
| `crates/vulture-cli` | Rust | command-line client and administration | Apache-2.0 |
| `ui/` (`vulture-ui`) | C++ / Qt6 | GUI client | GPLv3 |

## Build

Rust (only the vendored `protoc` is needed; a system one is not required):

```sh
cargo build --workspace
cargo test --workspace
cargo run -p vulture-server
cargo run -p vulture-cli -- --help
```

UI (requires Qt6 and CMake; gRPC/protobuf dependencies are described in `ui/conanfile.py`):

```sh
cmake -S ui -B ui/build   # without Qt6 the vulture-ui target is disabled with a warning
cmake --build ui/build
```

Example configuration: [config.example.toml](config.example.toml). Secrets are not
added to the repository.

## Contract (proto)

`proto/vulture/v1/`: `common.proto` (Exchange as a string, `Side`, `Liquidation`, `OpenInterest`,
`Gap`, `Event`), `stream.proto` (`Stream.Subscribe`), `history.proto` (`History.Query`,
`History.Aggregate`), `control.proto` (`Control`). Timestamps are int64, Unix epoch milliseconds;
numbers are `double`. Evolution rules: fields may only be added, numbers are not reused,
removed fields are marked `reserved`. Checks: `cd proto && buf lint`. Rust code is generated in
`build.rs` (vendored protoc, tonic server+client); C++ for the UI uses `protoc` and `grpc_cpp_plugin` from
conan (`ui/CMakeLists.txt`, target `vulture-proto-cpp`).

## CI

GitHub Actions, run on `pull_request` and on `push` to `develop` and `main`; `GITHUB_TOKEN` permissions are
`contents: read`; `pull_request_target` is not used.

| Workflow / job | What it does |
|---|---|
| `ci.yml` / `rust` | `cargo fmt --check`, `clippy -D warnings`, `cargo test --workspace`, `Swatinem/rust-cache` cache |
| `ci.yml` / `supply-chain` | `cargo-deny` (licenses, sources, advisories per `deny.toml`), `cargo-audit` |
| `ci.yml` / `secrets` | gitleaks over the whole history |
| `ci.yml` / `proto` | `buf lint`, `buf breaking` against the base revision |
| `ui.yml` / `ui` | ubuntu/macos/windows matrix: Qt 6 (`jurplel/install-qt-action`, version in `QT_VERSION`), conan 2 with the profiles in `ui/conan/profiles/`, conan cache; the build does not block the Rust jobs |

| `conan-deps.yml` / `deps` | ubuntu/macos/windows matrix: builds the missing conan dependencies of the UI (grpc, protobuf, ...) and uploads them to Nexus; runs on `vendor/**` branches and manually (`rebuild_all`), environment `nexus` |

### UI dependencies and Nexus

Third-party UI packages are built once and stored in Nexus (`https://repo.blissbit.cc/repository/conan-local/`,
remote `nexus-local`); `ui.yml` only downloads them. Identical `package_id` values for the builder and
the consumer are ensured by the pinned profiles `ui/conan/profiles/*` and `ui/conan.lock`.

Updating dependencies:

1. `git checkout -b vendor/<what>` (for example `vendor/grpc-1.85`).
2. Change `ui/conanfile.py` (and the profiles if needed), recreate the lock:
   `conan lock create ui -pr:a ui/conan/profiles/<profile> --lockfile-out=ui/conan.lock`
   for each profile, merging the results with `conan lock merge`.
3. `git push`: `conan-deps.yml` builds the missing packages and uploads them to Nexus.
4. Open a PR `vendor/<what>` into `develop`: `ui.yml` downloads the prebuilt binaries.

Secrets: the writing user `NEXUS_USERNAME` / `NEXUS_PASSWORD` are secrets of the `nexus` environment
(Settings -> Environments), used only in `conan-deps.yml`; the reading user `NEXUS_RO_USERNAME` /
`NEXUS_RO_PASSWORD` are repository secrets, used in `ui.yml`. Without them (forks) the remote is not
added and conan-center is used. The values never enter the repository.

Locally: `conan install ui -pr:a ui/conan/profiles/<profile> --lockfile=ui/conan.lock --build=missing`
(on a host with a different compiler version, copy the profile and adjust `compiler.version`; the lock
stays the same, but the packages will have to be built).

## Licenses

Rust code and proto: Apache-2.0 ([LICENSE-APACHE](LICENSE-APACHE)). UI: GPLv3
([ui/LICENSE](ui/LICENSE)). The server and the UI communicate only over gRPC.
