# vulture

Open source service that collects **liquidations** and **open interest** from crypto exchanges: a Rust server,
gRPC API, Redis Streams hot storage and ClickHouse history, plus a Qt6 client.

Status: scaffold, proto v0 contract, CI, and the server foundation (configuration, exchange trait, symbol and
open interest normalization, reconnecting WebSocket manager) and the Binance USDT-M connector. Storage, gRPC
and the other exchanges are not implemented yet; the server currently logs per-type event counts once a minute.

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
cargo run -p vulture-server -- config check --config config.example.toml
cargo run -p vulture-server -- run --config config.example.toml
cargo run -p vulture-cli -- --help
```

UI (requires Qt6 and CMake; gRPC/protobuf dependencies are described in `ui/conanfile.py`):

```sh
cmake -S ui -B ui/build   # without Qt6 the vulture-ui target is disabled with a warning
cmake --build ui/build
```

Example configuration: [config.example.toml](config.example.toml). Secrets are not
added to the repository.

## Server architecture

`crates/vulture-server` is a library plus a thin binary (`vulture-server run|config check --config <file>`).
- `config`: TOML plus `VULTURE__SECTION__KEY` environment overrides; the exchange list and symbols are data;
  secrets are wrapped so they never reach logs. `app`: shared context and graceful shutdown on SIGINT/SIGTERM.
- `exchange`: the `Exchange` trait (`name()`, `run(ctx, sink)`); a connector owns its loop and pushes
  `vulture_proto::v1::Event` into a bounded channel until the shutdown token fires.
- `normalize`: raw symbol to `BASE-QUOTE-PERP` (metadata first, string parsing only as a fallback), base asset
  aliases with multipliers (`data/base_aliases.toml`: `1000PEPE` to `PEPE` x 1000, `XBT` to `BTC`), open interest
  to `oi_base`/`oi_usd`, and the market filter (linear USDT/USDC perpetuals in trading state only).
- `ws`: the WebSocket manager shared by all connectors. One supervisor task per logical connection with a session
  task per socket; exponential backoff with jitter, silence and zombie detection on a liveness stream, keepalive,
  `Gap` events after a loss, make-before-break planned reconnect with deduplication, observable state
  (`Connecting`, `Healthy`, `Degraded`, `Stopped`). Timers run on the monotonic clock and tests drive them with
  `tokio::time::pause`. Defaults are documented in `research/ws-reconnect.md`.

### Binance connector (`exchange::binance`)

Public data only, no keys. Settings in `[[exchanges]] name = "binance"`: `enabled`, `symbols` (optional whitelist of
normalized names such as `BTC-USDT-PERP`; empty means all), `oi_poll_interval_secs` (default 30), and the optional
test overrides `ws_url` / `rest_url`.
- **Symbols:** REST `/fapi/v1/exchangeInfo` at start and every hour; perpetual, trading, USDT/USDC only
  (`TRADIFI_PERPETUAL` is excluded). Listings and removals are logged and applied to polling. Non-ASCII symbols
  are percent-encoded in URLs. If two symbols normalize to the same name (`PEPEUSDT` and `1000PEPEUSDT`), the
  aliased one is kept under its exchange-side name (`1000PEPE-USDT-PERP`) instead of being merged.
- **Liquidations:** one WebSocket to `wss://fstream.binance.com/market/ws` (the legacy `/ws` route accepts the
  subscription but stays silent) with `!forceOrder@arr` and `!markPrice@arr@1s`; the mark price stream doubles as
  the liveness signal. Server ping, planned make-before-break reconnect every 23 h 30 min +/- 10 min,
  deduplication by `(symbol, T, side, z, ap)` during the switch, `Gap` events after a loss. An order side of
  `SELL` is a liquidated long. Price and quantity are per real coin (for `1000PEPE` price/1000, quantity x1000;
  unchanged for ordinary symbols).
- **Mark price:** kept in memory only (never published) and used for `oi_usd`; a price older than 30 s is ignored.
- **Open interest:** REST `/fapi/v1/openInterest`, one snapshot per symbol per interval
  `max(oi_poll_interval_secs, N / 1440 * 60)` seconds, spread evenly (about `interval / N` apart), at most 12
  requests in flight, token bucket at 60% of the 2400 weight/min limit, slowdown above 60% of the limit as read
  from `X-MBX-USED-WEIGHT-1M`, pause on 429 (`Retry-After`) and on 418 (IP ban).
- **Status:** `BinanceConnector::status()` exposes counters (frames, parse errors, duplicates, retries, ban state,
  used weight) for the future metrics endpoint.
- **Live check:** `cargo test -p vulture-server --test binance -- --ignored binance_live --nocapture` connects to
  the real public endpoints for about 75 seconds (not run in CI).

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
