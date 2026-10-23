# vulture

Open source service that collects **liquidations** and **open interest** from crypto exchanges: a Rust server,
gRPC API, Redis Streams hot storage and ClickHouse history, plus a Qt6 client.

Status: scaffold, proto v0.2 contract, CI, the server foundation (configuration, exchange trait, symbol and
open interest normalization, reconnecting WebSocket manager), the Binance USDT-M connector, the Redis Streams
writer (24-hour hot window), the gRPC `Stream.Subscribe` with resume and the owner token. `History` and `Control`
are in the contract but answer `UNIMPLEMENTED`; TLS, metrics, ClickHouse history and the other exchanges are not
implemented yet. The server needs a Redis instance (`redis.url`) and logs per-type event counts once a minute.

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
cargo run -p vulture-server -- run --config config.example.toml   # needs redis.url, see "Redis storage"
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
- `store`: Redis Streams hot storage: the `RedisStore` trait (append, range read, stream bounds, a dedicated tail
  reader per client, health, server settings) with a `redis`-crate implementation and an in-memory double, and the
  `RedisWriter` that batches, survives outages and records gaps (see "Redis storage").
- `auth`: the owner token (file, rotation), the interceptor and the failure limiter; `grpc`: the tonic server with
  `Stream.Subscribe` and the `History`/`Control` stubs (see "gRPC API and token").
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
  from `X-MBX-USED-WEIGHT-1M`, pause on 429 (`Retry-After`) and on 418 (IP ban). The first poll waits for the
  first mark price frame (at most 10 s) so that the first snapshots already carry `oi_usd` and `mark_price`. A
  418 ban, and a 429 pause longer than one polling interval, publish a `Gap` (`stream` `open_interest`) from the
  start to the planned end of the pause.
- **Status:** `BinanceConnector::status()` exposes counters (frames, parse errors, duplicates, retries, ban state,
  used weight) for the future metrics endpoint.
- **Live check:** `cargo test -p vulture-server --test binance -- --ignored binance_live --nocapture` connects to
  the real public endpoints for about 75 seconds (not run in CI).

## Redis storage

Every event goes into a **single stream** (`redis.stream_key`, default `vulture:events`): one resume cursor for the
gRPC reader and one source for the ClickHouse flusher. The volume is small (about 20 open interest events/s and
about 1 liquidation/s), so filtering on read is cheap. Each entry has four fields:

| field | value |
|---|---|
| `t` | event type: `liq`, `oi`, `gap` or `sig` (signal, proto v0.2) |
| `x` | exchange (`binance`; `*` for gaps written by the writer itself) |
| `s` | normalized symbol (`BTC-USDT-PERP`); empty for gaps |
| `d` | the protobuf-encoded `Event` **without** `id` |

The Redis entry ID (`<ms>-<seq>`) is the public `Event.id`; readers set it from the entry ID when they decode `d`.
`t`, `x` and `s` let a reader filter without decoding.

- **Trimming** on every batch: `XTRIM MINID ~ <now - window_secs>` (24 h by default) and `XTRIM MAXLEN ~
  maxlen_safeguard`, so a broken time trimming never fills Redis. `~` trimming removes whole internal nodes, so the
  stream may hold slightly more than the window.
- **Writer:** one pipeline (`MULTI`/`EXEC`) per batch of up to `batch_size` (500) events or `flush_interval_ms`
  (50 ms), order preserved. Delivery is at least once: a batch is applied entirely or not at all, but if the
  connection dies after `EXEC` ran and before the reply arrived, the retry repeats that batch.
- **Outage:** the server starts even if Redis is down. Failed writes are retried with exponential backoff (1 s to
  30 s, jitter); meanwhile events wait in a bounded in-memory buffer (`buffer_limit`, 100 000). When it is full the
  oldest events are dropped and counted, and the first write after recovery starts with one `Gap`
  (`exchange` `*`, `stream` `redis-writer`, `from_ts`/`to_ts` span the timestamps of the dropped events). Connectors
  are never blocked.
- **Shutdown:** the writer flushes what it holds within `server.shutdown_timeout_secs` and stops.
- **Startup check** (best effort, warnings only): `appendonly yes`, `appendfsync everysec`, `maxmemory` set,
  `maxmemory-policy noeviction`. If `CONFIG GET` is not allowed this is logged at debug level.
- **Status** for the future metrics: written, batches, retries, buffered, dropped, last error, last write time and
  connection state (`RedisWriter::status()`).
- **Size:** measured on Redis 7, 20 000 entries with realistic values: about 138 bytes per open interest entry, 130
  per liquidation, 152 for the expected mix (`MEMORY USAGE`; the protobuf payload itself is about 80 bytes). At 21
  events/s for 24 hours that is roughly 260 MiB.

Recommended Redis settings: persistence `appendonly yes` with `appendfsync everysec`, `maxmemory 3gb` to `4gb`,
`maxmemory-policy noeviction` (a full Redis then refuses writes, which the writer survives, instead of silently
evicting history). `redis.url` is a secret: put it in `VULTURE__REDIS__URL` or `redis.url_file`; it is never
logged. `rediss://` is supported (rustls, bundled web PKI roots).

Tests: the writer and the storage layer have unit tests on the in-memory store (batching, ordering, overflow and
gap, shutdown). The integration tests in `crates/vulture-server/tests/redis_store.rs` need a Redis server and read
its URL from `VULTURE_TEST_REDIS_URL`; without it they skip with a message. Use a throwaway local instance:

```sh
docker run -d --rm --name vulture-test-redis -p 127.0.0.1:6379:6379 redis:7
VULTURE_TEST_REDIS_URL=redis://127.0.0.1:6379 cargo test -p vulture-server --test redis_store -- --nocapture
docker stop vulture-test-redis
```

`--nocapture` shows the stored size per entry. The tests kill all other client connections of the server
(`CLIENT KILL`) to check reconnection, so never point them at a shared Redis. CI runs them against a Redis service.

## gRPC API and token

`vulture-server run` serves three services on `server.listen_addr` (default `127.0.0.1:50051`): `Stream`
(implemented), `History` and `Control` (registered, every method answers `UNIMPLEMENTED`, so the contract is
visible to `grpcurl`). Every RPC of every service needs the token; there is no TLS yet (V1-8), so the server **refuses
a non-loopback `listen_addr`** unless `server.allow_plaintext = true` is set (a warning is logged). Put a TLS
terminator or an SSH tunnel in front, or wait for V1-8, before exposing it.

### Token

One owner, one token, role admin implied. Generated on the first start if missing: 32 random bytes from the OS
CSPRNG, base64url (43 characters), stored in `<server.data_dir>/token` (file mode `0600`, directory `0700`).

- **The token itself is stored, not a hash.** The owner wants `token show` to print it again, which a hash cannot do.
  The trade-off: anyone who can read the file (or a backup or image containing `data_dir`) can use the service, so the
  file is protected like a TLS private key. If it leaks, run `token rotate`.
- `vulture-server token show --config <file>` prints the token to stdout (only the token, so `$(...)` is safe).
  `token rotate` writes a new one and prints it; the old one stops working at once. A running server notices a
  rotation done by the CLI within `server.token_check_interval_ms` (1 s) and ends the open streams that used the old
  token with `UNAUTHENTICATED`. `token create` creates the token only if there is none and prints it once to stderr.
  A deleted file is never regenerated silently: only `token create` or the first start of the server do that.
- Clients send `authorization: Bearer <token>`. The comparison is constant-time. A missing, malformed or wrong
  token gives `UNAUTHENTICATED`; after 10 failures per minute from one peer address the address gets
  `RESOURCE_EXHAUSTED` (even for a valid token) until the failures age out. Failures are logged with the peer address
  and the reason, never with the presented value. The token never appears in logs, errors or panics; the first
  creation prints it once to stderr in a banner.

### `Stream.Subscribe`

`SubscribeRequest{exchanges, symbols, types, resume_from}`; empty filters mean "everything". Filtering uses the
cheap entry fields `t`, `x`, `s` without decoding; `d` is decoded only for matches and `Event.id` is the Redis entry
ID. A `Gap` has no symbol, so a symbol filter never excludes it; a server-wide gap (exchange `*`) also passes an
exchange filter; the `types` filter is exact.

| `resume_from` | behaviour |
|---|---|
| empty | live only, from the current tail |
| `0` | replay the whole hot window, then live |
| an `Event.id` | exclusive cursor: only strictly later entries, no duplicates, no gaps, then live |

Cursor rule: let `F` be the oldest entry still in the stream. A cursor older than `F` is answered `OUT_OF_RANGE`
("use History"), because entries between the cursor and `F` may have been trimmed. A cursor equal to `F`, inside
the stream, or beyond the tail (also on an empty stream) is accepted. The check is repeated between replay pages: if
trimming overtakes a slow replay the stream ends with `OUT_OF_RANGE`. The rule is conservative: after more than a
window of silence a cursor can be refused although nothing was lost. A malformed cursor is `INVALID_ARGUMENT`.

Data path: pages of 1000 entries are replayed first, then the stream switches to `XREAD BLOCK` on a **dedicated
Redis connection per client** (named `vulture-stream-reader` in `CLIENT LIST`, never the writer's connection;
dropped as soon as the client cancels). Each client has a bounded queue (`server.client_buffer`, 1024 events); if it
stays full for `server.slow_client_timeout_ms` (10 s) the client is disconnected with `RESOURCE_EXHAUSTED` and
can reconnect with its last id, other clients are not affected. `server.max_clients` (16) limits concurrent
streams, extra ones get `RESOURCE_EXHAUSTED`. If Redis fails while streaming, the stream stays open, retries with
backoff and continues from the last scanned cursor. HTTP/2 keepalive (`keepalive_interval_secs`,
`keepalive_timeout_secs`) drops vanished clients. Shutdown ends open streams with `UNAVAILABLE`. A new subscription
while Redis is down gets `UNAVAILABLE`. Counters for the future metrics endpoint: `GrpcStats` (clients,
subscriptions, events sent, slow disconnects, resume requests, out-of-range answers, rejected clients, Redis read
errors) and `AuthStats` (failures, rate-limited requests).

### Trying it locally

```sh
docker run -d --rm --name vulture-dev-redis -p 127.0.0.1:6379:6379 redis:7
cat > /tmp/vulture-dev.toml <<'CONFIG'
[server]
data_dir = "/tmp/vulture-dev-data"        # the token lands in /tmp/vulture-dev-data/token
[redis]
url = "redis://127.0.0.1:6379"
[[exchanges]]
name = "binance"                          # for a fully local run, point it at the mock below
ws_url = "ws://127.0.0.1:18081"
rest_url = "http://127.0.0.1:18080"
oi_poll_interval_secs = 2
symbols = ["BTC-USDT-PERP", "ETH-USDT-PERP"]
CONFIG
cargo run -p vulture-server --example mock_binance &          # no exchange traffic: a loopback stand-in
cargo run -p vulture-server -- run --config /tmp/vulture-dev.toml &   # prints the token once on the first start
export VULTURE_TOKEN=$(cargo run -q -p vulture-server -- token show --config /tmp/vulture-dev.toml)
cargo run -p vulture-server --example stream_client -- --resume-from 0 --max-events 20   # replay, then live
cargo run -p vulture-server --example stream_client -- --reconnect --exchange binance --type liquidation
```

`stream_client` prints one line per event, starting with its id; pass the last id to `--resume-from` after a
disconnect (`--reconnect` does that by itself). With [grpcurl](https://github.com/fullstorydev/grpcurl) (the server
has no reflection; give it the proto files), the contract is also visible:

```sh
grpcurl -plaintext -import-path proto -proto vulture/v1/stream.proto \
  -H "authorization: Bearer $VULTURE_TOKEN" -d '{"resume_from": "0", "exchanges": ["binance"]}' \
  127.0.0.1:50051 vulture.v1.Stream/Subscribe
grpcurl -plaintext -import-path proto -proto vulture/v1/control.proto \
  -H "authorization: Bearer $VULTURE_TOKEN" 127.0.0.1:50051 vulture.v1.Control/GetConnectorStatus
# -> UNIMPLEMENTED with a valid token, UNAUTHENTICATED without one
```

Tests: `crates/vulture-server/tests/grpc_stream.rs` (filters, resume, cursor rules, slow client, limits, outages, run
on the in-memory store and, with `VULTURE_TEST_REDIS_URL`, on a real Redis) and `tests/grpc_auth.rs` (every RPC,
rotation, limiter, no token in logs).

## Contract (proto)

`proto/vulture/v1/`: `common.proto` (Exchange as a string, `Side`, `Liquidation`, `OpenInterest`,
`Gap`, `Signal`, `Event`), `stream.proto` (`Stream.Subscribe`), `history.proto` (`History.Query`,
`History.Aggregate`), `control.proto` (`Control`).

v0.2 (additive, `buf breaking` green): the `Signal` message (`OI_SPIKE_UP`, `OI_SPIKE_DOWN`, `LIQ_CASCADE`, window,
`change_pct`, `change_abs` in base units, `threshold`, `ts`, a free-form `details` map) as `Event.signal = 13` with
`EVENT_TYPE_SIGNAL`; `Control.ListSymbolPolicies` / `SetSymbolPolicy` (mode `ALL` / `WHITELIST` / `BLACKLIST`, symbols,
per-symbol OI interval overrides; the older `ListSubscriptions` / `SetSubscription` stay but are documented as
superseded); and the OI movers ranking in `History.Aggregate` (`AggregateRequest.oi_movers` selects it, the answer is
`AggregateResponse.oi_movers` with top growth and top decline by `oi_base`, absolute and percent change and a
`partial` flag). Contract only: no server logic for these yet. Timestamps are int64, Unix epoch milliseconds;
numbers are `double`. Evolution rules: fields may only be added, numbers are not reused,
removed fields are marked `reserved`. Checks: `cd proto && buf lint`, and `buf breaking --against "../.git#ref=<base>,subdir=proto"` against the previous revision. Rust code is generated in
`build.rs` (vendored protoc, tonic server+client); C++ for the UI uses `protoc` and `grpc_cpp_plugin` from
conan (`ui/CMakeLists.txt`, target `vulture-proto-cpp`).

## CI

GitHub Actions, run on `pull_request` and on `push` to `develop` and `main`; `GITHUB_TOKEN` permissions are
`contents: read`; `pull_request_target` is not used.

| Workflow / job | What it does |
|---|---|
| `ci.yml` / `rust` | `cargo fmt --check`, `clippy -D warnings`, `cargo test --workspace` against a Redis service (`VULTURE_TEST_REDIS_URL`), `Swatinem/rust-cache` cache |
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
