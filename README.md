# vulture

Open source service that collects **liquidations** and **open interest** from crypto exchanges: a Rust server,
gRPC API, Redis Streams hot storage and ClickHouse history, plus a Qt6 client.

Status: scaffold, proto v0.3 contract, CI, the server foundation (configuration, exchange trait, symbol and
open interest normalization, reconnecting WebSocket manager), the Binance USDT-M connector, the Redis Streams
writer (24-hour hot window), the gRPC `Stream.Subscribe` with resume and the owner token, TLS with certificate hot
reload, Prometheus metrics, tracked symbols managed at runtime (`Control.ListSymbolPolicies` /
`SetSymbolPolicy`), and a Dockerfile with compose, certbot, systemd and Prometheus templates in `deploy/`.
The Bybit linear connector (V2a) with runtime subscriptions and a connection budget in the WebSocket manager, and the
OKX linear swap connector (V2b), are implemented. ClickHouse history (schema, migrations, the Redis-to-ClickHouse flusher, disk monitoring, backups) is implemented;
`History.Query`/`Aggregate` and the rest of `Control` are in the contract but answer `UNIMPLEMENTED`, and the other
exchanges except Bybit and OKX are not implemented yet. The server needs a Redis instance (`redis.url`); ClickHouse is optional
(`[clickhouse]`). Deployment is described in [deploy/README.md](deploy/README.md).

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

The toolchain is pinned in `rust-toolchain.toml` (exact release, currently 1.99.1; rustup fetches it on first
use). To bump it, change `channel` there and `ARG RUST_VERSION` in the `Dockerfile` (CI fails when the two differ).

UI (`ui/`, Qt 6 Widgets + grpc++ from conan; see [ui/README.md](ui/README.md) for usage, settings and limits). Qt 6.10 or
newer, CMake 3.24+, conan 2 with a remote that serves the prebuilt `grpc`/`protobuf` packages:

```sh
# dependencies (prebuilt binaries only; the profile selects the packages CI built)
conan install ui -pr:a ui/conan/profiles/macos-armv8 --lockfile=ui/conan.lock --build=never -o '&:require_qt=True' -of ui/build
# configure, build, test, run
source ui/build/build/Release/generators/conanbuild.sh
cmake -S ui -B ui/build/b -DCMAKE_TOOLCHAIN_FILE=$PWD/ui/build/build/Release/generators/conan_toolchain.cmake \
      -DCMAKE_PREFIX_PATH=$HOME/Qt/6.12.0/macos -DCMAKE_BUILD_TYPE=Release -DVULTURE_UI_REQUIRE_QT=ON
cmake --build ui/build/b -j
QT_QPA_PLATFORM=offscreen ctest --test-dir ui/build/b --output-on-failure -E tst_Perf
ui/build/b/vulture-ui
```

Without conan or Qt the UI targets are disabled with a warning. Besides grpc++, the UI vendors QCustomPlot (GPLv3) in
`ui/third_party/qcustomplot/` for the symbol chart; it adds no conan dependency. Tests never leave the machine: the fake
gRPC and HTTP servers listen on 127.0.0.1 (`QT_QPA_PLATFORM=offscreen`; screenshots with `VULTURE_UI_SHOT_DIR=<dir>`).

Example configuration: [config.example.toml](config.example.toml). Secrets are not
added to the repository.

## Server architecture

`crates/vulture-server` is a library plus a thin binary (`vulture-server run|config check|token|tls check|healthcheck
--config <file>`).
- `config`: TOML plus `VULTURE__SECTION__KEY` environment overrides; the exchange list and symbols are data;
  secrets are wrapped so they never reach logs. `app`: shared context and graceful shutdown on SIGINT/SIGTERM.
- `store`: Redis Streams hot storage: the `RedisStore` trait (append, range read, stream bounds, a dedicated tail
  reader per client, health, server settings) with a `redis`-crate implementation and an in-memory double, and the
  `RedisWriter` that batches, survives outages and records gaps (see "Redis storage").
- `auth`: the owner token (file, rotation), the interceptor and the failure limiter; `grpc`: the tonic server with
  `Stream.Subscribe` and the `History`/`Control` stubs (see "gRPC API and token").
- `alerts`: the analytics engine in the server process: per-symbol OI ring buffers, the OI spike and liquidation
  cascade detectors, rules and channels (`alerts.json`), and delivery to Telegram and Discord (see "Signals and
  alerts").
- `tls`: rustls (ring) for the gRPC listener with hot reload of the certificate (see "TLS"); `metrics`: Prometheus
  exposition, `/healthz` and `/readyz` (see "Metrics").
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

### Tracked symbols (`symbols`, V1-11)

Which symbols an exchange tracks is a **policy**: `ALL` (the exchange's whole universe: linear USDT/USDC
perpetuals as loaded from its symbol list), `WHITELIST` (only the listed ones) or `BLACKLIST` (the universe minus the
listed ones). The effective set is recomputed whenever the policy or the universe changes (for Binance the hourly
`exchangeInfo` reload), so a whitelisted symbol that is listed later starts being tracked and a blacklisted one stays
excluded when it is delisted and relisted.

- **Default and override.** The `symbols` list of `[[exchanges]]` is the default policy (empty = `ALL`, otherwise a
  whitelist). The owner's runtime changes are stored in `<data_dir>/state.json` (versioned, written atomically with
  mode `0600`) and override the default of that exchange. An unreadable, corrupt or unknown-version state file makes
  the server **refuse to start** with a clear message instead of falling back to the configuration (which could
  re-enable symbols the owner removed). The file is never overwritten by an image update.
- **Control RPCs.** `ListSymbolPolicies` returns the policy per exchange plus the tracked count and the universe
  size. `SetSymbolPolicy` replaces one exchange's policy: the exchange must be configured and enabled, names must
  be `BASE-USDT-PERP` / `BASE-USDC-PERP` (case-insensitive, duplicates collapse), `WHITELIST` needs at least one
  symbol, and **names the exchange does not list are rejected with `INVALID_ARGUMENT`** and named in the message (a
  typo must not silently track nothing; the check needs the symbol list, `FAILED_PRECONDITION` until it is loaded).
  `ALL` ignores the list. It is idempotent (a repeat answers `changed = false`), returns the tracked count and the
  added/removed diff, and stores the state before answering. Per-symbol OI interval overrides (`oi_intervals`) are
  not supported: a non-empty list is refused with `UNIMPLEMENTED`. The rule and channel RPCs are described in "Signals and alerts"; all other `Control` RPCs stay `UNIMPLEMENTED`.
  `cargo run -p vulture-server --example control_client -- --help` is a small client (TLS and token flags as in
  `stream_client`); the proper CLI comes in V5.
- **Applied on the fly.** The connector receives the policy through `Exchange::symbols()` (a `SymbolChannel`, two
  `watch` values: policy in, universe out; connectors that return `None` are unaffected). Binance: the OI poller
  re-reads the tracked set and `N` at the start of every cycle and skips a symbol removed mid-cycle at once, so a
  removed symbol stops being polled within one cycle and an added one starts at the next; liquidation frames of
  untracked symbols are dropped and counted (`vulture_connector_filtered_liquidations_total`); mark prices are still
  kept for all symbols. Symbols that stay tracked keep flowing with no `Gap` and no duplicates.
- **No gap for removed symbols.** Untracking is not an outage, so no `Gap` is published. History for a period in
  which a symbol was not tracked does not exist and cannot be backfilled.
- **Audit.** Every change is appended as one JSON line to `<data_dir>/audit.log` (time, exchange, mode, added,
  removed, counts, peer address; no secrets; rotated at 4 MiB to `audit.log.1`) and logged with `tracing`.

### Binance connector (`exchange::binance`)

Public data only, no keys. Settings in `[[exchanges]] name = "binance"`: `enabled`, `symbols` (optional whitelist of
normalized names such as `BTC-USDT-PERP`; empty means all; the default policy, see "Tracked symbols"), `oi_poll_interval_secs` (default 30), and the optional
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
  used weight); they are exported by the metrics endpoint (see "Metrics").
- **Live check:** `cargo test -p vulture-server --test binance -- --ignored binance_live --nocapture` connects to
  the real public endpoints for about 75 seconds (not run in CI).

### Runtime subscriptions and the connection budget (`ws`)

A connection can follow a changing set of topics without reconnecting. The connector holds a
`TopicControl` with the **desired** set; every session subscribes to the whole set right after connecting
(so a reconnect needs no special case) and then sends `subscribe` / `unsubscribe` requests for the difference
whenever the set changes. The handler builds the frames (`WsHandler::topic_requests`) and splits them to the
exchange's message limit (`chunk_topics`). Requests are tracked until their response arrives
(`subscribe_ack_timeout`, 10 s; a missing response ends the session and the usual reconnect applies). A refused
`subscribe` is not silent: topics named in the refusal are dropped from the session, counted
(`vulture_ws_topics_rejected_total`), logged at `warn` and listed by `TopicControl::rejected()`; the others are
sent again; a refusal that names nothing is split in halves until the offender is isolated. A refused topic stays
in the desired set (the owner's choice is not overridden) and is tried again on the next connection. With an
empty set the connection stays up (ping only) and the silence watchdog is off until a topic is subscribed.

`ConnectionLimiter` is a per-exchange budget (new connections per window, concurrent connections) that the
manager consults before every connection attempt, failed attempts included. Its default is half of the
documented limits.

### Bybit connector (`exchange::bybit`)

Public data only, no keys. Settings in `[[exchanges]] name = "bybit"`: `enabled`, `symbols` (default policy, as for
Binance), `oi_poll_interval_secs` (default 30, the interval of the open interest samples), and the optional test
overrides `ws_url` / `rest_url`. Formats and units were checked against the live exchange on 2026-10-24
(`research/bybit-live.md`).
- **Symbols:** REST `GET /v5/market/instruments-info?category=linear&limit=1000`, followed through
  `nextPageCursor` (the cursor goes back exactly as sent), at start and every hour. `LinearPerpetual` in `Trading`
  state with quote USDT or USDC. Base and quote come from `baseCoin` / `quoteCoin`; USDC perpetuals are named
  like `1000BONKPERP` and normalize to `BONK-USDC-PERP`. `1000`-prefixed bases use the alias table (section
  `bybit`); a prefixed name that is not in the table keeps its exchange-side name. The same collision rule as for
  Binance applies. A 403 pauses REST for 10 minutes.
- **WebSocket:** `wss://stream.bybit.com/v5/public/linear`. For every tracked symbol the topics
  `allLiquidation.{symbol}` and `tickers.{symbol}` are subscribed and unsubscribed at runtime as the policy
  changes, and re-sent in full after every reconnect. One connection normally carries everything (the `args` of a
  request are split at 20 000 characters; Bybit documents no topic cap for linear); more connections are opened
  only if a connection would exceed `max_topics_per_connection` (4 000 topics), within the connection budget.
  Application ping `{"op":"ping"}` every 20 s with a 10 s pong timeout; the ticker stream is the liveness stream
  (10 s silence). A handshake 403 waits at least 10 minutes. A subscribe request holding one invalid topic is
  refused as a whole by Bybit; the connector handles that as described above.
- **Liquidations:** `allLiquidation` pushes arrays of `{T, s, S, v, p}`. `S = Buy` means a **long** position was
  liquidated (`Side::Long`), `Sell` a short. **`p` is the bankruptcy price, not the fill price**, and is published
  as `price`; `v` is in the base coin (for `1000PEPE`-style symbols price/1000 and quantity x1000, like Binance);
  `notional_usd = p * v`. Records repeated within the last 10 000 are dropped as duplicates (key
  `symbol|T|S|v|p`).
- **Tickers:** `snapshot` first, then `delta` messages that carry only changed fields. The connector keeps the
  last `markPrice`, `openInterest` and `openInterestValue` per symbol; a missing field means unchanged and is
  never filled with zero. A new `openInterest` without a new value drops the stale value (the sample then uses
  size x mark). The state is dropped for the symbols of a connection that was lost, and the sampler skips
  symbols whose connection is down.
- **Open interest:** an interval sampler publishes one `OpenInterest` per tracked symbol every
  `oi_poll_interval_secs`, from the ticker state, spread evenly over the interval (never one event per delta, never
  one burst): `oi_base = openInterest` (times the alias multiplier), `oi_usd = openInterestValue`,
  `mark_price` per real coin, `exchange_ts` = time of the last ticker message. Symbols without data are skipped.
- **Gaps:** after a connection loss `Gap` events with the streams `liquidations` and `open_interest` (same range)
  are published before the first event of the recovered connection. Changing the tracked set never produces a
  `Gap`.
- **Status and metrics:** `BybitConnector::status()`; the series of the Binance connector with `exchange="bybit"`
  plus `vulture_ws_connections`, `vulture_ws_topics_subscribed`, `vulture_ws_topic_requests_total`,
  `vulture_ws_topics_rejected_total`, `vulture_ws_topic_ack_timeouts_total`,
  `vulture_ws_connection_permits_total`, `vulture_ws_connection_budget_waits_total`,
  `vulture_ws_connections_open`, `vulture_oi_samples_skipped_total`.
- **Live check:** `cargo test -p vulture-server --test bybit -- --ignored bybit_live --nocapture` (about 75 s,
  not run in CI).

### OKX connector (`exchange::okx`)

Public data only, no keys. Settings in `[[exchanges]] name = "okx"`: `enabled`, `symbols` (default policy, as for
Binance), `oi_poll_interval_secs` (default 30) and the optional test overrides `ws_url` / `rest_url`. The documentation
was read in full and the formats checked against the live exchange on 2026-10-24 (`research/okx-live.md`, which also
lists what is still unconfirmed).
- **Symbols:** REST `GET /api/v5/public/instruments?instType=SWAP` (one response), at start and every hour:
  `ctType = linear`, `state = live`, settled in USDT or USDC. The USD-margined `*-USD_UM-SWAP` family and the inverse
  `*-USD-SWAP` contracts are excluded. `BTC-USDT-SWAP` becomes `BTC-USDT-PERP`, `BTC-USDC-SWAP` becomes
  `BTC-USDC-PERP`. OKX names real coins (`PEPE`, with `ctVal` 10 000 000), so no price multiplier applies unless the
  alias table (section `okx`) says so. An instrument whose contract size is missing, not positive or not expressed in the base
  coin is dropped and logged (`vulture_unusable_instruments`). A 403 pauses REST for 10 minutes.
- **WebSocket:** `wss://ws.okx.com/ws/v5/public` (port 443; OKX stops port 8443 on 2026-10-31). The channel
  `liquidation-orders` is subscribed once with `instType = SWAP` on the first connection and covers every swap of the
  exchange; records of untracked instruments are filtered here and counted. Per tracked instrument the channels
  `mark-price` and `open-interest` are subscribed and unsubscribed at runtime as the policy changes (100 arguments per
  request) and re-sent in full after every reconnect. One connection carries everything (OKX documents no cap per
  connection); more are opened only above `max_topics_per_connection` (2 000 topics), within a connection budget of
  half of OKX's 3 new connections per second. Keepalive: the text `ping` every 20 s, `pong` expected within 5 s; the
  `mark-price` stream is the liveness stream (30 s of silence ends the session; OKX itself cuts after 30 s without data).
- **Subscription budget:** OKX allows 480 subscribe/unsubscribe requests per hour and connection. Changes of the tracked
  set wait 2 s for further changes and are then applied only if a sliding one-hour budget of 240 requests covers them,
  otherwise they are postponed (`vulture_ws_changes_deferred_total`, gauge `vulture_ws_request_budget_spent`).
  OKX answers a bad argument with an error naming it (code 60018) while the good ones of the same request are
  subscribed; the connector rejects that topic only. Rejected topics are reported as for Bybit.
- **Liquidations:** `sz` is in **contracts**: quantity = `sz x ctVal x ctMult` base coins, `price` = `bkPx`
  ("liquidation mark price", OKX's price of the transaction with its liquidation account, not a fill price of the
  user), `notional_usd = bkPx x quantity`, `exchange_ts` = `ts`. The side is the side of the liquidated position:
  `posSide long`/`short` (hedge mode) gives `Long`/`Short`; in net mode (`posSide net`) the closing order tells: `sell`
  closes a long (`Long`), `buy` closes a short. Details of one frame are separate events; repeated records (key
  `instId|ts|side|posSide|sz|bkPx`) are dropped. **OKX states that the feed is not the total number of liquidations and
  not in chronological order:** treat it as a lower bound.
- **Open interest:** `open-interest` pushes `oi` (contracts), `oiCcy` (base coin) and `oiUsd` when the value changes
  (the first push comes on subscribe); `mark-price` pushes `markPx`. The connector keeps the latest of each per
  instrument and an interval sampler publishes one `OpenInterest` per tracked instrument every `oi_poll_interval_secs`,
  spread evenly over the interval: `oi_base = oiCcy`, `oi_usd = oiUsd`, `mark_price` = the last `markPx`. A sample
  whose `oiCcy` differs from `oi x ctVal x ctMult` by more than 1% is still published but counted
  (`vulture_oi_unit_mismatch_total`; none in the live run). Instruments without data, or whose connection is down, are
  skipped.
- **Gaps:** as for Bybit: `Gap` events with the streams `liquidations` and `open_interest` after a connection loss,
  never for a change of the tracked set.
- **Status and metrics:** `OkxConnector::status()`; the series of the other connectors with `exchange="okx"`,
  including `vulture_ws_*` (connections, topics, requests, budget) and `vulture_mark_price_age_seconds`.
- **Live check:** `cargo test -p vulture-server --test okx -- --ignored okx_live --nocapture` (about 75 s, not run in
  CI).

### Choosing exchanges in a deployment

The Docker configs enable Binance and carry commented `bybit` and `okx` entries. Keep an untracked copy
(`deploy/config.local.toml`, ignored by git) and point `VULTURE_CONFIG_FILE` at it in `deploy/.env`; see
[deploy/README.md](deploy/README.md).

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
- **Status:** written, batches, retries, buffered, dropped, last error, last write time and connection state
  (`RedisWriter::status()`), exported by the metrics endpoint.
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

## ClickHouse history (V3a)

Redis keeps 24 hours; ClickHouse keeps everything, without a TTL. It is optional: without a `[clickhouse]` section
(or with `enabled = false`) no flusher runs and the Redis stream is trimmed by time only, exactly as before.

### Schema and migrations

Four `MergeTree` tables, created by numbered SQL files embedded in the binary
(`crates/vulture-server/migrations/`): `liquidations`, `open_interest`, `gaps`, `signals`. All of them have
`PARTITION BY toYYYYMM(<time>)` (about twelve partitions a year; `gaps` use `from_ts`, `signals` use `ts`), no TTL,
`ORDER BY (exchange, symbol, <time>)` (`gaps`: `(exchange, stream, from_ts)`), and these properties:

- Columns follow `common.proto`: both timestamps as `DateTime64(3, 'UTC')` with `DoubleDelta` + `ZSTD`, `exchange`,
  `symbol`, `symbol_raw` as `LowCardinality(String)`, numbers as `Float64` with `Gorilla` + `ZSTD`, `oi_usd` and
  `mark_price` as `Nullable(Float64)`, `side` and the signal `type` as `Enum8`, `Signal.details` as `Map(String,
  String)`, and `event_id String` holding the Redis entry ID (`<ms>-<seq>`) for tracing.
- The sorting key puts the exchange and symbol first because every query names a symbol (or scans all of them for a
  period) and within a symbol the timestamps are close together, which is what `DoubleDelta` and `Gorilla` like.
  On the synthetic week below a time-first key `(exchange_ts, symbol)` took 380 MiB instead of 288 MiB (+32%, 35.3 vs
  26.7 bytes/row) and would also scan every symbol for a one-symbol query.
- `non_replicated_deduplication_window = 10000` makes `insert_deduplication_token` work on these non-replicated
  tables (verified against a real server by `tests/clickhouse_history.rs`).
- `schema_migrations(version, name, checksum, applied_at)` records what ran. Applying is idempotent and additive
  (a unit test rejects `DROP`, `TRUNCATE`, `DELETE`, `RENAME`, `MODIFY`, TTL); a migration edited after it ran is refused
  with an explanatory error. `vulture-server migrate --config <file>` applies them by hand; `run` applies them at
  start (and creates the database if it does not exist).

### The flusher

A task inside `vulture-server run`, enabled by `[clickhouse]`. Every `flush_interval_secs` (default 600) it reads the
stream after a **watermark** (the last flushed entry ID, kept in Redis under `<stream_key>:flusher:watermark`) in
pages of `batch_max_rows`, maps entries to rows (`d` decoded, `event_id` set), inserts per table with
`RowBinary` over HTTP and, only when every table of the page is acknowledged, advances the watermark with an atomic
`MULTI`/`EXEC`.

- **No loss, no duplicates.** Each insert carries `insert_deduplication_token = vulture-v1:<table>:<first>:<last>`
  (the entry ID range of the page). Before inserting, the exact range is recorded in Redis (`...:flusher:inflight`),
  because after a crash a plain retry would read a *longer* page with a different token. After a restart the recorded
  range is re-read and re-inserted with the same tokens, and ClickHouse drops the blocks it already stored.
  `tests/clickhouse_history.rs` kills the "process" between the insert and the watermark update and checks that
  every entry exists exactly once.
- **Safe trimming.** With the flusher on, the writer's `XTRIM MINID` cutoff is `min(now - window, watermark)`: nothing
  unflushed is ever trimmed by time (the watermark starts at `0-0`, so nothing is trimmed by time until the first
  flush). The `MAXLEN` safeguard (`redis.maxlen_safeguard`) still caps Redis memory and is the **one place where data can
  be lost**: if ClickHouse is down so long that the backlog outgrows it, the oldest unflushed entries disappear.
  `vulture_flusher_backlog_maxlen_ratio` and an alert fire well before (at 70%), and
  `vulture_flusher_possible_loss_total` counts the cases where the flusher finds that it happened.
- **Outages.** Any failure aborts the cycle, keeps the watermark and retries after an exponential backoff with jitter
  (1 s to 60 s). Connectors and the gRPC stream do not depend on the flusher. After recovery the backlog is flushed
  page by page with a 50 ms pause between pages.
- **Poison entries.** An entry that cannot be decoded is skipped, logged and counted
  (`vulture_flusher_skipped_entries_total`); it never stops the flusher. Timestamps are clamped into the
  `DateTime64` range.
- **Shutdown.** After the writer has written its last events the flusher runs one more cycle within
  `server.shutdown_timeout_secs` (best effort; what is left stays in Redis and is flushed after the next start).
- **One flusher per stream.** Two server processes against one Redis would race for the watermark.

### Configuration

`[clickhouse]` (see `config.example.toml`): `enabled`, `url`, `database`, `user`, `password` (a `Secret`, also
`password_file` or `VULTURE__CLICKHOUSE__PASSWORD`), `flush_interval_secs` (600), `batch_max_rows` (100000),
`insert_timeout_secs` (60), `monitor_interval_secs` (60). The password travels in a request header, never in the URL,
and never appears in logs or errors.

### Metrics and alerts

| Series | Meaning |
|---|---|
| `vulture_flusher_rows_inserted_total{table}`, `..._batches_total{table}` | rows and inserts per table |
| `vulture_flusher_failures_total{stage}` | failed attempts (`insert`, `redis`, `migrate`) |
| `vulture_flusher_last_success_age_seconds` | seconds since the last page was flushed |
| `vulture_flusher_watermark_lag_seconds` | how long ago the flusher last confirmed that nothing unflushed is left (a sawtooth up to one interval; grows during an outage) |
| `vulture_flusher_backlog_entries`, `..._backlog_maxlen_ratio` | estimated unflushed entries and their share of `maxlen_safeguard` |
| `vulture_flusher_watermark_timestamp_seconds` | time part of the watermark |
| `vulture_flusher_clickhouse_up`, `..._clickhouse_state{state}` | ClickHouse reachability (pinged every 15 s) |
| `vulture_flusher_possible_loss_total`, `..._skipped_entries_total{reason}` | loss suspected, entries without rows |
| `vulture_clickhouse_disk_free_bytes{disk}`, `..._disk_total_bytes{disk}` | from `system.disks`, sampled every `monitor_interval_secs` |
| `vulture_clickhouse_table_{parts,max_parts_per_partition,bytes,rows}{table}` | from `system.parts` and `system.tables` |
| `vulture_clickhouse_sample_age_seconds`, `..._sample_errors_total` | freshness of the sample |

`deploy/prometheus/alerts.yml` has the rules: flusher lagging more than 30 minutes, ClickHouse down, flusher failing,
backlog near `maxlen_safeguard`, watermark within 10 minutes of the oldest entry in Redis, possible loss, skipped
entries, disk under 25% (or 20 GiB) free, disk critical, too many parts, stale statistics. Merges temporarily need
extra room (a merge writes the new part before the old ones go), so keep the disk well below full.

### Tests

Unit tests cover the entry-to-row mapping of every type (including `NULL` cases, gaps, signals), the token, the
migration list, the watermark and the trim cutoff, and the flusher on in-memory fakes (pages, crash/restart, outage
with backoff, final flush, loss detection). `crates/vulture-server/tests/clickhouse_history.rs` runs against real
servers: migrations twice, token deduplication, every row type with its values, Redis-to-ClickHouse end to end, the
crash between insert and watermark, an outage with catch-up, a backlog of several pages, the watermark across a
restart, trimming and metrics, the disk monitor, and the whole pipeline with a shutdown flush. It needs
`VULTURE_TEST_CLICKHOUSE_URL` (and `VULTURE_TEST_CLICKHOUSE_USER` / `VULTURE_TEST_CLICKHOUSE_PASSWORD`) and
`VULTURE_TEST_REDIS_URL`, and skips without them:

```sh
docker run -d --rm --name vulture-test-ch -p 127.0.0.1:8123:8123 -e CLICKHOUSE_PASSWORD=test clickhouse/clickhouse-server:25.8
docker run -d --rm --name vulture-test-redis -p 127.0.0.1:6379:6379 redis:7
VULTURE_TEST_CLICKHOUSE_URL=http://127.0.0.1:8123 VULTURE_TEST_CLICKHOUSE_PASSWORD=test \
  VULTURE_TEST_REDIS_URL=redis://127.0.0.1:6379 cargo test -p vulture-server --test clickhouse_history
```

CI runs them against ClickHouse and Redis service containers.

### Measurements on synthetic history (2026-10-24)

`cargo run --release -p vulture-server --example synth_history` fills a scratch ClickHouse with 7 days of open
interest for 560 symbols at 30 s (11.29 million rows) and 827 thousand liquidations (about 118 thousand a day, with
three five-minute cascades a day at 40 times the rate), pushed through the flusher's code path (entry, row mapping,
`RowBinary`, insert with token). ClickHouse 25.8 in a Docker container on a laptop; the data is a model (random walks
with realistic quantization), so compression of real values will differ.

| | rows | compressed | bytes/row | ratio |
|---|---|---|---|---|
| `open_interest` (after `OPTIMIZE FINAL`) | 11 289 600 | 287.7 MiB | 26.7 | 2.6x |
| `liquidations` | 826 752 | 28.5 MiB | 36.2 | 1.8x |

Per column of `open_interest` (bytes/row): `event_id` 7.8, `oi_usd` 5.9, `mark_price` 5.0, `oi_base` 4.5, both
timestamps 1.7 each, symbols and exchange about 0. The timestamps and symbols compress as hoped; the floats are
random walks and compress only about 1.5x. Inserted as pages of 100 000 entries, ClickHouse took 2.3 million rows/s
(mapping and protobuf decoding add 2.7 s, 1.15 million rows/s end to end for the whole 12 million rows). Queries
(warm, median of five): sum of liquidations per symbol per day over the week 10 ms; OI of one symbol over a day
(2 880 rows) 3 ms; hourly average of one symbol over the week 2 ms; first/last OI of all 560 symbols over a day 11 ms;
top 10 symbols by liquidated USD over the week 5 ms; `avg(oi_usd)` over all 11.3 million rows 13 ms.

Capacity: 560 symbols every 30 s is 588 million OI rows a year, about **15 GiB a year** at 26.7 bytes/row
(about 1.5 GiB for liquidations at 118 thousand a day), so **roughly 16 GiB a year and 65 GiB over four years**, in line
with the earlier 10-20 GB/year estimate although the bytes/row are higher than the 3-6 assumed (the row count is
lower). One column, the `event_id` text, costs 29% of an OI row; storing it as two integer columns (milliseconds and
sequence) would take about 2.9 instead of 7.8 bytes/row and save about 18% of the table (not done: the task asks for
the string). Re-measure on the real stream: repeated OI values compress much better than random walks.

## gRPC API and token

`vulture-server run` serves three services on `server.listen_addr` (default `127.0.0.1:50051`): `Stream`
(implemented), `History` and `Control` (`History` and all `Control` methods except the two symbol RPCs answer `UNIMPLEMENTED`; the
contract stays visible to `grpcurl`). Every RPC of every service needs the token. Without `[tls]` the connection is plaintext, so
the server **refuses a non-loopback `listen_addr`** unless `server.allow_plaintext = true` is set (a warning is
logged; for an internal network or a TLS-terminating proxy). With `[tls]` any address is accepted (see "TLS").

### Token

One owner, one token, role admin implied. Generated on the first start if missing: 32 random bytes from the OS
CSPRNG, base64url (43 characters), stored in `<server.data_dir>/token` (file mode `0600`, directory `0700`).

- **The token itself is stored, not a hash.** The owner wants `token show` to print it again, which a hash cannot do.
  The trade-off: anyone who can read the file (or a backup or image containing `data_dir`) can use the service, so the
  file is protected like a TLS private key. If it leaks, run `token rotate`.
- `vulture-server token show --config <file>` prints the token to stdout (only the token, so `$(...)` is safe).
  `token rotate` writes a new random one and prints it; `token set` reads a token of your choice from **stdin** (never
  from the command line) and stores it atomically with mode `0600`. The old token stops working at once.
- **The file is the interface.** In Docker the data directory is a host directory (`deploy/data` by default,
  `VULTURE_DATA_DIR`), so the owner reads the token with `cat data/token` and changes it by editing the file. A
  running server polls the file every `server.token_check_interval_ms` (1 s). New content is trimmed (whitespace and
  the trailing newline) and must have at least 32 characters from `A-Z a-z 0-9 - _`; then the server switches to it,
  ends the open streams that used the old token with `UNAUTHENTICATED` and logs `token changed from file` (never the
  value). An invalid edit is rejected: the previous token stays active, the error is logged and
  `vulture_token_file_invalid_edits_total` counts it (`vulture_token_file_changes_total` counts accepted changes).
- **Start and deletion.** A missing file is created at start (`token create` does the same on demand). An existing
  file is used as is and never overwritten; an unreadable or invalid one stops the start with a message that does not
  show its content. A file deleted while the server runs does not disable authentication: the last good token stays
  in memory, a warning is logged and the file is not recreated until the next start.
- **Banner only on a terminal.** The one-time banner with the token goes to stderr only when stderr is an interactive
  terminal. In Docker or systemd the log has a single line with the file's path and no value.
- `vulture-server config check` also validates `state.json` and, when present, the token file (valid content, a
  warning if group or others can read it), without printing secrets.
- Clients send `authorization: Bearer <token>`. The comparison is constant-time. A missing, malformed or wrong
  token gives `UNAUTHENTICATED`; after 10 failures per minute from one peer address, further *failed* attempts from
  it get `RESOURCE_EXHAUSTED` until the failures age out. The limiter only slows down guessing: **a correct token is
  never refused**, not even from an address that has failed before (the owner cannot be locked out). Failures are
  logged with the peer address and the reason, never with the presented value. The token never appears in logs, errors or panics.

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
while Redis is down gets `UNAVAILABLE`. Counters (exported as metrics): `GrpcStats` (clients, subscriptions,
events sent, slow disconnects, resume requests, out-of-range answers, rejected clients, Redis read errors) and
`AuthStats` (failures, rate-limited requests).

## TLS

`[tls]` (`cert`, `key`, optional `client_ca`, `reload_interval_secs`) makes the gRPC listener serve TLS: rustls with
the ring provider, TLS 1.2 and 1.3, HTTP/2 negotiated by ALPN. `cert` is a PEM chain (certbot's `fullchain.pem`),
`key` a PEM private key (PKCS#8, PKCS#1 or SEC1). With `client_ca` the server also requires a client certificate
from that CA (mTLS); a client without one is refused at the handshake. The token is still required on every RPC.

- **Hot reload.** tonic's own TLS config cannot change the certificate of a running server, so the handshake uses a
  rustls certificate resolver backed by an atomically swappable key and tonic is fed through a custom incoming TLS
  stream. A reload affects **new connections only**; streams that are open keep running on the old certificate.
  Triggers: `SIGHUP`, a poll of the files' mtime and size every `reload_interval_secs` (default 30, `0` = off), and
  nothing else (use `tls check` to validate without a server).
- **Bad replacement.** A reload that fails (unreadable or invalid PEM, key does not match the certificate,
  certificate expired or not yet valid) keeps the old certificate in use, logs the error (file names and reason,
  never key material) and counts it in `vulture_tls_reloads_total{result="error"}`; `vulture_tls_last_reload_ok`
  is `0` until a good pair arrives. A change is evaluated once; a broken pair is not retried until a file changes
  again or `SIGHUP` arrives.
- **Expiry.** `not_after` and the days left are logged at start and on every reload (a warning below 14 days) and
  exported as `vulture_tls_certificate_expiry_seconds`. `client_ca` is read at start only.
- **Validation.** `vulture-server tls check --config <file>` loads `[tls]` the way the server does and reports the
  expiry; `config check` does the same when `[tls]` is present (files exist and parse, key matches the certificate,
  certificate valid now). `vulture-server run` refuses to start with unusable files.
- **SIGHUP without TLS** is ignored (and logged), so deploy hooks may send it unconditionally.
- **Client.** `cargo run -p vulture-server --example stream_client -- --endpoint https://vulture.example.com:50051
  --ca-cert ca.pem [--domain name] [--client-cert c.pem --client-key k.pem]`; a certificate from a public CA needs no
  `--ca-cert`.

Certificates in tests are generated on the fly (`rcgen`); `tests/tls.rs` covers handshake, reload by poll with an open
stream, rejected replacements, mTLS and the file validation errors, `tests/tls_sighup.rs` a real `SIGHUP`.

## Signals and alerts (V4a)

An analytics engine runs **inside the server process**. It watches the event flow, publishes its own `Signal`
events into the same stream (so the writer, Redis, the gRPC `Stream` and the ClickHouse flusher treat them like any
other event) and sends messages to Telegram and Discord according to the owner's rules.

```
connectors ──► tap ──► Redis writer ──► Redis Stream ──► gRPC Stream, flusher
                │                           ▲
                └──► engine (own bounded queue)
                       │  OI ring buffers, spike detector, cascade detector, rules
                       ├──► Signal events ─────┘  (same channel as every event)
                       └──► alerts ──► one queue per channel ──► Telegram / Discord
```

- **Never in the writer's way.** A forwarder task between the connectors and the writer offers a copy of each event
  to the engine with `try_send`. If the engine's queue (`alerts.input_queue`) is full it drops *its own* input,
  counts it (`vulture_alerts_engine_input_dropped_total`) and logs at most every 10 s; the writer receives every
  event exactly as before. `[alerts] enabled = false` removes the tap entirely (and the rule and channel RPCs of
  `Control` answer `UNIMPLEMENTED` again).
- **Warm-up.** At start the engine replays the last `alerts.warmup_secs` (4 h) of the Redis stream through the
  existing reader API (`bounds` and paged `read_range`), so a restart does not blind the detectors. The replay only
  rebuilds state: no signal, no message. Stored `sig` entries restore the OI cooldowns, so a restart does not repeat
  a signal. Live events that arrive during the replay wait in the engine's queue; samples are de-duplicated by
  time, and liquidations at or before the replay's last timestamp are not counted twice.
- **`Gap` events.** A `Gap` of an exchange (any stream except `liquidations`) or of the writer (`exchange = "*"`) marks
  the open interest as discontinuous until its `to_ts`: windows whose reference point lies before that moment are
  not evaluated. A pause longer than `alerts.hole_secs` (300 s) between two samples of one symbol (for example a
  symbol that was untracked for a while) is treated the same way, because the change across a hole is not a market move. `hole_secs` must be at least twice the OI poll interval of every enabled exchange (`config
check` enforces it), otherwise every window would look like it spans a hole.

### OI spike detector (`OI_SPIKE_UP`, `OI_SPIKE_DOWN`)

Per symbol of every exchange the engine keeps a ring buffer of `(ts, oi_base, oi_usd, mark_price)` for the last
4 h plus 10 min (a sample is 32 bytes; at most `alerts.series_max_samples` = 1024 per symbol; samples closer than
15 s to the previous one are ignored). **Memory bound:** one 512-slot buffer (16 KiB) per symbol in the usual
case, at most `series_max_samples` x 32 B = 32 KiB when a connector samples every 15 s, and at most
`alerts.max_symbols` (5000) symbols: about 31 MiB for 2000 symbols, 80 MiB at the symbol limit, 160 MiB in the
absolute worst case; buffers of symbols that stopped reporting are dropped after the retention. The allocated bytes
are exported as `vulture_alerts_engine_memory_bytes`. On every new sample, for each enabled window `w` (1m, 5m, 15m, 1h, 4h):

1. `change_pct = 100 x (oi_base_now - oi_base_ref) / oi_base_ref`, the reference being the sample nearest to
   `now - w` within a tolerance of `w/8` (15 s to 300 s; the 1 m window works with 30 s sampling). **`oi_base`**,
   not `oi_usd`, is compared, so a price move alone never looks like a spike. `change_abs` is the change in base
   units; `change_usd = change_abs x mark_price`.
2. All of these must hold: `|change_pct| >= threshold_pct(w)`, `|change_usd| >= min_change_usd(w)`,
   `oi_usd_now >= min_oi_usd` (dust is ignored).
3. **Unusual for the symbol.** `|change_pct - median| >= z_min x sigma`, where `median` and `sigma` describe the
   symbol's own earlier changes over the same window (evaluated at up to 200 earlier points of the retained
   history, before the current window): `sigma = max(1.4826 x MAD, (p90 - p10) / 2.563)`, floored at
   `threshold_pct / 8`. The second term keeps a restless symbol (one that jumps in more than half of the
   samples, which collapses the MAD) from alarming at its usual size. The 4 h window has no earlier history; it
   uses the symbol's step-to-step volatility scaled by `sqrt(steps in the window)`. At least
   `min_baseline_points` (60) points are needed; without them nothing is signalled (`require_baseline = true`),
   so a symbol that has just appeared is silent for about 30 minutes after a cold start.
4. **Cooldown** per (exchange, symbol, window, direction): `cooldown_secs` (15 min). Inside it a move still
   signals when it reaches `extension_factor` (1.5) times the last signalled one.
5. **Overlapping windows** of one direction become one signal: the window with the largest
   `|change_pct| / threshold` is reported (`window_secs`, `change_pct`, `change_abs`, `threshold`), the others are
   listed in `details["windows"]`.

Defaults (`[alerts.oi_spike]`, replaceable with `[[alerts.oi_spike.windows]]`):

| window | threshold_pct | min_change_usd |
|---|---|---|
| 1m | 1.5 | 250 000 |
| 5m | 3 | 500 000 |
| 15m | 5 | 1 000 000 |
| 1h | 8 | 2 000 000 |
| 4h | 12 | 3 000 000 |

`min_oi_usd = 1 000 000`, `z_min = 4`. **Per-exchange and per-symbol overrides** are stored with the rules: an
`OiChange` rule with `detector_override = true` replaces `min_change_pct` and `min_change_usd` of the detector for
its window and its exchange/symbol filters (a symbol rule beats an exchange rule beats the default) and sends
nothing itself.

`Signal.details` carries `key` (a deterministic identity, `type:exchange:symbol:window:ts`), `severity`
(`|change_pct| / threshold` below 2 is `info`, below 4 `warning`, otherwise `critical`), `windows`,
`window_secs_all`, `oi_base`, `oi_base_ref`, `oi_usd`, `change_usd`, `mark_price`, `ref_ts`, `z` and `baseline`.
V3-8a's `History.Aggregate` ranking of OI moves must use the same change formula (reference sample nearest to
`now - w`, `oi_base`, gaps excluded); the arithmetic is `alerts::series::Series::change`.

### Liquidation cascade detector (`LIQ_CASCADE`)

Scopes: market-wide (`exchange = "*"`) and, with `per_exchange`, each exchange. Per scope:

- **Baseline:** the notional per clock-aligned minute over the previous `baseline_secs` (24 h), completed minutes
  only; a minute in which the process saw any event but no liquidation counts as zero, a minute it did not see is
  absent. `median` and `MAD` of those values, `sigma = max(1.4826 x MAD, 0.1 x median)`, recomputed when the minute
  changes. At least `min_baseline_minutes` (120) are required; before that the trigger is `3 x floor`. The baseline
  covers as much as the warm-up replayed (4 h by default) plus the uptime, up to 24 h; set `alerts.warmup_secs =
  86400` to replay the whole day at start (the replay costs about 10 s per 6 million entries, measured below).
- **Start:** the notional of the last 60 s (a sliding window, evaluated at every liquidation) is at least
  `max(floor, median + k_mad x sigma)` (`floor_usd` 2 M market-wide, `floor_usd_exchange` 1 M; `k_mad` 8) and the
  window holds at least `min_count` (5) liquidations. A signal with `details["phase"] = "start"` carries the
  running totals.
- **End:** the cascade is *hot* while the last `min(quiet_secs, 60)` seconds hold at least `end_ratio x trigger x
  min(quiet_secs, 60) / 60`; it ends after `quiet_secs` (120) without being hot. A closing signal
  (`phase = "end"`) carries `duration_secs`, `total_usd`, `count`, `long_usd`, `short_usd`, `long_share`,
  `peak_usd_per_min`, `top_symbols` and `price_low` / `price_high` of the top symbol.
- Signal fields: `symbol` empty, `window_secs` 60, `change_pct` the intensity relative to the baseline
  (`100 x value / max(median, 1000)`), `change_abs` the notional in USD (the window at the start, the total at the
  end), `threshold` the trigger in the same unit; severity `critical` at 3x the trigger, otherwise `warning`.

### Rules and channels

State file `<data_dir>/alerts.json`: versioned (`"version": 1`), written atomically (temporary file, `fsync`,
rename) with mode `0600`, and **a corrupt or unknown-version file stops the start** with a message that never quotes
the file's content (same policy as `state.json`; `vulture-server config check` validates it). While the file does
not exist, `[[alerts.rules]]` and `[[alerts.channels]]` of the configuration are in force; the first change through
`Control` writes the file with those defaults and the change, and from then on the file alone decides. Layout:

```json
{
  "version": 1,
  "channels": [
    {"id": "tg-main", "name": "Main chat", "kind": "telegram", "enabled": true,
     "chat_id": "-1001234567890", "secret": "<bot token>"},
    {"id": "dc-main", "name": "Discord", "kind": "discord", "enabled": true,
     "secret_env": "VULTURE_DISCORD_WEBHOOK"}
  ],
  "rules": [
    {"id": "big-liq", "name": "Liquidations over 500k", "enabled": true, "cooldown_secs": 60,
     "exchanges": [], "symbols": [], "channels": ["tg-main"],
     "kind": {"type": "liquidation", "min_notional_usd": 500000, "sides": []}},
    {"id": "signals", "name": "Spikes and cascades", "enabled": true, "channels": ["tg-main", "dc-main"],
     "kind": {"type": "signal", "types": [], "min_severity": "warning"}}
  ]
}
```

| rule `kind.type` | fields | meaning |
|---|---|---|
| `liquidation` | `min_notional_usd`, `sides` (`long`/`short`, empty = both) | one liquidation of at least that notional |
| `oi_change` | `window_secs` (30 to 14400), `min_change_pct`, `min_change_usd`, `detector_override` | `|change of oi_base|` over the window (both directions, no z-score); with `detector_override` it only retunes the detector |
| `signal` | `types` (`oi_spike_up`, `oi_spike_down`, `liq_cascade`; empty = all), `min_severity` | pass the server's own signals on |

Every rule has `exchanges` and `symbols` filters (empty = all; symbols are normalized, `BTC-USDT-PERP`), `channels`,
`cooldown_secs` and `enabled`. The cooldown is per (rule, exchange, symbol); for signals it also includes the
cascade phase, so a closing summary is never swallowed by the cooldown of the start. Rules are evaluated on the live
flow only (not during the warm-up) and swapped atomically: a change takes effect for the next event.

A channel's secret (bot token, or the whole webhook URL) comes from exactly one of `secret` (stored in
`alerts.json`), `secret_file` or `secret_env`, resolved each time a message is sent (so a rotated file is picked up).
**Secrets never leave the server**: API replies carry a mask (`****` and the last four characters, nothing for short
secrets), errors and logs never quote them (transport errors are logged without the URL, which holds the Telegram
token), the audit line records only `secret_changed`. Discord webhooks must be `https://discord.com/api/webhooks/...`
(also `discordapp.com`, `ptb.` and `canary.`) unless `alerts.delivery.allow_custom_endpoints` is set (tests).

`Control` RPCs (behind the token): `ListRules`, `UpsertRule`, `DeleteRule`, `ListChannels`, `UpsertChannel`,
`DeleteChannel` and the new `SendTestMessage`. Validation answers `INVALID_ARGUMENT` (unknown channel in a rule,
bad id or symbol, missing kind, malformed secret, ...), deleting a channel that rules use `FAILED_PRECONDITION`,
`SendTestMessage` on an unknown channel `NOT_FOUND`, gap rules `UNIMPLEMENTED` (system alerts, V4-5), a failed write
`INTERNAL` (nothing changed). Upserts are idempotent (a repeat writes nothing and leaves no audit line), an empty
`id` creates a rule or channel with a server-assigned one, an empty `secret` keeps the current one, deleting what does
not exist succeeds. Every change is one JSON line in `<data_dir>/audit.log` (the file the symbol policies use) and one
log line. `SendTestMessage` sends at once, without queue, batching or retry, and answers `delivered`, `dry_run` and a
short `detail` (`ok`, `HTTP 401 ...`, `dry run`). Per-channel `dry_run` logs the message instead of sending it.

### Delivery

One task and one bounded queue per channel. The task takes the first alert, waits `batch_window_ms` (3 s) for the
rest of the burst, and sends everything queued as **one message**: a single alert gets the full form, several a
compact digest ("12 alerts", the first `digest_max_items` listed, "and N more"). Telegram uses the Bot API
`sendMessage` with HTML parse mode (`<`, `>` and `&` escaped, timestamps in UTC); Discord a webhook POST with one
embed, markdown escaped and mentions switched off (`allowed_mentions.parse = []`). Compact numbers (`$1.23M`).

- **Rate limit** per channel: at least `telegram_min_interval_ms` (1100) / `discord_min_interval_ms` (2000) between
  messages and at most `*_max_per_minute` (20) per minute; per channel `min_interval_ms` overrides. A cascade of
  thousands of liquidations therefore becomes a few messages with correct totals.
- **429:** the wait is the platform's `retry_after` (Telegram `parameters.retry_after`, Discord JSON `retry_after`
  or `Retry-After`; capped at one hour), then the same message is sent again. Discord's
  `X-RateLimit-Remaining: 0` makes the task wait `X-RateLimit-Reset-After` before the next call.
- **Failures** (connection errors, timeouts, 5xx): exponential backoff with jitter from `retry_min_ms` (1 s) to
  `retry_max_ms` (60 s), `max_attempts` (8) per message; other 4xx are not retried. A message given up is counted
  as dropped and announced in the next one ("N messages dropped").
- **Overflow:** a queue above `queue_limit` (500) drops its oldest alerts; the next message says how many.

### Metrics

| series | content |
|---|---|
| `vulture_alerts_engine_events_total{type}`, `_engine_input_dropped_total`, `_engine_symbols_rejected_total` | events seen, the engine's own input drops |
| `vulture_alerts_signals_total{type}`, `_suppressed_total{detector,reason}`, `_cascades_total{phase}` | signals by type; candidates suppressed by cooldown, "usual for the symbol", missing baseline, dust (`oi_spike`) and by rule cooldown (`rules`) |
| `vulture_alerts_evaluations_total`, `_evaluation_seconds_sum`, `_evaluation_seconds_max` | evaluation time per event |
| `vulture_alerts_series`, `_samples`, `_engine_memory_bytes` | ring buffers: symbols, samples, allocated bytes |
| `vulture_alerts_warmup_seconds`, `_warmup_entries`, `_warmed_up` | the last warm-up |
| `vulture_alerts_queued_total`, `_rules_active`, `_config_changes_total`, `_config_rejected_total` | alerts handed to queues, rules in force, `Control` changes |
| `vulture_alerts_channel_{sent,alerts_sent,failed,dropped,retries,rate_limited,dry_run,disabled_skipped}_total{channel,kind}`, `_channel_queue_depth`, `_channel_last_success_age_seconds` | per channel (the age is absent before the first success) |

### Enabling real delivery

Delivery to the real services is switched on by the owner, never by the repository's tests (they use local mock
servers only). Telegram: create a bot with @BotFather, put the bot into the chat or channel and note its id; Discord:
create a webhook in the channel settings. Keep the secrets out of the configuration file, for example in files that
the container mounts read-only. Then, with a token from `vulture-server token show` and
[grpcurl](https://github.com/fullstorydev/grpcurl):

```sh
H="authorization: Bearer $VULTURE_TOKEN"; P="-import-path proto -proto vulture/v1/control.proto"
# 1. A channel in dry-run mode first (secret goes into alerts.json; or use secret_file / secret_env):
grpcurl -plaintext $P -H "$H" -d '{"channel": {"id": "tg-main", "name": "Main chat", "kind": "CHANNEL_KIND_TELEGRAM",
  "enabled": true, "chat_id": "-1001234567890", "dry_run": true}, "secret": "<bot token>"}' \
  127.0.0.1:50051 vulture.v1.Control/UpsertChannel
# 2. A rule that forwards warning signals:
grpcurl -plaintext $P -H "$H" -d '{"rule": {"id": "signals", "enabled": true, "channel_ids": ["tg-main"],
  "signal": {"min_severity": "SEVERITY_WARNING"}}}' 127.0.0.1:50051 vulture.v1.Control/UpsertRule
# 3. Watch the dry-run lines in the server log, then switch dry_run off (same call, "dry_run": false, empty secret
#    keeps the stored one) and send a test message:
grpcurl -plaintext $P -H "$H" -d '{"channel_id": "tg-main"}' 127.0.0.1:50051 vulture.v1.Control/SendTestMessage
```

(With TLS, drop `-plaintext` and pass the CA as in the examples above.) `delivered: true` means the platform accepted
the message. Editing `alerts.json` by hand while the server is stopped works as well.

### Measurements (2026-10-25, Apple M5 Pro, release build, synthetic data)

`cargo run --release -p vulture-server --example synth_alerts -- memory|load|warmup`:

| what | result |
|---|---|
| memory, 2000 symbols x 480 samples (4 h at 30 s) | ring buffers 31.3 MiB allocated (32 B per sample, 512 slots per symbol); process RSS grows by 32.5 MiB |
| CPU at 3 500 events/s (95 % open interest, each one a full five-window evaluation, 5 % liquidations; 30 s through the real tap and engine task) | mean 4.1 us per event, 14 ms of evaluation per second of load (1.4 % of one core); whole process 0.85 s CPU in 30 s including the generator (generator alone 0.31 s); maximum single evaluation 0.11 ms, no input dropped |
| the same with every symbol falling 8 % in one round (the detector's expensive path) | mean 3.9 us, 2 111 down signals, max 0.16 ms |
| warm-up from a 4 h stream on a local Redis (2000 symbols, 998 400 entries) | 1.58 s (630 000 entries/s), +42 MiB RSS while replaying; extrapolated to a full 24 h stream (about 6 million entries) roughly 10 s |

The synthetic market is a quiet random walk; real data will differ in how often candidates reach the baseline step.
Re-measure on the server with the metrics above.

## Metrics

A separate listener, `metrics.listen_addr` (default `127.0.0.1:9100`), serves `/metrics` (Prometheus text format),
`/healthz` (the process is up) and `/readyz` (Redis answers and the gRPC listener runs; 503 otherwise). **The port is
not authenticated**: keep it on loopback or an internal network, or put a reverse proxy with authentication in
front. `vulture-server healthcheck --config <file> [--ready]` calls it (used by the container health check).

The exposition is written by a small hand-made writer (no registry dependency): every component keeps its own
atomic counters and reports them at scrape time. Labels are low-cardinality on purpose (`exchange`, `type`, `state`,
`result`, `code`, `version`), never a symbol, peer address or client. Main series:

| prefix | content |
|---|---|
| `vulture_build_info`, `vulture_process_*` | version, uptime, start time |
| `vulture_events_total{exchange,type}`, `vulture_parse_errors_total{exchange,type}` | events published (liquidation, open_interest, gap) and unparsable frames |
| `vulture_connector_*`, `vulture_symbols`, `vulture_mark_price_age_seconds` | frames, filtered liquidations (untracked symbols included), universe size, liveness |
| `vulture_symbols_universe{exchange}`, `vulture_symbols_tracked{exchange}`, `vulture_symbol_policy_changes_total{exchange,result}` | symbols the exchange offers, symbols tracked under the policy, `SetSymbolPolicy` requests (`applied`, `unchanged`, `rejected`, `error`) |
| `vulture_oi_*`, `vulture_rest_*`, `vulture_exchange_info_loads_total` | open interest polling: requests, retries, failures, used weight, 429/418, pause and ban state, interval |
| `vulture_ws_*{exchange}` | WebSocket manager: state (`state` label, 1 for the current one), reconnects, gaps, duplicates, overflow drops, bans |
| `vulture_redis_*` | writer: written, batches, retries, buffered, dropped, gaps written, last write age, connection state; sampled stream length and oldest/newest entry age |
| `vulture_grpc_*`, `vulture_auth_*` | clients, subscriptions, events sent, slow-client disconnects, resumes, out-of-range, failed and rate-limited authentications |
| `vulture_flusher_*`, `vulture_clickhouse_*` | ClickHouse flusher and storage (see "ClickHouse history"): rows, failures, lag, backlog, reachability, disk, parts |
| `vulture_tls_*` | enabled, seconds until expiry, reloads by `result`, last reload ok, handshake failures |
| `vulture_alerts_*` | analytics engine, detectors, rules and per-channel delivery (see "Signals and alerts") |

The Redis stream length and entry ages are sampled every 15 s (they need round trips to Redis), the rest is read at
scrape time. `deploy/prometheus/` has a sample scrape configuration and alert rules (writer buffer growing, Redis
writes stalled, connector disconnected, certificate expiring, Binance ban, stream lag). `tests/metrics.rs` parses
the exposition with a Prometheus text parser and checks key series, movement after synthetic activity, bounded label
sets and `/readyz` against a failing store.

## Deployment

See [deploy/README.md](deploy/README.md): `Dockerfile` (multi-stage, distroless, non-root, `HEALTHCHECK` through
`vulture-server healthcheck`, `data_dir` as a volume, no secrets in the image), `deploy/docker-compose.yml` (vulture,
Redis with AOF and a password, an optional ClickHouse service with a pinned tag; ports published to `127.0.0.1`),
`deploy/certbot/` (DNS-01 and a deploy hook that copies the certificate and sends `SIGHUP`), `deploy/systemd/`, and
`deploy/prometheus/`. Everything is generic: placeholders instead of hostnames, secrets only in ignored `.env`
files. CI builds the image (no push) and validates `docker compose config`.

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
`partial` flag). Contract only: no server logic for these yet (the symbol policy RPCs got theirs in v0.3). v0.3 (additive): `SetSymbolPolicyResponse` gained `tracked_count`, `universe_size`, `added`, `removed`, `changed`, and
`ListSymbolPoliciesResponse` gained `statuses` (`SymbolPolicyStatus`: tracked count, universe size, whether the
list is loaded, whether the policy is an owner override). Timestamps are int64, Unix epoch milliseconds;
numbers are `double`. Evolution rules: fields may only be added, numbers are not reused,
removed fields are marked `reserved`. Checks: `cd proto && buf lint`, and `buf breaking --against "../.git#ref=<base>,subdir=proto"` against the previous revision. Rust code is generated in
`build.rs` (vendored protoc, tonic server+client); C++ for the UI uses `protoc` and `grpc_cpp_plugin` from
conan (`ui/CMakeLists.txt`, target `vulture-proto-cpp`).

## Git hooks (secret scan)

The repository is public, so a leaked token or key would be public too. The CI job `secrets` scans the whole history
with [gitleaks](https://github.com/gitleaks/gitleaks) on every push (rules and exceptions: `.gitleaks.toml`). To stop a
secret before it is even committed, enable the pre-commit hook once per clone:

```sh
scripts/setup-hooks.sh        # git config core.hooksPath .githooks
brew install gitleaks         # macOS; see the gitleaks page for other systems
```

The hook (`.githooks/pre-commit`) runs `gitleaks git --pre-commit --staged --redact` and refuses a commit whose staged
changes look like a secret (the value is hidden in the output). Without gitleaks it only warns. `git commit
--no-verify` skips it; CI still checks the pushed history. A secret that was already pushed must be treated as leaked and
revoked: deleting it in a new commit does not remove it from the history.

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
