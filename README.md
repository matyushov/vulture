# vulture

Open source service that collects **liquidations** and **open interest** from crypto exchanges: a Rust server,
gRPC API, Redis Streams hot storage and ClickHouse history, plus a Qt6 client.

Status: scaffold, proto v0.3 contract, CI, the server foundation (configuration, exchange trait, symbol and
open interest normalization, reconnecting WebSocket manager), the Binance USDT-M connector, the Redis Streams
writer (24-hour hot window), the gRPC `Stream.Subscribe` with resume and the owner token, TLS with certificate hot
reload, Prometheus metrics, tracked symbols managed at runtime (`Control.ListSymbolPolicies` /
`SetSymbolPolicy`), and a Dockerfile with compose, certbot, systemd and Prometheus templates in `deploy/`.
`History` and the rest of `Control` are in the contract but answer `UNIMPLEMENTED`; ClickHouse history and the other
exchanges are not implemented yet. The server needs a Redis instance (`redis.url`); deployment is described in
[deploy/README.md](deploy/README.md).

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

UI (requires Qt6 and CMake; gRPC/protobuf dependencies are described in `ui/conanfile.py`):

```sh
cmake -S ui -B ui/build   # without Qt6 the vulture-ui target is disabled with a warning
cmake --build ui/build
```

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
  not supported: a non-empty list is refused with `UNIMPLEMENTED`. All other `Control` RPCs stay `UNIMPLEMENTED`.
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
  `token rotate` writes a new one and prints it; the old one stops working at once. A running server notices a
  rotation done by the CLI within `server.token_check_interval_ms` (1 s) and ends the open streams that used the old
  token with `UNAUTHENTICATED`. `token create` creates the token only if there is none and prints it once to stderr.
  A deleted file is never regenerated silently: only `token create` or the first start of the server do that.
- Clients send `authorization: Bearer <token>`. The comparison is constant-time. A missing, malformed or wrong
  token gives `UNAUTHENTICATED`; after 10 failures per minute from one peer address, further *failed* attempts from
  it get `RESOURCE_EXHAUSTED` until the failures age out. The limiter only slows down guessing: **a correct token is
  never refused**, not even from an address that has failed before (the owner cannot be locked out). Failures are
  logged with the peer address and the reason, never with the presented value. The token never appears in logs, errors or panics; the first
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
| `vulture_tls_*` | enabled, seconds until expiry, reloads by `result`, last reload ok, handshake failures |

The Redis stream length and entry ages are sampled every 15 s (they need round trips to Redis), the rest is read at
scrape time. `deploy/prometheus/` has a sample scrape configuration and alert rules (writer buffer growing, Redis
writes stalled, connector disconnected, certificate expiring, Binance ban, stream lag). `tests/metrics.rs` parses
the exposition with a Prometheus text parser and checks key series, movement after synthetic activity, bounded label
sets and `/readyz` against a failing store.

## Deployment

See [deploy/README.md](deploy/README.md): `Dockerfile` (multi-stage, distroless, non-root, `HEALTHCHECK` through
`vulture-server healthcheck`, `data_dir` as a volume, no secrets in the image), `deploy/docker-compose.yml` (vulture,
Redis with AOF and a password, an optional ClickHouse placeholder profile; ports published to `127.0.0.1`),
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
