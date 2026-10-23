# S-4: WebSocket reconnect and zombie detection in the cran collector

Read 2026-10-23, read-only, no code written. Defaults below are given as `file:line`.

## 0. Where the logic actually lives

The README says "reconnect and zombie detection from cran (stream-collector)". Two code bases exist:

| Repo | State |
|---|---|
| `/Users/amatyushov/Projects/cran` (old C++ collector) | **Has the full logic**: `src/CollectorCore/src/BinanceCollectorCore.cpp` (watchdog), `src/Streams/src/BinancePublicStream.cpp` (reconnect), `src/WebSockets/src/Session.cpp`, `WebSoket.cpp` (transport), `src/MarketStore/src/MarketState.cpp` (state resync), design notes in `cran/PHASE21-collector-reliability.md` |
| `/Users/amatyushov/Projects/stream-collector` (new, Aeron/SBE) | Transport only: `src/WebSockets/src/Session.cpp` and `WebSoket.cpp` are the same code; **no reconnect, no watchdog** (`src/Service/CollectorCore.hpp:34`, `src/Service/src/ServiceCollector.cpp:44` say "no reconnect until SC-9"). Comment in `Session.cpp:145` ("watchdog in BinanceCollectorCore") is stale. SC-9 in `knowledge/apps/cran/WORK_PLAN.md:126` plans "silence -> probe -> reconnect -> escalation" |

So every line reference below is to **cran** unless it says stream-collector. The two Session.cpp files differ
only in the logging macro and a few includes; `Session.cpp` line numbers below are cran's (stream-collector's are
about 4 lines lower).

## 1. Algorithm, step by step

### 1.1 Layers
1. `WebSockets::WebSocket` owns an `io_context`, one thread and one `Session` (Boost.Beast, TLS 1.2 client).
2. `BinancePublicStream` owns **two** sockets: "market" (kline, forceOrder, markPrice) and "public"
   (depth, trade), each with its own activity stamps (`BinancePublicStream.cpp:23-60`, `237-262`).
3. `BinanceCollectorCore` owns the authoritative symbol list and a **watchdog thread** that decides when to
   reconnect (`BinanceCollectorCore.cpp:125-268`).

### 1.2 Connection establishment (`Session`)
1. Resolve DNS; on error log and stop (`Session.cpp:98-103`). Nothing retries here: the watchdog does.
2. TCP connect with `expires_after(30 s)` (`Session.cpp:112`), TLS SNI, TLS handshake with 30 s
   (`Session.cpp:126`), WS handshake with `handshake_timeout = 30 s` (`Session.cpp:151`).
3. After the handshake: `idle_timeout = none`, `keep_alive_pings = false` (`Session.cpp:152-153`), TCP deadline
   `expires_never()` (`Session.cpp:157`). The client never pings; it relies on Beast answering the server's
   ping with a pong (Binance pings, see `WS_SERVICE_MESSAGES.md`: futures every 3 min, 10 min pong timeout).
4. `WebSocket::start()` runs `io_context.run()` in a thread (exceptions logged and the loop resumed,
   P10 fix, `WebSoket.cpp:28-47`) and waits **10 s** for the "connected" promise (`WebSoket.cpp:48`). On
   timeout it logs "Connection timeout" and **returns normally** (is_running stays false); no exception.
5. Subscription is a single batched frame per socket with every symbol and stream type
   (`BinancePublicStream.cpp:97-125`); outgoing frames go through a strand-serialised queue instead of the old
   100 ms sleep (P11, `Session.cpp:40-60`). A write error clears the queue (`Session.cpp:83-89`).
6. Read errors and `closed` simply end the read loop (`Session.cpp:194-203`): **no callback, no reconnect
   trigger**. A dead connection is noticed only through silence.

### 1.3 Activity stamps
Each incoming message sets `lastActivity{Market,Public}` (steady clock, ms) at the top of the callback, before
parsing (`BinancePublicStream.cpp:333`, `415`). Stream-specific stamps are set when that message type is
parsed: `lastKline`, `lastLiquidation`, `lastMarkPrice` (market), `lastDepth`, `lastTrade` (public)
(`:342`, `:360`, `:381`, `:424`, `:442`). On reconnect all stamps of that socket are reset to 0
(`:271-274`, `:292-294`) and `lastReconnect{Market,Public}` is set to "now" after the subscription frame is sent
(`:278`, `:298`).

### 1.4 Watchdog loop (every 2 s)
Constants: `kSilenceThresholdMs = 5 000` (`Types/headers/CollectorTiming.hpp:18`), `kZombieThresholdMs = 5 000`
(`BinanceCollectorCore.cpp:19`), `kWatchdogIntervalMs = 2 000` (`:20`), `kMaxReconnectAttempts = 60` (`:21`).
Each tick (`:130-268`):
1. `market_dead` = `lastActivityMarket != 0 && now - lastActivityMarket >= 5 s`; same for `public_dead`
   (`:142-143`). "Dead" means *no message of any kind*, including subscribe acks, for 5 s.
2. `market_zombie` = not dead, a reconnect happened (`lastReconnectMarket != 0`), **no `mark_price` ever
   arrived since** (`lastMarkPrice == 0`) and 5 s have passed since the reconnect (`:157-158`); `public_zombie`
   uses `depth` and `lastReconnectPublic` (`:159-160`). Reason in the comment (`:145-152`): the subscription
   ack `{"result":null,"id":0}` refreshes `lastActivity`, so a reconnected-but-mute socket would never look
   silent; a connect timeout leaves `lastActivity == 0` which the silence test skips. `mark_price` is chosen
   because Binance fires it every 1 s per symbol; depth every 100-500 ms.
3. If nothing is dead or zombie, continue.
4. Otherwise copy the symbol list under a mutex (`:215-219`) and run `do_reconnect` for each affected socket
   **sequentially** (`:243-266`).
5. `do_reconnect`: loop up to 60 attempts; each attempt calls `reconnectMarket`/`reconnectPublic`, catches
   exceptions, logs a warning and **sleeps a fixed 1 s** (`:221-241`). No exponential backoff, no jitter, no
   cap per time window. After 60 failed attempts it logs an error and the next 2 s tick starts over, so the
   effective retry is endless.
6. `reconnectMarket` (`BinancePublicStream.cpp:266-285`): stop old socket (close frame, join thread), reset
   stamps, create a new `WebSocket` (connect), send the batched subscription, set `lastReconnect`, increment the
   reconnect metric. It throws nothing on a connect timeout, so the attempt loop counts it as success; the
   **zombie rule is what retries it** 5 s later. Observed cycle in the 2026-06-11 incident: about 16 s
   (10 s connect wait + 5 s zombie threshold + tick), three cycles in two minutes
   (`PHASE21-collector-reliability.md`, P2 table).
7. **Periodic proactive reconnect** at 02:00 and 12:00 UTC, only in second :30 to :33 of minute 0, once per
   encoded hhmm (`:23-45`, `:164-208`). Both sockets are reconnected in parallel threads, 60 attempts, 1 s sleep.
   Reason in comments: quietest windows, away from kline boundaries (the code does not say why twice a day;
   Binance futures sockets are forcibly closed after 24 h, which is a plausible background, not stated in the
   code).

### 1.5 State resync after reconnect
- **Subscriptions:** re-sent from `m_pairs`, an authoritative list kept in the core because stream state may
  be stale (`BinanceCollectorCore.hpp:42-45`).
- **Order books** (not needed by vulture): first depth diff after reconnect gives `pu` mismatch, state becomes
  `NotRequested`, a REST snapshot is fetched, buffered diffs replayed. Epoch counter invalidates in-flight
  snapshots; buffer capped at 10 000 diffs (`MarketState.hpp:147`, `MarketState.cpp:547-585`); first diff after a
  snapshot must straddle `lastUpdateId` (W6 fix); crossed book triggers a resync (`:595-602`).
- **Trades and candles:** nothing at runtime. Holes are repaired afterwards from REST (`gap_filler`,
  `trade_repair`); an auto-backfill of `[t_stop, t_ready]` is planned (W2/W4) but not implemented.
- **Backpressure coupling:** the Kafka producer's bounded retry (3 s) is statically asserted to be below the 5 s
  silence threshold, because a blocked read thread stops updating activity and would trigger a false reconnect
  (`CollectorTiming.hpp:18-23`).

### 1.6 What the field shows
`PHASE21` P2 table: eight WS incidents in three weeks (1-6 minutes); a 6-minute zombie on 2026-06-08 led to the
"6 s detector" fix (the 5 s constants); on 2026-06-11 TCP connects hung to the 10 s wait while REST kept working.
`STANDBY_FAILOVER.md` plans a 1 s silence threshold and probes; the code has 5 s; a numeric limit for
"reconnect does not help, escalate" is explicitly undecided there.

## 2. What to carry over, adapt, drop

### Carry over (idea and parameters)
- Two-level detection: *silence* (no frame at all) plus *zombie* (reconnected, but the expected data stream
  never started). This matches the S-1 failure exactly: the old Binance route `/ws/!forceOrder@arr` connects,
  subscribes, and stays mute; only a companion stream that always ticks (`!markPrice@arr@1s`) exposes it.
- Stamps reset on every reconnect, and a "last reconnect" time used by the zombie rule.
- The symbol list is held outside the connection object and used for re-subscription in one batched frame.
- Strand-serialised outgoing queue; clear it on write error.
- Proactive reconnect before the exchange's own forced disconnect (Binance 24 h), at a quiet moment.
- Timing contract: sink deadline < silence threshold, enforced at compile time or startup.
- Reconnect counter metric per connection; log of silence length.

### Adapt for Rust/tokio
- One task per connection owning a state machine `Connecting -> Subscribing -> Live -> Backoff`; `tokio::select!`
  over read, write queue, ping timer, silence timer, shutdown. Timers are deadlines on `tokio::time::Instant`
  (monotonic), not a polling thread; tests use `tokio::time::pause`.
- Read loop never blocks on the sink: forward to a bounded channel; when full apply the slow-consumer policy
  (drop with a counter or disconnect); pongs must be written even when the sink is slow.
- Emit `Gap{exchange, from_ts, to_ts}` (cran has no equivalent; it repairs by REST after the fact). Define
  `from_ts` = exchange or receive time of the last accepted event on that connection before loss; `to_ts` =
  receive time of the first accepted data event after resubscription. Emitted per affected connection and
  covering all symbols on it. For Binance forceOrder there is no REST history, so a Gap is the only record.
- Make the connect step fail loudly: a connect timeout returns an error (cran swallows it and relies on the
  zombie rule).
- Per-exchange liveness stream and ping mechanism (see section 3), because one 5 s rule fits only Binance.
- Handshake-level errors (HTTP 403/429/418, Bybit "403 wait 10 min", Deribit HTTP 429 over 32 connections) get
  their own backoff class with `Retry-After` when present.
- Bound reconnect frequency per exchange against documented limits (OKX 3 connects/s per IP, Bybit 500 per
  5 min, Bitget 300 per 5 min, BitMEX 60 per hour, Deribit 32 open per IP, Hyperliquid 30 per minute).
- Planned reconnect: make-before-break (open the second socket, subscribe, wait for first data, close the old
  one); deduplicate the overlap. Binance `forceOrder` has no event id, so use the key
  `(symbol, T, side, ap, z)`.

### Do not carry over
- Constant 1 s retry without jitter and the 60-attempt counter (a flapping exchange gets hammered; BitMEX's
  60 connections per hour would be exhausted in a minute; a restart of many connectors synchronizes).
- Treating "connect timeout" as a successful reconnect.
- Counting subscription acks and pongs as data activity for the zombie decision (keep them for transport
  liveness only).
- Synchronous `ws.close()` in `Session::stop()` with no timeout (`Session.cpp:62-75`): on a half-open TCP
  connection this may block the io thread until the OS gives up. This is a hypothesis from reading, not
  reproduced; in tokio use `timeout(close_handshake)` then drop.
- Hard-coded UTC hours 02:00/12:00 and the 24 h assumption: make it a per-exchange `max_connection_age`
  (Binance 24 h) with jittered scheduling.
- Two sockets per exchange sharing one watchdog thread, global `isRunning` flag, order-book resync, Kafka
  coupling.
- Fixed silence threshold 5 s for every stream (a 5 s mute is normal for liquidations of one exchange).

## 3. Proposed defaults for vulture

All values are per connection and configurable; "liveness stream" is the always-ticking subscription added to
every connection purely for detection (it may also carry mark price).

| Parameter | Default | Basis |
|---|---|---|
| Connect timeout (DNS + TCP + TLS + WS upgrade, total) | 10 s | cran waits 10 s overall (`WebSoket.cpp:48`) although stages allow 30 s |
| Subscribe-ack timeout | 10 s | zombie threshold 5 s plus slack; no ack = failed attempt |
| Transport idle timeout (no frame of any kind) | `ping_interval + pong_timeout + 5 s` (about 30 s with defaults) | per-exchange, below OKX 30 s data requirement |
| Data silence on liveness stream | Binance 10 s (`!markPrice@arr@1s`), Bybit 10 s (`tickers`, 100 ms), Deribit 10 s (`ticker.*.100ms`), OKX 30 s (`mark-price` keepalive up to 10 s), BitMEX 15 s (`instrument`, 5 s timer) | cran 5 s for a 1 Hz feed; 10 s = 10 missed ticks, avoids false reconnects on GC pauses, cran already had storm risk |
| Zombie rule | no data event of the liveness stream within `data_silence` after subscribe ack; counted from reconnect | cran `BinanceCollectorCore.cpp:157-160` |
| Watchdog tick | none; per-connection deadline timers | tokio |
| Client ping | Bybit `{"op":"ping"}` every 20 s; OKX text `ping` after 20 s idle (< 30 s limit); Bitget text `ping` every 30 s; Deribit `public/set_heartbeat(30)` and answer `test_request`; BitMEX text `ping` after 10 s idle (NOT CONFIRMED, see exchanges.md); Binance none (server ping every 3 min, auto-pong) | exchanges.md section 3 |
| Pong timeout | 10 s (OKX 5 s) | reconnect when exceeded |
| Backoff | exponential x2, min 1 s, max 60 s, "equal jitter" (`delay = d/2 + rand(0, d/2)`); first retry after a clean close immediately | AWS-style; cran had constant 1 s |
| Backoff reset | after the connection stayed `Live` and received liveness data for 60 s | prevents flapping reset |
| Handshake 403/429/418 | at least 60 s, honor `Retry-After`; Bybit 403: 10 min; cap 15 min | Bybit rate-limit page, Binance 418 |
| Connection budget | token bucket per exchange from section 3 limits, never exceed 50% | BitMEX 60/h is the tightest |
| Max connection age | Binance 23 h 30 min +/- 10 min jitter, make-before-break; others none | Binance 24 h rule (background, not read from the repo) |
| Escalation | after 5 consecutive failed attempts or 120 s of Gap: connector status `Degraded` and alert; never give up | STANDBY_FAILOVER leaves the number open |
| Reader to sink channel | bounded (default 10 000 events); on full: drop-oldest per symbol with counter for OI, **disconnect that client** for the gRPC path, never block the read loop | README: slow clients disconnected |
| Sink deadline | must be < data silence / 2 | CollectorTiming contract |
| Close handshake timeout | 3 s | tokio |

Notes: (1) OKX closes after 30 s of no data on the connection, so the liveness stream or the text `ping` must
produce traffic; (2) Bybit tickers deltas mean any `tickers` frame counts as liveness even when `openInterest`
is absent; (3) BitMEX `instrument` partial for all symbols is large: subscribe per symbol.

## 4. Test scenarios (mock WebSocket server, no real exchange)

1. **Hung socket**: server completes the upgrade, sends data, then stops sending and does not read (or the test
   freezes the server task). Expect: data-silence fires at the configured time, reconnect, `Gap` with
   `from_ts` = last event and `to_ts` = first event after resubscribe, symbols re-subscribed exactly once.
2. **Connect black hole**: TCP accepted, TLS never answers. Expect: connect timeout 10 s, error (not success),
   backoff steps 1, 2, 4, ... visible, jitter within bounds.
3. **Abrupt close**: TCP RST; close frame 1001/1006; OKX `notice` 64008; clean close 1000. Expect: reconnect
   (immediate for planned notices), no Gap for make-before-break notices where overlap exists.
4. **Mute after subscribe** (the S-1 case): ack arrives, liveness stream never ticks. Expect zombie reconnect
   after `data_silence`, even though acks and pongs keep arriving.
5. **Subscribe rejected / partial**: error ack for some symbols. Expect: `Degraded`, no false "healthy", retry
   of failed subset only after backoff.
6. **Ping/pong**: server ignores `ping` (Bybit/OKX/Bitget style); server sends `test_request` (Deribit) and
   expects an answer. Expect reconnect after pong timeout; answer sent.
7. **Slow client**: server emits 10 000 msg/s, sink sleeps. Expect bounded memory, counter of drops, pongs still
   answered, no false silence; for the gRPC path the slow subscriber is disconnected with the documented code.
8. **Flapping**: server accepts and closes immediately 200 times. Expect backoff reaches max, per-exchange
   connection budget respected (assert connects per simulated hour <= limit), reset only after 60 s healthy.
9. **Handshake bans**: 403/429/418 with and without `Retry-After`.
10. **Planned reconnect (max age)**: new socket opened before the old closes, overlap deduplicated by key, no
    Gap, no duplicates in the output stream.
11. **Thundering herd**: 20 connections drop together; reconnect times spread (jitter test with fixed seed).
12. **Clock**: all timers use the monotonic clock; a wall-clock jump does not trigger reconnects (use
    `tokio::time::pause`/`advance` and a fake wall clock for `Gap` timestamps).
13. **Sink stall inside the allowed deadline**: sink blocks for `data_silence/2`; no reconnect. Above it:
    documented behavior (drop/disconnect), never a silent stall.

## 5. Gaps for vulture

1. **No `Gap` concept** in cran; repair is offline REST. vulture needs `Gap` events, their persistence, and
   exclusion in analytics (README already requires it). Open: exact definition when several connections carry
   one exchange (per connection or per exchange union).
2. **Binance forceOrder cannot be backfilled** (no public history), so a Gap is a permanent hole; OI can be
   re-polled over REST right after reconnect (new sample, not history).
3. **Liveness stream per exchange** has to be added to every liquidation connection; cran only did it for
   Binance because trade/kline streams tick. Quiet liquidation-only sockets would otherwise be indistinguishable
   from healthy.
4. **Backoff, jitter, connection budgets and handshake-ban handling** do not exist in cran.
5. **No client-initiated ping** in cran (Binance only); needed for Bybit, OKX, Bitget, Deribit, BitMEX.
6. **Reconnect success must be verified by data**, not by returning from `start()`.
7. **Stale or inconsistent design numbers** to align: `STANDBY_FAILOVER.md` assumes 1 s silence, code uses 5 s,
   `PHASE21` calls the detector "6 s", `WS_SERVICE_MESSAGES.md` claims the OKX ping format is unknown (it is the
   text `ping`; see exchanges.md).
8. **Unverified hypotheses**: blocking synchronous close; reason for the twice-daily periodic reconnect;
   whether Binance futures forced-disconnect age is 24 h in 2026 (S-1 did not run long enough).
9. **Shutdown and partial subscribe** semantics (what happens to symbols whose subscribe was rejected) are
   untested in cran.
