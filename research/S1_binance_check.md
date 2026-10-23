# S-1: Binance USDT-M public data check

Measured 2026-10-23 (REST 17:01-17:20 UTC, WS `!forceOrder@arr` 17:09:44-17:24:36 UTC), from a home
machine in UTC+4. Binance changes limits and routes; these are preliminary numbers for V1-4.
Tools: `research/tools/s1/`. Fixtures: `fixtures/binance/`.

## Method
- Only public GET and WS market data, no keys. Region was not blocked (HTTP 200, no 451).
- `GET /fapi/v1/exchangeInfo`, counted by `contractType` / `quoteAsset` / `status`.
- `GET /fapi/v1/openInterest?symbol=X`: 60 symbols sequentially (5 req/s max) for latency; 560 symbols in one
  sequential paced pass (about 2 req/s); weight read from `X-MBX-USED-WEIGHT-1M`.
- `!forceOrder@arr` recorded for 892 s (14.9 min) on the `/market/ws/` route.

## exchangeInfo
- 931 symbols total, response 1.16 MB. `rateLimits`: REQUEST_WEIGHT 2400/min, ORDERS 1200/min and 300/10s.
- `contractType=PERPETUAL`, `status=TRADING`: **USDT 522, USDC 38** (560 total). Other quotes with perps:
  USD1 2, U 2, BTC 1 (out of scope).
- Not TRADING and must be filtered out: PERPETUAL USDT `SETTLING` 135, `PENDING_TRADING` 1, USDC `SETTLING` 1.
- New `contractType=TRADIFI_PERPETUAL` (USDT 224, USD1 1) exists: tokenized traditional assets. A filter by
  `contractType == "PERPETUAL"` excludes them; whether to include them is a product decision.
  Quarterlies: `CURRENT_QUARTER` 2, `NEXT_QUARTER` 2 (USDT).
- Symbol format: `BASEQUOTE` without separator (`BTCUSDT`, `BTCUSDC`), prefixes like `1000PEPEUSDT`, `1INCHUSDT`.
  **Some symbols are non-ASCII** (e.g. `币安人生USDT`): URL-encode in REST, the connector must treat symbols
  as UTF-8, not `[A-Z0-9]+`. Do not assume any base-asset length.
- Symbol object has `pair`, `baseAsset`, `quoteAsset`, `marginAsset`, `onboardDate`, `liquidationFee`, `filters`.
- `exchangeInfo` weight: header showed 1 after an idle period (the first measurement on a busy minute showed
  noise). Hourly reload is negligible. One call took 0.55 s once and 7.3 s once, so use a long timeout.

## openInterest REST
- Response: `{"symbol":"BTCUSDT","openInterest":"91959.654","time":1792775123713}`. `openInterest` is a decimal
  string in base-asset units (contracts = base coins for linear perps); USD value needs mark price (V1-4 plans
  the `!markPrice@arr@1s` stream). `time` is the exchange timestamp in ms.
- Weight: **1 per request** (header incremented by exactly 1 on consecutive calls: 30 -> 31, 65 -> 66).
- Latency from this machine (includes my RTT to Binance, so the server near the exchange should be better):
  first sample n=60: p50 351 ms, p99 1352 ms, max 2356 ms; second n=60: p50 353 ms, p99 1344 ms;
  paced pass n=560: p50 355 ms, p99 1449 ms. No 429/418 at any time; peak
  `X-MBX-USED-WEIGHT-1M` was 125 (the 560-request pass at about 2 req/s = 5% of the limit).
- Concurrency note: the sequential pass was latency bound (about 0.51 s per request incl. overhead, 288 s for
  560 symbols). A real poller must issue requests concurrently: rate x latency = in flight.

## Polling interval for all symbols (N = 560 USDT+USDC perps, weight 1 each)
Budget: 60% of 2400 = **1440 weight/min**, leaving room for exchangeInfo, retries and anything else on the IP.

| interval | weight/min | share of limit | request spacing | in flight at p50 / p99 latency |
|---|---|---|---|---|
| 15 s | 2240 | 93% | 27 ms | not allowed (over 60%) |
| 23.3 s | 1440 | 60% | 42 ms | minimum allowed |
| **30 s** | **1120** | **47%** | **54 ms (18.7 req/s)** | **about 7 / 26** |
| 60 s | 560 | 23% | 107 ms (9.3 req/s) | about 3 / 13 |

- Minimum interval at <= 60%: `N / 1440 * 60` = 23.3 s for 560 symbols; scales linearly with N
  (USDT only, 522: 21.8 s; 700 symbols: 29 s).
- Recommendation: **a snapshot per symbol every 30 s** (47% of the limit), configurable; 60 s as a safe
  default if the same IP is shared with other consumers. Spread requests evenly (spacing = interval / N,
  e.g. 54 ms) with a token bucket, never as a burst; the measured pass with even spacing never exceeded 125
  weight/min at 2 req/s and produced no throttling.
- The interval must be recomputed from N after each hourly `exchangeInfo` reload; guard: if
  `N / interval * 60 > 1440`, stretch the interval automatically.
- Backoff on 429 (honour `Retry-After`) and 418 (IP ban, longer) is still required; not triggered here, so
  the exact `Retry-After` behaviour is unverified.

## !forceOrder@arr
- **Route finding (important):** `wss://fstream.binance.com/ws/!forceOrder@arr` connects without error but
  delivered **0 messages in 9+ minutes** (also 0 for `btcusdt@aggTrade` over 15 s on the legacy `/ws/` and
  `/stream` routes), while `wss://fstream.binance.com/market/ws/!forceOrder@arr` and
  `.../market/ws/btcusdt@aggTrade` delivered immediately, and `.../public/ws/btcusdt@bookTicker` too.
  Binance now splits routes (`/public`, `/market`, private); liquidation and trade streams are on
  `/market`. A silent connection is a zombie-like failure mode: the connector must alarm on "connected but
  no data", and for `!forceOrder@arr` a quiet market is not a reliable signal, so cross-check against a
  stream that always ticks (e.g. `!markPrice@arr@1s`) on the same connection.
- Volume: **738 events in 892 s** (about 0.83/s, 50/min average); 177 distinct symbols; per minute
  55, 40, 31, 34, 22, 58, 88, 30, 16, 13, 17, 8, 22, 28, 276 (last minute is a burst); max 55 events in one
  second. Top symbols: IRYSUSDT 152, BIRBUSDT 85, JCTUSDT 41. Sum notional about 0.96 M USD, max single
  137 k USD. A calm-to-moderate period; cascade peaks are measured on the stand.
- Frame format: **one JSON object per frame** (not an array, despite `@arr`):

      {"e":"forceOrder","E":1792775340577,"o":{"s":"BIRBUSDT","S":"SELL","o":"LIMIT","f":"IOC",
       "q":"16009","p":"0.0898000","ap":"0.0913215","X":"FILLED","l":"7042","z":"16009",
       "T":1792775339573,"ps":"BIRBUSDT","st":1}}

- Fields of `o`: `s` symbol, `S` side (BUY/SELL: the side of the liquidation order, so SELL = a long was
  liquidated), `o` order type (always LIMIT), `f` time in force (always IOC), `q` original quantity (base
  asset, string), `p` order price, `ap` average fill price, `X` status (always FILLED here), `l` last filled
  quantity, `z` accumulated filled quantity, `T` trade time ms, `ps` (pair/symbol, equals `s` here) and `st`
  (new, always 1 here; meaning undocumented in this sample). Top level: `e` = "forceOrder", `E` event time ms.
- For the normalized event: price = `ap` (actual fill) in preference to `p` (`p != ap` in 696 of 738 events),
  qty = `z` (accumulated), ts = `T`. `l != z` in 468 events (partial fills; `z` is the total of the order
  so far). Because `q` is the original size, use `z` for filled size. Notional = `z * ap`.
- Latency: `recv_ts - E` p50 144 ms (min 143, max 882 ms); `E - T` 16 to 1021 ms.
- Duplicates / completeness: 39 same-symbol pairs less than 1000 ms apart in `T`, so the documented "one
  per symbol per 1000 ms" snapshot rule is not a hard guarantee on the message level; completeness is not
  verifiable from public data (the exchange states only the latest liquidation per 1000 ms is pushed).
  No dedupe key in the message; dedupe by `(s, T, S, z, ap)` if needed.

## Comments for V1-4
1. Use the `/market/ws/` route (verify again at deployment; routes have changed before).
2. Decimal strings everywhere; parse as decimals/double, keep raw for fixtures.
3. Symbols are UTF-8; filter `contractType == PERPETUAL && status == TRADING`, quote in {USDT, USDC}.
4. OI poller: concurrency 8-32 in flight, even spacing, 30 s default, auto-stretch by N.
5. Region: not blocked from this location; the server region must be re-checked (US blocks are expected).
6. Not measured (outside S-1 scope): 24 h stability, 418 behaviour, cascade peaks, WS reconnect limits.

## Files
- `fixtures/binance/force_order_sample.jsonl` - all 738 raw events, each `{recv_ts, msg}` (203 KB).
- `fixtures/binance/open_interest_sample.json` - 5 responses incl. USDC and a non-ASCII symbol.
- `fixtures/binance/exchange_info_sample.json` - header, rateLimits and 9 representative symbols (17 KB).
- Raw data in `research/data/` (git-ignored): full exchangeInfo, logs.
