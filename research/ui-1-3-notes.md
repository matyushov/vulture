# UI 1.3 notes (2026-10-29)

Investigations of an OI spike on the client (Binance USDT-M first): the context and trades of one instrument are read from the
exchange's public endpoints with the client's own IP, only when a signal fires or by hand. This file records the live checks, the
decisions and the findings. Usage and limits are in `ui/README.md`.

## Live check (2026-10-29, from the developer machine, public endpoints, no keys, one pass)

About 25 requests in total, spaced by at least 0.7 s, symbol `BTCUSDT`; no 429, no 418, no block. The weights were read from the
`X-MBX-USED-WEIGHT-1m` response header (differences of consecutive answers, at the start of a minute where possible).

| Endpoint | Checked | Result |
|---|---|---|
| `GET /fapi/v1/klines` | `interval=1h&limit=720`, `1m&limit=120`, `1m&limit=1500`, `limit=100/500/1000` | 12 columns as documented; taker buy base / quote volume at indexes 9 and 10. Weight **5** for limit 720 (documented 5 for 500 to 1000), 2 for 120, 10 for 1500. The counter showed 6 for one limit=100 call and 7 for one limit=120 call once (the documented 2); a counter that runs ahead of our own bookkeeping is what the reconciliation is for |
| `GET /fapi/v1/klines` with `startTime` | `1m&startTime=<minute>&limit=1` | the exact candle: `openTime == startTime` |
| `GET /fapi/v1/ticker/24hr?symbol=` | one symbol | one object (`lastPrice`, `priceChangePercent`, `volume`, `quoteVolume`, `count`, ...). Weight **1** in one measurement, 3 in another (the documentation says 1); the budget table keeps 1 |
| `GET /fapi/v1/aggTrades` | `limit=1000`; `startTime`; `fromId` | weight **20** (two measurements). Rows `a, p, q, nq, f, l, T, m`; `nq` is new (quantity without RPI trades) and ignored. `fromId = last + 1` continues without a gap or an overlap. `startTime` returns the first trades at or after it |
| `GET /futures/data/openInterestHist` | `period=1h&limit=500`; `endTime=<oldest> - 1`; `period=5m&limit=1&endTime` | **no `X-MBX-USED-WEIGHT` header** (weight 0, own limit 1000 requests / 5 min / IP). The latest 500 hourly rows, then 220 older ones: **720 hours = 30 days in all, two pages**. `limit=1&endTime` gives the latest point at or before `endTime`. **A `startTime` at the edge of the 30 days is refused: HTTP 400, code -1130 "parameter 'startTime' is invalid"** (a startTime of 29 days works). The client therefore never sends `startTime` to this endpoint |
| `GET /futures/data/takerlongshortRatio` | `period=1h&limit=72` | rows `buySellRatio, buyVol, sellVol, timestamp`; no weight header |
| `GET /futures/data/globalLongShortAccountRatio`, `topLongShortAccountRatio`, `topLongShortPositionRatio` | `period=1h&limit=72` | rows `longShortRatio, longAccount, shortAccount, timestamp`; no weight header |
| `GET /fapi/v1/fundingRate` | `limit=30` | rows `fundingTime, fundingRate, markPrice`; no weight header (documented: 500 requests / 5 min / IP, shared with `fundingInfo`) |
| WebSocket `wss://fstream.binance.com/market/stream` | combined stream with the streams in the URL, then SUBSCRIBE of two symbols; the same for `/public/stream` and the legacy `/stream` | **aggTrade arrives on `/market/stream`** (URL streams and `SUBSCRIBE` both work, the ack is `{"result":null,"id":1}`). On `/public/stream` and the legacy `/stream` the connection opens and **no aggTrade arrives within 8 s**: the documentation's "Important WebSocket Change Notice" moves aggTrade to `/market` and retires the legacy URLs (2026-04-23). Messages have the new fields `nq` and `st`, ignored |

Documentation read (developers.binance.com, USDS-M futures): 2400 REQUEST_WEIGHT per minute (`exchangeInfo`), 429 then an automatic
418 ban from 2 minutes to 3 days, the header `X-MBX-USED-WEIGHT-(intervalNum)(intervalLetter)`, aggTrades weight 20 with 1000 rows and the
48 hour limit, `startTime`/`endTime` span under one hour (the client sends `fromId` or `startTime`, never both), kline weights by limit,
`futures/data` 1000 requests / 5 min, funding 500 / 5 min. The pages for the 24h ticker and the taker ratio were not reachable as text;
their weights come from the live check. **Not verified live** (a ban cannot be provoked on purpose): the 418 body. The fixture
`binance_fut_418_body.json` is written from the documented message form ("... banned until <unix ms>"); the parser also accepts a
10-digit (seconds) value and falls back to `Retry-After`, then to five minutes.

Fixtures made from the live answers (trimmed to a few rows) are in `ui/tests/fixtures/binance_fut_*.json`.

## Decisions and why

* **One budget for everything.** `RateBudget` serves the investigations and the chart's Binance klines (the chart is "user"
  priority). A token bucket (burst of a quarter of the share) alone allows a burst plus a full minute of refill inside one minute,
  i.e. up to twice the share; a rolling ledger caps every window at the share exactly. The ledger also takes the exchange's counter:
  what `X-MBX-USED-WEIGHT-1m` shows above our own spending is booked as spent.
* **The three limits.** `/futures/data` (count, 1000 per 5 min) and `fundingRate` (count, 500 per 5 min) are separate buckets with the
  same 40 percent share; weight 0 for them.
* **Stage 2 starts at the jump, not at the signal.** The signal fires at the end of its observation window, so `ts - window_secs`
  is where the OI move began; the lead / lag view needs the 30 minutes before that. The backfill starts 30 minutes before the
  jump start.
* **No gap by construction.** Backfill, a reconnect and the first live trade after the backfill all use the same catch-up: live trades
  are buffered while REST pages by `fromId` run until they reach the first buffered id (or a short page), then the buffer is merged; ids
  deduplicate.
* **`p99` is estimated from the backfill window itself** (the spec asks for an estimate from the backfill). Consequence: at most about
  1 percent of the trades can be above it, so "3 trades above p99 in the 10 minutes before the jump" needs a few hundred trades in
  that window; the detail says when the estimate is unreliable (under 200 trades). A longer baseline of trades would cost
  20 weight per 1000 trades.
* **Price and OI at the trigger and at the horizons** are the open of the 1m candle and the 5 minute OI history point at or before the
  moment, so the changes are consistent. Prices are in the exchange's units (`1000PEPEUSDT` is per 1000 coins).
* **The log is append only**, the last line of an id is its state, compaction keeps one line per id. No SQLite, no database.
* **The watchlist is the log.** The state of a watched investigation (TTL end, pin) is a record like any other; a start reads them back.
* **Severity.** The `Signal` message has no severity; the minimum is `|change_pct| / threshold` (default 1: every signal).
* **Qt WebSockets.** It is an add-on module; CI installs it (`modules: qtwebsockets` in `ui.yml`). On the developer machine it was added
  to a separate prefix with aqtinstall (`aqt install-qt mac desktop 6.12.0 clang_64 -m qtwebsockets`) and `-DQt6WebSockets_DIR=...`.

## What the tests cover

`tst_RateBudget` (17), `tst_Binance` (24), `tst_Features` (23), `tst_Investigation` (23), `tst_InvestigationLog` (8),
`tst_InvestigationsUi` (7), all offscreen, local servers only. The stress test runs 12 simultaneous signals against a fake Binance
with a clock 60 times faster than the wall clock: 252 requests, 2496 weight in about 2.6 budget minutes; the ledger peaked at exactly
the share (960) and the arrival times at the fake server never showed more than 980 in a rolling minute (the margin is network latency
at 60x speed). The ten suites of the earlier stages are unchanged except for the tab count in `tst_Smoke`.

Stability: the whole suite (without `tst_Perf`) three times in a row and twice more under 36 busy loops on 18 cores: green.

## Screenshots (light and dark, `VULTURE_UI_SHOT_DIR`, not in the repository)

* The tab: table, watchlist, detail with the three-panel chart (price with two dashed markers, CVD bars and line, buy / sell
  volume), flags, rolling features, baseline and context, outcomes. Both themes read well; colors come from `semanticColors()`.
* Found and fixed: floating point noise (1e-13) in the CVD panel set the axis scale to "3e-13"; it is cleaned to zero. The fake
  market produced negative prices in the past (a test artefact), fixed in the test.
* Accepted: the through-cursor tooltip covers part of the price panel (as in 1.1), the first and the last panel titles sit on the
  bars (as in 1.1), the left table needs a horizontal scroll bar for the last columns at the default width.

## Open questions for the server

* A `severity` field (or a score) on `Signal` would replace the threshold ratio.
* `symbol_raw` on `Signal`: the client learns the exchange name from earlier OI and liquidation events, or derives `BTCUSDT` from the
  normalized name (wrong for `1000PEPEUSDT`-style names until the first event of that symbol).
* The server's hourly `Aggregate` is the only server call; it is one request per investigation.
