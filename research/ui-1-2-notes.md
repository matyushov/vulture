# UI 1.2 notes (2026-10-29)

Timeframe switch of the chart (1m, 5m, 15m, 1h, 4h, 1d) over `History.Aggregate`, a live bin from `Stream.Subscribe`, and
paged exchange klines.

## Live check of the kline pagination (one pass, public endpoints, no keys, from the developer machine)
About a dozen requests per exchange at most, spaced by 0.5 s; no blocks, no errors, no rate limit answers.

| Exchange | What was checked | Result |
|---|---|---|
| Binance USDT-M `/fapi/v1/klines` | `interval=1m&limit=1500`, then the same with `endTime=<oldest open time of page 1> - 1` | Both pages 200 with 1500 rows, ascending, 60 s steps; page 2 ends exactly one bin before page 1 starts (no gap, no overlap). `endTime` is inclusive on the open time. 1d and 4h rows open at multiples of 86 400 000 / 14 400 000 ms (UTC) |
| Bybit v5 `/v5/market/kline?category=linear` | `interval=1&limit=1000`, then `end=<oldest start of page 1> - 1` | Both pages 200, `retCode 0`, 1000 rows, newest first; page 2's newest is one bin before page 1's oldest. `interval=D` and `240` open at UTC multiples |
| OKX `/api/v5/market/candles` | `bar=1m&limit=300`, then `after=<oldest ts of page 1>`; `limit=301`; `bar=1D`, `1Dutc`, `4H` | Both pages 200, `code "0"`, 300 rows, newest first; `after` returns records strictly earlier (page 2 contiguous). `limit=301` is silently clamped to 300 (no error). **`1D` opens at 16:00 UTC (offset 57 600 000 ms from a UTC day), `1Dutc` at 00:00 UTC**; `4H` is UTC-aligned |

Consequences in the code: page limits 1500 / 1000 / 300 (`klinePageLimit()`), end bounds `endTime = to - 1` (Binance), `end = to - 1`
(Bybit), `after = to` (OKX), and **OKX daily candles use `1Dutc`** instead of the `1D` of the task text, otherwise every daily candle
would land 16 h away from the UTC-aligned bin it belongs to. Bybit `D` and Binance `1d` are already UTC.

## Decisions and why
* **Subscribe before the aggregate request.** An event between "request sent" and "subscription established" would be in neither.
  The cost is one wasted live-only subscription on a server without History (it is dropped and replaced by the replay one).
* **Merging the live bin.** The aggregate answer carries no "as of" marker, so the client uses the moment it sent the request:
  live liquidations with a time at or after it are added to the server's last bin, earlier ones are assumed to be in the answer. A
  rare event between the server's query and the client's request time can be counted twice, or lost, for up to one bin; the refresh
  at the next bin start replaces the numbers with the server's. OI needs no merge (the last sample sets the level).
* **One bin before the range.** The request starts one bin earlier so that the server can measure the first visible bin's OI change.
* **Server bins have no per-side liquidation count** (`METRIC_LIQ_COUNT` is the total), so the tooltip shows `(n events)` for them.
* **Stale chart while loading.** The previous snapshot (same exchange only) stays on screen with "Loading 1h data..." until any panel
  of the new selection has data; a switch to another exchange shows its own loading panels.
* **`History` retry.** A failed aggregate is requested again after 30 s (monotonic timer); the next bin rollover requests it as well.
* **OI Δ5m / Δ15m columns are unchanged.** `oi_movers` returns the top lists (`limit` per side), not a value for every symbol;
  switching would blank most rows. A per-symbol value needs an aggregate per symbol or a server list without a limit.
* **Fallback for an old server.** 1m, 5m, 15m from the bounded replay (at most 6 h: 15m shows 6 h, not 24 h), 1h/4h/1d disabled.

## Findings from the screenshots (light and dark, 1000x640 offscreen grabs of the widget for each timeframe, and the main window)
* All six timeframes draw candles, OI bars and level line, liquidation bars in both themes; hatched partial bins (three bins) are
  visible over all three panels and the "partial data" badge sits in the OI panel's corner; the date/time labels read well
  (`HH:mm` for 1m, `MM-dd HH:mm` for 1h, `MM-dd` for 4h and 1d).
* The OI panel's through-cursor tooltip is a child label (as in 1.1) and covers part of the bars; accepted.
* In the main window the toolbar shows 1m 5m 15m 1h 4h 1d, 5m checked; with the fake server that has no History the three larger
  buttons are disabled (greyed).
* The screenshots are not in the repository (`VULTURE_UI_SHOT_DIR`).

## Performance (Apple silicon, offscreen, release)
* `snapshotAndPaintSpeedForEveryTimeframe`: 120 to 180 bins with 500 live events on top, 20 snapshots in under 1 ms (below the timer
  resolution) and `setSnapshot` + a full replot in 0 to 1 ms for every timeframe.
* `tst_Perf` unchanged: 5 000 events/s paced, GUI timer gaps p99 19 ms; 51 000 events/s unthrottled.
* Aggregate request/answer on the fake server: a few milliseconds locally; not measured against the real server.

## Tests
`tst_Chart` 76 cases, `tst_History` 23 (new), `tst_Smoke` 13, `tst_App` 22; the whole CTest suite (9 tests) is green offscreen, 8 repeats
in a row (without `tst_Perf`), and 3 repeats under 54 busy loops on 18 cores. An ASan build (same sources) is clean for
`tst_Chart`, `tst_History`, `tst_Smoke` with `ASAN_OPTIONS=detect_container_overflow=0`: with the container check on, ASan reports a
`container-overflow` inside protobuf's own `RepeatedField` copy constructor (reading an `AggregateRequest` copy), which is the
well-known mismatch with a library that was not built with the instrumentation; no other report.
Tests wait with `QTRY_*`, never for fixed delays, except the places that assert that nothing more happens (a short `qWait` after
the expected count was reached).

## Open questions for the server
* An "as of" marker in `AggregateResponse` (for example the id or the time of the newest event it includes) would let the client merge
  live events exactly, without relying on its own clock.
* `METRIC_LIQ_COUNT_LONG` / `METRIC_LIQ_COUNT_SHORT` (counts by side) would restore the per-side counts in the tooltip.
* A way to ask for the latest bin without knowing the server's clock (`to_ts = 0` = now exists for the movers only).
* Carried over from UI 1.1: an end-of-replay marker in `Stream.Subscribe`; the server marking replayed events.

## Manual acceptance against the real server
Connect (File > Connect), open the Binance tab, select `BTC-USDT-PERP`.
1. **5m (default)**: 72 bins, 6 hours. Price candles from Binance, OI bars/line and liquidation bars from the server. The title says
   "5m candles, last 6h"; hover the title: "Open interest and liquidations: server (clickhouse+redis)". The last bin (tooltip "current bin,
   partial") grows as live liquidations and OI samples arrive; at the next 5 minute boundary the label "Updating..." flashes once.
2. **1m**: 120 bins, 2 h, `HH:mm` labels. **15m**: 96 bins, 24 h. **1h**: 168 bins, 7 days, `MM-dd HH:mm` labels. **4h**: 180 bins,
   30 days, `MM-dd` labels. **1d**: 90 days (needs 90 days of history on the server; fewer days show empty bins on the left,
   with "no samples" in the tooltip). Use the buttons or Ctrl/Cmd+1 ... +6. The choice is kept after restarting the app.
3. Switch quickly between timeframes and symbols: the previous chart stays until the new data arrives, "Loading 4h data..." shows, and
   only the last choice ends on screen (check the Binance candle count and the OI against the tooltip).
4. **Partial markers**: after a server restart or a connector outage the affected bins are hatched in all three panels and the OI
   panel shows "partial data"; the tooltip of such a bin says "server data partial: gap or hole, sums are lower bounds". Liquidation
   sums there are lower bounds (Binance's `!forceOrder` feed is incomplete by nature and is not flagged).
5. **Candles failing**: block `fapi.binance.com` (hosts file) and reselect: the price panel shows an "approximate" badge, candles come
   from the server's mark prices (30 s samples); the badge tooltip names the error.
6. **Old server**: against a server without History the 1h/4h/1d buttons are greyed with a tooltip, 15m shows 6 hours.
7. Zoom with the wheel, drag, double click resets to the default range; "times in UTC" / View > UTC switch relabels the axis.
