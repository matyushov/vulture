# vulture-ui

Desktop client for the vulture server (GPLv3, Qt 6 Widgets, grpc++, vendored QCustomPlot). Stage 1: connection and live
views; stage 1.1: resizable columns, OI change columns, the three-panel symbol chart; stage 1.2: the chart's timeframe
switch (1m to 1d) over `History.Aggregate` and exchange klines. Alert settings and administration come in later stages.

## Build and test

See the UI section of the top-level README for the exact commands. The code uses only Qt 6.10 APIs (CI builds Windows
with 6.10 and Linux/macOS with 6.12). Tests (`ctest`, offscreen): `tst_Util`, `tst_Models`, `tst_App`,
`tst_StreamClient` (in-process fake gRPC server on 127.0.0.1, plaintext and TLS), `tst_Columns` (header behavior, OI
delta, replay cursor, sliced ingest), `tst_Chart` (kline parsers from `tests/fixtures/`, binning, price source against a
local fake HTTP server, chart controller, widget), `tst_Smoke` (main window against the fake servers; set
`VULTURE_UI_SHOT_DIR` to also write light/dark screenshots, the chart ones included), `tst_Perf` (throughput). Investigations:
`tst_RateBudget` (token bucket, priorities, 429 and 418, reconciliation with the exchange's counter, fake clock),
`tst_Binance` (parsers on live fixtures, the REST client against a local fake HTTP server, the aggTrade WebSocket against a
local fake WS server), `tst_Features` (robust statistics, baseline, CVD, VWAP, p99, lead / lag z-scores and flags on
constructed data), `tst_Investigation` (the engine against an in-memory exchange: stages, limits, watchlist, restart,
follow-ups), `tst_InvestigationLog` (JSON Lines, rotation, CSV), `tst_InvestigationsUi` (the whole path from a fake server
signal to the tab, the restart and a day later; the budget stress test).
Qt 6.10 compatibility: only APIs that exist in 6.10 are used (checked by review; the Windows CI job builds with 6.10).
The code compiles with `-Wall -Wextra` without warnings in the UI sources; QCustomPlot's own warnings are silenced in its
CMake target only.

## Connecting

File > Connect (Ctrl/Cmd+K) opens the dialog: `host:port`, token (masked, "Show" toggle), TLS (on by default),
optional CA file (PEM) for a private deployment, "Remember the token". Plaintext is accepted only for loopback
hosts. With TLS and no CA file the client trusts the operating system roots (fed to gRPC as PEM). The token is the
content of the server's `token` file.

Status bar: connecting, live (age of the last event), reconnecting (attempt, delay, reason), error. Reconnects use
exponential backoff with jitter (0.5 s up to 30 s) and resume from the last received `Event.id`. `OUT_OF_RANGE`
resubscribes live and adds a warning row; `UNAUTHENTICATED` stops retrying; `RESOURCE_EXHAUSTED` is retried. Server
`Gap` events and client reconnects appear as marker rows in the feed.

## Layout

One tab per exchange (Binance, Bybit, OKX, then others), created when the stream shows the exchange, plus Alerts
and Admin placeholder tabs. Each exchange tab has movable, closable docks: Symbols, Live feed, Chart,
Summary. View > Panels re-opens a closed dock, View > Reset layout restores the defaults.

* Symbols: favorites star, search, favorites-only, sortable columns (latest OI USD, **OI Δ5m**, **OI Δ15m**, mark price,
  session liquidation count and USD, last event time). Selecting a symbol filters the feed and shows its chart.
  Rankings are not computed in the client; they need `History.Aggregate`.
* Columns of every table (symbols, feed): all sections are resizable by the mouse (including the first and the last),
  movable, double click on a border fits to content, header context menu shows/hides columns and resets them. Widths,
  order and visibility are saved per exchange in the settings (`tables/...`) and restored at start. A horizontal scroll bar
  appears when the columns do not fit.
* OI Δ5m / Δ15m (INTERIM, measured by the client until `History.Aggregate` exists): the change of `oi_base` over the window
  in percent, with the base-unit and USD change in the tooltip; sign and color (blue up, violet down), an em dash when unknown.
  A per-symbol buffer of about 21 minutes of OI snapshots (<= 80 samples, spacing >= 10 s) is kept; the reference is the
  sample nearest to `newest - window` within window/8 (15 s .. 300 s), like `Series::change` on the server. A window that
  spans a Gap of the exchange (any stream but `liquidations`, or a server-wide one) or a hole longer than 120 s shows
  an em dash; so does a symbol whose newest sample is more than 120 s behind the exchange's stream.
* Bounded replay on connect: the first subscription of a session uses `resume_from = "<now - 20 min>-0"` (the id is
  `<unix ms>-<seq>`, the cursor is exclusive), so the OI columns fill right after connecting. Reconnects resume from the
  last received id. If the server holds less history than that (OUT_OF_RANGE on this first cursor) the client asks for
  `"0"` (everything it has) and says so in the feed without a warning. Replayed events are marked: they do not notify, are
  not highlighted, do not count in the session counters; replayed OI snapshots only feed the symbol list. The replay
  (tens of thousands of events) is ingested in slices (<= 8 ms per event-loop turn). The client clock should be NTP
  synchronized: events written before the call start minus 2 s (server id time against the local clock) count as replayed.
* Chart dock (the selected symbol): three panels (price candles; OI change per bin as bars with the OI level as a line on the
  right axis; liquidations per bin, USD of liquidated shorts up and longs down) on one time axis, with a **timeframe switch**
  in the dock's header and a through cursor.
  * **Timeframes.** Buttons **1m, 5m, 15m, 1h, 4h, 1d** (checkable, one active; the same choice is in View > Chart timeframe
    with Ctrl/Cmd+1 ... Ctrl/Cmd+6). The choice is global (not per symbol) and saved in the settings (`chart/timeframe`, default
    5m; an unknown saved value reads as 5m). Default ranges (owner's decision of 2026-10-25), ending with the bin that contains
    "now":

    | Timeframe | 1m | 5m | 15m | 1h | 4h | 1d |
    |---|---|---|---|---|---|---|
    | Range | 2 h | 6 h | 24 h | 7 days | 30 days | 90 days |
    | Bins | 120 | 72 | 96 | 168 | 180 | 90 |

    Bins are aligned to UTC for every timeframe (4h and 1d bins start at 00:00 UTC multiples), exactly like the server's
    `Aggregate` bins. Axis labels follow the timeframe: time of day (`HH:mm`) for 1m, 5m, 15m; `MM-dd HH:mm` for 1h; the date
    (`MM-dd`) for 4h and 1d (zoomed in, so that ticks are closer than a day, the date-only format gains the time); the UTC/local
    switch applies to labels and tooltip. The tooltip starts with the bin start (`yyyy-MM-dd HH:mm`, the date only for 1d), the
    zone and the timeframe, then price, OI and liquidations. A zoom belongs to one series: another symbol or timeframe shows
    the full range; double click or "Reset zoom" returns to the default range.
  * **Data sources.**
    * Open interest and liquidations: one `History.Aggregate` request per chart load (group by symbol, filtered to the exchange
      and symbol, the step of the timeframe, the range widened by one bin before it so the first bin has an OI change). Metrics:
      liquidation USD by side and the count, OI level and OI change per bin (base units), mark price OHLC (kept as the price
      fallback). The forming (last) bin stays live from a dedicated `Stream.Subscribe` filtered to the symbol, live only (no
      replay), started **before** the request so nothing falls between them: events whose time is at or after the moment the
      request was sent are added to the server's numbers (liquidation sums, counts; the last OI sample sets the level and the
      change against the bin before), older ones are taken to be in the answer already. When the next bin starts the aggregate
      is requested once more (and the candles once); live events then move the new bin. The client clock should be NTP
      synchronized (the same as for the replay marking). The server counts liquidations without the side, so the tooltip of a
      server bin shows `(n events)` instead of a count per side.
    * Price candles: the exchanges' public REST klines, no keys (Binance USDT-M `1m/5m/15m/1h/4h/1d`, Bybit v5
      `1/5/15/60/240/D`, OKX `1m/5m/15m/1H/4H/1Dutc`: OKX's plain `1D` opens at 16:00 UTC, `1Dutc` at 00:00 UTC like our bins).
      The range is read in **pages** walking back from the newest candle (one page is at most 1500 candles on Binance, 1000 on
      Bybit, 300 on OKX; the default ranges fit one page, a free range may need several), requests to one exchange are spaced
      at least 250 ms, an HTTP 429 or 418 fails the request and is never retried, answers are cached per (exchange, raw symbol,
      interval) for 20 s and serve any later request they cover (so switching back and forth does not hit the exchange), and
      prices of multiplier contracts are scaled as in 1.1. The newest candle is the forming one and follows the stream's mark
      price.
    * Fallbacks: if the klines fail, candles are built from the server's mark price OHLC of the bins (an approximation, the
      "approximate" badge with the reason) and, without that, from the marks seen in the stream; with nothing at all the price
      panel shows the error. If the aggregate fails (`UNAVAILABLE`, `OUT_OF_RANGE`, `RESOURCE_EXHAUSTED`, a timeout) the panels
      say so, or, when data is already on screen, a note appears beside it and the request is repeated after 30 s.
  * **An old server (History `UNIMPLEMENTED`).** The chart falls back to the stage 1.1 behavior: a bounded stream replay of
    at most six hours for 1m, 5m and 15m (15m shows 6 h instead of 24 h), and the buttons for 1h, 4h and 1d are disabled with
    a tooltip; with 1h/4h/1d selected the OI and liquidation panels say that the server has no History API while the exchange
    candles still show. A new connection forgets this and asks the server again.
  * **Honest states.** "Loading 1h data..." (a label over the chart) while a request is in flight; the previous chart (same
    exchange) stays visible until something of the new one arrives; "Updating..." for the refresh at a bin rollover;
    "No open interest data in the last 6h", errors, "Not connected" notes. A request in flight is cancelled when the symbol or
    timeframe changes (one aggregate request, one candle request and one chart subscription at a time); an answer of an
    earlier request is dropped by its id and never overwrites a newer selection.
  * **`partial` markers.** A bin the server marks `partial` (it overlaps a Gap of a source the metrics need, or lies inside a
    data hole) is hatched across all three panels, the OI panel carries a "partial data" badge, and its tooltip says "server
    data partial": liquidation sums there are lower bounds and the OI change is an estimate. Stream `Gap` events are still
    shaded (solid tint). Bins the server did not return, and OI the server reported as absent, stay empty (no bar), never
    zero.
  * **Free ranges.** The range is a first-class value (`ChartRange {from, to}` in `chart/Timeframe.h`, `ChartSpec::custom`,
    `ChartController::setCustomRange()`): the aggregate request, the kline pages and the model's bins all work for any range
    within the server's limits (a range that ends in the past has no forming bin and no live stream). There is no UI for it
    yet (planned: selection by mouse and "load history for the interval").
  * Server Gap events are shaded across the panels; the OI Δ5m / Δ15m columns of the symbols list are unchanged (still from the
    20 minute replay): `oi_movers` returns only the top lists (`limit` per side), not a value for every symbol, so it cannot
    replace them as they are.
* Live feed: newest first, last 50 000 events kept; filters (kind, side, minimum USD, symbol), search (Ctrl/Cmd+F),
  pause with queued counter (Space), copy, CSV export of the visible rows. UTC/local switch in View. USD sign: a
  liquidated long is a forced sale (minus, down arrow), a liquidated short a forced purchase (plus, up arrow).
* Large liquidations (default 100 000 USD, Preferences) are highlighted; a system notification is shown through
  `QSystemTrayIcon::showMessage`, at most one per 10 s with the rest merged into a digest, no sound.
  Notifications need an app environment that allows them (an unbundled binary may be silenced by macOS).

## Investigations (UI 1.3)

An **Investigations** tab (not per exchange) holds the table of investigations, the watchlist and, for the selected row, the
detail view. The idea: the server's Binance polling budget is full, so the deep look at one instrument is done **by the client,
from the client's own IP, only when a signal fires**, and the numbers say whether the instrument deserves attention. Everything is
descriptive (z-scores against a 30-day norm, plain values, a short list of flags); there is no score, no verdict, no "insider"
label.

* **Triggers.** A server `Signal` of type `oi_spike_up` / `oi_spike_down` on a Binance symbol starts an investigation (Preferences >
  Investigations: automatic start on or off, default on). **Investigate** in the context menu of a symbol row or of the chart, or the
  button in the tab (a name such as `BTCUSDT` or `BTC-USDT-PERP`), starts one by hand. Limits (settings): at most **6 automatic
  investigations per rolling hour**, at most **2 running at once**; a signal over a limit is listed in the table as **skipped
  (limit)** and written to the file (nothing is silently lost). The contract carries no severity, so "minimum severity" is the
  signal's size relative to its threshold (default 1, every signal). A symbol that is already under investigation or on the
  watchlist is not started again: its TTL is renewed (a counter of merged signals shows on the detail). A manual start waits for a
  free slot instead of being skipped. Replayed signals (the bounded replay on connect) never start anything.
* **Stage 1, context** (about ten requests, all through the shared budget): 1h klines for 30 days with the taker buy volumes
  (the baseline of volume, taker share, hourly range), 1m klines for 2 hours, `openInterestHist` 1h for 30 days (the endpoint returns
  500 rows per page and keeps 30 days, so two pages; the UI notes a shorter history) and 5m for a few hours, the funding history
  (10 days), the 24h ticker, the taker buy/sell ratio and the global / top-trader account and position ratios (1h, 3 days), and
  from **our own server** `History.Aggregate` hourly for 30 days (OI level and change, liquidated USD by side): that part costs the
  exchange nothing, supplies the liquidation baseline and is cross-checked against the exchange's OI history (the number of matching
  hours and the median difference). Optional requests may fail (a warning shows); a failure of the hourly or minute klines fails the
  investigation. Baseline = median and robust sigma (1.4826 x MAD; with MAD 0 the mean absolute deviation, with no spread no
  z-score; fewer than 5 values no sigma) per hour; windows shorter than an hour are scaled with the square root of their length
  (an approximation).
* **Stage 2, trades.** `aggTrades` are backfilled forward from **30 minutes before the OI jump began** (the signal fires at the end
  of its window, so the jump start is `ts - window_secs`) by `fromId` pages of 1000 (20 weight each, at most 40 pages by default),
  then the symbol joins the live `<symbol>@aggTrade` stream (combined stream, **at most 5 symbols**, subscribe / unsubscribe
  without reconnecting, reconnect with backoff, a ping every 20 s, renewal before the 24 h limit). A hole between the stored trades and the live feed (a reconnect, or the
  first live trade after the backfill) is closed from REST by `fromId`; ids make duplicates impossible. A backfill that hits the page cap says so on
  the detail (a gap note). Per minute bar and for the sliding windows 1, 5, 15 and 30 minutes the tab shows trades, buy and sell
  volume, delta, buy share, average and largest trade, trades above p99, VWAP, price move, the volume z-score and the OI change
  per unit of volume (OI from the server's stream plus the exchange's 5 minute history). The **lead / lag** rows show the same for
  the 10 and 30 minutes before the jump. "Large" means above the 99th percentile of the notional of the backfilled trades (a
  limitation: it is estimated from the same window, so it is unreliable under 200 trades and the share above it is about 1 percent by
  construction; the tab says so).
* **Flags** (examples with their wording): `volume 6.2 sigma above the 30-day hourly norm (last 15 min)`, `taker buy share 78 percent
  (last 15 min, 400 trades)`, `OI +1.30 percent in 2 minutes with price move +0.10 percent`, `3 trades above p99 (5000 USDT) in
  the 10 minutes before the jump`, `volume 3.1 sigma above the norm in the 30 minutes before the jump`, `last funding rate +0.12
  percent`. The thresholds are in `investigation/Flags.h` (`FlagRules`) and are starting points to calibrate against the outcomes.
* **Watchlist.** An investigated symbol goes on the watchlist (TTL default 2 hours, at most 5 symbols; Preferences): live trades and
  the rolling features keep updating (every 2 s). When it is full, the unpinned symbol with the **fewest flags** is dropped (a tie
  drops the newest; the newcomer is a candidate too; with everything pinned the newcomer does not get in) and shown as dropped. A new signal
  or **Renew** restarts the TTL, **Pin** removes the expiry, **Remove** ends it. Cancel / remove also stops a running stage.
  Closing the application saves the state; the next start restores the table and the watchlist (stage 1 does not run again, a
  35 minute backfill rebuilds the features). An investigation that was running at the close is marked failed (interrupted).
* **The file.** `investigations.jsonl` in the application data directory (macOS `~/Library/Application Support/vulture/vulture-ui/`,
  Linux `~/.local/share/vulture/vulture-ui/`, Windows `%LOCALAPPDATA%/vulture/vulture-ui/`; `data/dir` in the settings overrides;
  "Open data folder" in the tab) is JSON Lines, one object per line, appended at every change of an investigation (start, stage
  results, the snapshots at the end of the first 5, 15 and 30 minutes, every filled follow-up, the end); **the last line of an id
  is its state**. A record holds the trigger (signal id, type, time, symbol, percent and base change), the baseline summary, the
  context, the features, the flags and the follow-ups: price (the open of the 1m candle at the horizon) and OI (the 5 minute
  history point at or before it) at **+5 min, +15 min, +1 h, +4 h, +24 h** after the trigger, with the price and OI at the trigger
  (the open of the 1m candle containing it, the OI history point at or before it). They are filled when their time comes, and after a
  restart the missing ones are filled from the klines history; a request that fails is retried (1 min, 2 min ... up to five
  tries, then the outcome is recorded with the reason); `"complete": true` when the investigation is over and all five outcomes are
  recorded. The size is bounded (Preferences: 2000 records and 20 MB by default): past the limit the file is compacted to one line
  per id (the newest records and every unfinished one) and, if it is still too big, rotated to `investigations.jsonl.1` with the
  unfinished records carried over. **Export CSV...** writes both files, one row per investigation (flags, z-scores, outcomes as
  percent changes from the trigger price and OI).
* **The request budget.** One limiter (`RateBudget`) serves every REST call of the program to an exchange, the chart's Binance klines
  included (user priority). Binance USDT-M allows 2400 weight per minute per IP; the program uses at most the configured share
  (default **40 percent, 960 per minute**) in any rolling minute: a token bucket (burst of a quarter of the share) plus a rolling ledger.
  The limits that are not weight are separate buckets with the same share: `/futures/data/*` (1000 requests per 5 minutes) and
  `fundingRate` (500 per 5 minutes). Queue: user (the chart, a manual start) before active investigations before background work (follow-ups,
  restore); at most 4 requests at once. The exchange's own counter (`X-MBX-USED-WEIGHT-1m`) is reconciled: what it shows above our own
  spending is booked as spent (somebody else uses the address), and a counter at 90 percent of the limit pauses until the next minute. HTTP
  **429**: all calls to the exchange wait for `Retry-After` (60 s without it). HTTP **418** (IP ban): nothing is sent until the end of the ban
  (from the body, else `Retry-After`, else 5 minutes), never earlier. The weights are in one table, `investigation/BinanceApi.h`.
  The bottom line of the tab and the status bar show the budget (percent of our share, the exchange's counter, queued requests) and
  any pause or ban with its end.
* **Headless core.** All the logic is in the static library `vulture-investigation` (`src/investigation/`): Qt Core, Network and
  WebSockets only, no Widgets, no Gui, no gRPC. The exchange is behind `ExchangeMarketData` (Binance USDT-M now; Bybit and OKX
  later with the same calls), the server's history behind `ServerHistory` (the application adapts `History.Aggregate`), the
  clock is injected. The Widgets part is a thin layer: `InvestigationService` (wiring), `InvestigationsTab`, the model and the chart.
* **The detail chart** reuses the symbol chart's view: price candles from the trades with the signal marked (dashed lines at the
  signal and at the start of the jump), the CVD (bars: delta per minute, line: cumulative), and the aggressive buy volume up and
  sell volume down, with the through-cursor and the same colors (`semanticColors`).
* **Notifications.** A system notification when an investigation finishes stage 2 with at least one flag: on for automatic
  investigations, off for manual ones (Preferences).

Preferences > Investigations: automatic start, minimum severity, per-hour and running limits, TTL, watchlist size, budget share,
trade backfill page limit, the two notification switches, records and size kept. Settings keys: `investigations/*` (see
`app/Settings.cpp`).

Not in this version (by design): any score or automatic keep / drop decision, sentiment, Bybit and OKX data sources, order-book
data, a database (persistence is the JSON Lines file only), alerts to Telegram (the server owns those).

## Settings

INI file in the user profile (macOS: `~/Library/Preferences/vulture/vulture-ui.ini`); `--settings-file <path>`
overrides it. It is created and kept with mode 0600 where the platform supports it (not on Windows). It holds the
server address, TLS flag, CA path, the token (only if "Remember" is ticked, in plain text), window geometry, dock
layouts, UTC switch, the chart timeframe, favorites, threshold and notification flag.

## Colors

All colors come from one function, `semanticColors(QPalette)` in `src/util/SemanticColors.cpp` (struct in the header); change the
look there. They derive from the palette (hue fixed, lightness chosen against the base color), so light and dark system
themes work. Meaning: liquidated long = forced sale = orange-red (down); liquidated short = forced purchase = teal-green
(up); price candles up/down use the same two colors; OI change positive = blue, negative = violet, OI level = text color;
warning/gap/large-liquidation = amber tints. Colors never carry meaning alone: signs, arrows in tooltips and headers, bar
direction. The unit test checks contrast against the base color in both themes.

## Performance (local, fake server, offscreen, Apple silicon)

See `research/ui-stage1-notes.md` and `research/ui-1-1-notes.md`.

## Limitations

Investigations exist for Binance only; prices and volumes there are in the exchange's units (a `1000PEPEUSDT` price is per 1000 coins, not
scaled); the trades of a very busy symbol can take more pages than the backfill cap, which the detail says; no alerts or admin yet; no free interval selection (the data path supports it, the UI does not); one server at a time; the
chart needs direct access to the exchange REST endpoints (otherwise the approximate fallback is shown); the server counts
liquidations without the side, so server bins have no per-side counts; the chart's live bin and the server's last aggregate
can double count an event for a moment around the request (corrected by the next refresh); color scheme follows the system
(Qt color scheme API; on the offscreen plugin it is ignored).

Settings from stage 1: the saved dock layouts are ignored once (key `layout_v2`), because the chart dock needs more room.
