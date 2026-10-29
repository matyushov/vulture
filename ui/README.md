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
`VULTURE_UI_SHOT_DIR` to also write light/dark screenshots, the chart ones included), `tst_Perf` (throughput).
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

No alerts or admin yet; no free interval selection (the data path supports it, the UI does not); one server at a time; the
chart needs direct access to the exchange REST endpoints (otherwise the approximate fallback is shown); the server counts
liquidations without the side, so server bins have no per-side counts; the chart's live bin and the server's last aggregate
can double count an event for a moment around the request (corrected by the next refresh); color scheme follows the system
(Qt color scheme API; on the offscreen plugin it is ignored).

Settings from stage 1: the saved dock layouts are ignored once (key `layout_v2`), because the chart dock needs more room.
