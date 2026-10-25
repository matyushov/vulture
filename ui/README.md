# vulture-ui

Desktop client for the vulture server (GPLv3, Qt 6 Widgets, grpc++, vendored QCustomPlot). Stage 1: connection and live
views; stage 1.1: resizable columns, OI change columns, the three-panel symbol chart. History/Aggregate screens, alert
settings and administration come in later stages.

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
and Admin placeholder tabs. Each exchange tab has movable, closable docks: Symbols, Live feed, Chart (placeholder),
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
* Chart dock (the selected symbol, one chart stream at a time): three panels over 2 hours with 5 minute bins aligned to UTC
  (the timeframe is a parameter, `ChartSpec` in `chart/Timeframe.h`): price candles, OI change per bin (bars, up/down) with
  the OI level as a line on the right axis, liquidations per bin (USD of liquidated shorts up, longs down). One time axis,
  synchronized zoom/pan on X (wheel, drag, double click or "Reset zoom"), Y scaled independently to the visible bins. A
  through cursor: a vertical line across all panels snapped to bin centers, a horizontal guide in the hovered panel and one
  tooltip with the time and the values of all three panels (arrows and signs, not only colors). Server Gap events are
  shaded across the panels. States are written in the panels ("Loading 2h of history...", "No data...", the error text,
  "Not connected").
  * Candles come from the exchange's public REST klines (no keys; Binance USDT-M `/fapi/v1/klines`, Bybit v5
    `/v5/market/kline?category=linear`, OKX `/api/v5/market/candles`), 24 candles, refreshed at each bin rollover; the
    forming candle follows the stream's mark price. Prices of multiplier contracts are divided and volumes multiplied by the
    multiplier: the leading digits of the raw symbol when they are a power of ten above 1 (`1000PEPEUSDT`, `10000...`) or `1M`
    (a million), only when the normalized base lost the prefix (`PEPE-USDT-PERP`; `1000PEPE-USDT-PERP` after an alias
    collision is unscaled); `1INCH` is not a multiplier; OKX is never scaled. If the request fails, candles are built from
    the mark prices of the stream (30 s samples) and the panel carries an "approximate" badge with the reason.
  * OI and liquidations come from a dedicated `Stream.Subscribe` with the exchange and symbol filter and `resume_from = now - 2h
    - one bin` (one bin more gives the first bin a baseline), cancelled when the symbol changes.
  * Binning: OI change of a bin = last level of the bin minus the last level before it (across empty bins); the first bin
    without an earlier level uses the first sample of the bin, with a single sample it is unknown (no bar).
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
layouts, UTC switch, favorites, threshold and notification flag.

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

No alerts or admin yet; the chart has a fixed 5 minute timeframe and no History data (only what the hot window holds, up
to 2 hours); one server at a time; the chart needs direct access to the exchange REST endpoints (otherwise the approximate
fallback is shown); color scheme follows the system (Qt color scheme API; on the offscreen plugin it is ignored).

Settings from stage 1: the saved dock layouts are ignored once (key `layout_v2`), because the chart dock needs more room.
