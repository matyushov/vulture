# vulture-ui

Desktop client for the vulture server (GPLv3, Qt 6 Widgets, grpc++). Stage 1: connection and live views.
Charts, History/Aggregate screens, alert settings and administration come in later stages.

## Build and test

See the UI section of the top-level README for the exact commands. The code uses only Qt 6.10 APIs (CI builds Windows
with 6.10 and Linux/macOS with 6.12). Tests (`ctest`, offscreen): `tst_Util`, `tst_Models`, `tst_App`,
`tst_StreamClient` (in-process fake gRPC server on 127.0.0.1, plaintext and TLS), `tst_Smoke` (main window against the
fake server; set `VULTURE_UI_SHOT_DIR` to also write light/dark screenshots), `tst_Perf` (throughput measurement).

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

* Symbols: favorites star, search, favorites-only, sortable columns (latest OI USD, mark price, session liquidation
  count and USD, last event time). Selecting a symbol filters the feed; "All symbols" clears it. OI change over a
  window and rankings are not computed in the client; they need `History.Aggregate`.
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

## Performance (local, fake server, offscreen, Apple silicon)

See `research/ui-stage1-notes.md`.

## Limitations

No charts, alerts or admin yet; no replay of the hot window (the first connection is live only); one server at a time;
color scheme follows the system (Qt color scheme API; on the offscreen plugin it is ignored).
