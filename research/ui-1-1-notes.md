# UI 1.1 notes (2026-10-25)

## Live check of the public kline endpoints (one request each, no keys, from the developer machine)
| Exchange | Request | Result |
|---|---|---|
| Binance USDT-M | `GET https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT&interval=5m&limit=24` | HTTP 200, 24 rows, ascending; row = `[openTime, "o", "h", "l", "c", "volume", closeTime, "quoteVol", trades, "takerBase", "takerQuote", "0"]`, prices and volume are strings, volume in base coin |
| Bybit v5 | `GET https://api.bybit.com/v5/market/kline?category=linear&symbol=BTCUSDT&interval=5&limit=24` | HTTP 200, `retCode 0`, `result.list` of 24 rows, **newest first**; row = `["startMs", "o", "h", "l", "c", "volume", "turnover"]` (strings) |
| OKX | `GET https://www.okx.com/api/v5/market/candles?instId=BTC-USDT-SWAP&bar=5m&limit=24` | HTTP 200, `code "0"`, `data` of 24 rows, **newest first**; row = `["ts", "o", "h", "l", "c", "vol", "volCcy", "volCcyQuote", "confirm"]`; `vol` is in contracts (0.01 BTC each), `volCcy` is in base coin, the parser uses `volCcy` |

No blocking or errors. The newest row is the forming candle (OKX `confirm = 0`). The first six rows of each response (trimmed)
are in `ui/tests/fixtures/*_live.json`; the `*_doc.json` fixtures follow the documentation examples, the `*_error_*.json` ones
the documented error shapes.

## Findings from the screenshots (light and dark, `QWidget::grab()` of the main window, chart filled by fake servers)
* Light theme: candles, OI bars and liquidation bars readable; dark theme: same hues, lighter; both checked for the presence of all
  six chart colors by pixel in the test.
* Fixed after looking: the gap band was never painted when it sat on a custom layer (now on the overlay layer with a light
  alpha); the OI panel title overlapped the OI bars and the right axis (shorter title, more headroom above the bars);
  Y ranges of the lower panels gave the bars no room above the title.
* The through-cursor tooltip is a child label in the chart widget (palette ToolTip colors), so it also appears in grabs. In the
  light offscreen palette the tooltip background is the style's pale yellow; accepted.
* The screenshots are not in the repository (`VULTURE_UI_SHOT_DIR`).

## Performance (Apple silicon, offscreen, release)
* 2 h of one symbol (240 OI + 400 liquidation events): ingest and 20 snapshots in 0 ms (below the timer resolution).
* Sliced ingest of a 30 000-event batch: 31 slices, longest GUI gap 14 ms (budget 8 ms per turn plus the slice itself).
* Stage 1 numbers still hold: 5 000 events/s paced, GUI gaps p99 19 ms; 48 000 events/s unthrottled.
* The 20 minute replay is tens of thousands of events (about 600 symbols x 3 exchanges x 40 OI samples plus liquidations); not
  measured against the real server.

## CI failures of stage 1 fixed here
1. `tst_StreamClient` SIGABRT/SIGTRAP on loaded machines. Reproduced locally with 12 busy loops (`yes`) in 3 of 4 runs; the macOS
   crash report showed malloc free-list corruption on the main thread, an ASan build (`-fsanitize=address`, same sources)
   named it: `stack-use-after-scope` in `eventsCrossInBatchesNotOneByOne`. The test's `Collector` (declared after the client,
   so destroyed first) was still connected when `~StreamClient()` emitted its "Disconnected" status, which wrote into the dead
   collector. Fixes: the collector disconnects in its destructor (test), and `~StreamClient()` no longer emits signals (product:
   receivers may be gone in any destruction order). 12 ASan runs under load afterwards: clean.
2. `tst_Smoke::dropShowsReconnectAndResumes`: not a product bug. The "Reconnected, resumed after event" marker is pushed to the
   next 33 ms batch while the status indicator turns Live earlier; the test looked for the row at once. It now waits for the row
   (10 s). The "GUI stalls" line is only an info message (max 128 ms on that runner, no assertion).

## Open questions for the server
* `Stream.Subscribe` with a 2 h cursor and a symbol filter scans all entries of the window server-side (filtering by `t/x/s`
  without decoding); acceptable for one chart, but a cheaper path (a per-symbol index or `History.Aggregate`) is worth it.
* There is no "replay finished" marker in the stream; the client treats the first non-replayed event, or 3 s of silence on a
  live stream, as the end of the replay.
* The server could mark replayed events (or send an end-of-replay event) instead of the client comparing the id time with its
  own clock.
* `symbol_raw` is needed for the kline request; it is present in `Liquidation` and `OpenInterest` (used as is).
