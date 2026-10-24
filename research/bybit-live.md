# Bybit live check (V2a), 2026-10-24

One opt-in run of `cargo test -p vulture-server --test bybit bybit_live -- --ignored --nocapture`: public
read-only market data, no keys, 76 s, two REST calls, two WebSocket connections (the connector with the full
universe and a raw probe socket). No HTTP 403, no region block from the machine used for the run.

## Confirmed

| Question (research/exchanges.md, section 7) | Result |
|---|---|
| Universe | `GET /v5/market/instruments-info?category=linear&limit=500` returned a `nextPageCursor`; the second call with that cursor returned the rest and an empty cursor. 876 tradable linear perpetuals (USDT and USDC) after filtering. |
| Cursor format | Already percent-encoded (`first%3D0GUSDT%26last%3DMETISUSDT`). It must go back unchanged; the connector encodes only a cursor without any `%`. |
| USDC perpetual symbol format | Names are `<prefix><base>PERP`, for example `1000BONKPERP` (baseCoin `1000BONK`, quoteCoin `USDC`) and `1000NEIROCTO` USDC. Base and quote from `baseCoin`/`quoteCoin` give `BONK-USDC-PERP`; splitting the symbol would not work. |
| Unit of `allLiquidation.v` | Base coin (contracts equal coins for linear). Examples: `SOLUSDT v=36.8 p=98.56` (notional 3 627 USD, multiple of qtyStep 0.1), `NEARUSDT v=732.5 p=4.388`, `XRPUSDT v=177.5 p=1.2546`, `PUMPFUNUSDT v=111100 p=0.00412`. Notional `p * v` is in quote currency. 11 liquidations in 75 s over the whole universe; both sides were seen (`Buy` mapped to Long, `Sell` to Short). The meaning of `Buy` (a long was liquidated) is from the documentation; the run cannot prove it independently. |
| Ticker snapshot/delta | 70 985 ticker frames in about 75 s for 876 symbols (about 950 frames/s). Every symbol had `openInterest`, `openInterestValue` and `markPrice` in the merged state (876 symbols sampled, zero samples without a mark price, zero parse errors). Raw delta text was not captured (the probe batch was refused, see below), so "missing field = unchanged" rests on the documentation and the unit tests. |
| Partially invalid subscribe batch | A request holding one valid (`tickers.BTCUSDT`) and one invalid topic (`tickers.NOSUCHCOINXYZUSDT`) is refused **as a whole**: `{"success":false,"ret_msg":"error:handler not found,topic:tickers.NOSUCHCOINXYZUSDT","req_id":"p1","op":"subscribe"}`; no frames of the valid topic arrived. The message names the offending topic, so the connector drops that topic and resends the rest. |
| Subscribe acknowledgement | `{"success":true,"ret_msg":"","conn_id":"...","req_id":"p2","op":"subscribe"}`. |
| Pong | `{"success":true,"ret_msg":"pong","conn_id":"...","req_id":"","op":"ping"}`. |
| Request size | All 1 752 topics of the full universe (about 47 000 characters) went out in a few requests of at most 20 000 characters each; all were acknowledged. |
| Prefixed bases | `1000000BABYDOGE`, `1000BONK` (USDC) resolved by the alias table. Not in the table before this run: `1000000MOG`, `10000NEX`, `10000SATS` (added). The universe list was not saved, so the table may still miss other prefixed names; they stay under their exchange-side name (for example `1000NEIROCTO-USDC-PERP`) instead of being guessed. |

## Not confirmed / unexpected

- The wall clock of the machine jumped by about 66 minutes during the run (liquidation `T` and receive times
  after the jump are 3 960 s later than before; the monotonic test duration was 76 s). The run also shows one
  connection loss with four failed attempts, one recovery and one pair of `Gap` events (streams `liquidations` and
  `open_interest`), and the metrics show all 1 752 topics subscribed again afterwards. The most likely cause is
  the machine sleeping and its network coming back; the logs were not enabled, so this is not proven. It did
  exercise reconnect, resubscription of the whole set and the gap path against the real exchange.
- Raw `snapshot` and `delta` ticker examples and raw `allLiquidation` frames were not saved. The probe socket
  printed nothing for them because its tickers batch was refused on purpose.
- Rates: with `oi_poll_interval_secs = 10` and 876 symbols, 3 883 open interest events were published in the
  run; 2 624 sampler slots were skipped while the connection was down (no live session, no data).
