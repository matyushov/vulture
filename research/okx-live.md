# OKX live check (V2b), 2026-10-24

Two sources: the official documentation (read in full this time) and one opt-in run of
`cargo test -p vulture-server --test okx okx_live -- --ignored --nocapture`: public read-only market data, no keys,
76 s, two REST calls, two WebSocket connections (the connector with the whole universe and a raw probe socket). No
HTTP 403 and no region block from the machine used for the run. The raw frames used as fixtures are in
`fixtures/okx/live_2026-10-24.jsonl`.

## Documentation

The single-page docs (`https://www.okx.com/docs-v5/en/`, 5 MB of HTML) were downloaded whole with `curl` and read as
text; the earlier study had fetched only the first 400 000 characters. The Public Data sections are now read:

| Item | Text of the documentation |
|---|---|
| `liquidation-orders` | "Retrieve the recent liquidation orders. This data doesn't represent the total number of liquidations on OKX." Args `{"channel":"liquidation-orders","instType":"SWAP"}` (also `FUTURES`, `MARGIN`, `OPTION`). Path `/ws/v5/public`. "Liquidation data comes from different data sources, so the updated data is not necessarily in chronological order." |
| fields | `details[].side` order side `buy`/`sell`; `posSide` `long`/`short` (hedge mode) or `net` (net mode); `bkPx` "Liquidation mark price. The price of the transaction with the system's liquidation account"; `sz` "for FUTURES/SWAP the unit is contract"; `bkLoss`; `ts` ms. Documentation example: `posSide=short`, `side=buy`, `sz=13`, `bkPx=0.007831`. |
| `open-interest` | "Data will be pushed every 3 seconds when there are updates." Args `instId` (per instrument). `oi` contracts, `oiCcy` "in currency units, like BTC", `oiUsd` USD, `ts`. |
| `mark-price` | "pushed every 200 ms when the mark price changes, and every 10 seconds when the mark price does not change". Args `instId`. `markPx`, `ts`. |
| Public WebSocket | `wss://ws.okx.com/ws/v5/public`. The changelog (update 2026-10-16) stops port 8443 on 2026-10-31 (public internet) and 2026-11-16 (colo); port 443 already works. |
| Limits | 3 connection requests per second per IP; 480 subscribe/unsubscribe/login requests per hour per connection; one subscribe request at most 64 KB; 30 connections per channel per sub-account. The connection is cut when nothing is pushed for 30 s: send the text `ping` after N < 30 s of silence and expect `pong`. No limit on the number of subscriptions per connection is documented. |
| Instruments | `GET /api/v5/public/instruments?instType=SWAP`, 20 requests per 2 s per IP and instrument type. `ctVal` "Face value of one contract... for linear contracts ctVal is in the base currency (e.g., ctVal=0.01 BTC for BTC-USDT-SWAP)", `ctMult`, `ctValCcy`, `ctType` `linear`/`inverse`, `state` `live`..., `settleCcy`. The `*-USD_UM-SWAP` family (`instFamily` `BTC-USD_UM`) is settled in USD stablecoins. |

## Confirmed by the live run

| Question (research/exchanges.md) | Result |
|---|---|
| Universe | One response (no pagination) held the whole swap list. After filtering (linear, `live`, USDT/USDC, no `USD_UM`) **507** instruments, all USDT-quoted. Every one had a usable `ctVal`/`ctMult`/`ctValCcy` as far as the 507 samples show (the dropped count was not exported in this run; the gauge `vulture_unusable_instruments` now is). |
| Contract size and `oiCcy` | REST `open-interest` for `BTC-USDT-SWAP`: `oi=3037369.61`, `oiCcy=30373.6961`, i.e. 0.01 BTC per contract, equal to `ctVal x ctMult`. Over the 507 instruments the sampler's cross-check `oiCcy` against `oi x ctVal x ctMult` found **0 mismatches** (`vulture_oi_unit_mismatch_total`). So `oi_base = oiCcy` and `oi_usd = oiUsd` hold, and the contract size is only a check. |
| `open-interest` cadence | A push arrives **at once on subscribe** (carrying an older `ts`), then only when the value changed. For `BTC-USDT-SWAP` the gaps between pushes were 2.3 s to 13.7 s (mean 7.7 s, 10 pushes in 70 s). Slower instruments push less often, so the sampler repeats the last value. |
| `mark-price` cadence | Pushes at once on subscribe, then every ~0.2 s while the price moves (gaps 0.02 to 0.21 s for BTC). 192 332 data frames in 76 s (about 2 500 per second) for 507 instruments x 2 channels; nothing was dropped (`vulture_ws_dropped_overflow_total` 0). |
| `oiUsd` | Present on every push and in REST. |
| Subscribe acknowledgement | One `{"id":"p1","event":"subscribe","arg":{...},"connId":"..."}` **per argument**, echoing the request `id`. |
| Invalid argument | A request with two valid arguments and `open-interest` for `NOSUCHCOIN-USDT-SWAP` produced `{"event":"error","msg":"Subscribe failed, wrong URL or channel:open-interest,instId:NOSUCHCOIN-USDT-SWAP doesn't exist. Please use the correct URL, channel and parameters referring to API document.","code":"60018","id":"p1",...}` **before** the acknowledgements of the valid arguments, which were subscribed and delivered data. So the exchange answers per argument; the error names channel and instrument in its text (no `arg` field). |
| Repeated subscribe | Subscribing again to an active channel is simply acknowledged. Unsubscribing something never subscribed is acknowledged as well (no error). |
| Ping | The text `ping` is answered by the text `pong`. |
| Liquidations | One `liquidation-orders` frame in ~70 s over all swaps (a quiet period): `LIT-USDT-SWAP`, `posSide=short`, `side=buy`, `sz=158`, `bkPx=3.1994`, `ts` equal to the event time (receive delay about 1 s). The connector turned it into a `Short` liquidation of 158 LIT, notional 505.5 USD (`ctVal x ctMult` = 1 for LIT, taken from the instrument list). Hedge-mode mapping (`posSide` short with `side` buy is a short) matches the documentation example. |
| Region | The handshake answered 101 and REST answered 200 from the machine used; nothing was blocked. |
| Connection budget | One connection, 11 subscribe requests for 1 014 topics (100 arguments per request) plus the liquidation request: far below 480 per hour. |
| Shutdown | The connector stopped cleanly on cancellation. |

## Not confirmed

- **USDC swaps.** The universe held no `-USDC-PERP` symbol. Either OKX lists none at this time or the filter dropped
  them; the instrument list body was not saved and no third REST call was made (the run is limited to two). The USDC
  path (`settleCcy=USDC`, `BTC-USDC-SWAP` to `BTC-USDC-PERP`) is covered by fixtures only. Check the first deployment:
  `vulture_symbols{exchange="okx"}` against the count of USDC instruments in the REST list, and
  `vulture_unusable_instruments`.
- **Net-mode liquidations** (`posSide=net`): not seen. The mapping (`sell` closes a long) follows from how OKX closes
  positions and is covered by unit tests; confirm when a net-mode record appears.
- **Completeness.** The documentation says the feed is not the total. One record in 70 s says nothing about sampling
  (Tardis' "at most one update per second per contract" was not testable). The README documents the feed as a lower
  bound.
- **Ordering** across the "different data sources" mentioned by the documentation: not observed.
- **`ctMult` other than 1** and instruments with a `ctValCcy` that is not the base coin: none seen, the filter
  reports them as unusable instead of guessing.
- **Handling of a bad argument by the connector** was verified against the mock (both orders of error and
  acknowledgements, and the whole-request-failure variant); the live run subscribed only valid topics, the raw probe
  produced the error format above.
- **Reconnect** against the real exchange was not exercised in this run (no connection loss happened).
