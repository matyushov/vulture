# S-3: public liquidation and open-interest channels of the exchanges

Checked 2026-10-23 against the official documentation (web only; no connection to any exchange API was
opened). Every statement carries a source and the check date; "NOT CONFIRMED" means the documentation was
unreachable, silent or contradictory, with the reason. Binance numbers come from `S1_binance_check.md`
(measured, not documentation). Scope per README: linear USDT/USDC perpetuals; inverse and quanto contracts
only where V7-0 needs them.

Conventions used below: "OI base" = open interest in base coin, "OI USD" = notional in quote/USD. Fixtures
copied verbatim from documentation are in `fixtures/<exchange>/*_doc.json`. The sample files in
`stream-collector/src/StreamTesting/data/` are **constructed, not captured** (their README says so) and were
deliberately not used as evidence.

## 1. Summary table (V2 and V7 exchanges)

| Exchange | Liquidation channel | Fields | Complete? | OI source | OI unit | Mark price in OI stream | Limits (WS) | Ping | Notes | Source, date |
|---|---|---|---|---|---|---|---|---|---|---|
| Binance (S-1) | WS `!forceOrder@arr` on `/market/ws/` | `s,S,o,f,q,p,ap,X,l,z,T` | No: last per symbol per 1000 ms | REST `/fapi/v1/openInterest` only | base coin | `!markPrice@arr@1s` | 2400 weight/min REST | server ping | old `/ws` route silent | S1 2026-10-23 |
| Bybit | `allLiquidation.{symbol}`, 500 ms | `T,s,S,v,p` | Yes per docs ("push all liquidations"); old `liquidation.{symbol}` (1 msg/s) ended 2025-11-21 | WS `tickers.{symbol}` field `openInterest` | base coin for linear (REST doc: BTCUSDT in BTC; inverse in USD); `openInterestValue` = OI x mark in quote | yes (`markPrice` in same topic, delta-encoded) | 500 conn/5 min per domain; 1000 market-data conn per IP per product; args string <= 21 000 chars; linear: no arg-count limit | `{"op":"ping"}` every 20 s; idle cut 10 min (`max_active_time` 30-600 s) | tickers are snapshot + delta, absent field = unchanged | bybit-exchange.github.io/docs/v5, 2026-10-23 |
| OKX | `liquidation-orders` (`instType=SWAP`), path `/ws/v5/public`, at most 1 update per second per contract (Tardis, secondary) | `details[]{side,posSide,sz,bkPx,bkLoss,ts}`, `instId`, `instFamily` | NOT CONFIRMED from OKX text (docs section unreachable, see 3.2); per Tardis a per-contract per-second cap, so treat as possibly sampled | WS `open-interest` channel, per `instId`, every 3 s on change | `oi` in contracts, `oiCcy` in base coin, `oiUsd` in USD | separate `mark-price` channel (200 ms, keepalive up to 10 s per Tardis) | 3 new conn/s per IP; 480 subscribe/unsubscribe/login requests per hour per connection; subscribe message <= 64 KB; 30 conn per channel per sub-account | text `ping` when idle < 30 s; server cuts after 30 s without data | `ctVal`, `ctMult`, `ctValCcy` from REST instruments; port 8443 being removed (notice 2026-10-16); `notice` code 64008 before upgrades | okx.com/docs-v5/en, log_en, 2026-10-23 |
| BitMEX | WS table `liquidation` | `orderID,symbol,side,price,leavesQty` | NOT CONFIRMED: carries liquidation orders entering the book (insert/update/delete), not fills | WS table `instrument`, fields `openInterest`, `openValue` | `openInterest` in contracts (int64), `openValue` in settlement-currency base units (satoshi-like); NOT CONFIRMED from docs | yes: `instrument.markPrice` | official page unreachable (see 3.3); secondary: 60 conn/hour, subscribe consumes one request token, 50 subscriptions on connect, 20 args per subscribe message | NOT CONFIRMED | quanto and inverse mix with linear in the same table | swagger.json (BitMEX GitHub), apis.io AsyncAPI, search snippets, 2026-10-23 |
| Deribit | no dedicated channel: `trades.{kind}.{currency}.{interval}` with optional field `liquidation` ("M","T","MT") | trade fields + `liquidation` | Yes as far as trades are complete; aggregated intervals batch but do not drop; `raw` needs auth | WS `ticker.{instrument}.{interval}` field `open_interest` | USD for inverse (BTC-PERPETUAL, ETH-PERPETUAL); base coin for linear (e.g. BTC_USDC-PERPETUAL) per `get_contract_size`; ticker text is ambiguous for linear perps | yes: `mark_price` in same ticker | 32 conn per IP; `public/subscribe` ~3.3 req/s sustained, burst 10; up to 500 channels per subscribe request; `connection_too_slow` if client lags | `public/set_heartbeat` (interval >= 10 s) then answer `test_request` with `public/test`; otherwise connection closed | `raw` interval only for authenticated users, use `100ms` | docs.deribit.com, 2026-10-23 |

## 2. Short table (V8 candidates)

| Exchange | Public liquidation feed | OI | Verdict | Source, date |
|---|---|---|---|---|
| Bitget | Yes: topic `liquidation` (`instType` usdt-futures / coin-futures / usdc-futures; UTA docs), 1 s push; **sampled**: only the largest long and largest short liquidation per pair per second | futures `ticker` topic has `openInterest` (unit NOT CONFIRMED), 200 ms | OI ok; liquidations incomplete by design (lower bound). Include with a quality flag | bitget.com/docs/uta/websocket/public, 2026-10-23 |
| Gate | Yes: `futures.public_liquidates` (contract list or `!all`); since the 2026-08-20 docs change all liquidations of a contract inside the aggregation period (default 1 s, early push at 20 orders) are sent as an array, so now complete; earlier docs said other orders in the period were dropped | `futures.tickers` `total_size` (contracts, decimal string with header `X-Gate-Size-Decimal: 1`), multiply by `quanto_multiplier` from `futures.contract_info` to get base coin; `open_interest` in REST `contract_stats` | Good candidate. WS limits NOT CONFIRMED (section not found in the 243 KB page) | gate.com/docs/developers/futures/ws/en, 2026-10-23 |
| Hyperliquid | No public liquidation stream. Liquidation appears only in `userEvents`/`userFills` of a specific user (`WsLiquidation`, `FillLiquidation`), and public `trades` (`WsTrade`) has no liquidation flag | `activeAssetCtx` `openInterest` (unit NOT CONFIRMED), `markPx`, `oraclePx` per coin | OI only. Limits: 10 conn/IP, 30 new conn/min, 1000 subscriptions, 2000 msg/min; ping format NOT CONFIRMED | hyperliquid.gitbook.io, 2026-10-23 |
| KuCoin Futures | NOT CONFIRMED: official docs unreachable from the fetcher (docs-new URL 404); Tardis capture list has no liquidation channel | `contract/details` and `contractMarket/snapshot` carry OI (Tardis); unit NOT CONFIRMED | skip liquidations (low priority); revisit with live docs | docs.tardis.dev (secondary), 2026-10-23 |
| MEXC Futures | NOT CONFIRMED: official WS pages unreachable (only landing page fetched); Tardis capture list has no liquidation or OI channel; `push.ticker` has price and volume only | NOT CONFIRMED | skip (low priority) | mexc.com/api-docs/futures, docs.tardis.dev, 2026-10-23 |

Bitget endpoint: the fetched UTA pages do not state the WS URL. The classic docs give
`wss://ws.bitget.com/v2/ws/public` (classic docs, 2026-10-23); the UTA endpoint is NOT CONFIRMED until V8
starts.

## 3. Per-exchange sections

### 3.1 Bybit (V2-1)

Sources: `https://bybit-exchange.github.io/docs/v5/websocket/public/all-liquidation`,
`.../public/ticker`, `.../ws/connect`, `.../market/instrument`, `.../market/open-interest`,
`.../rate-limit`; Tardis `docs.tardis.dev/historical-data-details/bybit` (secondary, for the retired
channel). Checked 2026-10-23.

Endpoint: `wss://stream.bybit.com/v5/public/linear` carries USDT and USDC perpetuals and USDT futures;
inverse is a separate socket (out of scope).

**Liquidations.** Topic `allLiquidation.{symbol}`, push every 500 ms, `type` always `snapshot`, `data` is an
array (a push may hold several records). Fields: `T` ms timestamp, `s` symbol, `S` position side
(`Buy` = a **long** position was liquidated; the docs define only `Buy`), `v` executed size (unit **not
stated**; the documentation example `ROSEUSDT v=20000 p=0.04499` and Binance parity indicate base coin for
linear, NOT CONFIRMED), `p` **bankruptcy price**, not the fill price. The page says it pushes "all
liquidations"; no sampling is documented. Subscribe args are per symbol (`allLiquidation.BTCUSDT`); there is
no wildcard in the docs. Fixture: `fixtures/bybit/all_liquidation_doc.json`.

```json
{"topic":"allLiquidation.ROSEUSDT","type":"snapshot","ts":1739502303204,
 "data":[{"T":1739502302929,"s":"ROSEUSDT","S":"Sell","v":"20000","p":"0.04499"}]}
```

**Open interest.** Linear tickers: `tickers.{symbol}`, 100 ms for derivatives, `type` `snapshot` then
`delta`; "if a response param is not found in the message, its value has not changed". Fields used:
`openInterest` (size, both sides), `openInterestValue` (value), `markPrice`, `indexPrice`, `singleOpenInterest*`.
The ticker page does not state units. Consistency check on the documentation's own example: `openInterest`
492373.72 x `markPrice` 66666.60 = 3.2825e10 = `openInterestValue` 32824881841.75, so `openInterest` is base
coin and `openInterestValue` is quote (USDT) notional. The REST page states it explicitly for
`/v5/market/open-interest`: "BTCUSDT: BTC, BTCUSD: USD". There is no contract-size field for linear
instruments (`lotSize` only), so contracts equal base coins and **no conversion is needed**. Implementation
consequence: keep per-symbol state and merge deltas; a delta without `openInterest` must not emit a zero.
REST history exists (`intervalTime` 5min..1d) but is useless for 30 s slices; WS is the source.
Fixture: `fixtures/bybit/tickers_linear_snapshot_doc.json`.

**Mark price.** In the same ticker topic (`markPrice`), so no extra subscription.

**Limits (WS).** No more than 500 connections per 5 minutes (counted per WebSocket domain); 1000 market-data
connections per IP, counted separately for Spot, Linear, Inverse, Options (rate-limit page); `args` array of a
subscribe request <= 21 000 characters; spot <= 10 args per request, linear/inverse: no arg-count limit.
HTTP: 600 requests per 5 s per IP; a 403 means terminate all sessions and wait at least 10 minutes. Other
REST limits are per UID and not relevant to public data.

**Ping/timeout.** Client sends `{"op":"ping"}` (optional `req_id`) every 20 s; linear/inverse/spot reply
`{"success":true,"ret_msg":"pong","op":"ping",...}`. Without ping or data the server cuts after 10 minutes by
default; `?max_active_time=1m` changes it (30 s - 600 s, scan granularity 30 s).

**Symbols.** `GET /v5/market/instruments-info?category=linear&limit=1000` with `cursor`/`nextPageCursor`
(default page 500, there are more than 500 linear symbols). Filter `contractType == "LinearPerpetual"`,
`status == "Trading"`; `status` defaults to `Trading` and `PendingOpen`, `PreLaunch` needs an explicit
`status`. `settleCoin` and `quoteCoin` identify USDT vs USDC. The page gives no symbol-format example for
USDC perps (the exchange names them `BTCPERP`; NOT CONFIRMED from the page), so the normalizer must use
`baseCoin`/`quoteCoin`, not parse the symbol. Prefixed bases (`1000PEPE`) arrive in `baseCoin` as is.
Normalization: `{baseCoin}-{quoteCoin}-PERP`.

**Regions.** The API docs have no restriction list; they only map local domains (`stream.bybit.kz` etc.).
Terms of service exclude the US among others (secondary: The Block, datawallet); EEA residents are directed
to Bybit EU. NOT CONFIRMED for the planned server location; test from the server.

**Changes.** `allLiquidation` since 2025-02-25; `liquidation.{symbol}` removed 2025-11-21 (Tardis).

### 3.2 OKX (V2-2)

Sources: `https://www.okx.com/docs-v5/en/` (overview and REST Get instruments readable), changelog
`https://www.okx.com/docs-v5/log_en/`, third-party: Tardis OKX Swap page, ccxt issue #23969.
Checked 2026-10-23.

**Documentation gap.** The fetched copy of the single-page OKX docs is cut at about 400 000 characters, before
the Public Data section, so the exact text of the `liquidation-orders`, `open-interest` and `mark-price`
sections could not be read. The channel names, `instType`/`instId` arguments and the `/ws/v5/public` path for
the liquidation channel come from search snippets of the official docs and from client libraries; field
semantics below are marked accordingly. Owner action for V2-2: read these three sections in a browser and
confirm the items flagged NOT CONFIRMED before writing the parser.

Endpoints (confirmed, overview): public `wss://ws.okx.com/ws/v5/public`, business `/ws/v5/business`. The
changelog (2026-10-16 upcoming change) says port 8443 is being discontinued on every path, so use the default
port 443. Regional hosts: `openapi.okx.com` does not work for US/AU and EU users (`us.okx.com`,
`eea.okx.com`); regional WS hostnames are not listed (NOT CONFIRMED).

**Liquidations.** Channel `liquidation-orders`, args `{"channel":"liquidation-orders","instType":"SWAP"}`
(other `instType` values exist, e.g. `FUTURES`), one subscription covers all swaps. Message shape (field names confirmed by
client-library models and the stream-collector parser; values constructed there, not captured):
`data[].{instType, instId, instFamily, uly, details[]{side, posSide, sz, bkPx, bkLoss, ccy, ts}}`.
`bkPx` is the **bankruptcy price**. `sz` is the size in **contracts** (NOT CONFIRMED from the text; convert with
`ctVal * ctMult`). `side`/`posSide`: in long/short mode `posSide=long` with `side=sell` means a long was
liquidated (NOT CONFIRMED). Completeness: Tardis states "at most one update per second per contract", so
treat the OKX stream as a lower bound until the docs are read.

**Open interest.** Channel `open-interest`, args `{"channel":"open-interest","instId":"BTC-USDT-SWAP"}`
(per instrument; no instType-wide variant in the library docs), update about every 3 s when OI changes
(Tardis; the official page says the same according to the search summary). Fields: `oi` (contracts),
`oiCcy` (coin), `oiUsd` (USD, added to the WS channel by the 2025-03-26 changelog entry), `ts`. Evidence of
units: ccxt issue sample `NEIRO-USDT-SWAP oi=14546007 oiCcy=14546007000 oiUsd=30921319.84`, i.e. 1000 NEIRO
per contract. Therefore `oi_base = oiCcy` directly; `ctVal` is needed only if `oiCcy` is absent. Mark price
is not in this message.

**Mark price.** Channel `mark-price`, args `instId`, 200 ms and keepalive up to 10 s when unchanged (Tardis);
a separate subscription. Alternative: `tickers` has no mark price. For 30 s OI slices subscribe `mark-price`
for the same instruments.

**Contract size.** REST `GET /api/v5/account/instruments?instType=SWAP` (private endpoint; rate limit 20 per
2 s per user and instType) or public `GET /api/v5/public/instruments` (NOT CONFIRMED path; same fields).
Fields: `ctVal` contract value, `ctMult` multiplier, `ctValCcy` currency of the contract value, `ctType`
`linear`/`inverse`, `settleCcy`, `state` (`live`...), `instId`. Quantities of derivatives (`lotSz`, `minSz`)
are in contracts. The conversion formula (`ctVal x ctMult` units of `ctValCcy`) is not written on the page.

**Symbols.** `instId` is `{Base}-{Quote}-SWAP`. Table in the docs: `BTC-USDT-SWAP` (USDT margin),
`BTC-USDC-SWAP` (USDC margin), `BTC-USD_UM-SWAP` (USD-margined, settles in USD stablecoins, added
2025-09-26, `instFamily` `BTC-USD_UM`), `BTC-USD-SWAP` (coin-margined inverse). Filter: `instType=SWAP`,
`ctType=linear`, `settleCcy` in {USDT, USDC}, `state=live`; decide separately about `USD_UM`. Normalization:
`BASE-QUOTE-SWAP` becomes `BASE-QUOTE-PERP`.

**Limits and ping (confirmed, overview).** 3 connection requests per second per IP; 30 connections per
channel per sub-account (events `channel-conn-count`, `channel-conn-count-error`); subscribe/unsubscribe/login
<= 480 requests per hour per connection; total channel length of one subscribe request <= 64 KB. The
connection breaks if no subscription is made or **no data is pushed for 30 s**; recommended: timer N < 30 s
after each message, then send the plain text `ping` and expect `pong`, reconnect if none within N seconds.
This closes the "OKX ping format" gap of `WS_SERVICE_MESSAGES.md`. The server can send
`{"event":"notice","code":"64008",...}` before closing for an upgrade (now also on business, 2026-06-11).

### 3.3 BitMEX (V7-1, V7-0)

Sources: `https://github.com/BitMEX/api-connectors/blob/master/swagger.json` (official, downloaded and
parsed), AsyncAPI mirror `apis.io/asyncapis/bitmex/bitmex-asyncapi/`, GitHub issues, search snippets of the
official WS page. Checked 2026-10-23.

**Documentation gap.** From the fetcher's network every page under `bitmex.com/app/*`
(wsAPI, restAPI, apiChangelog, contract pages) answered 302 to `account-closure.bitmex.com` and then to an
OAuth login; other hosts (`www2`, `services`) did not resolve. The reason is unknown (it may be the region of
the fetcher, which is itself a data point for region blocking). So the official WS/REST text is NOT CONFIRMED
this session; items marked (S) are from secondary sources and must be re-read in a browser at the start of V7.

Endpoint: `wss://ws.bitmex.com/realtime` (AsyncAPI). Topics via `{"op":"subscribe","args":["liquidation","instrument:XBTUSDT"]}`
or the `subscribe` query parameter. Table messages: `{table, action: partial|insert|update|delete, data, keys}`.

**Liquidations.** Table `liquidation`: `orderID` (uuid), `symbol`, `side`, `price` (double), `leavesQty` (int64)
(swagger, no descriptions). Semantics (S): "liquidation orders as they enter the book": `insert` when the
liquidation order is placed, `update` when `leavesQty`/`price` changes, `delete` when it leaves; so an
execution price and fill size are **not** given. A connector should emit a liquidation event on `insert`
(price and `leavesQty` as the initial size) and ignore updates, or reconstruct fills from `trade` (not
subscribed). V7 decision needed. Completeness NOT CONFIRMED.

**Open interest.** Table `instrument` (swagger): `openInterest` int64 and `openValue` int64, no descriptions.
Per BitMEX contract pages (S, 2022 snapshot, XBTUSD) open interest is counted in contracts; `openValue` is
the corresponding value in the **settlement currency's smallest unit** (satoshi for XBT; micro-USDt for USDt,
NOT CONFIRMED). `instrument` partial/updates carry only changed fields, so merge state. Mark price:
`instrument.markPrice` in the same table, no extra subscription. `instrument` subscription without a symbol
returns all instruments (huge partial); filter client-side or subscribe per symbol.

**Contract size and kinds** (V7-0). Instrument fields (swagger) that decide the kind: `typ` (`FFWCSX` =
perpetual per the `/instrument` description; futures `FFCCSX`), `isInverse`, `isQuanto`, `settlCurrency`,
`quoteCurrency`, `underlying`, `positionCurrency`, `multiplier`, `underlyingToPositionMultiplier`,
`underlyingToSettleMultiplier`, `quoteToSettleMultiplier`, `lotSize`, `tickSize`, `state`.
- Inverse: `XBTUSD` (1 contract = 1 USD, margin and PnL in XBT).
- Quanto: `ETHUSD` is a quanto perpetual settled in XBT (BitMEX contracts table via search snippet).
- Linear USDT: `XBTUSDT`, `ETHUSDT` and the alt perpetuals (`APEUSDT`, `GMTUSDT` seen in the contracts table;
  list must be built from live `/instrument/active`). Contract quantity is in contracts; for linear
  contracts 1 contract = `1 / underlyingToPositionMultiplier` underlying units (e.g. XBTUSDT-type contract
  0.000001 XBT in a 2022 snapshot, lot size 1000) - NOT CONFIRMED for current contracts.
- Rule for the filter (proposal): `typ == "FFWCSX" && !isInverse && !isQuanto && state == "Open"` and
  `settlCurrency` in USDT/USDC. Everything else (XBTUSD inverse, ETHUSD quanto, dated futures) is out under the
  "linear USDT/USDC perps" decision. If the owner wants XBTUSD, add a contract-type field to the schema and
  convert OI: `oi_usd = openInterest x 1` (1 USD per contract), `oi_base = oi_usd / markPrice`.
- BitMEX root symbols use `XBT` for bitcoin; normalization needs an alias `XBT -> BTC` (`XBTUSDT` becomes
  `BTC-USDT-PERP`). Base is available as `underlying`, quote as `quoteCurrency`.

**Limits (S).** Connection rate limit: 60 per hour per IP on a separate limiter (current docs per search
summary; an old blog said 20 per hour); a subscribe consumes one token from the request limiter, no limit on
the data once connected; 50 subscriptions at connection initialization (2018 changelog); subscribe message
rejects more than 20 args ("Too many args sent. Max length is 20", GitHub issue 2019). REST: 300 requests per
5 minutes for unauthenticated clients (third-party; NOT CONFIRMED). Ping: the official WS page documents a
plain-text `ping` answered by `pong`, with a recommendation to ping when nothing arrived for a few seconds
(NOT CONFIRMED; client libraries use 5-30 s). Welcome message contains `limit.remaining`.

**Regions.** The BitMEX terms exclude US and Quebec citizens (The Block, 2018, secondary); see the redirect
note above.

### 3.4 Deribit (V7-2, V7-0)

Sources (all official, downloaded and parsed): `https://docs.deribit.com/` pages under
`/api-reference/`, `/subscriptions/`, `/articles/` (rate-limits, connection-management-best-practices,
market-data-collection-best-practices, notifications) and `/specifications/deribit_openapi.json`,
`deribit_asyncapi.json`. Checked 2026-10-23.

Endpoints: `wss://www.deribit.com/ws/api/v2` (prod), `wss://test.deribit.com/ws/api/v2`. JSON-RPC 2.0.

**Liquidations.** No dedicated public liquidation channel. Use `trades.{kind}.{currency}.{interval}`, e.g.
`trades.future.any.100ms` (`kind` future/option/spot/..., `currency` BTC/ETH/USDC/USDT/EURR/any) or the
per-instrument `trades.{instrument}.{interval}`. A trade caused by liquidation has the optional field
`liquidation`: `"M"` maker side liquidated, `"T"` taker side liquidated, `"MT"` both. Trade fields:
`trade_id, trade_seq, timestamp, instrument_name, direction, price, amount, contracts, mark_price,
index_price, tick_direction, iv, block_*`. `amount`: USD for perpetual and inverse futures, base coin for
options and linear futures; `contracts` is in contract units. Interval `raw` is available **only to
authorized users**; use `100ms` (or `agg2`, about 1 s batches) for anonymous connections; aggregation batches
events in one message, it does not drop them. The `liquidation` flag marks fills and names the liquidated side
(taker or maker); mapping that to long/short needs `direction` (the taker's direction) and is a V7-2 test
item. The sample in the docs lacks a liquidation record; fixture
`fixtures/deribit/trades_doc.json` is the docs' option example without the flag (NOT a liquidation sample).

**Open interest.** `ticker.{instrument}.{interval}` (`100ms`, `agg2`; `raw` authorized only) with
`open_interest` and `mark_price` in the same message (also `incremental_ticker.{instrument}`: snapshot then
changed fields). Unit statement in the ticker schema: "total amount of outstanding contracts in the
corresponding amount units. For perpetual and inverse futures the amount is in USD units. For options and
linear futures it is the underlying base currency coin." That wording is ambiguous for **linear perpetuals**
(`BTC_USDC-PERPETUAL`); `public/get_contract_size` and `contract_size` of `get_instruments` resolve it: "USD
for inverse (reversed) futures and perpetuals; base currency coin for options, spots, and linear futures and
perpetuals" (BTC-PERPETUAL contract size 10 USD, BTC_USDC-PERPETUAL 0.0001 BTC). Hence: inverse perps
`oi_usd = open_interest`, `oi_base = open_interest / mark_price`; linear perps `oi_base = open_interest`,
`oi_usd = oi_base x mark_price`. Verify on live data in V7-2. Documentation example:
`BTC-PERPETUAL open_interest 502097590` (USD). Fixture `fixtures/deribit/ticker_btc_perpetual_doc.json`.
There is no OI-only WS channel and no separate mark price channel needed.

**Linear vs inverse (V7-0).** `public/get_instruments` (`currency=any`, `kind=future`), fields:
`instrument_type` (`linear` | `reversed`; Present for options and spots too), `settlement_period`
(`perpetual`), `settlement_currency` (base coin for inverse, typically USDC for linear), `base_currency`,
`quote_currency`, `counter_currency`, `contract_size`, `min_trade_amount`, `kind`, `state`, `price_index`.
Documented perpetuals: `BTC-PERPETUAL`, `ETH-PERPETUAL` (inverse, `reversed`, USD amounts, contract 10 USD
for BTC), `BTC_USDC-PERPETUAL` (linear, contract 0.0001 BTC); `base_currency` is documented as "not limited to
BTC/ETH - altcoins, RWAs and other listed bases", so more linear perps exist; the full list comes from
`get_instruments` and is NOT enumerated in the docs. No quanto perpetuals are
described. Filter: `kind=future && settlement_period=perpetual && instrument_type=linear && state=open`.
Subscribe to `instrument.state.{kind}.{currency}` (`future.any`) for listings and delistings; `get_instruments`
costs 10 000 credits (1 request/s sustained, burst 50), so call rarely.
Normalization: `BTC_USDC-PERPETUAL` becomes `BTC-USDC-PERP`; if inverse included, `BTC-PERPETUAL` becomes
`BTC-USD-PERP` with contract type `inverse`.

**Limits.** Connections: 32 per IP (HTTP and WS together; the 33rd is rejected with HTTP 429), 16 sessions per
API key. `public/subscribe`: cost 3000 credits, pool 30 000, sustained about 3.3/s, burst 10; one request may
carry up to 500 channels (best-practices article). Public non-authorized requests are limited per IP
(numbers not published), so authenticated connections are advised even for public data (we have no keys, so
stay conservative). Slow reader gets `connection_too_slow`. HTTP connections expire after 15 minutes
(irrelevant for WS). Exceeding credits returns error 10028 `too_many_requests` and **terminates the session**.

**Heartbeat.** `public/set_heartbeat {"interval": N}` with N >= 10 s (no authentication needed); the server then
sends `heartbeat` (informational) and `test_request` messages; the client must answer `test_request` with
`public/test`, otherwise the server closes the connection immediately. WebSocket has no fixed lifetime.
Reconnection (notifications article): re-subscribe to every channel; non-book channels may miss updates during
the gap (so emit `Gap`).

**Regions.** Deribit support article "Restricted Jurisdictions" lists the United States, Canada, Japan,
Russia, Iran, Cuba, Puerto Rico and others (secondary view via search; the official article was not fetched).
NOT CONFIRMED against the server location.

### 3.5 V8 candidates (short)

Details are in the short table. Notes: Bitget liquidation docs include a worked example
(`fixtures/bitget/liquidation_doc.json`), `side=buy` means a long was liquidated, `amount` is in **quote**
coin, so it can go straight to USD; limits from the classic doc: 300 connect requests per IP per 5 minutes,
max 100 connections per IP, 240 subscribe requests per hour per connection, max 1000 channels per connection
(recommended < 50), 10 messages per second including `ping`, ping string every 30 s, server cuts after 2 min
without ping. Gate sample: `fixtures/gate/public_liquidates_doc.json`, `size` negative for a liquidated long
(sign convention NOT CONFIRMED), endpoint `wss://fx-ws.gateio.ws/v4/ws/usdt`, application ping
`futures.ping`/`futures.pong` plus protocol pings.

## 4. Symbol lists and normalization

| Exchange | REST for symbols | Filter | Raw example | Normalized |
|---|---|---|---|---|
| Binance | `GET /fapi/v1/exchangeInfo` | `contractType=PERPETUAL`, `status=TRADING`, quote USDT/USDC | `BTCUSDT`, `1000PEPEUSDT`, `币安人生USDT` | `BTC-USDT-PERP` (use `baseAsset`/`quoteAsset`) |
| Bybit | `/v5/market/instruments-info?category=linear` (paged) | `LinearPerpetual`, `Trading`, settle USDT/USDC | `BTCUSDT`, `1000PEPEUSDT` | `{baseCoin}-{quoteCoin}-PERP` |
| OKX | `instruments?instType=SWAP` | `ctType=linear`, `settleCcy` USDT/USDC, `state=live` | `BTC-USDT-SWAP` | replace `SWAP` with `PERP` |
| BitMEX | `GET /instrument/active` | see 3.3 | `XBTUSDT` | `BTC-USDT-PERP` (alias XBT) |
| Deribit | `public/get_instruments` | see 3.4 | `BTC_USDC-PERPETUAL` | `BTC-USDC-PERP` |

Always derive base/quote from structured fields, never by splitting the raw symbol. Multiplier prefixes
(`1000PEPE` on Binance/Bybit, `PEPE` with `ctVal` 10 000 000 on OKX) mean the same raw symbol does not
mean the same base unit across exchanges: for cross-exchange `oi_base` sums the connector must normalize to
real coins (divide by the prefix) or sum by a per-exchange alias table. Open question for V1/V2.

## 5. Mark price per exchange (for the OI slice)

| Exchange | Stream | Notes |
|---|---|---|
| Binance | `!markPrice@arr@1s` | S-1 |
| Bybit | `markPrice` in `tickers` | delta-encoded, keep last |
| OKX | `mark-price` channel | separate subscription, 200 ms, keepalive 10 s |
| BitMEX | `instrument.markPrice` | in the OI table |
| Deribit | `mark_price` in `ticker` | same message as OI |

## 6. Discrepancies with the README (to fix)

1. README: Bybit "OI in `tickers` (WS)" - correct, but add that tickers are delta-encoded (state merge) and
   that `allLiquidation` push is 500 ms with `p` = bankruptcy price and unspecified `v` unit.
2. README: OKX "OI channel `open-interest`" - correct; add `oi` contracts, `oiCcy` coin, `oiUsd` USD, per-`instId`
   subscription (not instType-wide), and that mark price needs the separate `mark-price` channel (README says
   mark price arrives "in tickers" for OKX; OKX `tickers` has no mark price: wrong).
3. README: Bybit and OKX mark price "в тикерах" - Bybit yes, OKX no (see 2).
4. README: BitMEX "публичный канал `liquidation`, OI в данных инструмента" - correct, but the liquidation table
   is order events (insert/update/delete with `leavesQty`), not fills; units of `openValue` differ per
   settlement currency; XBT alias needed.
5. README: Deribit "OI в тикере" - correct; add `raw` needs authentication so use `100ms`/`agg2`, and that OI
   is USD for inverse and base coin for linear; no dedicated liquidation channel.
6. README: Bitget/Gate "публичный поток ликвидаций может быть неполным или приватным" - Bitget public but
   sampled (largest long and short per pair per second); Gate public and (since 2026-08-20) complete;
   Hyperliquid has no public liquidation stream; KuCoin and MEXC not confirmed.
7. Epic V2-2 note "ping строкой `ping`" is correct; `WS_SERVICE_MESSAGES.md` says OKX ping format unknown:
   it is the plain text `ping`/`pong`, idle limit 30 s.
8. OKX port 8443 is being retired (changelog entry of 2026-10-16): use 443.
9. Epic V7: "Символы: `XBTUSDT`, `ETHUSDT`" - raw names fine, normalized base must be BTC, not XBT.

## 7. Open items (not confirmed)

- OKX Public Data section text (liquidation completeness, `sz` unit, `side/posSide`, 3 s OI cadence).
- BitMEX official WS and REST pages (ping, limits, liquidation semantics, `openValue` unit, current linear list).
- Bybit `allLiquidation.v` unit; USDC perpetual naming.
- Region blocking for the deploy server for Bybit, OKX, BitMEX, Deribit.
- Bitget `openInterest` unit and UTA endpoint; Gate WS limits; Hyperliquid ping and `openInterest` unit;
  KuCoin and MEXC channels.
- Live measurement of real update cadence and message sizes belongs to a Binance-style short check done on
  the stand per exchange (not done: no connections to exchanges were allowed in S-3).
