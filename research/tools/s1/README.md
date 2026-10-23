# S-1: Binance USDT-M public data check

Throwaway measurement tools (not product code). Public read-only endpoints, no keys.

Setup (dependencies stay local to this directory):

    python3 -m venv .venv && .venv/bin/pip install websockets

Scripts:
- `rest_check.py [PACE_SECONDS]` - `exchangeInfo` statistics (writes `../../data/exchange_info_full.json`),
  `openInterest` latency on 60 symbols, 3-symbol OI sample, weight header check, then one paced pass over all
  TRADING PERPETUAL USDT+USDC symbols (sequential, spread over >= PACE seconds). Symbols are URL-encoded
  (some are Chinese). Aborts on any HTTP error (prints status and headers).
- `force_order.py OUT.jsonl SECONDS` - records `!forceOrder@arr` to JSONL as `{recv_ts, msg}`.
  Uses the `/market/ws/` route (the legacy `/ws/` route connects but delivers nothing, see
  `../../S1_binance_check.md`).
- `analyze_force_order.py OUT.jsonl` - counts, rate per minute, field set, latency.

Raw data goes to `research/data/` (git-ignored). Report: `research/S1_binance_check.md`.
