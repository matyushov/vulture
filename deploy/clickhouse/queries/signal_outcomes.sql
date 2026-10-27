-- What the mark price did after each OI spike signal of the last 7 days: the price at the signal and 5, 15 and 60
-- minutes later (the first OI sample at or after each time, at most 2 minutes late), and the return in percent.
-- `analyze signals` (vulture-server) computes the same with hit rates and quartiles; this is the raw list.
-- Edit: the window, `type` filter.
WITH marks AS (
    SELECT exchange, symbol, exchange_ts, mark_price
    FROM open_interest
    WHERE exchange_ts >= now() - INTERVAL 7 DAY - INTERVAL 5 MINUTE
      AND exchange_ts < now()
      AND mark_price IS NOT NULL
)
SELECT
    s.ts AS signal_ts,
    s.type AS type,
    s.exchange AS exchange,
    s.symbol AS symbol,
    s.window_secs AS window_secs,
    round(s.change_pct, 2) AS change_pct,
    m0.mark_price AS price_at_signal,
    round(100 * (m5.mark_price / m0.mark_price - 1), 3) AS return_5m_pct,
    round(100 * (m15.mark_price / m0.mark_price - 1), 3) AS return_15m_pct,
    round(100 * (m60.mark_price / m0.mark_price - 1), 3) AS return_60m_pct
FROM (
    SELECT * FROM signals
    WHERE type IN ('oi_spike_up', 'oi_spike_down')
      AND ts >= now() - INTERVAL 7 DAY AND ts < now()
) AS s
ASOF LEFT JOIN marks AS m0 ON s.exchange = m0.exchange AND s.symbol = m0.symbol AND s.ts <= m0.exchange_ts
ASOF LEFT JOIN marks AS m5 ON s.exchange = m5.exchange AND s.symbol = m5.symbol AND s.ts + INTERVAL 5 MINUTE <= m5.exchange_ts
ASOF LEFT JOIN marks AS m15 ON s.exchange = m15.exchange AND s.symbol = m15.symbol AND s.ts + INTERVAL 15 MINUTE <= m15.exchange_ts
ASOF LEFT JOIN marks AS m60 ON s.exchange = m60.exchange AND s.symbol = m60.symbol AND s.ts + INTERVAL 60 MINUTE <= m60.exchange_ts
ORDER BY signal_ts DESC
LIMIT 200
