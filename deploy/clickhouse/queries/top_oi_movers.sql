-- Top open interest movers over the last 24 hours: first and last oi_base of every series, absolute and percent
-- change, current OI in USD. oi_base (not oi_usd) so that a price move alone never looks like a mover.
-- Edit: the window (24 HOUR), the dust filter (oi_usd_end > 1e6), LIMIT. Pin the clock by replacing now().
SELECT
    exchange,
    symbol,
    argMin(oi_base, exchange_ts) AS oi_base_start,
    argMax(oi_base, exchange_ts) AS oi_base_end,
    oi_base_end - oi_base_start AS change_abs,
    round(100 * change_abs / oi_base_start, 2) AS change_pct,
    argMaxIf(oi_usd, exchange_ts, oi_usd IS NOT NULL) AS oi_usd_end,
    count() AS samples
FROM open_interest
WHERE exchange_ts >= now() - INTERVAL 24 HOUR
  AND exchange_ts < now()
GROUP BY exchange, symbol
HAVING oi_base_start > 0 AND oi_usd_end > 1000000 AND samples > 10
ORDER BY abs(change_pct) DESC
LIMIT 20
