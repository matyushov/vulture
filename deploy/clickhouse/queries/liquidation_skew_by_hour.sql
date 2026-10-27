-- Long versus short liquidations by hour and exchange over the last 24 hours. `long_share` near 1 means longs
-- were being liquidated (a falling market), near 0 shorts (a squeeze). Binance reports at most one liquidation per
-- symbol per second, so its sums are lower bounds.
-- Edit: the window, `exchange` filter (add AND exchange = 'bybit').
SELECT
    toStartOfHour(exchange_ts) AS hour,
    exchange,
    count() AS events,
    round(sumIf(notional_usd, side = 'long')) AS long_usd,
    round(sumIf(notional_usd, side = 'short')) AS short_usd,
    round(long_usd / nullIf(long_usd + short_usd, 0), 3) AS long_share,
    round(max(notional_usd)) AS largest_usd
FROM liquidations
WHERE exchange_ts >= now() - INTERVAL 24 HOUR
  AND exchange_ts < now()
GROUP BY hour, exchange
ORDER BY hour DESC, exchange
