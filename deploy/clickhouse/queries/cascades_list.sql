-- Liquidation cascades of the last 7 days (the "end" record of each: totals, duration, price range). Market-wide
-- cascades have exchange '*'; per-exchange ones carry the exchange name.
-- Edit: the window, `exchange` filter.
SELECT
    toDateTime(toUInt64OrZero(details['start_ts']) / 1000, 'UTC') AS started,
    exchange,
    toUInt32OrZero(details['duration_secs']) AS duration_secs,
    toUInt64OrZero(details['count']) AS liquidations,
    round(toFloat64OrZero(details['total_usd'])) AS total_usd,
    round(toFloat64OrZero(details['peak_usd_per_min'])) AS peak_usd_per_min,
    round(toFloat64OrZero(details['long_share']), 3) AS long_share,
    toUInt32OrZero(details['symbols']) AS symbols,
    details['top_symbols'] AS top_symbols,
    details['severity'] AS severity
FROM signals
WHERE type = 'liq_cascade'
  AND details['phase'] = 'end'
  AND ts >= now() - INTERVAL 7 DAY
  AND ts < now()
ORDER BY total_usd DESC
LIMIT 50
