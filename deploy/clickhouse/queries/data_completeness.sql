-- Data completeness of the last 24 hours per exchange and hour: OI samples against what the symbols and the polling
-- interval would give (`expected` assumes the 30-second default; change 30 if you poll at another interval), the
-- number of symbols, and the gaps the connectors recorded (minutes of interruption overlapping the hour).
-- A `coverage` well under 1 with no recorded gap means samples were lost without the connector noticing: look at
-- vulture_oi_* and vulture_ws_* metrics.
SELECT
    o.hour AS hour,
    o.exchange AS exchange,
    o.symbols AS symbols,
    o.samples AS samples,
    o.symbols * 3600 / 30 AS expected,
    round(o.samples / expected, 3) AS coverage,
    round(coalesce(g.gap_secs, 0) / 60, 1) AS gap_minutes
FROM (
    SELECT toStartOfHour(exchange_ts) AS hour, exchange, uniqExact(symbol) AS symbols, count() AS samples
    FROM open_interest
    WHERE exchange_ts >= now() - INTERVAL 24 HOUR AND exchange_ts < now()
    GROUP BY hour, exchange
) AS o
LEFT JOIN (
    SELECT
        hour,
        exchange,
        sum(greatest(0, least(toUnixTimestamp64Milli(to_ts), toUnixTimestamp(hour) * 1000 + 3600000) - greatest(toUnixTimestamp64Milli(from_ts), toUnixTimestamp(hour) * 1000)) / 1000) AS gap_secs
    FROM gaps
    ARRAY JOIN arrayMap(i -> toStartOfHour(now() - toIntervalHour(i)), range(25)) AS hour
    WHERE stream != 'liquidations'
      AND from_ts < hour + INTERVAL 1 HOUR AND to_ts >= hour
      AND to_ts >= now() - INTERVAL 25 HOUR
    GROUP BY hour, exchange
) AS g ON o.hour = g.hour AND o.exchange = g.exchange
ORDER BY hour DESC, exchange
