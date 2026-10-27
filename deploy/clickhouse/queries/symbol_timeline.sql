-- Per-minute timeline of one series: OI (base and USD), mark and index price, basis, funding rate and the
-- liquidations of the minute, for the last 6 hours.
-- Edit: the three constants in the first line (exchange, symbol, hours).
WITH 'binance' AS ex, 'BTC-USDT-PERP' AS sym, 6 AS hours
SELECT
    t.minute AS minute,
    t.oi_base AS oi_base,
    t.oi_usd AS oi_usd,
    t.mark AS mark,
    t.index AS index,
    t.basis_pct AS basis_pct,
    t.funding_pct AS funding_pct,
    coalesce(l.long_usd, 0) AS liq_long_usd,
    coalesce(l.short_usd, 0) AS liq_short_usd
FROM (
    SELECT
        toStartOfMinute(exchange_ts) AS minute,
        argMax(oi_base, exchange_ts) AS oi_base,
        argMaxIf(oi_usd, exchange_ts, oi_usd IS NOT NULL) AS oi_usd,
        argMaxIf(mark_price, exchange_ts, mark_price IS NOT NULL) AS mark,
        argMaxIf(index_price, exchange_ts, index_price IS NOT NULL) AS index,
        round((mark - index) / index * 100, 3) AS basis_pct,
        round(argMaxIf(funding_rate, exchange_ts, funding_rate IS NOT NULL) * 100, 4) AS funding_pct
    FROM open_interest
    WHERE exchange = ex AND symbol = sym
      AND exchange_ts >= now() - toIntervalHour(hours) AND exchange_ts < now()
    GROUP BY minute
) AS t
LEFT JOIN (
    SELECT
        toStartOfMinute(exchange_ts) AS minute,
        round(sumIf(notional_usd, side = 'long')) AS long_usd,
        round(sumIf(notional_usd, side = 'short')) AS short_usd
    FROM liquidations
    WHERE exchange = ex AND symbol = sym
      AND exchange_ts >= now() - toIntervalHour(hours) AND exchange_ts < now()
    GROUP BY minute
) AS l ON t.minute = l.minute
ORDER BY minute
