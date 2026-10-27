-- Funding extremes: the latest funding rate of every series seen in the last 3 hours, the 10 highest and the 10
-- lowest among series with more than 1 million USD of open interest. Rows before funding data was collected
-- (migration 2) have NULL funding and are skipped. `basis_pct` is mark against index in percent.
-- Edit: the OI floor, the number of rows.
WITH latest AS (
    SELECT
        exchange,
        symbol,
        argMaxIf(funding_rate, exchange_ts, funding_rate IS NOT NULL) AS rate,
        argMaxIf(oi_usd, exchange_ts, oi_usd IS NOT NULL) AS oi,
        argMaxIf((mark_price - index_price) / index_price * 100, exchange_ts,
                 mark_price IS NOT NULL AND index_price IS NOT NULL AND index_price > 0) AS basis,
        countIf(funding_rate IS NOT NULL) AS funding_samples
    FROM open_interest
    WHERE exchange_ts >= now() - INTERVAL 3 HOUR AND exchange_ts < now()
    GROUP BY exchange, symbol
    HAVING funding_samples > 0 AND oi > 1000000
)
SELECT * FROM (SELECT 'highest' AS side, exchange, symbol, round(rate * 100, 4) AS funding_pct, round(oi) AS oi_usd, round(basis, 3) AS basis_pct FROM latest ORDER BY rate DESC LIMIT 10)
UNION ALL
SELECT * FROM (SELECT 'lowest' AS side, exchange, symbol, round(rate * 100, 4) AS funding_pct, round(oi) AS oi_usd, round(basis, 3) AS basis_pct FROM latest ORDER BY rate ASC LIMIT 10)
