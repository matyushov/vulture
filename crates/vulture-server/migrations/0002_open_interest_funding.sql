-- Funding rate, index price and next funding time next to each open interest sample (task V3b, B1). Additive:
-- three nullable columns, so rows written before this migration read back as NULL and nothing is rewritten.
-- Basis (mark - index) is derived in queries, not stored.
ALTER TABLE open_interest ADD COLUMN IF NOT EXISTS funding_rate Nullable(Float64) CODEC(Gorilla, ZSTD(1));
ALTER TABLE open_interest ADD COLUMN IF NOT EXISTS index_price Nullable(Float64) CODEC(Gorilla, ZSTD(1));
ALTER TABLE open_interest ADD COLUMN IF NOT EXISTS next_funding_ts Nullable(DateTime64(3, 'UTC')) CODEC(DoubleDelta, ZSTD(1));
