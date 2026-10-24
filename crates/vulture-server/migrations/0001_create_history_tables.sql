-- History tables of vulture (task V3a). Forward-only and additive: later migrations add tables or columns,
-- they never drop or rewrite data. No TTL anywhere: history is kept indefinitely.
--
-- Common choices
--   * PARTITION BY toYYYYMM(<time>): about 12 partitions a year, cheap to back up and to detach by month.
--   * ORDER BY (exchange, symbol, <time>): every query names an exchange/symbol (or scans all of them for one
--     period), and within a symbol the timestamps are close together, so DoubleDelta compresses them to almost
--     nothing and Gorilla sees slowly changing neighbours.
--   * non_replicated_deduplication_window: makes `insert_deduplication_token` work on this non-replicated
--     table. The flusher derives the token from the stream entry ID range, so a retried insert is dropped
--     by the server instead of creating duplicates. The window counts the most recent inserted blocks.
--   * event_id holds the Redis stream entry ID ("<ms>-<seq>"): tracing back to the source and a natural key.

CREATE TABLE IF NOT EXISTS liquidations
(
    exchange     LowCardinality(String),
    symbol       LowCardinality(String),
    symbol_raw   LowCardinality(String),
    exchange_ts  DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
    recv_ts      DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
    side         Enum8('unspecified' = 0, 'long' = 1, 'short' = 2),
    price        Float64 CODEC(Gorilla, ZSTD(1)),
    qty          Float64 CODEC(Gorilla, ZSTD(1)),
    notional_usd Float64 CODEC(Gorilla, ZSTD(1)),
    event_id     String CODEC(ZSTD(1))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(exchange_ts)
ORDER BY (exchange, symbol, exchange_ts)
SETTINGS non_replicated_deduplication_window = 10000;

CREATE TABLE IF NOT EXISTS open_interest
(
    exchange    LowCardinality(String),
    symbol      LowCardinality(String),
    symbol_raw  LowCardinality(String),
    exchange_ts DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
    recv_ts     DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
    oi_base     Float64 CODEC(Gorilla, ZSTD(1)),
    oi_usd      Nullable(Float64) CODEC(Gorilla, ZSTD(1)),
    mark_price  Nullable(Float64) CODEC(Gorilla, ZSTD(1)),
    event_id    String CODEC(ZSTD(1))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(exchange_ts)
ORDER BY (exchange, symbol, exchange_ts)
SETTINGS non_replicated_deduplication_window = 10000;

-- A gap has no exchange timestamp: from_ts (last event before the interruption) plays that role.
CREATE TABLE IF NOT EXISTS gaps
(
    exchange LowCardinality(String),
    stream   LowCardinality(String),
    from_ts  DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
    to_ts    DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
    event_id String CODEC(ZSTD(1))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(from_ts)
ORDER BY (exchange, stream, from_ts)
SETTINGS non_replicated_deduplication_window = 10000;

-- Mirrors the Signal message (proto v0.2). `ts` is the time the signal fired.
CREATE TABLE IF NOT EXISTS signals
(
    type        Enum8('unspecified' = 0, 'oi_spike_up' = 1, 'oi_spike_down' = 2, 'liq_cascade' = 3),
    exchange    LowCardinality(String),
    symbol      LowCardinality(String),
    window_secs UInt32,
    change_pct  Float64 CODEC(Gorilla, ZSTD(1)),
    change_abs  Float64 CODEC(Gorilla, ZSTD(1)),
    threshold   Float64 CODEC(Gorilla, ZSTD(1)),
    ts          DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
    details     Map(String, String),
    event_id    String CODEC(ZSTD(1))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (exchange, symbol, ts)
SETTINGS non_replicated_deduplication_window = 10000;
