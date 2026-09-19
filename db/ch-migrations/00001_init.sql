-- +goose Up

-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS statements
(
    id            UInt64,
    server_name   LowCardinality(String),
    database_name LowCardinality(String),
    user_name     LowCardinality(String),
    query_id      Int64,
    query_full    String              DEFAULT '' CODEC(ZSTD(3)),
    query_short   String              DEFAULT '' CODEC(ZSTD(3)),
    query_kind    UInt8               DEFAULT 0,
    tags          Map(String, String) DEFAULT map(),
    version       UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY id;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS statement_deltas
(
    statement_id    UInt64,
    collected_at    DateTime('UTC') CODEC(DoubleDelta, LZ4),
    server_name     LowCardinality(String),
    database_name   LowCardinality(String),
    calls           UInt32,
    "rows"          UInt32,
    total_exec_time Float64,
    total_io_time   Float64
)
ENGINE = MergeTree
PARTITION BY toDate(collected_at)
PRIMARY KEY (server_name, database_name, collected_at)
ORDER BY (server_name, database_name, collected_at, statement_id)
TTL collected_at + INTERVAL 30 DAY
SETTINGS non_replicated_deduplication_window = 100;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS statement_samples
(
    id                UInt64,
    statement_id      UInt64,
    collected_at      DateTime('UTC') CODEC(DoubleDelta, LZ4),
    occurred_at       DateTime64(3, 'UTC') CODEC(DoubleDelta, LZ4),
    server_name       LowCardinality(String),
    database_name     LowCardinality(String),
    query             String          CODEC(ZSTD(3)),
    duration_ms       Float64,
    parameters        Array(String),
    explain_plan_json String          CODEC(ZSTD(3)),
    tags              Map(String, String)
)
ENGINE = MergeTree
PARTITION BY toDate(collected_at)
ORDER BY (statement_id, collected_at)
TTL collected_at + INTERVAL 30 DAY
SETTINGS non_replicated_deduplication_window = 100;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS log_events
(
    id                  UInt64,
    server_name         LowCardinality(String),
    collected_at        DateTime('UTC') CODEC(DoubleDelta, LZ4),
    occurred_at         DateTime64(3, 'UTC') CODEC(DoubleDelta, LZ4),
    log_level           UInt8,
    classification      UInt8,
    message             String          CODEC(ZSTD(3)),
    pid                 UInt32          CODEC(T64, LZ4),
    user_name           LowCardinality(String),
    database_name       LowCardinality(String),
    application_name    LowCardinality(String),
    detail              String          CODEC(ZSTD(3)),
    hint                String          CODEC(ZSTD(3)),
    context             String          CODEC(ZSTD(3)),
    statement           String          CODEC(ZSTD(3)),
    backend_type        LowCardinality(String),
    state_code          LowCardinality(String),
    statement_sample_id UInt64
)
ENGINE = MergeTree
PARTITION BY toDate(collected_at)
ORDER BY (server_name, occurred_at)
TTL collected_at + INTERVAL 30 DAY
SETTINGS non_replicated_deduplication_window = 100;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS transaction_activity
(
    transaction_id   UInt64,
    server_name      LowCardinality(String),
    database_name    LowCardinality(String),
    user_name        LowCardinality(String),
    application_name String          CODEC(ZSTD(3)),
    pid              UInt32,
    backend_start    DateTime64(3, 'UTC') CODEC(DoubleDelta, LZ4),
    xact_start       DateTime64(3, 'UTC') CODEC(DoubleDelta, LZ4),
    collected_at     DateTime('UTC') CODEC(DoubleDelta, LZ4),
    query_start      DateTime64(3, 'UTC') CODEC(DoubleDelta, LZ4),
    query            String          CODEC(ZSTD(3)),
    query_tags       Map(String, String) CODEC(ZSTD(3)),
    state            LowCardinality(String),
    wait_event_type  LowCardinality(String),
    wait_event       LowCardinality(String),
    blocked_by_pid   UInt32,
    lock_wait_start  Nullable(DateTime64(3, 'UTC')) CODEC(DoubleDelta, LZ4),
    lock_mode        LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toDate(collected_at)
ORDER BY (server_name, database_name, xact_start, pid, collected_at)
TTL collected_at + INTERVAL 30 DAY;
-- +goose StatementEnd

-- +goose Down

DROP TABLE IF EXISTS transaction_activity;
DROP TABLE IF EXISTS log_events;
DROP TABLE IF EXISTS statement_samples;
DROP TABLE IF EXISTS statement_deltas;
DROP TABLE IF EXISTS statements;
