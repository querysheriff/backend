-- +goose Up

CREATE TABLE statement_latency_bins (
    minute_start  TIMESTAMPTZ NOT NULL,
    server_name   TEXT        NOT NULL,
    database_name TEXT        NOT NULL,
    bins          SMALLINT[]  NOT NULL,
    weights       INTEGER[]   NOT NULL
) PARTITION BY RANGE (minute_start);

CREATE TABLE statement_latency_bins_default PARTITION OF statement_latency_bins DEFAULT;

CREATE INDEX ON statement_latency_bins (server_name, database_name, minute_start);

-- +goose Down

DROP TABLE statement_latency_bins;
