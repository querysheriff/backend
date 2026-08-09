-- +goose Up

CREATE INDEX log_events_server_occurred_counts_idx
    ON log_events (server_name, occurred_at DESC) INCLUDE (log_level, classification);

DROP INDEX log_events_server_name_occurred_at_idx;

-- +goose Down

CREATE INDEX log_events_server_name_occurred_at_idx
    ON log_events (server_name, occurred_at DESC);

DROP INDEX log_events_server_occurred_counts_idx;
