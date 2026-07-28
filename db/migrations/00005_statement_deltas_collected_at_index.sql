-- +goose Up

CREATE INDEX statement_deltas_collected_at_idx ON statement_deltas (collected_at);

-- +goose Down

DROP INDEX statement_deltas_collected_at_idx;
