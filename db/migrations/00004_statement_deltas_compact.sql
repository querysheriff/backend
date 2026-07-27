-- +goose Up

ALTER TABLE statement_deltas DROP CONSTRAINT statement_deltas_pkey;
ALTER TABLE statement_deltas DROP COLUMN id;
ALTER TABLE statement_deltas ALTER COLUMN calls TYPE INTEGER;

-- +goose Down

ALTER TABLE statement_deltas ALTER COLUMN calls TYPE BIGINT;
ALTER TABLE statement_deltas ADD COLUMN id BIGINT GENERATED ALWAYS AS IDENTITY;
ALTER TABLE statement_deltas ADD PRIMARY KEY (id, collected_at);
