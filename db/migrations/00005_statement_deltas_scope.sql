-- +goose Up

ALTER TABLE statement_deltas ADD COLUMN server_name   TEXT NOT NULL DEFAULT '';
ALTER TABLE statement_deltas ADD COLUMN database_name TEXT NOT NULL DEFAULT '';

UPDATE statement_deltas d
SET server_name = s.server_name, database_name = s.database_name
FROM statements s
WHERE s.id = d.statement_id;

ALTER TABLE statement_deltas ALTER COLUMN server_name   DROP DEFAULT;
ALTER TABLE statement_deltas ALTER COLUMN database_name DROP DEFAULT;

CREATE INDEX statement_deltas_collected_at_idx ON statement_deltas (collected_at);

-- +goose Down

DROP INDEX statement_deltas_collected_at_idx;
ALTER TABLE statement_deltas DROP COLUMN database_name;
ALTER TABLE statement_deltas DROP COLUMN server_name;
