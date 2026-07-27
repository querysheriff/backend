-- +goose Up

ALTER TABLE log_events ADD COLUMN statement_sample_id BIGINT;
ALTER TABLE log_events ALTER COLUMN message DROP NOT NULL;
ALTER TABLE statement_samples DROP COLUMN log_event_id;

-- +goose Down

ALTER TABLE statement_samples ADD COLUMN log_event_id BIGINT;
UPDATE log_events SET message = '' WHERE message IS NULL;
ALTER TABLE log_events ALTER COLUMN message SET NOT NULL;
ALTER TABLE log_events DROP COLUMN statement_sample_id;
