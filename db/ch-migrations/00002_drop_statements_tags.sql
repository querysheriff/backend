-- +goose Up

ALTER TABLE statements DROP COLUMN IF EXISTS tags;

-- +goose Down

ALTER TABLE statements ADD COLUMN IF NOT EXISTS tags Map(String, String) DEFAULT map() AFTER query_kind;
