-- +goose Up

CREATE TABLE alert_fires (
    server_name TEXT        NOT NULL,
    alert_key   TEXT        NOT NULL,
    fired_at    TIMESTAMPTZ NOT NULL
);

-- +goose Down

DROP TABLE alert_fires;
