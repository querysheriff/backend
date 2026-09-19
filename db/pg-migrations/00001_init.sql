-- +goose Up

CREATE TABLE users (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name            TEXT        NOT NULL,
    email           TEXT        NOT NULL UNIQUE,
    password_hash   TEXT        NOT NULL,
    is_super_admin  BOOLEAN     NOT NULL DEFAULT false,
    allowed_servers TEXT[]      NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX users_single_super_admin ON users (is_super_admin) WHERE is_super_admin;

CREATE TABLE user_sessions (
    token_hash TEXT        PRIMARY KEY,
    user_id    BIGINT      NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE collector_tokens (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    server_name TEXT        NOT NULL,
    token_hash  TEXT        NOT NULL UNIQUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE collector_health (
    server_name  TEXT        PRIMARY KEY,
    collected_at TIMESTAMPTZ NOT NULL,
    databases    TEXT[]      NOT NULL
);

CREATE TABLE alert_settings (
    server_name       TEXT PRIMARY KEY,
    slack_webhook_url TEXT NOT NULL DEFAULT ''
);

CREATE TABLE alert_toggles (
    server_name TEXT    NOT NULL,
    alert_key   TEXT    NOT NULL,
    enabled     BOOLEAN NOT NULL,
    PRIMARY KEY (server_name, alert_key)
);

CREATE TABLE alert_notifications (
    server_name   TEXT        NOT NULL,
    alert_key     TEXT        NOT NULL,
    last_fired_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (server_name, alert_key)
);

CREATE TABLE alert_fires (
    server_name TEXT        NOT NULL,
    alert_key   TEXT        NOT NULL,
    fired_at    TIMESTAMPTZ NOT NULL
);

-- +goose Down

DROP TABLE alert_fires;
DROP TABLE alert_notifications;
DROP TABLE alert_toggles;
DROP TABLE alert_settings;
DROP TABLE collector_health;
DROP TABLE collector_tokens;
DROP TABLE user_sessions;
DROP TABLE users;
