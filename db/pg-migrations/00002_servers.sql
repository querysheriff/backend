-- +goose Up

CREATE TABLE servers (
    server_name       TEXT PRIMARY KEY CHECK (server_name <> ''),
    slack_webhook_url TEXT CHECK (slack_webhook_url <> '')
);

INSERT INTO servers (server_name, slack_webhook_url)
SELECT t.server_name, nullif(a.slack_webhook_url, '')
FROM (SELECT DISTINCT server_name FROM collector_tokens) t
LEFT JOIN alert_settings a ON a.server_name = t.server_name;

CREATE TABLE user_servers (
    user_id     BIGINT NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    server_name TEXT   NOT NULL REFERENCES servers (server_name) ON DELETE CASCADE,
    PRIMARY KEY (user_id, server_name)
);

INSERT INTO user_servers (user_id, server_name)
SELECT DISTINCT u.id, s.server_name
FROM users u
CROSS JOIN LATERAL unnest(u.allowed_servers) AS a (server_name)
JOIN servers s ON s.server_name = a.server_name;

DELETE FROM collector_health h WHERE NOT EXISTS (SELECT 1 FROM servers s WHERE s.server_name = h.server_name);
DELETE FROM alert_toggles t WHERE NOT EXISTS (SELECT 1 FROM servers s WHERE s.server_name = t.server_name);
DELETE FROM alert_fires;

ALTER TABLE alert_fires
    ADD CONSTRAINT alert_fires_pkey PRIMARY KEY (server_name, alert_key, fired_at),
    ADD CONSTRAINT alert_fires_server_name_fkey
        FOREIGN KEY (server_name) REFERENCES servers (server_name) ON DELETE CASCADE;

ALTER TABLE alert_toggles
    ADD CONSTRAINT alert_toggles_server_name_fkey
        FOREIGN KEY (server_name) REFERENCES servers (server_name) ON DELETE CASCADE;

ALTER TABLE collector_health
    ADD CONSTRAINT collector_health_server_name_fkey
        FOREIGN KEY (server_name) REFERENCES servers (server_name) ON DELETE CASCADE;

ALTER TABLE collector_tokens
    ADD CONSTRAINT collector_tokens_server_name_fkey FOREIGN KEY (server_name) REFERENCES servers (server_name);

DROP TABLE alert_settings, alert_notifications;
ALTER TABLE users DROP COLUMN allowed_servers;

-- +goose Down

ALTER TABLE users ADD COLUMN allowed_servers TEXT[] NOT NULL DEFAULT '{}';

UPDATE users u
SET allowed_servers = ARRAY(
    SELECT us.server_name FROM user_servers us WHERE us.user_id = u.id ORDER BY us.server_name
);

CREATE TABLE alert_settings (
    server_name       TEXT PRIMARY KEY,
    slack_webhook_url TEXT NOT NULL DEFAULT ''
);

INSERT INTO alert_settings (server_name, slack_webhook_url)
SELECT server_name, slack_webhook_url FROM servers WHERE slack_webhook_url IS NOT NULL;

CREATE TABLE alert_notifications (
    server_name   TEXT        NOT NULL,
    alert_key     TEXT        NOT NULL,
    last_fired_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (server_name, alert_key)
);

INSERT INTO alert_notifications (server_name, alert_key, last_fired_at)
SELECT server_name, alert_key, max(fired_at) FROM alert_fires GROUP BY server_name, alert_key;

ALTER TABLE collector_tokens DROP CONSTRAINT collector_tokens_server_name_fkey;
ALTER TABLE collector_health DROP CONSTRAINT collector_health_server_name_fkey;
ALTER TABLE alert_toggles DROP CONSTRAINT alert_toggles_server_name_fkey;
ALTER TABLE alert_fires DROP CONSTRAINT alert_fires_server_name_fkey, DROP CONSTRAINT alert_fires_pkey;

DROP TABLE user_servers, servers;
