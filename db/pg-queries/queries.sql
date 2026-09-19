-- name: UpsertCollectorHealth :exec
INSERT INTO collector_health (server_name, collected_at, databases)
VALUES ($1, $2, $3)
ON CONFLICT (server_name) DO UPDATE
SET collected_at = EXCLUDED.collected_at,
    databases = EXCLUDED.databases;

-- name: ListMonitoredServers :many
SELECT server_name, collected_at, databases
FROM collector_health
WHERE collected_at >= now() - interval '24 hours'
  AND (sqlc.narg('allowed_servers')::text[] IS NULL OR server_name = ANY(sqlc.narg('allowed_servers')::text[]))
ORDER BY server_name;

-- name: CountUsers :one
SELECT count(*) AS total FROM users;

-- name: CreateUser :one
INSERT INTO users (name, email, password_hash, is_super_admin, allowed_servers)
VALUES ($1, $2, $3, $4, $5)
RETURNING id, name, email, is_super_admin, created_at, allowed_servers;

-- name: GetUserByEmail :one
SELECT id, name, email, password_hash, is_super_admin, created_at, allowed_servers
FROM users
WHERE email = $1;

-- name: GetUserByID :one
SELECT id, name, email, password_hash, is_super_admin, created_at, allowed_servers
FROM users
WHERE id = $1;

-- name: ListUsers :many
SELECT id, name, email, is_super_admin, created_at, allowed_servers
FROM users
ORDER BY created_at, id;

-- name: UpdateUser :one
UPDATE users
SET name = sqlc.arg('name'),
    email = sqlc.arg('email'),
    password_hash = coalesce(sqlc.narg('password_hash')::text, password_hash),
    allowed_servers = sqlc.arg('allowed_servers')
WHERE id = sqlc.arg('id')
RETURNING id, name, email, is_super_admin, created_at, allowed_servers;

-- name: DeleteUser :exec
DELETE FROM users WHERE id = $1;

-- name: CreateSession :exec
INSERT INTO user_sessions (token_hash, user_id, expires_at)
VALUES ($1, $2, $3);

-- name: GetSessionUser :one
SELECT u.id, u.name, u.email, u.is_super_admin, u.created_at, u.allowed_servers
FROM user_sessions s
JOIN users u ON u.id = s.user_id
WHERE s.token_hash = $1 AND s.expires_at > now();

-- name: DeleteSession :exec
DELETE FROM user_sessions WHERE token_hash = $1;

-- name: CreateCollectorToken :one
INSERT INTO collector_tokens (server_name, token_hash)
VALUES ($1, $2)
RETURNING id, server_name, created_at;

-- name: GetCollectorServerByHash :one
SELECT server_name FROM collector_tokens WHERE token_hash = $1;

-- name: ListCollectorTokens :many
SELECT id, server_name, created_at FROM collector_tokens ORDER BY created_at, id;

-- name: DeleteCollectorToken :one
DELETE FROM collector_tokens WHERE id = $1
RETURNING server_name;

-- name: CountCollectorTokensForServer :one
SELECT count(*) AS total FROM collector_tokens WHERE server_name = $1;

-- name: RemoveServerFromUsers :exec
UPDATE users SET allowed_servers = array_remove(allowed_servers, sqlc.arg('server_name')::text);

-- name: ListAlertWebhooks :many
SELECT server_name, slack_webhook_url
FROM alert_settings
WHERE (sqlc.narg('allowed_servers')::text[] IS NULL OR server_name = ANY(sqlc.narg('allowed_servers')::text[]));

-- name: ListAlertToggles :many
SELECT server_name, alert_key, enabled
FROM alert_toggles
WHERE (sqlc.narg('allowed_servers')::text[] IS NULL OR server_name = ANY(sqlc.narg('allowed_servers')::text[]));

-- name: UpsertAlertWebhook :exec
INSERT INTO alert_settings (server_name, slack_webhook_url)
VALUES ($1, $2)
ON CONFLICT (server_name) DO UPDATE SET slack_webhook_url = EXCLUDED.slack_webhook_url;

-- name: UpsertAlertToggle :batchexec
INSERT INTO alert_toggles (server_name, alert_key, enabled)
VALUES ($1, $2, $3)
ON CONFLICT (server_name, alert_key) DO UPDATE SET enabled = EXCLUDED.enabled;

-- name: DeleteAlertConfigForServer :exec
WITH cleared_settings AS (
    DELETE FROM alert_settings WHERE alert_settings.server_name = sqlc.arg('server_name')
),
cleared_toggles AS (
    DELETE FROM alert_toggles WHERE alert_toggles.server_name = sqlc.arg('server_name')
),
cleared_fires AS (
    DELETE FROM alert_fires WHERE alert_fires.server_name = sqlc.arg('server_name')
)
DELETE FROM alert_notifications WHERE alert_notifications.server_name = sqlc.arg('server_name');

-- name: GetAlertWebhook :one
SELECT slack_webhook_url FROM alert_settings WHERE server_name = $1;

-- name: GetAlertEnabled :one
SELECT enabled FROM alert_toggles WHERE server_name = $1 AND alert_key = $2;

-- name: TryClaimAlertNotification :one
WITH claimed AS (
    INSERT INTO alert_notifications (server_name, alert_key, last_fired_at)
    VALUES (sqlc.arg('server_name'), sqlc.arg('alert_key'), now())
    ON CONFLICT (server_name, alert_key)
    DO UPDATE SET last_fired_at = now()
    WHERE alert_notifications.last_fired_at < now() - sqlc.arg('cooldown')::interval
    RETURNING last_fired_at
),
recorded AS (
    INSERT INTO alert_fires (server_name, alert_key, fired_at)
    SELECT sqlc.arg('server_name'), sqlc.arg('alert_key'), claimed.last_fired_at
    FROM claimed
)
SELECT last_fired_at FROM claimed;

-- name: CountRecentAlertFires :many
SELECT server_name, alert_key, count(*) AS fires
FROM alert_fires
WHERE fired_at >= now() - sqlc.arg('history_window')::interval
  AND (sqlc.narg('allowed_servers')::text[] IS NULL OR server_name = ANY(sqlc.narg('allowed_servers')::text[]))
GROUP BY server_name, alert_key;

-- name: PruneAlertFires :exec
DELETE FROM alert_fires WHERE fired_at < now() - sqlc.arg('history_window')::interval;

-- name: ListStaleServers :many
SELECT server_name
FROM collector_health
WHERE collected_at < now() - sqlc.arg('stale_after')::interval
  AND collected_at >= now() - interval '24 hours'
ORDER BY server_name;

-- name: ListServersWithAlertEnabled :many
SELECT s.server_name
FROM alert_settings s
LEFT JOIN alert_toggles t
  ON t.server_name = s.server_name AND t.alert_key = sqlc.arg('alert_key')
WHERE s.slack_webhook_url <> ''
  AND coalesce(t.enabled, true)
ORDER BY s.server_name;

