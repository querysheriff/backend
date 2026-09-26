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
RETURNING *;

-- name: GetUserByEmail :one
SELECT * FROM users WHERE email = $1;

-- name: GetUserByID :one
SELECT * FROM users WHERE id = $1;

-- name: ListUsers :many
SELECT * FROM users ORDER BY created_at, id;

-- A new password ends every session opened with the old one, except the caller's own.
-- name: UpdateUser :one
WITH revoked AS (
    DELETE FROM user_sessions
    WHERE user_id = sqlc.arg('id')
      AND token_hash <> sqlc.arg('caller_session_hash')
      AND sqlc.narg('password_hash')::text IS NOT NULL
)
UPDATE users
SET name = sqlc.arg('name'),
    email = sqlc.arg('email'),
    password_hash = coalesce(sqlc.narg('password_hash')::text, password_hash),
    allowed_servers = sqlc.arg('allowed_servers')
WHERE id = sqlc.arg('id')
RETURNING *;

-- name: DeleteUser :exec
DELETE FROM users WHERE id = $1;

-- name: CreateSession :exec
INSERT INTO user_sessions (token_hash, user_id, expires_at)
VALUES ($1, $2, $3);

-- name: GetSessionUser :one
SELECT * FROM users
WHERE id = (SELECT user_id FROM user_sessions WHERE token_hash = $1 AND expires_at > now());

-- name: DeleteSession :exec
DELETE FROM user_sessions WHERE token_hash = $1;

-- name: DeleteExpiredSessions :exec
DELETE FROM user_sessions WHERE expires_at <= now();

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

-- name: UpsertAlertToggle :exec
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

-- name: GetEnabledAlertWebhook :one
SELECT s.slack_webhook_url
FROM alert_settings s
LEFT JOIN alert_toggles t
  ON t.server_name = s.server_name AND t.alert_key = sqlc.arg('alert_key')
WHERE s.server_name = sqlc.arg('server_name')
  AND s.slack_webhook_url <> ''
  AND coalesce(t.enabled, true);

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

-- name: ReleaseAlertClaim :exec
WITH released_fire AS (
    DELETE FROM alert_fires
    WHERE alert_fires.server_name = sqlc.arg('server_name')
      AND alert_fires.alert_key = sqlc.arg('alert_key')
      AND alert_fires.fired_at = sqlc.arg('fired_at')
)
DELETE FROM alert_notifications
WHERE alert_notifications.server_name = sqlc.arg('server_name')
  AND alert_notifications.alert_key = sqlc.arg('alert_key')
  AND alert_notifications.last_fired_at = sqlc.arg('fired_at');

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

