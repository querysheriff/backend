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
INSERT INTO users (name, email, password_hash, is_super_admin)
VALUES ($1, $2, $3, $4)
RETURNING *;

-- name: GetUserByEmail :one
SELECT sqlc.embed(users), ARRAY(
    SELECT us.server_name FROM user_servers us WHERE us.user_id = users.id ORDER BY us.server_name
)::text[] AS allowed_servers
FROM users
WHERE email = $1;

-- name: GetUserByID :one
SELECT sqlc.embed(users), ARRAY(
    SELECT us.server_name FROM user_servers us WHERE us.user_id = users.id ORDER BY us.server_name
)::text[] AS allowed_servers
FROM users
WHERE id = $1;

-- name: ListUsers :many
SELECT sqlc.embed(users), ARRAY(
    SELECT us.server_name FROM user_servers us WHERE us.user_id = users.id ORDER BY us.server_name
)::text[] AS allowed_servers
FROM users
ORDER BY created_at, id;

-- A new password ends every session opened with the old one, except the caller's own.
-- name: UpdateUser :execrows
WITH revoked AS (
    DELETE FROM user_sessions
    WHERE user_id = sqlc.arg('id')
      AND token_hash <> sqlc.arg('caller_session_hash')
      AND sqlc.narg('password_hash')::text IS NOT NULL
)
UPDATE users
SET name = sqlc.arg('name'),
    email = sqlc.arg('email'),
    password_hash = coalesce(sqlc.narg('password_hash')::text, password_hash)
WHERE id = sqlc.arg('id');

-- name: SetUserServers :exec
WITH wanted AS (
    SELECT unnest(sqlc.arg('server_names')::text[]) AS server_name
),
revoked AS (
    DELETE FROM user_servers
    WHERE user_id = sqlc.arg('user_id')
      AND server_name NOT IN (SELECT server_name FROM wanted)
)
INSERT INTO user_servers (user_id, server_name)
SELECT sqlc.arg('user_id')::bigint, server_name FROM wanted
ON CONFLICT (user_id, server_name) DO NOTHING;

-- name: DeleteUser :exec
DELETE FROM users WHERE id = $1;

-- name: CreateSession :exec
INSERT INTO user_sessions (token_hash, user_id, expires_at)
VALUES ($1, $2, $3);

-- name: GetSessionUser :one
SELECT sqlc.embed(users), ARRAY(
    SELECT us.server_name FROM user_servers us WHERE us.user_id = users.id ORDER BY us.server_name
)::text[] AS allowed_servers
FROM users
WHERE id = (SELECT user_id FROM user_sessions WHERE token_hash = $1 AND expires_at > now());

-- name: DeleteSession :exec
DELETE FROM user_sessions WHERE token_hash = $1;

-- name: DeleteExpiredSessions :exec
DELETE FROM user_sessions WHERE expires_at <= now();

-- name: CreateCollectorToken :one
WITH server AS (
    INSERT INTO servers (server_name) VALUES (sqlc.arg('server_name')) ON CONFLICT DO NOTHING
)
INSERT INTO collector_tokens (server_name, token_hash)
VALUES (sqlc.arg('server_name'), sqlc.arg('token_hash'))
RETURNING id, server_name, created_at;

-- name: GetCollectorServerByHash :one
SELECT server_name FROM collector_tokens WHERE token_hash = $1;

-- name: ListCollectorTokens :many
SELECT id, server_name, created_at FROM collector_tokens ORDER BY created_at, id;

-- name: DeleteCollectorToken :one
DELETE FROM collector_tokens WHERE id = $1
RETURNING server_name;

-- name: DeleteServerIfUnused :exec
DELETE FROM servers s
WHERE s.server_name = $1
  AND NOT EXISTS (SELECT 1 FROM collector_tokens t WHERE t.server_name = s.server_name);

-- name: ListAlertWebhooks :many
SELECT server_name, slack_webhook_url::text AS slack_webhook_url
FROM servers
WHERE slack_webhook_url IS NOT NULL
  AND (sqlc.narg('allowed_servers')::text[] IS NULL OR server_name = ANY(sqlc.narg('allowed_servers')::text[]));

-- name: ListAlertToggles :many
SELECT server_name, alert_key, enabled
FROM alert_toggles
WHERE (sqlc.narg('allowed_servers')::text[] IS NULL OR server_name = ANY(sqlc.narg('allowed_servers')::text[]));

-- name: UpdateServerWebhook :execrows
UPDATE servers
SET slack_webhook_url = nullif(sqlc.arg('slack_webhook_url')::text, '')
WHERE server_name = sqlc.arg('server_name');

-- name: UpsertAlertToggle :exec
INSERT INTO alert_toggles (server_name, alert_key, enabled)
VALUES ($1, $2, $3)
ON CONFLICT (server_name, alert_key) DO UPDATE SET enabled = EXCLUDED.enabled;

-- name: GetEnabledAlertWebhook :one
SELECT s.slack_webhook_url::text AS slack_webhook_url
FROM servers s
LEFT JOIN alert_toggles t
  ON t.server_name = s.server_name AND t.alert_key = sqlc.arg('alert_key')
WHERE s.server_name = sqlc.arg('server_name')
  AND s.slack_webhook_url IS NOT NULL
  AND coalesce(t.enabled, true);

-- name: LockAlertClaim :exec
SELECT pg_advisory_xact_lock(hashtextextended(sqlc.arg('server_name')::text || '/' || sqlc.arg('alert_key')::text, 0));

-- name: ClaimAlertFire :one
INSERT INTO alert_fires (server_name, alert_key, fired_at)
SELECT sqlc.arg('server_name')::text, sqlc.arg('alert_key')::text, now()
WHERE NOT EXISTS (
    SELECT 1 FROM alert_fires
    WHERE server_name = sqlc.arg('server_name')::text
      AND alert_key = sqlc.arg('alert_key')::text
      AND fired_at >= now() - sqlc.arg('cooldown')::interval
)
RETURNING fired_at;

-- name: DeleteAlertFire :exec
DELETE FROM alert_fires WHERE server_name = $1 AND alert_key = $2 AND fired_at = $3;

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
FROM servers s
LEFT JOIN alert_toggles t
  ON t.server_name = s.server_name AND t.alert_key = sqlc.arg('alert_key')
WHERE s.slack_webhook_url IS NOT NULL
  AND coalesce(t.enabled, true)
ORDER BY s.server_name;

