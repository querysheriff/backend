#!/usr/bin/env bash
# Replaces the local dev Postgres data with a copy of production.
set -euo pipefail

prod_url=${PROD_POSTGRES_URL:?set PROD_POSTGRES_URL, e.g. postgres://user:pass@localhost:5433/querysheriff}
local_url=${POSTGRES_URL:-postgres://querysheriff_backend:querysheriff_backend@localhost:5432/querysheriff?sslmode=disable}

dump=$(mktemp)
trap 'rm -f "$dump"' EXIT

# Dump fully first, so a broken connection never leaves a half-restored database.
pg_dump --clean --if-exists --no-owner --no-privileges --file "$dump" "$prod_url"

# Null the webhooks so local alerts never post to real Slack channels.
psql --quiet --output /dev/null --single-transaction --set ON_ERROR_STOP=1 \
	--file "$dump" \
	--command 'UPDATE public.servers SET slack_webhook_url = NULL;' \
	"$local_url"
