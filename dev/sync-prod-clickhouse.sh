#!/usr/bin/env bash
# Replaces the local dev ClickHouse data with a copy of production, table by table.
# The local schema must exist first (make ch-migrate-up).
set -euo pipefail

prod_url=${PROD_CLICKHOUSE_HTTP_URL:-http://localhost:8124}
prod_auth="${PROD_CLICKHOUSE_USER:?set PROD_CLICKHOUSE_USER}:${PROD_CLICKHOUSE_PASSWORD:?set PROD_CLICKHOUSE_PASSWORD}"
prod_db=${PROD_CLICKHOUSE_DB:-querysheriff}
local_url=http://localhost:8123
local_auth=querysheriff_backend:querysheriff_backend
local_db=querysheriff

local_query() { curl -sSf -u "$local_auth" "$local_url" --data-binary "$1"; }

tables=$(local_query "
	SELECT name FROM system.tables
	WHERE database = '$local_db' AND engine LIKE '%MergeTree' AND name NOT LIKE 'goose%'
	FORMAT TSV")

for table in $tables; do
	# Same explicit column list on both sides, so column order cannot drift.
	columns=$(local_query "
		SELECT arrayStringConcat(groupArray(name), ', ') FROM (
			SELECT name FROM system.columns
			WHERE database = '$local_db' AND table = '$table'
			  AND default_kind NOT IN ('MATERIALIZED', 'ALIAS')
			ORDER BY position)
		FORMAT TSV")

	echo "syncing $table"
	local_query "TRUNCATE TABLE $local_db.$table"

	# Few threads and small blocks keep the load on production low.
	curl -sSf --compressed -u "$prod_auth" \
		"$prod_url/?enable_http_compression=1&max_threads=2&max_block_size=8192" \
		--data-binary "SELECT $columns FROM $prod_db.$table FORMAT Native" |
		# Small insert blocks keep wide rows under the dev 1 GiB query limit (dev/ch/users.xml).
		curl -sS --fail-with-body -u "$local_auth" -X POST -T - "$local_url/" \
			--url-query "max_partitions_per_insert_block=0" \
			--url-query "min_insert_block_size_bytes=16777216" \
			--url-query "query=INSERT INTO $local_db.$table ($columns) FORMAT Native"

	echo "  $(local_query "SELECT count() FROM $local_db.$table") rows"
done
