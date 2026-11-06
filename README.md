# QuerySheriff backend

The backend receives monitoring data from [collectors](https://github.com/querysheriff/collector), stores it, and serves it to the [dashboard](https://github.com/querysheriff/frontend) over Connect RPC. Monitoring data goes to ClickHouse. Users, sessions, servers, collector tokens and alert settings go to PostgreSQL.

One container image holds three binaries:

| Binary | Source | Role |
| --- | --- | --- |
| `querysheriff-api` | `cmd/api` | Connect API for collectors and the dashboard. Fires the live alerts while it ingests data. |
| `querysheriff-jobs` | `cmd/jobs` | Scheduler for the monitoring-stopped alert, the weekly report and pruning of old alert history. Run exactly one. |
| `querysheriff-migrate` | `cmd/migrate` | Applies the PostgreSQL and ClickHouse migrations, then sets the ClickHouse TTL from `RETENTION_DAYS`. |

Deployment is covered in the [deployment guide](https://github.com/querysheriff/docs); this README is about working on the code.

## Requirements

- Go 1.26.3
- Docker with Compose, for the dev databases and the integration tests

buf, sqlc, goose and golangci-lint are pinned in the `Makefile` and run through `go run`. Nothing else needs installing.

## Local development

```sh
cp .env.example .env   # local settings, including DEV_AUTO_LOGIN=true
make dev-up            # PostgreSQL 17, ClickHouse 25.12, Adminer and ch-ui
make pg-migrate-up
make ch-migrate-up
make dev               # API on http://localhost:3000
make dev-jobs          # scheduler, in a second terminal if you need alerts
make dev-down          # stop the containers and delete their volumes
```

| Service | Port |
| --- | --- |
| PostgreSQL | 5432 |
| ClickHouse | 9000 (native), 8123 (HTTP) |
| Adminer | 8080 |
| ch-ui | 8081 |

The API and the scheduler read `.env` from the working directory. Variables already set in the environment win, and `make dev` / `make dev-jobs` set `POSTGRES_URL` and `CLICKHOUSE_URL` to the dev containers. With `DEV_AUTO_LOGIN=true`, a request without a session cookie acts as the super admin, so the dashboard opens without a login. The default `CORS_ALLOWED_ORIGINS` already allows the frontend dev server on `http://localhost:3001`.

## Configuration

All three binaries read the same environment variables.

| Variable | Default | Description |
| --- | --- | --- |
| `POSTGRES_URL` | required | PostgreSQL connection URL. Prepared statements are unnamed, so PgBouncer in transaction pooling mode works. |
| `CLICKHOUSE_URL` | required | ClickHouse DSN for the native protocol, e.g. `clickhouse://user:password@host:9000/querysheriff`. |
| `LISTEN_ADDR` | `localhost:3000` | API listen address. Use `0.0.0.0:3000` in a container. |
| `CORS_ALLOWED_ORIGINS` | `http://localhost:3001` | Comma-separated origins allowed to call the API with cookies. |
| `COOKIE_SECURE` | `false` | `true` adds the `Secure` flag to the session cookie. Set it when the dashboard is served over HTTPS. |
| `DASHBOARD_URL` | empty | Public dashboard URL, used for links in the weekly report. |
| `RETENTION_DAYS` | `30` | Days of ClickHouse data to keep, 1 to 365. Takes effect when `querysheriff-migrate` runs. |
| `DEV_AUTO_LOGIN` | `false` | `true` makes requests without a session act as the super admin. Refused unless `LISTEN_ADDR` is a loopback address. |

Boolean variables are on only when set to the exact string `true`.

## HTTP endpoints

| Path | Description |
| --- | --- |
| `/api/querysheriff.v1.<Service>/<Method>` | Connect RPC. |
| `/healthz` | Liveness. Always 200. |
| `/readyz` | Readiness. 200 when PostgreSQL and ClickHouse both answer a ping, 503 otherwise. |
| `/schema.zip` | The proto sources. The collector and frontend generate their clients from it. |

| Service | Used by |
| --- | --- |
| `ActivityService`, `StatementService`, `LogService`, `HealthService` | Collectors call the `Report*` RPCs with a collector token. The dashboard calls `List*` and `Get*` with a session cookie. |
| `AuthService` | Login, logout and the current user. |
| `AdminService` | Collector tokens and users. Super admin only. |
| `AlertService` | Slack webhook and alert toggles for each server. |

Every call goes through one interceptor, `internal/server/auth_interceptor.go`. A collector sends its `qsc_` token as a bearer token, and the backend looks up which server it belongs to, so collectors never send a server name. Collector and session tokens are stored as SHA-256 hashes, passwords with bcrypt. The first login on an empty database creates the super admin. Other users only see the servers assigned to them.

## Storage

| Store | Tables |
| --- | --- |
| ClickHouse | `statements`, `statement_deltas`, `statement_samples`, `log_events`, `transaction_activity` |
| PostgreSQL | `users`, `user_sessions`, `servers`, `user_servers`, `collector_tokens`, `collector_health`, alert settings and fire history |

There is no retention job. Every ClickHouse table except `statements` is partitioned by day and has a TTL of `RETENTION_DAYS`, so ClickHouse drops old data itself. `statements` keeps one row per normalized query and never expires. A server exists in PostgreSQL while it has at least one collector token, and deleting its last token deletes the server's user access, health and alert rows with it.

## Project structure

```
cmd/
  api/                  Connect API server
  jobs/                 alert scheduler
  migrate/              schema migrations and retention TTL
proto/querysheriff/v1/  wire schema for every component
gen/                    Go code generated from proto/ by buf
db/
  pg-migrations/        PostgreSQL migrations (goose)
  pg-queries/           SQL queries compiled by sqlc
  ch-migrations/        ClickHouse migrations (goose) and the TTL update
internal/               application code, see the table below
dev/                    Compose file and database config for local runs
```

| Package | Responsibility |
| --- | --- |
| `internal/server` | Connect handlers, one file per service. Converts between proto and domain types, checks access, and scopes every read to the user's servers. |
| `internal/clickhouse` | One file per ClickHouse table, with its batch inserts and read queries. Filtering and aggregation run in SQL. |
| `internal/alerts` | Alert catalog, live checks on incoming data, the scheduler that `cmd/jobs` runs, and Slack delivery. |
| `internal/auth` | Token generation and hashing, password hashing, the authenticated principal in the request context. |
| `internal/config` | Environment variables. |
| `internal/postgres` | Opens the pgx pool. |
| `internal/gen/db` | Query code generated by sqlc. |
| `internal/sqltext` | SQL cleanup, previews and query kind detection. |
| `internal/tagfilter` | Validation of query tag filters. |
| `internal/timeseries` | Bucket bounds for chart series. |
| `internal/logtaxonomy` | Groups log classifications into the categories the dashboard shows. |
| `internal/humanize` | Durations and counts in Slack messages. |
| `internal/testdb` | PostgreSQL and ClickHouse test containers shared by the integration tests. |

## Commands

| Command | Description |
| --- | --- |
| `make dev-up` / `make dev-down` | Start or remove the dev databases. |
| `make dev` / `make dev-jobs` | Run the API or the scheduler against the dev databases. |
| `make pg-migrate-up` / `make ch-migrate-up` | Apply PostgreSQL or ClickHouse migrations. `-down` and `-status` variants exist for both. |
| `make pg-migrate-create name=<name>` | New PostgreSQL migration in `db/pg-migrations/`. |
| `make ch-migrate-create name=<name>` | New ClickHouse migration in `db/ch-migrations/`. One statement per file. |
| `make test-unit` | Unit tests. |
| `make test` | Unit and integration tests. The integration tests start PostgreSQL and ClickHouse in Docker. |
| `make fmt` | Format the code with golangci-lint. |
| `make lint` | golangci-lint, with and without the `integration` build tag. |
| `make buf-fmt` / `make buf-lint` | Format or lint the proto files. |
| `make buf-generate` | Regenerate `gen/` after a proto change. |
| `make sqlc-generate` | Regenerate `internal/gen/db/` after a change in `db/pg-queries/` or `db/pg-migrations/`. |
| `make tidy` | `go mod tidy`. |
| `make check` | All formatters, linters and generators above. It does not run the tests. |

To run a single test:

```sh
go test ./internal/config/ -run TestParseListenAddr -v -count=1
```

Integration tests carry the `integration` build tag, so add `-tags=integration` to run one of them.

## Changing the wire schema

The proto files in `proto/querysheriff/v1/` are the only definition of the API. The collector and frontend generate their clients from `/schema.zip`.

1. Edit the proto files, then run `make buf-fmt buf-lint buf-generate`.
2. Start the backend with `make dev`.
3. Regenerate the clients: `make proto-update` in collector, `npm run proto:update` in frontend. Commit their generated code.

Deployed collectors can't be updated, so the `Report*` RPCs and every message they send or read keep their names, field numbers and types. Adding a new optional field is the only allowed change. The dashboard RPCs can change freely. Before you push, compare against `master`; no reported change may touch a message the collector uses:

```sh
go run github.com/bufbuild/buf/cmd/buf@v1.70.0 breaking --against '.git#branch=master'
```

## Further reading

- [Deployment guide](https://github.com/querysheriff/docs): database requirements, Helm chart, troubleshooting
