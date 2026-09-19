# Backend

Ingests PostgreSQL stats from collectors and serves them to the frontend.

## Local development

```sh
make dev-up          # postgres, clickhouse and adminer
make pg-migrate-up   # postgres schema
make ch-migrate-up   # clickhouse schema
make dev             # listens on localhost:3000
```

`make seed` inserts dev data (a collector token plus the `admin@dev.dev` / `123123` super admin).

## Check

```sh
make check # buf fmt/lint/generate + sqlc-generate + fmt + lint
```

## Regenerating code

```sh
make buf-generate   # after editing proto/
make sqlc-generate  # after editing db/pg-queries/ or db/pg-migrations/
```

Never hand-edit anything under a `gen/` directory: `gen/` (proto) or `internal/gen/db` (sqlc).

## Build & release

```sh
make release VERSION=0.0.1   # checks, tags v0.0.1, pushes, then CI builds and publishes images to GHCR
```

Or build the image directly:

```sh
docker build -t querysheriff-backend .   # ships the api, jobs and migrate binaries
```
