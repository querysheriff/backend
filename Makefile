GOLANGCI := go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2
BUF := go run github.com/bufbuild/buf/cmd/buf@v1.70.0
SQLC := go run github.com/sqlc-dev/sqlc/cmd/sqlc@v1.31.1
GOOSE := go run github.com/pressly/goose/v3/cmd/goose@v3.27.1

POSTGRES_URL ?= postgres://querysheriff_backend:querysheriff_backend@localhost:5432/querysheriff?sslmode=disable
CLICKHOUSE_URL ?= clickhouse://querysheriff_backend:querysheriff_backend@localhost:9000/querysheriff
PG_MIGRATIONS_DIR := db/pg-migrations
CH_MIGRATIONS_DIR := db/ch-migrations

.PHONY: check
check:
	$(MAKE) buf-fmt
	$(MAKE) buf-lint
	$(MAKE) buf-generate
	$(MAKE) sqlc-generate
	$(MAKE) tidy
	$(MAKE) fmt
	$(MAKE) lint

.PHONY: buf-generate
buf-generate:
	$(BUF) generate

.PHONY: buf-lint
buf-lint:
	$(BUF) lint

.PHONY: buf-fmt
buf-fmt:
	$(BUF) format -w

.PHONY: lint
lint:
	$(GOLANGCI) run -c .golangci.yml
	$(GOLANGCI) run -c .golangci.yml --build-tags=integration

.PHONY: fmt
fmt:
	$(GOLANGCI) fmt -c .golangci.yml

.PHONY: sqlc-generate
sqlc-generate:
	$(SQLC) generate

.PHONY: tidy
tidy:
	go mod tidy

.PHONY: pg-migrate-up
pg-migrate-up:
	$(GOOSE) -dir $(PG_MIGRATIONS_DIR) postgres "$(POSTGRES_URL)" up

.PHONY: pg-migrate-down
pg-migrate-down:
	$(GOOSE) -dir $(PG_MIGRATIONS_DIR) postgres "$(POSTGRES_URL)" down

.PHONY: pg-migrate-status
pg-migrate-status:
	$(GOOSE) -dir $(PG_MIGRATIONS_DIR) postgres "$(POSTGRES_URL)" status

.PHONY: pg-migrate-create
pg-migrate-create:
	$(GOOSE) -dir $(PG_MIGRATIONS_DIR) create $(name) sql

.PHONY: ch-migrate-up
ch-migrate-up:
	$(GOOSE) -dir $(CH_MIGRATIONS_DIR) clickhouse "$(CLICKHOUSE_URL)" up

.PHONY: ch-migrate-down
ch-migrate-down:
	$(GOOSE) -dir $(CH_MIGRATIONS_DIR) clickhouse "$(CLICKHOUSE_URL)" down

.PHONY: ch-migrate-status
ch-migrate-status:
	$(GOOSE) -dir $(CH_MIGRATIONS_DIR) clickhouse "$(CLICKHOUSE_URL)" status

.PHONY: ch-migrate-create
ch-migrate-create:
	$(GOOSE) -dir $(CH_MIGRATIONS_DIR) create $(name) sql

.PHONY: seed
seed:
	POSTGRES_URL="$(POSTGRES_URL)" go run ./cmd/seed

.PHONY: dev-up
dev-up:
	docker compose -f dev/docker-compose.yml up -d --wait

.PHONY: dev-down
dev-down:
	docker compose -f dev/docker-compose.yml down -v

.PHONY: dev
dev:
	POSTGRES_URL="$(POSTGRES_URL)" CLICKHOUSE_URL="$(CLICKHOUSE_URL)" go run ./cmd/api

.PHONY: dev-jobs
dev-jobs:
	POSTGRES_URL="$(POSTGRES_URL)" CLICKHOUSE_URL="$(CLICKHOUSE_URL)" go run ./cmd/jobs

.PHONY: test
test:
	go test -tags=integration -p 1 ./...

.PHONY: test-unit
test-unit:
	go test ./...

# Usage: `make release VERSION=0.0.1`.
# Validates -> tags -> pushes -> fires .github/workflows/release.yml -> builds and publishes images to GHCR.
.PHONY: release
release:
	@echo "$(VERSION)" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$$' \
		|| { echo "error: pass a semantic version, e.g. make release VERSION=0.1.0"; exit 1; }
	@git diff --quiet && git diff --cached --quiet \
		|| { echo "error: uncommitted changes — commit them before releasing"; exit 1; }
	@if git rev-parse -q --verify "refs/tags/v$(VERSION)" >/dev/null; then \
		echo "error: tag v$(VERSION) already exists"; exit 1; fi
	$(MAKE) check
	@git diff --quiet && git diff --cached --quiet \
		|| { echo "error: 'make check' reformatted files — commit them, then re-run"; exit 1; }
	git tag -a "v$(VERSION)" -m "v$(VERSION)"
	git push origin "v$(VERSION)"
	@echo "Tagged and pushed v$(VERSION). GitHub Actions is building and publishing the images."