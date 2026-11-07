<p align="center">
  <img src="https://raw.githubusercontent.com/querysheriff/frontend/master/static/logo.svg" alt="QuerySheriff logo" width="96">
</p>

<h1 align="center">QuerySheriff backend</h1>

The backend is a Go service that receives statistics and logs from the [collectors](https://github.com/querysheriff/collector) over Connect RPC, stores them, and serves them to the QuerySheriff [dashboard](https://github.com/querysheriff/frontend). Monitoring data goes to ClickHouse, configuration to PostgreSQL.

It runs as three binaries:

- `querysheriff-api`: API for collectors and the dashboard
- `querysheriff-jobs`: alert scheduler
- `querysheriff-migrate`: database migrations

## Installation

All three binaries ship in one Docker image, `ghcr.io/querysheriff/backend` (linux/amd64). The Helm chart in the [deployment guide](https://github.com/querysheriff/docs/blob/master/deployment.md#4-install-the-backend-and-frontend) installs it together with the frontend.

## Configuration

Configuration comes from environment variables. Only `POSTGRES_URL` and `CLICKHOUSE_URL` are required.

| Variable | Default | Description |
| --- | --- | --- |
| `POSTGRES_URL` | required | PostgreSQL URL |
| `CLICKHOUSE_URL` | required | ClickHouse native protocol URL |
| `LISTEN_ADDR` | `localhost:3000` | API address |
| `CORS_ALLOWED_ORIGINS` | `http://localhost:3001` | Allowed browser origins, comma-separated |
| `COOKIE_SECURE` | `false` | `true` when served over HTTPS |
| `DASHBOARD_URL` | empty | Dashboard link in weekly reports |
| `RETENTION_DAYS` | `30` | Days of ClickHouse data to keep, max 365 |
| `DEV_AUTO_LOGIN` | `false` | Skip login. Only works on a loopback `LISTEN_ADDR`. |

## Development

Requires Go 1.26.3 and Docker.

```sh
cp .env.example .env
make dev-up                        # PostgreSQL and ClickHouse in Docker
make pg-migrate-up ch-migrate-up
make dev                           # API on http://localhost:3000
```
