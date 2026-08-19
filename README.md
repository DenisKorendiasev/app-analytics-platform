# App Analytics Platform

Backend platform for collecting and analyzing mobile application events. The project is being built incrementally with Go, PostgreSQL, RabbitMQ, and ClickHouse.

## Current increment

Increment 004 exposes the App feature over HTTP:

- a Go HTTP server;
- `GET /health` health check;
- environment-based configuration;
- structured JSON logging;
- graceful shutdown on `SIGINT` and `SIGTERM`;
- a PostgreSQL connection pool verified before the HTTP server starts;
- a PostgreSQL-only Docker Compose service;
- clean pool shutdown after the HTTP server stops;
- the `App` domain model and service validation;
- PostgreSQL `App` repository operations (`Create`, `GetByID`, and `Exists`);
- reversible SQL migrations for the `apps` table;
- `POST /api/v1/apps` to create an application;
- `GET /api/v1/apps/{id}` to retrieve an application.

RabbitMQ, ClickHouse, event ingestion, and API containerization are intentionally outside this increment.

## Requirements

- Go 1.25 or newer
- Docker with Docker Compose for local PostgreSQL

## Configuration

| Variable | Default | Description |
| --- | --- | --- |
| `HTTP_PORT` | `8080` | HTTP server port (1–65535) |
| `SHUTDOWN_TIMEOUT` | `10s` | Maximum graceful shutdown duration in Go duration format |
| `LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARN`, or `ERROR` |
| `POSTGRES_HOST` | `localhost` | PostgreSQL host |
| `POSTGRES_PORT` | `5432` | PostgreSQL port (1–65535) |
| `POSTGRES_DB` | `app_analytics` | PostgreSQL database |
| `POSTGRES_USER` | `app_analytics` | PostgreSQL user |
| `POSTGRES_PASSWORD` | `app_analytics` | Local development password; override outside local development |
| `POSTGRES_SSLMODE` | `disable` | PostgreSQL SSL mode |

Copy `.env.example` to `.env` to customize Docker Compose. Export the same values in your shell when running the API with non-default settings.

## Start PostgreSQL

```bash
cp .env.example .env
docker compose up -d postgres
docker compose ps
```

Apply or roll back the `apps` table manually with the local development credentials:

```bash
docker compose exec -T postgres psql -U app_analytics -d app_analytics < migrations/postgres/000001_create_apps.up.sql
docker compose exec -T postgres psql -U app_analytics -d app_analytics < migrations/postgres/000001_create_apps.down.sql
```

## Run locally

```bash
go run ./cmd/api
```

Check the service:

```bash
curl http://localhost:8080/health
```

Expected response:

```json
{"status":"ok"}
```

Create and retrieve an application after applying the up migration:

```bash
curl -i -X POST http://localhost:8080/api/v1/apps \
  -H 'Content-Type: application/json' \
  -d '{"name":"Spotify","publisher":"Spotify AB","category":"music"}'

curl -i http://localhost:8080/api/v1/apps/<app-id>
```

Stop the process with `Ctrl+C`; the server will stop accepting new connections and wait for active requests to complete.

Run the PostgreSQL integration tests while the Compose service is healthy. The repository test applies the up migration and verifies the down migration:

```bash
POSTGRES_INTEGRATION_TEST=1 go test ./internal/postgres -v
```

## Verify

```bash
go fmt ./...
go vet ./...
go test ./...
go build ./...
docker compose config
```
