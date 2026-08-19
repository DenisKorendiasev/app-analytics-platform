# App Analytics Platform

Backend platform for collecting and analyzing mobile application events. The project is being built incrementally with Go, PostgreSQL, RabbitMQ, and ClickHouse.

## Current increment

Increment 006 adds the Event ingestion API:

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
- `GET /api/v1/apps/{id}` to retrieve an application;
- a durable RabbitMQ direct exchange, queue, and binding;
- a persistent-message RabbitMQ publisher connected to the API lifecycle;
- RabbitMQ Management UI for local development;
- the `Event` domain model and validation for supported event types and platforms;
- application existence checks before event acceptance;
- `POST /api/v1/events` to publish accepted events to RabbitMQ.

Worker consumption, ClickHouse storage and analytics, retry/DLQ policies, and API containerization are intentionally outside this increment.

## Requirements

- Go 1.25 or newer
- Docker with Docker Compose for local PostgreSQL and RabbitMQ

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
| `RABBITMQ_URL` | `amqp://app_analytics:app_analytics@localhost:5672/` | RabbitMQ connection URL |
| `RABBITMQ_EXCHANGE` | `app.events` | Durable direct exchange |
| `RABBITMQ_QUEUE` | `app.events` | Durable event queue |
| `RABBITMQ_ROUTING_KEY` | `app.events` | Queue binding and publish routing key |

Copy `.env.example` to `.env` to customize Docker Compose. Export the same values in your shell when running the API with non-default settings.

## Start infrastructure

```bash
cp .env.example .env
docker compose up -d postgres rabbitmq
docker compose ps
```

RabbitMQ Management UI is available at [http://localhost:15672](http://localhost:15672) with the local credentials from `.env.example`.

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

Publish an event for an existing application. A valid request returns `202 Accepted` and the generated event is written to the `app.events` RabbitMQ queue:

```bash
curl -i -X POST http://localhost:8080/api/v1/events \
  -H 'Content-Type: application/json' \
  -d '{"app_id":"<app-id>","event_type":"purchase","country":"RS","platform":"android","revenue_cents":999,"timestamp":"2026-08-18T12:35:02Z"}'
```

Supported event types are `install`, `session`, and `purchase`. Supported platforms are `android` and `ios`. Purchase revenue must be a non-negative integer number of cents.

Stop the process with `Ctrl+C`; the server will stop accepting new connections and wait for active requests to complete.

Run the PostgreSQL integration tests while the Compose service is healthy. The repository test applies the up migration and verifies the down migration:

```bash
POSTGRES_INTEGRATION_TEST=1 go test ./internal/postgres -v
```

Run the RabbitMQ publish/consume integration test while RabbitMQ is healthy:

```bash
RABBITMQ_INTEGRATION_TEST=1 go test ./internal/rabbitmq -v
```

Run the complete Event ingestion integration test against both services:

```bash
POSTGRES_INTEGRATION_TEST=1 RABBITMQ_INTEGRATION_TEST=1 go test ./internal/event -v
```

## Verify

```bash
go fmt ./...
go vet ./...
go test ./...
go build ./...
docker compose config
```
