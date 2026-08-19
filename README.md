# App Analytics Platform

Backend platform for collecting and analyzing mobile application events. The project is being built incrementally with Go, PostgreSQL, RabbitMQ, and ClickHouse.

## Current increment

Increment 008 adds ClickHouse infrastructure:

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
- `POST /api/v1/events` to publish accepted events to RabbitMQ;
- a second Go application at `cmd/worker`;
- a RabbitMQ consumer with manual acknowledgement and a one-message prefetch;
- typed event decoding and structured event logging;
- graceful Worker shutdown that finishes the current delivery before closing RabbitMQ;
- a ClickHouse 26.3 LTS Docker Compose service with health checks;
- environment-based ClickHouse connection settings;
- a native ClickHouse connection pool with verified startup and clean lifecycle;
- reversible `events` table migrations;
- a ClickHouse Event repository with single-event insertion.

Worker-to-ClickHouse persistence, analytics APIs, batch processing, retry/DLQ policies, and application containerization are intentionally outside this increment.

## Requirements

- Go 1.25 or newer
- Docker with Docker Compose for local PostgreSQL, RabbitMQ, and ClickHouse

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
| `CLICKHOUSE_HOST` | `localhost` | ClickHouse host |
| `CLICKHOUSE_PORT` | `9000` | ClickHouse native protocol port (1–65535) |
| `CLICKHOUSE_DATABASE` | `app_analytics` | ClickHouse database |
| `CLICKHOUSE_USER` | `app_analytics` | ClickHouse user |
| `CLICKHOUSE_PASSWORD` | `app_analytics` | Local development password; override outside local development |

Copy `.env.example` to `.env` to customize Docker Compose. Export the same values in your shell when running the API with non-default settings.

## Start infrastructure

```bash
cp .env.example .env
docker compose up -d postgres rabbitmq clickhouse
docker compose ps
```

RabbitMQ Management UI is available at [http://localhost:15672](http://localhost:15672) with the local credentials from `.env.example`.

Apply or roll back the `apps` table manually with the local development credentials:

```bash
docker compose exec -T postgres psql -U app_analytics -d app_analytics < migrations/postgres/000001_create_apps.up.sql
docker compose exec -T postgres psql -U app_analytics -d app_analytics < migrations/postgres/000001_create_apps.down.sql
```

Apply or roll back the ClickHouse `events` table:

```bash
docker compose exec -T clickhouse clickhouse-client --user app_analytics --password app_analytics --database app_analytics < migrations/clickhouse/000001_create_events.up.sql
docker compose exec -T clickhouse clickhouse-client --user app_analytics --password app_analytics --database app_analytics < migrations/clickhouse/000001_create_events.down.sql
```

The `events` table uses `MergeTree`, partitions by event month, and sorts by `(app_id, timestamp, event_id)`. This append-oriented layout supports future per-application time-range analytics while retaining deterministic ordering within equal timestamps. Event type, country, and platform use `LowCardinality(String)` because they are repeated analytical dimensions.

## Run locally

Start the API:

```bash
go run ./cmd/api
```

Start the Worker in another terminal:

```bash
go run ./cmd/worker
```

The Worker subscribes to `app.events`, logs each decoded event as structured JSON, and acknowledges it after the current log-only processing completes.

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

Run the Worker publish/receive/log/ack integration test while RabbitMQ is healthy:

```bash
RABBITMQ_INTEGRATION_TEST=1 go test ./internal/worker -v
```

Run the isolated ClickHouse migration/insert/select/down integration test while ClickHouse is healthy:

```bash
CLICKHOUSE_INTEGRATION_TEST=1 go test ./internal/clickhouse -v
```

## Verify

```bash
go fmt ./...
go vet ./...
go test ./...
go build ./...
docker compose config
```
