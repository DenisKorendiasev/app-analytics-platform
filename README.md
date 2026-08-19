# App Analytics Platform

Backend platform for collecting and analyzing mobile application events. The project is being built incrementally with Go, PostgreSQL, RabbitMQ, and ClickHouse.

## Current increment

Increment 011 adds application rankings backed by ClickHouse:

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
- a ClickHouse Event repository with single-event insertion;
- Worker persistence of each consumed event in ClickHouse;
- RabbitMQ acknowledgement only after a successful ClickHouse insert;
- unacknowledged delivery on ClickHouse persistence failure;
- graceful Worker shutdown that finishes an in-flight ClickHouse insert;
- a ClickHouse connection integrated into the API lifecycle;
- `GET /api/v1/apps/{id}/stats` with event counts and purchase revenue;
- optional `from`, `to`, `country`, and `platform` statistics filters;
- `GET /api/v1/rankings` ordered by install count;
- optional ranking filters and a bounded result limit.

Additional ranking metrics, batch processing, retry/DLQ policies, and application containerization are intentionally outside this increment.

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

The Worker subscribes to `app.events`, inserts each decoded event into ClickHouse, logs the successful persistence as structured JSON, and only then acknowledges the RabbitMQ delivery. A ClickHouse error stops the Worker without acknowledging the failed delivery; RabbitMQ requeues it when the consumer connection closes. Retry and DLQ policies are intentionally deferred.

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

Get application statistics from ClickHouse:

```bash
curl 'http://localhost:8080/api/v1/apps/<app-id>/stats?from=2026-08-01&to=2026-08-18&country=RS&platform=android'
```

Example response:

```json
{
  "app_id": "b8edbe8d-4fa6-42fd-a351-9a98d17d8b83",
  "installs": 12543,
  "sessions": 98421,
  "purchases": 741,
  "revenue_cents": 839231
}
```

`from` and `to` use `YYYY-MM-DD` in UTC. Both dates are inclusive; internally the upper bound is the beginning of the following day. `platform`, when present, must be `android` or `ios`. An application with no matching events returns zero metrics.

Get application rankings from ClickHouse:

```bash
curl 'http://localhost:8080/api/v1/rankings?metric=installs&country=RS&from=2026-08-01&to=2026-08-18&limit=10'
```

Example response:

```json
{
  "metric": "installs",
  "rankings": [
    {
      "app_id": "b8edbe8d-4fa6-42fd-a351-9a98d17d8b83",
      "value": 12543
    }
  ]
}
```

The only supported metric in this increment is `installs`; omitting `metric` selects it by default. Rankings are ordered by value descending and then by application ID ascending for deterministic ties. `limit` defaults to `10` and accepts values from `1` to `100`. Date filtering uses the same inclusive UTC semantics as the Statistics API. Applications without matching installs are omitted.

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

Run the Worker publish/persist/ack end-to-end integration test while RabbitMQ and ClickHouse are healthy:

```bash
RABBITMQ_INTEGRATION_TEST=1 CLICKHOUSE_INTEGRATION_TEST=1 go test ./internal/worker -v
```

Run the isolated ClickHouse repository integration tests, including statistics and rankings queries, while ClickHouse is healthy:

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
