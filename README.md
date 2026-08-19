# App Analytics Platform

Backend platform for collecting and analyzing mobile application events. The project is being built incrementally with Go, PostgreSQL, RabbitMQ, and ClickHouse.

## Current increment

Increment 016 adds reproducible performance measurements:

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
- a RabbitMQ consumer with manual acknowledgement and a 500-message prefetch;
- typed event decoding and structured event logging;
- graceful Worker shutdown that finishes the current delivery before closing RabbitMQ;
- a ClickHouse 26.3 LTS Docker Compose service with health checks;
- environment-based ClickHouse connection settings;
- a native ClickHouse connection pool with verified startup and clean lifecycle;
- reversible `events` table migrations;
- a ClickHouse Event repository with native batch insertion;
- Worker batches of up to 500 events or one second of waiting;
- RabbitMQ acknowledgement only after a successful ClickHouse batch insert;
- unacknowledged delivery on ClickHouse persistence failure;
- graceful Worker shutdown that stops consumption, flushes the remaining batch, and then acknowledges it;
- a ClickHouse connection integrated into the API lifecycle;
- `GET /api/v1/apps/{id}/stats` with event counts and purchase revenue;
- optional `from`, `to`, `country`, and `platform` statistics filters;
- `GET /api/v1/rankings` ordered by install count;
- optional ranking filters and a bounded result limit;
- one multi-stage Dockerfile with dedicated API and Worker targets;
- non-root runtime containers with only the compiled application and runtime certificates;
- a complete Docker Compose stack for PostgreSQL, RabbitMQ, ClickHouse, API, and Worker;
- dependency health checks and ordered application startup;
- automatic schema initialization for fresh PostgreSQL and ClickHouse volumes;
- configurable host ports for isolated local stacks;
- one tagged integration suite that starts PostgreSQL, RabbitMQ, and ClickHouse with Testcontainers;
- isolated PostgreSQL schemas, ClickHouse databases, and RabbitMQ topologies for every test;
- PostgreSQL repository create/read/existence, not-found, constraint, and migration coverage;
- RabbitMQ publish, consume, persistence, and JSON message-format coverage;
- ClickHouse single insert, batch insert, statistics, rankings, and migration coverage;
- an end-to-end HTTP scenario from application creation and event ingestion through Worker processing to ClickHouse statistics;
- a third Go command at `cmd/generator` for producing configurable application and event volumes;
- synthetic application names, publishers, and categories;
- balanced install, session, and purchase events across generated applications;
- randomized countries, Android/iOS platforms, and timestamps from the previous 30 days;
- positive purchase revenue with zero revenue for non-purchase events;
- all generated data sent through the public API and the real ingestion pipeline;
- an isolated ClickHouse performance command comparing native inserts with batch sizes 1, 100, 500, and 1000;
- raw and median events/sec, processing-duration, and ClickHouse insert-duration measurements;
- automatic temporary-database creation and cleanup that leaves the source events table unchanged;
- `EXPLAIN indexes = 1` analysis of the per-application statistics query on 100,000 synthetic events;
- measured methodology, environment, raw results, limitations, and reproduction steps in `docs/performance.md`.

Additional ranking metrics, retry/DLQ policies, and continuous integration are intentionally outside this increment.

## Requirements

- Go 1.25 or newer for running outside containers
- Docker with Docker Compose for the complete local platform

## Configuration

| Variable | Default | Description |
| --- | --- | --- |
| `HTTP_PORT` | `8080` | HTTP server port (1–65535) |
| `API_HOST_PORT` | `8080` | Host port published for the containerized API |
| `SHUTDOWN_TIMEOUT` | `10s` | Maximum graceful shutdown duration in Go duration format |
| `LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARN`, or `ERROR` |
| `POSTGRES_HOST` | `localhost` | PostgreSQL host |
| `POSTGRES_PORT` | `5432` | PostgreSQL port (1–65535) |
| `POSTGRES_DB` | `app_analytics` | PostgreSQL database |
| `POSTGRES_USER` | `app_analytics` | PostgreSQL user |
| `POSTGRES_PASSWORD` | `app_analytics` | Local development password; override outside local development |
| `POSTGRES_SSLMODE` | `disable` | PostgreSQL SSL mode |
| `POSTGRES_HOST_PORT` | `5432` | Host port published for PostgreSQL |
| `RABBITMQ_URL` | `amqp://app_analytics:app_analytics@localhost:5672/` | RabbitMQ connection URL |
| `RABBITMQ_EXCHANGE` | `app.events` | Durable direct exchange |
| `RABBITMQ_QUEUE` | `app.events` | Durable event queue |
| `RABBITMQ_ROUTING_KEY` | `app.events` | Queue binding and publish routing key |
| `RABBITMQ_HOST_PORT` | `5672` | Host port published for AMQP |
| `RABBITMQ_MANAGEMENT_HOST_PORT` | `15672` | Host port published for RabbitMQ Management UI |
| `CLICKHOUSE_HOST` | `localhost` | ClickHouse host |
| `CLICKHOUSE_PORT` | `9000` | ClickHouse native protocol port (1–65535) |
| `CLICKHOUSE_DATABASE` | `app_analytics` | ClickHouse database |
| `CLICKHOUSE_USER` | `app_analytics` | ClickHouse user |
| `CLICKHOUSE_PASSWORD` | `app_analytics` | Local development password; override outside local development |
| `CLICKHOUSE_HTTP_HOST_PORT` | `8123` | Host port published for ClickHouse HTTP protocol |
| `CLICKHOUSE_NATIVE_HOST_PORT` | `9000` | Host port published for ClickHouse native protocol |

Copy `.env.example` to `.env` to customize Docker Compose. Export the same values in your shell when running the API with non-default settings.

## Start the platform with Docker Compose

```bash
cp .env.example .env
docker compose up --build -d
docker compose ps
```

Compose builds separate API and Worker targets from the same multi-stage Dockerfile. A small ClickHouse target adds the database-aware init script to the official image. The API becomes healthy only after PostgreSQL, RabbitMQ, and ClickHouse are healthy; the Worker also waits for RabbitMQ and ClickHouse. The API is available at [http://localhost:8080](http://localhost:8080) by default.

On first startup, the database images apply the checked-in `apps` and `events` migrations while initializing their data volumes. Existing volumes are preserved and are not initialized again. Use the manual commands below when applying or rolling back migrations in an existing development volume.

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

To run the Go applications directly, start only the infrastructure and apply migrations as described above:

```bash
docker compose up -d postgres rabbitmq clickhouse
```

Start the API:

```bash
go run ./cmd/api
```

Start the Worker in another terminal:

```bash
go run ./cmd/worker
```

The Worker subscribes to `app.events` with a prefetch of 500 and starts a batch timer when the first delivery enters an empty batch. It flushes when the batch reaches 500 events or after one second, whichever happens first. ClickHouse receives the whole batch through one native insert, and RabbitMQ deliveries are acknowledged only after that insert succeeds. A ClickHouse error leaves the entire batch unacknowledged; RabbitMQ requeues those deliveries when the consumer connection closes. During graceful shutdown the Worker stops accepting deliveries, flushes the remaining batch, acknowledges successful messages, and then releases its resources. Retry and DLQ policies are intentionally deferred.

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

Stop the containerized platform without deleting persisted data:

```bash
docker compose down
```

## Integration tests

The integration suite requires a running Docker daemon, but it does not require the Compose stack or any integration-test environment variables. Testcontainers starts one PostgreSQL, RabbitMQ, and ClickHouse container for the suite and removes them afterward. Individual tests create isolated schemas, databases, and messaging topologies.

```bash
go test -tags=integration ./test/integration -v -count=1
```

The `integration` build tag keeps the ordinary unit-test loop fast. The end-to-end test exercises the HTTP application and event handlers, PostgreSQL application repository, RabbitMQ publisher and consumer, Worker batching, ClickHouse event repository, and Statistics API in one scenario.

## Generate synthetic data

Start the complete Compose stack, then run the generator against its public API:

```bash
go run ./cmd/generator \
  --api-url=http://localhost:8080 \
  --events=100000 \
  --apps=100
```

`--api-url` defaults to `http://localhost:8080`, `--apps` defaults to `10`, and `--events` defaults to `1000`. Both counts must be greater than zero. Applications are created before events, so every generated event references an application accepted by the API. The command stops on the first rejected request, supports interruption with `Ctrl+C`, and prints a JSON summary after success:

```json
{"apps_created":100,"events_accepted":100000}
```

The generator deliberately reports accepted counts. Use the isolated performance command below for ClickHouse insertion measurements rather than treating HTTP generation time as a storage benchmark.

## Measure ClickHouse performance

Start ClickHouse, then run the reproducible insertion and analytics measurement:

```bash
go run ./cmd/performance \
  --events=1000 \
  --runs=3 \
  --batches=1,100,500,1000
```

The command uses the standard `CLICKHOUSE_*` configuration, clones the configured `events` schema into a temporary database, and removes that database when it finishes. The source database is not modified. Its JSON report contains every run and the median metrics for each batch size, followed by a real statistics-query result and its ClickHouse index plan.

See [docs/performance.md](docs/performance.md) for the exact methodology, measured environment, raw numbers, interpretation, limitations, and isolated Compose reproduction commands.

## Verify

```bash
go fmt ./...
go vet ./...
go test ./...
go test -race ./...
go test -tags=integration ./test/integration -v -count=1
go build ./...
go run ./cmd/generator --help
go run ./cmd/performance --help
docker build --target api -t app-analytics-api:local .
docker build --target worker -t app-analytics-worker:local .
docker compose config
docker compose up -d
docker compose ps
```
