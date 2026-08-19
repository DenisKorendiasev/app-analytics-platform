# App Analytics Platform

Backend platform for collecting and analyzing mobile application events. The project is being built incrementally with Go, PostgreSQL, RabbitMQ, and ClickHouse.

## Current increment

Increment 001 provides the API application foundation:

- a Go HTTP server;
- `GET /health` health check;
- environment-based configuration;
- structured JSON logging;
- graceful shutdown on `SIGINT` and `SIGTERM`.

PostgreSQL, RabbitMQ, ClickHouse, Docker, and business endpoints are intentionally outside this increment.

## Requirements

- Go 1.22 or newer

## Configuration

| Variable | Default | Description |
| --- | --- | --- |
| `HTTP_PORT` | `8080` | HTTP server port (1–65535) |
| `SHUTDOWN_TIMEOUT` | `10s` | Maximum graceful shutdown duration in Go duration format |
| `LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARN`, or `ERROR` |

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

Stop the process with `Ctrl+C`; the server will stop accepting new connections and wait for active requests to complete.

## Verify

```bash
go fmt ./...
go vet ./...
go test ./...
go build ./...
```
