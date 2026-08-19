FROM golang:1.25-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

FROM build AS api-build
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/api ./cmd/api

FROM build AS worker-build
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/worker ./cmd/worker

FROM alpine:3.22 AS runtime

RUN apk add --no-cache ca-certificates \
    && addgroup -S app \
    && adduser -S -G app app

USER app

FROM runtime AS api
COPY --from=api-build /out/api /app/api
EXPOSE 8080
ENTRYPOINT ["/app/api"]

FROM runtime AS worker
COPY --from=worker-build /out/worker /app/worker
ENTRYPOINT ["/app/worker"]

FROM clickhouse/clickhouse-server:26.3-alpine AS clickhouse
COPY --chmod=0644 docker/clickhouse-init.sh /docker-entrypoint-initdb.d/000001_create_events.sh
COPY --chmod=0644 migrations/clickhouse/000001_create_events.up.sql /migrations/000001_create_events.up.sql
