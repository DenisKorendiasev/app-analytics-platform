#!/bin/bash

clickhouse-client \
  --multiquery \
  --host 127.0.0.1 \
  --port 9000 \
  --user "$CLICKHOUSE_USER" \
  --password "$CLICKHOUSE_PASSWORD" \
  --database "$CLICKHOUSE_DB" \
  < /migrations/000001_create_events.up.sql
