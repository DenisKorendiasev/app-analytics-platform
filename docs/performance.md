# Performance Measurements

This document records reproducible performance measurements. The numbers below were produced by a real local run on August 19, 2026; they are not estimates.

## Scope

The insertion benchmark isolates the production ClickHouse `EventRepository` and compares the cost of writing the same events with native batches of 1, 100, 500, and 1000 rows. It does not include HTTP ingestion, PostgreSQL application lookup, RabbitMQ publishing or consumption, or acknowledgement latency, so these results are not end-to-end platform throughput or an SLA.

The measurement command creates a uniquely named temporary ClickHouse database, clones the current `events` table schema into it, and removes that database after the report is produced. It never truncates or inserts into the configured source database.

## Environment

| Component | Measured environment |
| --- | --- |
| Host | Apple M4, 16 GiB RAM, arm64 |
| Operating system | Darwin 25.5.0 |
| Docker Desktop allocation | 8 CPUs, 12,529,250,304 bytes memory |
| Docker Engine | 29.1.2 |
| ClickHouse | 26.3.19.3 |
| Go | 1.26.2 darwin/arm64 |
| Transport | Native ClickHouse protocol over localhost |

Docker Desktop storage, host load, thermal state, and hardware can materially change the results. Compare future changes on the same machine and configuration.

## Method

The measured command was:

```bash
CLICKHOUSE_PORT=39100 go run ./cmd/performance \
  --events=1000 \
  --runs=3 \
  --batches=1,100,500,1000
```

The harness generated one deterministic set of 1000 events and reused it for every scenario. Before measurement it inserted up to 1000 rows once to warm the connection, then truncated only the temporary table. Each scenario performed three sequential runs; the table was truncated before every run.

Metrics are defined as follows:

- `processing duration`: wall-clock time around the complete insertion loop, excluding event generation, warm-up, and table reset;
- `ClickHouse insert duration`: sum of wall-clock time spent inside `EventRepository.InsertBatch` calls;
- `events/sec`: event count divided by processing duration;
- reported table values: median of the three observed runs, not an average or extrapolation.

## Insertion results

| Batch size | Insert operations per run | Processing duration, ms | ClickHouse insert duration, ms | Events/sec | Relative to single insert |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1000 | 62,883.317 | 62,883.082 | 15.90 | 1.0x |
| 100 | 10 | 601.273 | 601.270 | 1,663.14 | 104.6x |
| 500 | 2 | 127.519 | 127.518 | 7,841.99 | 493.1x |
| 1000 | 1 | 66.019 | 66.019 | 15,147.05 | 952.5x |

Raw observations:

| Batch size | Run | Processing duration, ms | ClickHouse insert duration, ms | Events/sec |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 1 | 62,722.511 | 62,722.267 | 15.94 |
| 1 | 2 | 62,883.317 | 62,883.082 | 15.90 |
| 1 | 3 | 62,906.529 | 62,906.302 | 15.90 |
| 100 | 1 | 635.532 | 635.529 | 1,573.49 |
| 100 | 2 | 601.273 | 601.270 | 1,663.14 |
| 100 | 3 | 554.857 | 554.855 | 1,802.27 |
| 500 | 1 | 127.519 | 127.518 | 7,841.99 |
| 500 | 2 | 126.337 | 126.337 | 7,915.32 |
| 500 | 3 | 136.950 | 136.950 | 7,301.93 |
| 1000 | 1 | 57.921 | 57.921 | 17,264.86 |
| 1000 | 2 | 66.019 | 66.019 | 15,147.05 |
| 1000 | 3 | 71.588 | 71.588 | 13,968.79 |

On this setup the insertion call dominates the processing duration. Reducing the number of native insert operations produced the largest gain; batch 1000 was fastest among the tested sizes. This result supports the existing Worker batch size of 500 as a substantial improvement over single-row insertion, but it does not by itself prove that 1000 is the best production Worker setting because larger batches also affect queue latency, memory use, acknowledgement timing, and failure replay size.

## Analytics query analysis

The harness separately loaded 100,000 deterministic events in batches of 1000, ran `OPTIMIZE TABLE events FINAL`, and analyzed the same per-application aggregation shape used by the Statistics API:

```sql
SELECT
    countIf(event_type = 'install'),
    countIf(event_type = 'session'),
    countIf(event_type = 'purchase'),
    sumIf(revenue_cents, event_type = 'purchase')
FROM events
WHERE app_id = ?;
```

It used ClickHouse `EXPLAIN indexes = 1`. The relevant observed plan was:

```text
ReadFromMergeTree (...events)
Indexes:
  MinMax
    Parts: 2/2
    Granules: 13/13
  Partition
    Parts: 2/2
    Granules: 13/13
  PrimaryKey
    Keys:
      app_id
    Parts: 2/2
    Granules: 2/13
    Search Algorithm: binary search
```

The actual aggregation returned 334 installs, 333 sessions, 333 purchases, and 1,694,157 revenue cents for the selected application. Its observed wall-clock duration was 4.220 ms.

The table order key starts with `app_id`, so the primary-key index reduced the read from 13 granules to 2 for this application. The monthly partition key could not prune parts because the query had no timestamp filter; both parts were considered. A date-bounded Statistics API query can additionally benefit from the timestamp component of the order key and, when it excludes whole months, partition pruning.

This is a small synthetic dataset and a single query observation. It demonstrates that the intended index is used, but it is not a production latency distribution.

## Reproduce

Start an isolated ClickHouse service on unused host ports:

```bash
COMPOSE_PROJECT_NAME=performance \
CLICKHOUSE_HTTP_HOST_PORT=38124 \
CLICKHOUSE_NATIVE_HOST_PORT=39100 \
docker compose up -d --wait clickhouse
```

Run the measurement and retain the JSON output if results need to be compared:

```bash
CLICKHOUSE_PORT=39100 go run ./cmd/performance \
  --events=1000 \
  --runs=3 \
  --batches=1,100,500,1000
```

Stop the isolated service and remove only its test volume:

```bash
COMPOSE_PROJECT_NAME=performance \
CLICKHOUSE_HTTP_HOST_PORT=38124 \
CLICKHOUSE_NATIVE_HOST_PORT=39100 \
docker compose down -v
```

The command requires the configured source database to contain the migrated `events` table. Its JSON report includes all raw runs, medians, the measured analytics result, and the full `EXPLAIN indexes = 1` plan.
