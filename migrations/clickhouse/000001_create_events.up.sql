CREATE TABLE events (
    event_id UUID,
    app_id UUID,
    event_type LowCardinality(String),
    country LowCardinality(String),
    platform LowCardinality(String),
    revenue_cents Int64,
    timestamp DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (app_id, timestamp, event_id);
