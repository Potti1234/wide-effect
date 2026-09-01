CREATE TABLE IF NOT EXISTS wide_effect_spans
(
  schema_version UInt16,
  trace_id FixedString(32),
  span_id FixedString(16),
  parent_span_id Nullable(FixedString(16)),
  segment_id Nullable(FixedString(16)),
  service_name LowCardinality(String),
  service_version String,
  environment LowCardinality(String),
  deployment_id Nullable(String),
  commit_hash Nullable(String),
  region Nullable(String),
  instance_id Nullable(String),
  operation LowCardinality(String),
  kind LowCardinality(String),
  outcome LowCardinality(String),
  start_time DateTime64(3, 'UTC'),
  end_time DateTime64(3, 'UTC'),
  duration_ms Float64,
  http_method Nullable(String),
  http_route Nullable(String),
  http_status Nullable(UInt16),
  failure_type Nullable(String),
  failure_category Nullable(String),
  failure_code Nullable(String),
  failure_retryable Nullable(Bool),
  interaction_id Nullable(String),
  request_id Nullable(String),
  ingested_at DateTime64(3, 'UTC'),
  canonical_envelope String,
  INDEX trace_bloom trace_id TYPE bloom_filter GRANULARITY 1,
  INDEX status_minmax http_status TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toDate(start_time)
ORDER BY (service_name, start_time, trace_id, span_id)
TTL toDateTime(start_time) + INTERVAL 7 DAY DELETE;
