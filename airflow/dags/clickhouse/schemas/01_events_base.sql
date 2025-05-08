-- Example content of 01_events_base.sql
CREATE TABLE IF NOT EXISTS events_base (
  event_id         UUID,
  event_time       DateTime,
  event_properties JSON
)
ENGINE = ReplacingMergeTree(event_time)
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_id, event_time);