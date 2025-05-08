-- Materialized view for daily event counts
-- This view automatically aggregates events by date
-- Useful for quick daily event count queries without scanning the full events table

CREATE MATERIALIZED VIEW IF NOT EXISTS events_aggregated
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY date AS
SELECT
    toDate(event_time) AS date,
    count()            AS event_count
FROM events_base
GROUP BY date; 