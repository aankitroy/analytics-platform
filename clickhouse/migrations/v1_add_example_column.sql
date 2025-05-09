-- your-analytics-monorepo/clickhouse/migrations/v1_add_example_column.sql
-- This is a schema migration script.
-- It will be applied to each client's database.

-- Example: Add a new column 'example_string_field' to the events_base table
-- Note: ALTER TABLE ADD COLUMN is an online operation in ClickHouse, generally safe.
-- Other ALTER operations (like changing type) can be more complex.
ALTER TABLE events_base
ADD COLUMN IF NOT EXISTS example_string_field String DEFAULT '';