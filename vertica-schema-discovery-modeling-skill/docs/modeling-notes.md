# Modeling Notes

## Fact vs Dimension quick test
- Fact table: large row count, event-like grain, numeric measures, foreign keys
- Dimension: lower row count, descriptive attributes, slowly changing

## Relationship validation queries (safe)
- NULL rate:
  SELECT 100.0 * SUM(CASE WHEN key IS NULL THEN 1 ELSE 0 END) / COUNT(*) AS null_pct FROM schema.table;

- distinct count:
  SELECT COUNT(*) AS rows, COUNT(DISTINCT key) AS distinct_keys FROM schema.table;

- join match rate:
  SELECT
    COUNT(*) AS left_rows,
    SUM(CASE WHEN r.key IS NULL THEN 1 ELSE 0 END) AS unmatched
  FROM left l
  LEFT JOIN right r ON l.key = r.key;
