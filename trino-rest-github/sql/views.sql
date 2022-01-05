CREATE OR REPLACE VIEW unique_pulls SECURITY INVOKER AS
WITH latest_pulls AS (
  SELECT id, max(updated_at) AS updated_at
  FROM pulls
  GROUP BY id
)
SELECT p.*
FROM pulls p
JOIN latest_pulls latest ON (latest.id, latest.updated_at) = (p.id, p.updated_at);
