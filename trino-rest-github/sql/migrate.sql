-- Execute these statements to generate queries to migrate data from old schema to new one.
-- Replace `default` and `attempts` with valid schema names.
SELECT '-- Create `attempts` and create tables by executing base.sql';
SELECT '-- Alter defaults for new columns in the statements below if needed';

WITH columns AS (
  SELECT table_schema, table_name, listagg(column_name , ', ') WITHIN GROUP (ORDER BY ordinal_position) AS column_names
  FROM hive.information_schema.columns
  GROUP BY 1, 2
  ORDER BY 1, 2
)
SELECT
  'INSERT INTO hive.attempts.' || table_name || ' (' || (SELECT c.column_names FROM columns c WHERE c.table_schema = t.table_schema AND c.table_name = t.table_name) || ') ' ||
  'SELECT * FROM hive.default.' || table_name || ';'
FROM hive.information_schema.tables t
WHERE table_schema = 'default';
