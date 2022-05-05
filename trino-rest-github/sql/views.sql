CREATE OR REPLACE VIEW unique_pulls SECURITY INVOKER AS
WITH latest AS (
  SELECT id, max(updated_at) AS updated_at
  FROM pulls
  GROUP BY id
)
SELECT p.*
FROM pulls p
JOIN latest ON (latest.id, latest.updated_at) = (p.id, p.updated_at);

CREATE OR REPLACE VIEW unique_pull_commits SECURITY INVOKER AS
WITH latest AS (
  SELECT pull_number, parent_shas, max(committer_date) AS committer_date
  FROM pull_commits
  GROUP BY pull_number, parent_shas
)
SELECT p.*
FROM pull_commits c
JOIN latest ON (latest.pull_number, latest.parent_shas, latest.committer_date) = (c.pull_number, c.parent_shas, c.committer_date);

CREATE OR REPLACE VIEW unique_review_comments SECURITY INVOKER AS
WITH latest AS (
  SELECT id, max(updated_at) AS updated_at
  FROM review_comments
  GROUP BY id
)
SELECT r.*
FROM review_comments r
JOIN latest ON (latest.id, latest.updated_at) = (r.id, r.updated_at);

CREATE OR REPLACE VIEW unique_issues SECURITY INVOKER AS
WITH latest AS (
  SELECT id, max(updated_at) AS updated_at
  FROM issues
  GROUP BY id
)
SELECT i.*
FROM issues i
JOIN latest ON (latest.id, latest.updated_at) = (i.id, i.updated_at);

CREATE OR REPLACE VIEW unique_issue_comments SECURITY INVOKER AS
WITH latest AS (
  SELECT id, max(updated_at) AS updated_at
  FROM issue_comments
  GROUP BY id
)
SELECT i.*
FROM issue_comments i
JOIN latest ON (latest.id, latest.updated_at) = (i.id, i.updated_at);
