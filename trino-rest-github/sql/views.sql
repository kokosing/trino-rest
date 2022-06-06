CREATE OR REPLACE VIEW unique_pulls SECURITY INVOKER AS
WITH latest AS (
  SELECT *, row_number() OVER (PARTITION BY owner, repo, number ORDER BY updated_at DESC) AS rownum
  FROM pulls
)
SELECT *
FROM latest
WHERE rownum = 1;

CREATE OR REPLACE VIEW unique_pull_commits SECURITY INVOKER AS
WITH latest AS (
  SELECT *, row_number() OVER (PARTITION BY owner, repo, pull_number, parent_shas ORDER BY committer_date DESC) AS rownum
  FROM pull_commits
)
SELECT *
FROM latest
WHERE rownum = 1;

CREATE OR REPLACE VIEW unique_pull_stats SECURITY INVOKER AS
WITH latest AS (
  SELECT *, row_number() OVER (PARTITION BY owner, repo, pull_number ORDER BY updated_at DESC) AS rownum
  FROM pull_stats
)
SELECT *
FROM latest
WHERE rownum = 1;

CREATE OR REPLACE VIEW unique_review_comments SECURITY INVOKER AS
WITH latest AS (
  SELECT *, row_number() OVER (PARTITION BY owner, repo, id ORDER BY updated_at DESC) AS rownum
  FROM review_comments
)
SELECT *
FROM latest
WHERE rownum = 1;

CREATE OR REPLACE VIEW unique_issues SECURITY INVOKER AS
WITH latest AS (
  SELECT *, row_number() OVER (PARTITION BY owner, repo, id ORDER BY updated_at DESC) AS rownum
  FROM issues
)
SELECT *
FROM latest
WHERE rownum = 1;

CREATE OR REPLACE VIEW unique_issue_comments SECURITY INVOKER AS
WITH latest AS (
  SELECT *, row_number() OVER (PARTITION BY owner, repo, id ORDER BY updated_at DESC) AS rownum
  FROM issue_comments
)
SELECT *
FROM latest
WHERE rownum = 1;

CREATE OR REPLACE VIEW latest_teams SECURITY INVOKER AS
SELECT org, id, node_id, url, html_url, name, slug, description, privacy, permission, members_url, repositories_url, parent_id, parent_slug
FROM timestamped_teams
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14
HAVING max(removed_at) IS NULL OR max(removed_at) < max(created_at);

CREATE OR REPLACE VIEW all_teams SECURITY INVOKER AS
SELECT
  org, id, node_id, url, html_url, name, slug, description, privacy, permission, members_url, repositories_url, parent_id, parent_slug
  -- created_at is an approximate, recorded when membership was checked; assume membership is as old as possible, so stretch it to the end of the previous row
  , coalesce(lag(removed_at) OVER (PARTITION BY org, slug ORDER BY created_at) + interval '1' second, timestamp '0001-01-01') AS created_at
  , coalesce(removed_at, timestamp '9999-12-31') AS removed_at
FROM timestamped_teams;

CREATE OR REPLACE VIEW latest_members SECURITY INVOKER AS
SELECT org, team_slug, login, id, avatar_url, gravatar_id, type, site_admin
FROM timestamped_members
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
HAVING max(removed_at) IS NULL OR max(removed_at) < max(joined_at);

CREATE OR REPLACE VIEW all_members SECURITY INVOKER AS
SELECT
  org, team_slug, login, id, avatar_url, gravatar_id, type, site_admin
  -- joined_at is an approximate, recorded when membership was checked; assume membership is as old as possible, so stretch it to the end of the previous row
  , coalesce(lag(removed_at) OVER (PARTITION BY org, login, team_slug ORDER BY joined_at) + interval '1' second, timestamp '0001-01-01') AS joined_at
  , coalesce(removed_at, timestamp '9999-12-31') AS removed_at
FROM timestamped_members;

CREATE OR REPLACE VIEW latest_collaborators SECURITY INVOKER AS
SELECT owner, repo, login, id, avatar_url, gravatar_id, type, site_admin, permission_pull, permission_triage, permission_push, permission_maintain, permission_admin, role_name
FROM timestamped_collaborators
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14
HAVING max(removed_at) IS NULL OR max(removed_at) < max(joined_at);


CREATE OR REPLACE VIEW all_collaborators SECURITY INVOKER AS
SELECT
  owner, repo, login, id, avatar_url, gravatar_id, type, site_admin, permission_pull, permission_triage, permission_push, permission_maintain, permission_admin, role_name
  -- joined_at is an approximate, recorded when membership was checked; assume membership is as old as possible, so stretch it to the end of the previous row
  , coalesce(lag(removed_at) OVER (PARTITION BY owner, login ORDER BY joined_at) + interval '1' second, timestamp '0001-01-01') AS joined_at
  , coalesce(removed_at, timestamp '9999-12-31') AS removed_at
FROM timestamped_collaborators;
