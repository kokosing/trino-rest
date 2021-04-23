/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pl.net.was.rest.github;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class Sync
{
    private static final Logger log;

    static {
        System.setProperty("java.util.logging.SimpleFormatter.format",
                "%1$tF %1$tT %4$s %2$s %5$s%6$s%n");
        log = Logger.getLogger(Sync.class.getName());
    }

    private Sync() {}

    public static void main(String[] args)
    {
        String url = "jdbc:trino://localhost:8080/github/default";
        String username = "admin";
        String password = "";

        Map<String, String> env = System.getenv();

        // TODO combine with System.Properties
        List<String> names = Arrays.asList(
                "GITHUB_OWNER",
                "GITHUB_REPO",
                "GITHUB_TOKEN",
                "TRINO_DEST_SCHEMA",
                "TRINO_SRC_SCHEMA",
                "TRINO_URL",
                "TRINO_USERNAME",
                "TRINO_PASSWORD",
                "EMPTY_INSERT_LIMIT",
                "CHECK_STEPS_DUPLICATES");
        Map<String, String> defaults = Map.of(
                "TRINO_URL", url,
                "TRINO_USERNAME", username,
                "TRINO_PASSWORD", password,
                "EMPTY_INSERT_LIMIT", "1",
                "CHECK_STEPS_DUPLICATES", "false");
        for (String name : names) {
            String value = env.getOrDefault(name, defaults.getOrDefault(name, ""));
            if (!value.isEmpty() && (name.equals("TRINO_PASSWORD") || name.equals("GITHUB_TOKEN"))) {
                value = "***";
            }
            log.info(format("%s=%s", name, value));
        }

        if (env.containsKey("TRINO_URL")) {
            url = env.get("TRINO_URL");
        }
        if (env.containsKey("TRINO_USERNAME")) {
            username = env.get("TRINO_USERNAME");
        }
        if (env.containsKey("TRINO_PASSWORD")) {
            password = env.get("TRINO_PASSWORD");
        }

        String owner = System.getenv("GITHUB_OWNER");
        String repo = System.getenv("GITHUB_REPO");
        String destSchema = System.getenv("TRINO_DEST_SCHEMA");
        String srcSchema = System.getenv("TRINO_SRC_SCHEMA");

        requireNonNull(owner, "GITHUB_OWNER environmental variable must be set");
        requireNonNull(repo, "GITHUB_REPO environmental variable must be set");
        requireNonNull(destSchema, "TRINO_DEST_SCHEMA environmental variable must be set");
        requireNonNull(srcSchema, "TRINO_SRC_SCHEMA environmental variable must be set");

        try (Connection conn = DriverManager.getConnection(url, username, password)) {
            syncRuns(conn, owner, repo, destSchema, srcSchema);
            syncJobs(conn, owner, repo, destSchema, srcSchema);
            syncSteps(conn, owner, repo, destSchema, srcSchema, 2);
            syncLogs(conn, owner, repo, destSchema, srcSchema);
        }
        catch (Exception e) {
            log.severe("Got an exception! ");
            log.severe(e.getMessage());
            e.printStackTrace();
        }
    }

    private static void syncIssues(Connection conn, String owner, String repo, String destSchema, String srcSchema)
            throws SQLException
    {
        /*
         TODO these steps:
         * create if not exists temp table in mem catalog
         * fetch one page and save into the temp table
         * insert new
         * update existing where update_at is newer
         * drop temp table
         */
    }

    private static void syncRuns(Connection conn, String owner, String repo, String destSchema, String srcSchema)
            throws SQLException
    {
        conn.createStatement().executeUpdate(
                "CREATE TABLE IF NOT EXISTS " + destSchema + ".runs AS SELECT * FROM " + srcSchema + ".runs WITH NO DATA");
        // consider adding some indexes:
        // ALTER TABLE runs ADD PRIMARY KEY (id);
        // CREATE INDEX ON runs(workflow_id);
        // CREATE INDEX ON runs(node_id);

        // save only completed runs to avoid having to update them later
        PreparedStatement statement = conn.prepareStatement(
                "INSERT INTO " + destSchema + ".runs " +
                        "SELECT src.* FROM unnest(runs(?, ?, ?, ?)) src " +
                        "LEFT JOIN " + destSchema + ".runs dst ON dst.id = src.id " +
                        "WHERE dst.id IS NULL AND src.status = 'completed'");
        statement.setString(1, "Bearer " + System.getenv("GITHUB_TOKEN"));
        statement.setString(2, owner);
        statement.setString(3, repo);

        int page = 1;
        int breaker = getEmptyLimit();
        while (true) {
            log.info(format("Fetching page number %d", page));
            long startTime = System.currentTimeMillis();
            statement.setInt(4, page++);
            int rows = retryExecute(statement);
            log.info(format("Inserted %d rows, took %s", rows, Duration.ofMillis(System.currentTimeMillis() - startTime)));
            if (rows == 0) {
                if (breaker-- == 0) {
                    break;
                }
            }
            else {
                breaker = getEmptyLimit();
            }
        }
    }

    private static void syncJobs(Connection conn, String owner, String repo, String destSchema, String srcSchema)
            throws SQLException
    {
        conn.createStatement().executeUpdate(
                "CREATE TABLE IF NOT EXISTS " + destSchema + ".jobs AS SELECT * FROM " + srcSchema + ".jobs WITH NO DATA");
        // consider adding some indexes:
        // ALTER TABLE jobs ADD PRIMARY KEY (id);
        // CREATE INDEX ON jobs(run_id);
        // CREATE INDEX ON jobs(node_id);

        // only fetch up to 20 jobs for completed runs not older than 2 months, without any jobs
        PreparedStatement statement = conn.prepareStatement(
                "INSERT INTO " + destSchema + ".jobs " +
                        "SELECT src.* " +
                        "FROM (" +
                        "SELECT r.id " +
                        "FROM " + destSchema + ".runs r " +
                        "LEFT JOIN " + destSchema + ".jobs j ON j.run_id = r.id " +
                        "WHERE r.status = 'completed' AND r.created_at > NOW() - INTERVAL '2' MONTH " +
                        "GROUP BY r.id " +
                        "HAVING COUNT(j.id) = 0 " +
                        "ORDER BY r.id DESC LIMIT 20" +
                        ") r " +
                        "CROSS JOIN unnest(jobs(?, ?, ?, r.id)) src " +
                        "LEFT JOIN " + destSchema + ".jobs dst ON (dst.run_id, dst.id) = (src.run_id, src.id) " +
                        "WHERE dst.id IS NULL");
        statement.setString(1, "Bearer " + System.getenv("GITHUB_TOKEN"));
        statement.setString(2, owner);
        statement.setString(3, repo);

        while (true) {
            log.info("Fetching jobs");
            long startTime = System.currentTimeMillis();
            int rows = retryExecute(statement);
            log.info(format("Inserted %d rows, took %s", rows, Duration.ofMillis(System.currentTimeMillis() - startTime)));
            if (rows == 0) {
                break;
            }
        }
    }

    private static void syncSteps(Connection conn, String owner, String repo, String destSchema, String srcSchema, int batchSize)
            throws SQLException
    {
        conn.createStatement().executeUpdate(
                "CREATE TABLE IF NOT EXISTS " + destSchema + ".steps AS SELECT * FROM " + srcSchema + ".steps WITH NO DATA");
        // consider adding some indexes:
        // ALTER TABLE steps ADD PRIMARY KEY (job_id, number);

        if (batchSize > 2) {
            syncStepsBatches(conn, owner, repo, destSchema, srcSchema, batchSize);
            return;
        }
        // if the batchSize is small, it's completely ignored and runs will be processed one by one

        // only fetch steps from jobs from completed runs not older than 2 months, without any job steps
        String runsQuery =
                "SELECT r.id " +
                        "FROM " + destSchema + ".runs r " +
                        "JOIN " + destSchema + ".jobs j ON j.run_id = r.id " +
                        "LEFT JOIN " + destSchema + ".steps s ON s.job_id = j.id " +
                        "WHERE r.status = 'completed' AND r.created_at > NOW() - INTERVAL '2' MONTH " +
                        "GROUP BY r.id " +
                        "HAVING COUNT(s.number) = 0 " +
                        "ORDER BY r.id DESC";
        // since there's no difference between jobs without steps and those we have not checked or yet,
        // we need to know the last checked run id and add a condition to fetch lesser ids
        PreparedStatement idStatement = conn.prepareStatement(runsQuery);

        String insertQuery =
                "INSERT INTO " + destSchema + ".steps " +
                        "SELECT src.* " +
                        "FROM unnest(steps(?, ?, ?, ?)) src " +
                        "LEFT JOIN " + destSchema + ".steps dst ON (dst.job_id, dst.number) = (src.job_id, src.number) " +
                        "WHERE dst.number IS NULL";
        // the LEFT JOIN used to avoid duplicate errors always fetches all steps
        // and gets costly if there are many (>1 million) of those
        // allow to disable it, which should be safe when dst database supports transactions
        // WARNING: ensure that steps table has a primary key or an unique index on job_id and number columns
        String checkDuplicates = System.getenv("CHECK_STEPS_DUPLICATES");
        if (checkDuplicates != null && checkDuplicates.equals("false")) {
            insertQuery =
                    "INSERT INTO " + destSchema + ".steps " +
                            "SELECT * FROM unnest(steps(?, ?, ?, ?)) src";
        }
        PreparedStatement insertStatement = conn.prepareStatement(insertQuery);
        insertStatement.setString(1, "Bearer " + System.getenv("GITHUB_TOKEN"));
        insertStatement.setString(2, owner);
        insertStatement.setString(3, repo);

        log.info("Fetching run ids to get steps for");
        if (!idStatement.execute()) {
            return;
        }
        ResultSet resultSet = idStatement.getResultSet();
        while (resultSet.next()) {
            long runId = resultSet.getLong(1);
            insertStatement.setLong(4, runId);

            log.info(format("Fetching steps for jobs of run %d", runId));
            long startTime = System.currentTimeMillis();
            int rows = retryExecute(insertStatement);
            log.info(format("Inserted %d rows, took %s", rows, Duration.ofMillis(System.currentTimeMillis() - startTime)));
        }
    }

    private static void syncStepsBatches(Connection conn, String owner, String repo, String destSchema, String srcSchema, int batchSize)
            throws SQLException
    {
        // only fetch steps from jobs from up to 2 completed runs not older than 2 months, without any job steps
        String runsQuery = format(
                "SELECT r.id " +
                        "FROM " + destSchema + ".runs r " +
                        "JOIN " + destSchema + ".jobs j ON j.run_id = r.id " +
                        "LEFT JOIN " + destSchema + ".steps s ON s.job_id = j.id " +
                        "WHERE r.status = 'completed' AND r.created_at > NOW() - INTERVAL '2' MONTH AND r.id < ? " +
                        "GROUP BY r.id " +
                        "HAVING COUNT(s.number) = 0 " +
                        "ORDER BY r.id DESC LIMIT %d", batchSize);
        // since there's no difference between jobs without steps and those we have not checked or yet,
        // we need to know the last checked run id and add a condition to fetch lesser ids
        PreparedStatement idStatement = conn.prepareStatement("SELECT min(r.id) FROM (" + runsQuery + ") r");

        PreparedStatement insertStatement = conn.prepareStatement(
                "INSERT INTO " + destSchema + ".steps " +
                        "SELECT src.* " +
                        "FROM (" + runsQuery + ") r " +
                        "CROSS JOIN unnest(steps(?, ?, ?, r.id)) src " +
                        "LEFT JOIN " + destSchema + ".steps dst ON (dst.job_id, dst.number) = (src.job_id, src.number) " +
                        "WHERE dst.number IS NULL");
        insertStatement.setString(2, "Bearer " + System.getenv("GITHUB_TOKEN"));
        insertStatement.setString(3, owner);
        insertStatement.setString(4, repo);

        long previousId = Long.MAX_VALUE;
        while (true) {
            idStatement.setLong(1, previousId);
            insertStatement.setLong(1, previousId);

            log.info("Checking for next runs with jobs without steps");
            ResultSet resultSet = idStatement.executeQuery();
            if (!resultSet.next()) {
                break;
            }
            previousId = resultSet.getLong(1);
            if (resultSet.wasNull()) {
                break;
            }

            log.info(format("Fetching steps for jobs of runs with id less than %d", previousId));
            long startTime = System.currentTimeMillis();
            int rows = retryExecute(insertStatement);
            log.info(format("Inserted %d rows, took %s", rows, Duration.ofMillis(System.currentTimeMillis() - startTime)));
        }
    }

    private static void syncLogs(Connection conn, String owner, String repo, String destSchema, String srcSchema)
            throws SQLException
    {
        conn.createStatement().executeUpdate(
                "CREATE TABLE IF NOT EXISTS " + destSchema + ".logs (job_id BIGINT, log VARCHAR)");
        // run this directly in the backend:
        // CREATE TABLE logs (job_id BIGINT, log VARCHAR, PRIMARY KEY (job_id));

        // only fetch up to 5 job logs for completed runs not older than 2 months, without any job logs
        PreparedStatement statement = conn.prepareStatement(
                "INSERT INTO " + destSchema + ".logs " +
                        "SELECT j.id, job_logs(?, ?, ?, j.id) " +
                        "FROM (" +
                        "SELECT j.id " +
                        "FROM " + destSchema + ".runs r " +
                        "JOIN " + destSchema + ".jobs j ON j.run_id = r.id " +
                        "LEFT JOIN " + destSchema + ".logs l ON l.job_id = j.id " +
                        "WHERE r.status = 'completed' AND r.conclusion IS DISTINCT FROM 'success' AND r.created_at > NOW() - INTERVAL '2' MONTH " +
                        "AND j.conclusion IS DISTINCT FROM 'success' " +
                        "GROUP BY j.id " +
                        "HAVING COUNT(l.job_id) = 0 " +
                        "ORDER BY j.id DESC LIMIT 5" +
                        ") j");
        statement.setString(1, "Bearer " + System.getenv("GITHUB_TOKEN"));
        statement.setString(2, owner);
        statement.setString(3, repo);

        while (true) {
            log.info("Fetching logs");
            long startTime = System.currentTimeMillis();
            int rows = retryExecute(statement);
            log.info(format("Inserted %d rows, took %s", rows, Duration.ofMillis(System.currentTimeMillis() - startTime)));
            if (rows == 0) {
                break;
            }
        }
    }

    private static int getEmptyLimit()
    {
        String limit = System.getenv("EMPTY_INSERT_LIMIT");
        if (limit == null) {
            return 1;
        }
        return Integer.parseInt(limit);
    }

    private static int retryExecute(PreparedStatement statement)
            throws SQLException
    {
        int breaker = 3;
        while (true) {
            try {
                return statement.executeUpdate();
            }
            catch (SQLException e) {
                if (breaker-- == 1) {
                    throw e;
                }
                log.severe(e.getMessage());
                log.severe(format("Retrying %d more times", breaker));
            }
        }
    }
}
