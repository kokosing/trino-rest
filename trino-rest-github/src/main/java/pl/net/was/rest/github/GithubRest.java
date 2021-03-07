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

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.type.TimestampWithTimeZoneType;
import pl.net.was.rest.Rest;
import pl.net.was.rest.github.model.Issue;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.stream.Collectors.toList;

public class GithubRest
        implements Rest
{
    public static final String SCHEMA_NAME = "default";

    private final String token;
    private final String owner;
    private final String repo;
    private final GithubService service = new Retrofit.Builder()
            .baseUrl("https://api.github.com/")
            .addConverterFactory(JacksonConverterFactory.create())
            .build()
            .create(GithubService.class);

    public static final Map<String, List<ColumnMetadata>> columns = ImmutableMap.of(
            "issues", ImmutableList.of(
                    new ColumnMetadata("number", BIGINT),
                    new ColumnMetadata("state", createUnboundedVarcharType()),
                    new ColumnMetadata("user", createUnboundedVarcharType()),
                    new ColumnMetadata("title", createUnboundedVarcharType())),
            "runs", ImmutableList.of(
                    new ColumnMetadata("id", BIGINT),
                    new ColumnMetadata("name", createUnboundedVarcharType()),
                    new ColumnMetadata("node_id", createUnboundedVarcharType()),
                    new ColumnMetadata("head_branch", createUnboundedVarcharType()),
                    new ColumnMetadata("head_sha", createUnboundedVarcharType()),
                    new ColumnMetadata("run_number", BIGINT),
                    new ColumnMetadata("event", createUnboundedVarcharType()),
                    new ColumnMetadata("status", createUnboundedVarcharType()),
                    new ColumnMetadata("conclusion", createUnboundedVarcharType()),
                    new ColumnMetadata("workflow_id", BIGINT),
                    new ColumnMetadata("created_at", TimestampWithTimeZoneType.createTimestampWithTimeZoneType(3)),
                    new ColumnMetadata("updated_at", TimestampWithTimeZoneType.createTimestampWithTimeZoneType(3))),
            "jobs", ImmutableList.of(
                    new ColumnMetadata("id", BIGINT),
                    new ColumnMetadata("run_id", BIGINT),
                    new ColumnMetadata("run_url", createUnboundedVarcharType()),
                    new ColumnMetadata("node_id", createUnboundedVarcharType()),
                    new ColumnMetadata("head_sha", createUnboundedVarcharType()),
                    new ColumnMetadata("url", createUnboundedVarcharType()),
                    new ColumnMetadata("html_url", createUnboundedVarcharType()),
                    new ColumnMetadata("status", createUnboundedVarcharType()),
                    new ColumnMetadata("conclusion", createUnboundedVarcharType()),
                    new ColumnMetadata("started_at", TimestampWithTimeZoneType.createTimestampWithTimeZoneType(3)),
                    new ColumnMetadata("completed_at", TimestampWithTimeZoneType.createTimestampWithTimeZoneType(3)),
                    new ColumnMetadata("name", createUnboundedVarcharType()),
                    new ColumnMetadata("check_run_url", createUnboundedVarcharType())),
            "steps", ImmutableList.of(
                    new ColumnMetadata("job_id", BIGINT),
                    new ColumnMetadata("name", createUnboundedVarcharType()),
                    new ColumnMetadata("status", createUnboundedVarcharType()),
                    new ColumnMetadata("conclusion", createUnboundedVarcharType()),
                    new ColumnMetadata("number", BIGINT),
                    new ColumnMetadata("started_at", TimestampWithTimeZoneType.createTimestampWithTimeZoneType(3)),
                    new ColumnMetadata("completed_at", TimestampWithTimeZoneType.createTimestampWithTimeZoneType(3))));

    public GithubRest(String token, String owner, String repo)
    {
        this.token = token;
        this.owner = owner;
        this.repo = repo;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(SchemaTableName schemaTableName)
    {
        return new ConnectorTableMetadata(
                schemaTableName,
                columns.get(schemaTableName.getTableName()));
    }

    @Override
    public List<String> listSchemas()
    {
        return ImmutableList.of(SCHEMA_NAME);
    }

    @Override
    public List<SchemaTableName> listTables(String schema)
    {
        return columns
                .keySet()
                .stream()
                .map(name -> new SchemaTableName(SCHEMA_NAME, name))
                .collect(toList());
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(SchemaTablePrefix schemaTablePrefix)
    {
        return columns.entrySet()
                .stream()
                .collect(Collectors.toMap(
                        e -> new SchemaTableName(schemaTablePrefix.getSchema().orElse(""), e.getKey()),
                        Map.Entry::getValue));
    }

    @Override
    public Collection<? extends List<?>> getRows(SchemaTableName schemaTableName)
    {
        switch (schemaTableName.getTableName()) {
            case "issues":
                return getIssues();
            case "runs":
                // don't implement any API that has pagination, just expose functions to fetch that data
                // and put it into tables in other persistent dbs
                // TODO document an example how to get new data, that is add a condition to fetch new runs
                // and loop "INSERT INTO SELECT FROM" until page is empty (inserted zero rows)
                throw new UnsupportedOperationException("Use workflow_runs function instead");
            case "jobs":
                throw new UnsupportedOperationException("Use workflow_jobs function instead");
            case "steps":
                throw new UnsupportedOperationException("Use workflow_steps function instead");
        }
        return null;
    }

    private Collection<? extends List<?>> getIssues()
    {
        Response<List<Issue>> execute;
        try {
            execute = service.listIssues("Bearer " + token, owner, repo).execute();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        if (!execute.isSuccessful()) {
            throw new IllegalStateException("Unable to read: " + execute.message());
        }
        List<Issue> issues = execute.body();
        return issues.stream().map(Issue::toRow).collect(toList());
    }

    @Override
    public Consumer<List> createRowSink(SchemaTableName schemaTableName)
    {
        throw new IllegalStateException("This connector does not support write");
    }
}
