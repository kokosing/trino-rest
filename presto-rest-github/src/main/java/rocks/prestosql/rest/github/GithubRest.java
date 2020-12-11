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

package rocks.prestosql.rest.github;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.BigintType;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;
import rocks.prestosql.rest.Rest;
import rocks.prestosql.rest.github.model.Issue;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.stream.Collectors.toList;

public class GithubRest
        implements Rest
{
    public static final String SCHEMA_NAME = "default";

    private final GithubService service = new Retrofit.Builder()
            .baseUrl("https://api.github.com/")
            .addConverterFactory(JacksonConverterFactory.create())
            .build()
            .create(GithubService.class);

    @Override
    public ConnectorTableMetadata getTableMetadata(SchemaTableName schemaTableName)
    {
        return new ConnectorTableMetadata(
                schemaTableName,
                ImmutableList.of(
                        new ColumnMetadata("number", BigintType.BIGINT),
                        new ColumnMetadata("state", createUnboundedVarcharType()),
                        new ColumnMetadata("user", createUnboundedVarcharType()),
                        new ColumnMetadata("title", createUnboundedVarcharType())));
    }

    @Override
    public List<String> listSchemas()
    {
        return ImmutableList.of(SCHEMA_NAME);
    }

    @Override
    public List<SchemaTableName> listTables(String schema)
    {
        return ImmutableList.of(new SchemaTableName(SCHEMA_NAME, "prestodb_issues"));
    }

    @Override
    public Collection<? extends List<?>> getRows(SchemaTableName schemaTableName)
    {
        try {
            Response<List<Issue>> execute = service.listPrestoIssues().execute();
            if (!execute.isSuccessful()) {
                throw new IllegalStateException("Unable to read: " + execute.message());
            }
            List<Issue> issues = execute.body();
            return issues.stream()
                    .map(issue -> ImmutableList.of(issue.getNumber(), issue.getState(), issue.getUser().getLogin(), issue.getTitle()))
                    .collect(toList());
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public Consumer<List> createRowSink(SchemaTableName schemaTableName)
    {
        throw new IllegalStateException("This connector does not support write");
    }
}
