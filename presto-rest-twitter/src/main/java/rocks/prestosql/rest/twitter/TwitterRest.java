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

package rocks.prestosql.rest.twitter;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.SchemaTableName;
import retrofit2.Response;
import rocks.prestosql.rest.Rest;
import rocks.prestosql.rest.twitter.model.SearchResult;
import rocks.prestosql.rest.twitter.model.Status;
import rocks.prestosql.rest.twitter.rest.TwitterService;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;

public class TwitterRest
        implements Rest
{
    private static final String SCHEMA = "default";

    private final TwitterService service;

    public TwitterRest(String consumerKey, String consumerSecret, String token, String secret)
    {
        service = TwitterService.create(consumerKey, consumerSecret, token, secret);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(SchemaTableName schemaTableName)
    {
        return new ConnectorTableMetadata(
                schemaTableName,
                ImmutableList.of(
                        new ColumnMetadata("id", createUnboundedVarcharType()),
                        new ColumnMetadata("text", createUnboundedVarcharType()),
                        new ColumnMetadata("retweet_count", BIGINT),
                        new ColumnMetadata("user_name", createUnboundedVarcharType()),
                        new ColumnMetadata("user_screen_name", createUnboundedVarcharType())));
    }

    @Override
    public List<String> listSchemas()
    {
        return ImmutableList.of(SCHEMA);
    }

    @Override
    public List<SchemaTableName> listTables(String schema)
    {
        if (schema.equalsIgnoreCase(SCHEMA)) {
            return ImmutableList.of(
                    new SchemaTableName(SCHEMA, "whug"),
                    new SchemaTableName(SCHEMA, "prestosql"),
                    new SchemaTableName(SCHEMA, "teradata"),
                    new SchemaTableName(SCHEMA, "hive"));
        }
        return ImmutableList.of();
    }

    @Override
    public Collection<? extends List<?>> getRows(SchemaTableName schemaTableName)
    {
        return searchTweets("#" + schemaTableName.getTableName());
    }

    private Collection<? extends List<?>> searchTweets(String query)
    {
        try {
            Response<SearchResult> response = service.searchTweets(query, 100, "recent").execute();
            if (!response.isSuccessful()) {
                throw new IllegalStateException("Unable to search tweets for '" + query + "' dues: " + response.message());
            }
            List<Status> statuses = response.body().getStatuses();
            return statuses.stream()
                    .map(status -> asList(status.getId(), status.getText(), status.getRetweetCount(), status.getUser().getName(), status.getUser().getScreenName()))
                    .collect(toList());
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public Consumer<List> createRowSink(SchemaTableName schemaTableName)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }
}
