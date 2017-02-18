package rocks.prestodb.rest.github;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.BigintType;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.teradata.rest.Rest;
import rocks.prestodb.rest.github.model.Issue;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.stream.Collectors.toList;

public class GithubRest
        implements Rest
{
    public static final String SCHEMA_NAME = "default";
    private final String token;

    private final GithubService service = new Retrofit.Builder()
            .baseUrl("https://api.github.com/")
            .addConverterFactory(JacksonConverterFactory.create())
            .build()
            .create(GithubService.class);

    public GithubRest(String token) {this.token = token;}

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
