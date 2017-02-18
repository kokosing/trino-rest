package rocks.prestodb.rest.slack;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import rocks.prestodb.rest.Rest;
import rocks.prestodb.rest.slack.model.Channel;
import rocks.prestodb.rest.slack.model.Channels;
import rocks.prestodb.rest.slack.model.History;
import rocks.prestodb.rest.slack.model.Im;
import rocks.prestodb.rest.slack.model.Ims;
import rocks.prestodb.rest.slack.model.SlackResponse;
import rocks.prestodb.rest.slack.model.User;
import rocks.prestodb.rest.slack.model.Users;
import rocks.prestodb.rest.slack.rest.SlackService;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Arrays.asList;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class SlackRest
        implements Rest
{
    private static final String CHANNEL_SCHEMA = "channel";
    private static final String IM_SCHEMA = "im";

    private final SlackService service = new Retrofit.Builder()
            .baseUrl("https://slack.com/api/")
            .addConverterFactory(JacksonConverterFactory.create())
            .build()
            .create(SlackService.class);

    private final Map<String, Channel> channels;
    private final Map<String, User> users;
    private final Map<String, Im> ims;
    private final String token;

    public SlackRest(String token)
    {
        this.token = token;
        try {
            Channels channels = service.listChannels(token).execute().body();
            if (channels.getError() != null) {
                throw new IllegalStateException("Error during communication with slack: " + channels.getError());
            }
            this.channels = channels.getChannels().stream()
                    .filter(Channel::isMember)
                    .collect(toMap(Channel::getName, identity()));

            Users users = service.listUsers(token).execute().body();
            if (users.getError() != null) {
                throw new IllegalStateException("Error during communication with slack: " + channels.getError());
            }
            this.users = users.getUsers().stream()
                    .collect(toMap(User::getName, identity()));

            Ims ims = service.listIms(token).execute().body();
            if (ims.getError() != null) {
                throw new IllegalStateException("Error during communication with slack: " + channels.getError());
            }
            this.ims = ims.getIms().stream()
                    .collect(toMap(Im::getUser, identity()));
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(SchemaTableName schemaTableName)
    {
        return new ConnectorTableMetadata(
                schemaTableName,
                ImmutableList.of(
                        new ColumnMetadata("type", createUnboundedVarcharType()),
                        new ColumnMetadata("user", createUnboundedVarcharType()),
                        new ColumnMetadata("text", createUnboundedVarcharType())));
    }

    @Override
    public List<String> listSchemas()
    {
        return ImmutableList.of(CHANNEL_SCHEMA, IM_SCHEMA);
    }

    @Override
    public List<SchemaTableName> listTables(String schema)
    {
        if (CHANNEL_SCHEMA.equalsIgnoreCase(schema)) {
            return channels.keySet().stream()
                    .map(tableName -> new SchemaTableName(CHANNEL_SCHEMA, tableName))
                    .collect(toList());
        }
        if (IM_SCHEMA.equalsIgnoreCase(schema)) {
            return users.keySet().stream()
                    .map(tableName -> new SchemaTableName(IM_SCHEMA, tableName))
                    .collect(toList());
        }
        return ImmutableList.of();
    }

    @Override
    public Collection<? extends List<?>> getRows(SchemaTableName schemaTableName)
    {
        History history = getHistory(schemaTableName);

        if (history.getError() != null) {
            throw new IllegalStateException("Unable to read from '" + schemaTableName + "' dues: " + history.getError());
        }
        return history
                .getMessages().stream()
                .map(message -> asList(message.getType(), message.getUser(), message.getText()))
                .collect(toList());
    }

    private History getHistory(SchemaTableName schemaTableName)
    {
        String schemaName = schemaTableName.getSchemaName();
        try {
            if (CHANNEL_SCHEMA.equalsIgnoreCase(schemaName)) {
                return service.channelHistory(token, getChannelId(schemaTableName))
                        .execute()
                        .body();
            }
            if (IM_SCHEMA.equalsIgnoreCase(schemaName)) {
                return service.imHistory(token, getChannelId(schemaTableName))
                        .execute()
                        .body();
            }
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        return new History(true, "no such schema", ImmutableList.of());
    }

    private String getChannelId(SchemaTableName schemaTableName)
    {
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();
        if (CHANNEL_SCHEMA.equalsIgnoreCase(schemaName)) {
            return channels.get(tableName).getId();
        }
        if (IM_SCHEMA.equalsIgnoreCase(schemaName)) {
            String userId = users.get(tableName).getId();
            return ims.get(userId).getId();
        }
        throw new IllegalArgumentException("Unknown schema: " + schemaName);
    }

    @Override
    public Consumer<List> createRowSink(SchemaTableName schemaTableName)
    {
        return list -> {
            try {
                SlackResponse body = service.postMessage(token, getChannelId(schemaTableName), (String) list.get(2)).execute().body();
                if (body.getError() != null) {
                    throw new IllegalStateException("Unable to write to '" + schemaTableName + "' dues: " + body.getError());
                }
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        };
    }
}
