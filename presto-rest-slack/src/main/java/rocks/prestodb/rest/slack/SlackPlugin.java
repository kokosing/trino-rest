package rocks.prestodb.rest.slack;

import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.common.collect.ImmutableList;
import rocks.prestodb.rest.RestConnectorFactory;

public class SlackPlugin
        implements Plugin
{
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new RestConnectorFactory(
                "slack", config -> new SlackRest(config.get("token"))
        ));
    }
}
