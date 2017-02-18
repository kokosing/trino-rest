package rocks.prestodb.rest.github;

import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.common.collect.ImmutableList;
import com.teradata.rest.RestConnectorFactory;

public class GithubPlugin
        implements Plugin
{
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new RestConnectorFactory(
                "github", config -> new GithubRest(config.get("token"))
        ));
    }
}
