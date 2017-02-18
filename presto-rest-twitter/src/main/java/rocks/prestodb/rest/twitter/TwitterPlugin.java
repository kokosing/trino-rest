package rocks.prestodb.rest.twitter;

import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.common.collect.ImmutableList;
import rocks.prestodb.rest.RestConnectorFactory;

public class TwitterPlugin
        implements Plugin
{
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new RestConnectorFactory(
                "twitter",
                config -> new TwitterRest(
                        config.get("customer_key"),
                        config.get("customer_secret"),
                        config.get("token"),
                        config.get("secret"))
        ));
    }
}
