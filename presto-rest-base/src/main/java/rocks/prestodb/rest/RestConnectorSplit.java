package rocks.prestodb.rest;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class RestConnectorSplit
        implements ConnectorSplit
{
    private final RestTableHandle tableHandle;
    private final List<HostAddress> addresses;

    @JsonCreator
    public RestConnectorSplit(
            @JsonProperty("tableHandle") RestTableHandle tableHandle,
            @JsonProperty("addresses") List<HostAddress> addresses)
    {
        this.tableHandle = tableHandle;
        this.addresses = addresses;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    @JsonProperty("addresses")
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @Override
    public Object getInfo()
    {
        return "Slack split";
    }

    @JsonProperty("tableHandle")
    public RestTableHandle getTableHandle()
    {
        return tableHandle;
    }
}
