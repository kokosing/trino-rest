package rocks.prestodb.rest;

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class RestConnectorTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final RestTableHandle tableHandle;

    @JsonCreator
    public RestConnectorTableLayoutHandle(
            @JsonProperty("tableHandle") RestTableHandle tableHandle)
    {
        this.tableHandle = tableHandle;
    }

    @JsonProperty("tableHandle")
    public RestTableHandle getTableHandle()
    {
        return tableHandle;
    }
}
