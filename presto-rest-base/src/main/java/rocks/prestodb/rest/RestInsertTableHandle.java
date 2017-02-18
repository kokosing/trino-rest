package rocks.prestodb.rest;

import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class RestInsertTableHandle
        implements ConnectorInsertTableHandle
{
    private final RestTableHandle tableHandle;

    @JsonCreator
    public RestInsertTableHandle(
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
