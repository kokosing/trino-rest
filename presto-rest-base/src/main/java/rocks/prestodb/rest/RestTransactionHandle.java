package rocks.prestodb.rest;

import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class RestTransactionHandle
        implements ConnectorTransactionHandle
{
    private final int id;

    @JsonCreator
    public RestTransactionHandle(
            @JsonProperty("id") int id
    )
    {
        this.id = id;
    }

    @JsonProperty("id")
    public int getId()
    {
        return id;
    }
}
