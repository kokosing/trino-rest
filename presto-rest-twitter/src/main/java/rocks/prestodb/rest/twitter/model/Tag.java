package rocks.prestodb.rest.twitter.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;

public class Tag
{
    private final String value;
    private final String creator;
    private final Date lastSet;

    private Tag(
            @JsonProperty("value") String value,
            @JsonProperty("creator") String creator,
            @JsonProperty("last_set") Date lastSet)
    {
        this.value = value;
        this.creator = creator;
        this.lastSet = lastSet;
    }

    @JsonProperty("value")
    public String getValue()
    {
        return value;
    }

    @JsonProperty("creator")

    public String getCreator()
    {
        return creator;
    }

    @JsonProperty("last_set")
    public Date getLastSet()
    {
        return lastSet;
    }
}
