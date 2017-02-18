package rocks.prestodb.rest.slack.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Channel
{
    private final String id;
    private final String name;
    private final boolean isMember;
    private final boolean isArchived;

    @JsonCreator
    public Channel(
            @JsonProperty("id") String id,
            @JsonProperty("name") String name,
            @JsonProperty("is_member") boolean isMember,
            @JsonProperty("is_archived") boolean isArchived)
    {
        this.id = id;
        this.name = name;
        this.isMember = isMember;
        this.isArchived = isArchived;
    }

    public String getName()
    {
        return name;
    }

    public boolean isMember()
    {
        return isMember;
    }

    public boolean isArchived()
    {
        return isArchived;
    }

    public String getId()
    {
        return id;
    }
}
