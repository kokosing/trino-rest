package rocks.prestodb.rest.slack.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Im
{
    private final String id;
    private final String user;

    @JsonCreator
    public Im(
            @JsonProperty("id") String id,
            @JsonProperty("user") String user)
    {
        this.id = id;
        this.user = user;
    }

    public String getUser()
    {
        return user;
    }

    public String getId()
    {
        return id;
    }
}
