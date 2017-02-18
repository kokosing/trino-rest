package rocks.prestodb.rest.slack.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class User
{
    private final String id;
    private final String name;

    @JsonCreator
    public User(
            @JsonProperty("id") String id,
            @JsonProperty("name") String name)
    {
        this.id = id;
        this.name = name;
    }

    public String getName()
    {
        return name;
    }

    public String getId()
    {
        return id;
    }
}
