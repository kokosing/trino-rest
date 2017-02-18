package rocks.prestodb.rest.twitter.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class User
{
    private final String name;
    private final String screenName;

    public User(
            @JsonProperty("name") String name,
            @JsonProperty("screen_name") String screenName)
    {
        this.name = name;
        this.screenName = screenName;
    }

    public String getName()
    {
        return name;
    }

    public String getScreenName()
    {
        return screenName;
    }
}
