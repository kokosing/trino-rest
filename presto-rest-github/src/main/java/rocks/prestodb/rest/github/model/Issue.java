package rocks.prestodb.rest.github.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Issue
{
    private final long number;
    private final String state;
    private final String title;
    private final User user;

    public Issue(
            @JsonProperty("number") long number,
            @JsonProperty("state") String state,
            @JsonProperty("title") String title,
            @JsonProperty("user") User user)
    {
        this.number = number;
        this.state = state;
        this.title = title;
        this.user = user;
    }

    public long getNumber()
    {
        return number;
    }

    public String getState()
    {
        return state;
    }

    public String getTitle()
    {
        return title;
    }

    public User getUser()
    {
        return user;
    }
}
