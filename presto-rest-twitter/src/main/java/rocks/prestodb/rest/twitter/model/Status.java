package rocks.prestodb.rest.twitter.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Status
{
    private final String id;
    private final String text;
    private final long retweetCount;
    private final User user;

    public Status(
            @JsonProperty("id_str") String id,
            @JsonProperty("text") String text,
            @JsonProperty("retweet_count") long retweetCount,
            @JsonProperty("user") User user)
    {

        this.id = id;
        this.text = text;
        this.retweetCount = retweetCount;
        this.user = user;
    }

    public String getId()
    {
        return id;
    }

    public String getText()
    {
        return text;
    }

    public long getRetweetCount()
    {
        return retweetCount;
    }

    public User getUser()
    {
        return user;
    }
}
