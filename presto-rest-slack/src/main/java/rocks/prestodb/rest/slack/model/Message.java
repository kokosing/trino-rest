package rocks.prestodb.rest.slack.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Message
{
    private final String type;
    private final String user;
    private final String text;

    public Message(
            @JsonProperty("type") String type,
            @JsonProperty("user") String user,
            @JsonProperty("text") String text)
    {
        this.type = type;
        this.user = user;
        this.text = text;
    }

    public String getType()
    {
        return type;
    }

    public String getUser()
    {
        return user;
    }

    public String getText()
    {
        return text;
    }
}
