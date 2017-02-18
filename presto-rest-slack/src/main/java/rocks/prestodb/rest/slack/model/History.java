package rocks.prestodb.rest.slack.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class History
        extends SlackResponse
{
    private final List<Message> messages;

    public History(
            @JsonProperty("ok") boolean ok,
            @JsonProperty("error") String error,
            @JsonProperty("messages") List<Message> messages)
    {
        super(ok, error);
        this.messages = messages;
    }

    public List<Message> getMessages()
    {
        return messages;
    }
}
