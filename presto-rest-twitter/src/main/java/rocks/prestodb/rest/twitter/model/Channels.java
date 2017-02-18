package rocks.prestodb.rest.twitter.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class Channels
        extends SlackResponse
{
    private final List<Channel> channels;

    public Channels(
            @JsonProperty("ok") boolean ok,
            @JsonProperty("error") String error,
            @JsonProperty("channels") List<Channel> channels)
    {
        super(ok, error);
        this.channels = channels;
    }

    public List<Channel> getChannels()
    {
        return channels;
    }
}
