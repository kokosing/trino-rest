package rocks.prestodb.rest.slack.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SlackResponse
{
    private final boolean ok;
    private final String error;

    public SlackResponse(
            @JsonProperty("ok") boolean ok,
            @JsonProperty("error") String error)
    {
        this.ok = ok;
        this.error = error;
    }

    public String getError()
    {
        return error;
    }

    public boolean isOk()
    {
        return ok;
    }
}
