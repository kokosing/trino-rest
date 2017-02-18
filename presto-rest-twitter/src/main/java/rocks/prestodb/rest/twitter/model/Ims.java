package rocks.prestodb.rest.twitter.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Ims
        extends SlackResponse
{
    private final List<Im> ims;

    public Ims(@JsonProperty("ok") boolean ok, @JsonProperty("error") String error, @JsonProperty("ims") List<Im> ims)
    {
        super(ok, error);
        this.ims = ims;
    }

    public List<Im> getIms()
    {
        return ims;
    }
}
