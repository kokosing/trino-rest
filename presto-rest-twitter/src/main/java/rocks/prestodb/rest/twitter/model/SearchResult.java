package rocks.prestodb.rest.twitter.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SearchResult
{
    private final List<Status> statuses;

    public SearchResult(
            @JsonProperty("statuses") List<Status> statuses)
    {
        this.statuses = statuses;
    }

    public List<Status> getStatuses()
    {
        return statuses;
    }
}
