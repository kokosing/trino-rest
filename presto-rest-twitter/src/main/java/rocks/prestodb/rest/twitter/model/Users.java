package rocks.prestodb.rest.twitter.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Users
        extends SlackResponse
{
    private final List<User> users;

    public Users(@JsonProperty("ok") boolean ok, @JsonProperty("error") String error, @JsonProperty("members") List<User> users)
    {
        super(ok, error);
        this.users = users;
    }

    public List<User> getUsers()
    {
        return users;
    }
}
