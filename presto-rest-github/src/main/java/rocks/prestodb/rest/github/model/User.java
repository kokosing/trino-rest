package rocks.prestodb.rest.github.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class User
{
    private final String login;

    public User(
            @JsonProperty("login") String login)
    {
        this.login = login;
    }

    public String getLogin()
    {
        return login;
    }
}
