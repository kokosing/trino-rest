package rocks.prestodb.rest.github;

import rocks.prestodb.rest.github.model.Issue;
import retrofit2.Call;
import retrofit2.http.GET;

import java.util.List;

public interface GithubService
{
    @GET("/repos/prestodb/presto/issues")
    Call<List<Issue>> listPrestoIssues();
}
