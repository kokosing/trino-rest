package rocks.prestodb.rest.twitter.rest;

import rocks.prestodb.rest.twitter.model.SearchResult;
import retrofit2.Call;
import retrofit2.http.GET;
import retrofit2.http.Query;

public interface TwitterService
{
    @GET("search/tweets.json")
    Call<SearchResult> searchTweets(
            @Query("q") String query,
            @Query("count") int count,
            @Query("result_type") String resultType
    );
}
