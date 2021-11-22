/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pl.net.was.rest.twitter.rest;

import okhttp3.OkHttpClient;
import pl.net.was.rest.twitter.model.SearchResult;
import retrofit2.Call;
import retrofit2.http.GET;
import retrofit2.http.Query;
import se.akerfeldt.okhttp.signpost.OkHttpOAuthConsumer;
import se.akerfeldt.okhttp.signpost.SigningInterceptor;

import static pl.net.was.rest.RestModule.getService;

public interface TwitterService
{
    static TwitterService create(
            OkHttpClient.Builder clientBuilder,
            String consumerKey,
            String consumerSecret,
            String token,
            String secret)
    {
        OkHttpOAuthConsumer consumer = new OkHttpOAuthConsumer(consumerKey, consumerSecret);
        consumer.setTokenWithSecret(token, secret);
        clientBuilder.addInterceptor(new SigningInterceptor(consumer));

        return getService(TwitterService.class, "https://api.twitter.com/1.1/", clientBuilder);
    }

    @GET("search/tweets.json")
    Call<SearchResult> searchTweets(
            @Query("q") String query,
            @Query("count") int count,
            @Query("result_type") String resultType);
}
