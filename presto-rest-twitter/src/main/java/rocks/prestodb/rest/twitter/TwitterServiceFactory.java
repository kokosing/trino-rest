package rocks.prestodb.rest.twitter;

import rocks.prestodb.rest.twitter.rest.TwitterService;
import okhttp3.OkHttpClient;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;
import se.akerfeldt.okhttp.signpost.OkHttpOAuthConsumer;
import se.akerfeldt.okhttp.signpost.SigningInterceptor;

public class TwitterServiceFactory
{
    public static TwitterService createService(String consumerKey, String consumerSecret, String token, String secret)
    {
        OkHttpOAuthConsumer consumer = new OkHttpOAuthConsumer(consumerKey, consumerSecret);
        consumer.setTokenWithSecret(token, secret);

        OkHttpClient client = new OkHttpClient.Builder()
                .addInterceptor(new SigningInterceptor(consumer))
                .build();

        return new Retrofit.Builder()
                .baseUrl("https://api.twitter.com/1.1/")
                .addConverterFactory(JacksonConverterFactory.create())
                .client(client)
                .build()
                .create(TwitterService.class);
    }
}
