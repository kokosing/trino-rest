package rocks.prestodb.rest.slack.rest;

import rocks.prestodb.rest.slack.model.History;
import rocks.prestodb.rest.slack.model.Channels;
import rocks.prestodb.rest.slack.model.Ims;
import rocks.prestodb.rest.slack.model.Users;
import rocks.prestodb.rest.slack.model.SlackResponse;
import retrofit2.Call;
import retrofit2.http.GET;
import retrofit2.http.POST;
import retrofit2.http.Query;

public interface SlackService
{
    @GET("channels.list")
    Call<Channels> listChannels(
            @Query("token") String token);

    @GET("channels.history")
    Call<History> channelHistory(
            @Query("token") String token,
            @Query("channel") String channel);

    @POST("chat.postMessage")
    Call<SlackResponse> postMessage(
            @Query("token") String token,
            @Query("channel") String channel,
            @Query("text") String text);

    @GET("users.list")
    Call<Users> listUsers(
            @Query("token") String token);

    @GET("im.list")
    Call<Ims> listIms(
            @Query("token") String token);

    @GET("im.history")
    Call<History> imHistory(
            @Query("token") String token,
            @Query("channel") String channel);
}
