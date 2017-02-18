package rocks.prestodb.rest.twitter.rest;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.google.common.base.Throwables;
import rocks.prestodb.rest.twitter.TwitterServiceFactory;
import rocks.prestodb.rest.twitter.model.SearchResult;
import rocks.prestodb.rest.twitter.model.Status;
import io.airlift.slice.Slice;
import retrofit2.Response;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;

public class TwitterFunctions
{
    private static final TwitterService twitterService = TwitterServiceFactory.createService(
            "y7jFqsxVWwiElTqvhXJjIMezH",
            "s5vnRBPgtSBnUPUfg26m2ilMPOjWeVhIcZER69UlypIcPggmMZ",
            "810602029697667077-ZEzt8LLmgFrANAMF4lOs0eapR1d4ZRd",
            "3ztHgYykSs7UIVCszFW6pOFozLltdjIlmelbmaTPFvtK1");

    @Description("search tweets")
    @ScalarFunction
    @LiteralParameters("x")
    @SqlType("array(row(varchar(128),varchar(1024),bigint,varchar(256),varchar(256)))")
    public Block searchTweets(@SqlType("varchar(x)") Slice querySlice)
    {
        String query = querySlice.toStringUtf8();

        try {
            Response<SearchResult> response = twitterService.searchTweets("#whug", 100).execute();
            if (!response.isSuccessful()) {
                throw new IllegalStateException("Unable to search tweets for '" + query + "' due: " + response.message());
            }
            List<Status> statuses = response.body().getStatuses();

            statuses.stream()
                    .map(status -> asList(status.getId(), status.getText(), status.getRetweetCount(), status.getUser().getName(), status.getUser().getScreenName()))
                    .collect(toList());
            return null;
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
}
