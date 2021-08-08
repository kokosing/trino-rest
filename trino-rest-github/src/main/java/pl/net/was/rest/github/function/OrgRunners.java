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

package pl.net.was.rest.github.function;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import pl.net.was.rest.Rest;
import pl.net.was.rest.github.GithubTable;
import pl.net.was.rest.github.model.Runner;
import pl.net.was.rest.github.model.RunnersList;
import retrofit2.Response;

import java.io.IOException;
import java.util.List;

import static io.trino.spi.type.StandardTypes.INTEGER;
import static io.trino.spi.type.StandardTypes.VARCHAR;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.util.Objects.requireNonNull;
import static pl.net.was.rest.github.GithubRest.RUNNERS_TABLE_TYPE;
import static pl.net.was.rest.github.GithubRest.getRowType;

@ScalarFunction(value = "org_runners", deterministic = false)
@Description("Get organization self-hosted runners")
public class OrgRunners
        extends BaseFunction
{
    public OrgRunners()
    {
        RowType rowType = getRowType(GithubTable.RUNNERS);
        arrayType = new ArrayType(rowType);
        pageBuilder = new PageBuilder(ImmutableList.of(arrayType));
    }

    @SqlType(RUNNERS_TABLE_TYPE)
    public Block getPage(@SqlType(VARCHAR) Slice org, @SqlType(INTEGER) long page)
            throws IOException
    {
        // there should not be more than a few pages worth of runners, so try to get all of them
        Response<RunnersList> response = service.listOrgRunners(
                "Bearer " + token,
                org.toStringUtf8(),
                PER_PAGE,
                (int) page).execute();
        if (response.code() == HTTP_NOT_FOUND) {
            return null;
        }
        Rest.checkServiceResponse(response);
        RunnersList envelope = response.body();
        List<Runner> items = requireNonNull(envelope, "response body is null").getItems();
        items.forEach(i -> i.setOrg(org.toStringUtf8()));
        return buildBlock(items);
    }
}
