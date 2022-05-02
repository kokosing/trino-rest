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
import pl.net.was.rest.github.model.Team;
import retrofit2.Response;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static io.trino.spi.type.StandardTypes.VARCHAR;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.util.Objects.requireNonNull;
import static pl.net.was.rest.github.GithubRest.TEAMS_TABLE_TYPE;
import static pl.net.was.rest.github.GithubRest.getRowType;

@ScalarFunction(value = "teams", deterministic = false)
@Description("Get teams")
public class Teams
        extends BaseFunction
{
    public Teams()
    {
        RowType rowType = getRowType(GithubTable.TEAMS);
        arrayType = new ArrayType(rowType);
        pageBuilder = new PageBuilder(ImmutableList.of(arrayType));
    }

    @SqlType(TEAMS_TABLE_TYPE)
    public Block getPage(@SqlType(VARCHAR) Slice org)
            throws IOException
    {
        // there should not be more than a few pages worth of members, so try to get all of them
        List<Team> teams = new ArrayList<>();
        int page = 1;
        while (true) {
            Response<List<Team>> response = service.listOrgTeams(
                    "Bearer " + token,
                    org.toStringUtf8(),
                    PER_PAGE,
                    page++).execute();
            if (response.code() == HTTP_NOT_FOUND) {
                break;
            }
            Rest.checkServiceResponse(response);
            List<Team> items = requireNonNull(response.body(), "response body is null");
            if (items.size() == 0) {
                break;
            }
            items.forEach(i -> i.setOrg(org.toStringUtf8()));
            teams.addAll(items);
            if (items.size() < PER_PAGE) {
                break;
            }
        }
        return buildBlock(teams);
    }
}
