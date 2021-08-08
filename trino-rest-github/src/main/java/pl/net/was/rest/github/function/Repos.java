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
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import pl.net.was.rest.Rest;
import pl.net.was.rest.github.GithubTable;
import pl.net.was.rest.github.model.Repository;
import retrofit2.Response;

import java.io.IOException;
import java.util.List;

import static io.trino.spi.type.StandardTypes.BIGINT;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static pl.net.was.rest.github.GithubRest.REPOS_TABLE_TYPE;
import static pl.net.was.rest.github.GithubRest.getRowType;

@ScalarFunction(value = "repos", deterministic = false)
@Description("Get public repositories")
public class Repos
        extends BaseFunction
{
    public Repos()
    {
        RowType rowType = getRowType(GithubTable.REPOS);
        arrayType = new ArrayType(rowType);
        pageBuilder = new PageBuilder(ImmutableList.of(arrayType));
    }

    @SqlType(REPOS_TABLE_TYPE)
    public Block getPage(@SqlType(BIGINT) long sinceId)
            throws IOException
    {
        Response<List<Repository>> response = service.listRepos(
                "Bearer " + token,
                sinceId).execute();
        if (response.code() == HTTP_NOT_FOUND) {
            return null;
        }
        Rest.checkServiceResponse(response);
        List<Repository> items = response.body();
        return buildBlock(items);
    }
}
