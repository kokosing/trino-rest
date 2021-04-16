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
import pl.net.was.rest.github.GithubRest;
import pl.net.was.rest.github.model.Job;
import pl.net.was.rest.github.model.JobsList;
import retrofit2.Response;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static io.trino.spi.type.StandardTypes.BIGINT;
import static io.trino.spi.type.StandardTypes.VARCHAR;
import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;

@ScalarFunction("workflow_jobs")
@Description("Get workflow jobs")
public class WorkflowJobs
        extends BaseFunction
{
    public WorkflowJobs()
    {
        List<RowType.Field> fields = GithubRest.columns.get("jobs")
                .stream()
                .map(columnMetadata -> RowType.field(
                        columnMetadata.getName(),
                        columnMetadata.getType()))
                .collect(Collectors.toList());
        RowType rowType = RowType.from(fields);

        arrayType = new ArrayType(rowType);
        pageBuilder = new PageBuilder(ImmutableList.of(arrayType));
    }

    // TODO can this be constructed automatically? it must match GithubRest.columns
    @SqlType("array(row(" +
            "id bigint, " +
            "run_id bigint, " +
            "run_url varchar, " +
            "node_id varchar, " +
            "head_sha varchar, " +
            "url varchar, " +
            "html_url varchar, " +
            "status varchar, " +
            "conclusion varchar, " +
            "created_at timestamp(3) with time zone, " +
            "updated_at timestamp(3) with time zone, " +
            "name varchar, " +
            "check_run_url varchar" +
            "))")
    public Block getPage(@SqlType(VARCHAR) Slice token, @SqlType(VARCHAR) Slice owner, @SqlType(VARCHAR) Slice repo, @SqlType(BIGINT) long runId)
            throws IOException
    {
        // there should not be more than a few pages worth of jobs, so try to get all of them
        List<Job> jobs = new ArrayList<>();
        long total = Long.MAX_VALUE;
        int page = 1;
        while (jobs.size() < total) {
            Response<JobsList> response = service.listJobs(
                    token.toStringUtf8(),
                    owner.toStringUtf8(),
                    repo.toStringUtf8(),
                    runId,
                    "all",
                    100,
                    page++).execute();
            if (response.code() == HTTP_NOT_FOUND) {
                break;
            }
            if (!response.isSuccessful()) {
                throw new IllegalStateException(format("Invalid response, code %d, message: %s", response.code(), response.message()));
            }
            JobsList jobsList = response.body();
            if (jobsList == null) {
                throw new IllegalStateException("Invalid response");
            }

            total = jobsList.getTotalCount();
            List<Job> pageJobs = jobsList.getJobs();
            if (pageJobs.size() == 0) {
                break;
            }
            jobs.addAll(pageJobs);
        }
        return buildBlock(jobs);
    }
}