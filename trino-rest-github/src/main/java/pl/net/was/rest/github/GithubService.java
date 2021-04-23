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

package pl.net.was.rest.github;

import okhttp3.ResponseBody;
import pl.net.was.rest.github.model.Issue;
import pl.net.was.rest.github.model.JobsList;
import pl.net.was.rest.github.model.RunsList;
import retrofit2.Call;
import retrofit2.http.GET;
import retrofit2.http.Header;
import retrofit2.http.Headers;
import retrofit2.http.Path;
import retrofit2.http.Query;

import java.util.List;

public interface GithubService
{
    @Headers("accept: application/vnd.github.v3+json")
    @GET("/repos/{owner}/{repo}/issues")
    Call<List<Issue>> listIssues(
            @Header("Authorization") String auth,
            @Path("owner") String owner,
            @Path("repo") String repo,
            @Query("per_page") int perPage,
            @Query("page") int page);

    @Headers("accept: application/vnd.github.v3+json")
    @GET("/repos/{owner}/{repo}/actions/runs")
    Call<RunsList> listRuns(
            @Header("Authorization") String auth,
            @Path("owner") String owner,
            @Path("repo") String repo,
            @Query("per_page") int perPage,
            @Query("page") int page);

    @Headers("accept: application/vnd.github.v3+json")
    @GET("/repos/{owner}/{repo}/actions/runs/{run_id}/jobs")
    Call<JobsList> listJobs(
            @Header("Authorization") String auth,
            @Path("owner") String owner,
            @Path("repo") String repo,
            @Path("run_id") long runId,
            @Query("filter") String filter,
            @Query("per_page") int perPage,
            @Query("page") int page);

    @Headers("accept: application/vnd.github.v3+json")
    @GET("/repos/{owner}/{repo}/actions/jobs/{job_id}/logs")
    Call<ResponseBody> jobLogs(
            @Header("Authorization") String auth,
            @Path("owner") String owner,
            @Path("repo") String repo,
            @Path("job_id") long jobId);
}
