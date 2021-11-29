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

package pl.net.was.rest.github.service;

import pl.net.was.rest.github.model.CheckRunAnnotation;
import pl.net.was.rest.github.model.CheckRunsList;
import pl.net.was.rest.github.model.CheckSuitesList;
import retrofit2.Call;
import retrofit2.http.GET;
import retrofit2.http.Header;
import retrofit2.http.Headers;
import retrofit2.http.Path;
import retrofit2.http.Query;

import java.util.List;

public interface CheckService
{
    @Headers("accept: application/vnd.github.v3+json")
    @GET("/repos/{owner}/{repo}/commits/{ref}/check-suites")
    Call<CheckSuitesList> listCheckSuites(
            @Header("Authorization") String auth,
            @Path("owner") String owner,
            @Path("repo") String repo,
            @Path("ref") String ref,
            @Query("per_page") int perPage,
            @Query("page") int page);

    @Headers("accept: application/vnd.github.v3+json")
    @GET("/repos/{owner}/{repo}/commits/{ref}/check-runs")
    Call<CheckRunsList> listCheckRuns(
            @Header("Authorization") String auth,
            @Path("owner") String owner,
            @Path("repo") String repo,
            @Path("ref") String ref,
            @Query("per_page") int perPage,
            @Query("page") int page);

    @Headers("accept: application/vnd.github.v3+json")
    @GET("/repos/{owner}/{repo}/check-runs/{check_run_id}/annotations")
    Call<List<CheckRunAnnotation>> listCheckRunAnnotations(
            @Header("Authorization") String auth,
            @Path("owner") String owner,
            @Path("repo") String repo,
            @Path("check_run_id") long checkRunId,
            @Query("per_page") int perPage,
            @Query("page") int page);
}
