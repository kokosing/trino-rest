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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.airlift.slice.Slice;
import io.trino.spi.HostAddress;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.SortItem;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.connector.TopNApplicationResult;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TypeOperators;
import okhttp3.ResponseBody;
import pl.net.was.rest.Rest;
import pl.net.was.rest.RestColumnHandle;
import pl.net.was.rest.RestConfig;
import pl.net.was.rest.RestConnectorSplit;
import pl.net.was.rest.RestTableHandle;
import pl.net.was.rest.github.filter.ArtifactFilter;
import pl.net.was.rest.github.filter.CheckRunAnnotationFilter;
import pl.net.was.rest.github.filter.CheckRunFilter;
import pl.net.was.rest.github.filter.FilterApplier;
import pl.net.was.rest.github.filter.FilterType;
import pl.net.was.rest.github.filter.IssueCommentFilter;
import pl.net.was.rest.github.filter.IssueFilter;
import pl.net.was.rest.github.filter.JobFilter;
import pl.net.was.rest.github.filter.JobLogFilter;
import pl.net.was.rest.github.filter.OrgFilter;
import pl.net.was.rest.github.filter.PullCommitFilter;
import pl.net.was.rest.github.filter.PullFilter;
import pl.net.was.rest.github.filter.RepoFilter;
import pl.net.was.rest.github.filter.ReviewCommentFilter;
import pl.net.was.rest.github.filter.ReviewFilter;
import pl.net.was.rest.github.filter.RunFilter;
import pl.net.was.rest.github.filter.RunnerFilter;
import pl.net.was.rest.github.filter.StepFilter;
import pl.net.was.rest.github.filter.UserFilter;
import pl.net.was.rest.github.function.JobLogs;
import pl.net.was.rest.github.model.Artifact;
import pl.net.was.rest.github.model.ArtifactsList;
import pl.net.was.rest.github.model.Envelope;
import pl.net.was.rest.github.model.Job;
import pl.net.was.rest.github.model.JobsList;
import pl.net.was.rest.github.model.Organization;
import pl.net.was.rest.github.model.Repository;
import pl.net.was.rest.github.model.RunnersList;
import pl.net.was.rest.github.model.Step;
import pl.net.was.rest.github.model.User;
import retrofit2.Call;
import retrofit2.Response;

import javax.inject.Inject;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Verify.verify;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_ORDER_BY;
import static io.trino.spi.StandardErrorCode.INVALID_ROW_FILTER;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static pl.net.was.rest.RestModule.getService;
import static pl.net.was.rest.github.function.Artifacts.download;

public class GithubRest
        implements Rest
{
    Logger log = Logger.getLogger(GithubRest.class.getName());
    public static final String SCHEMA_NAME = "default";

    private static final int PER_PAGE = 100;

    private static String token;
    private final GithubService service = getService(GithubService.class, "https://api.github.com/");

    public static final Map<GithubTable, List<ColumnMetadata>> columns = new ImmutableMap.Builder<GithubTable, List<ColumnMetadata>>()
            .put(GithubTable.ORGS, ImmutableList.of(
                    new ColumnMetadata("login", VARCHAR),
                    new ColumnMetadata("id", BIGINT),
                    new ColumnMetadata("description", VARCHAR),
                    new ColumnMetadata("name", VARCHAR),
                    new ColumnMetadata("company", VARCHAR),
                    new ColumnMetadata("blog", VARCHAR),
                    new ColumnMetadata("location", VARCHAR),
                    new ColumnMetadata("email", VARCHAR),
                    new ColumnMetadata("twitter_username", VARCHAR),
                    new ColumnMetadata("is_verified", BOOLEAN),
                    new ColumnMetadata("has_organization_projects", BOOLEAN),
                    new ColumnMetadata("has_repository_projects", BOOLEAN),
                    new ColumnMetadata("public_repos", BIGINT),
                    new ColumnMetadata("public_gists", BIGINT),
                    new ColumnMetadata("followers", BIGINT),
                    new ColumnMetadata("following", BIGINT),
                    new ColumnMetadata("created_at", TimestampWithTimeZoneType.createTimestampWithTimeZoneType(3)),
                    new ColumnMetadata("updated_at", TimestampWithTimeZoneType.createTimestampWithTimeZoneType(3)),
                    new ColumnMetadata("type", VARCHAR),
                    new ColumnMetadata("total_private_repos", BIGINT),
                    new ColumnMetadata("owned_private_repos", BIGINT),
                    new ColumnMetadata("private_gists", BIGINT),
                    new ColumnMetadata("disk_usage", BIGINT),
                    new ColumnMetadata("collaborators", BIGINT),
                    new ColumnMetadata("billing_email", VARCHAR),
                    new ColumnMetadata("default_repository_permission", VARCHAR),
                    new ColumnMetadata("members_can_create_repositories", BOOLEAN),
                    new ColumnMetadata("two_factor_requirement_enabled", BOOLEAN),
                    new ColumnMetadata("members_allowed_repository_creation_type", VARCHAR),
                    new ColumnMetadata("members_can_create_public_repositories", BOOLEAN),
                    new ColumnMetadata("members_can_create_private_repositories", BOOLEAN),
                    new ColumnMetadata("members_can_create_internal_repositories", BOOLEAN),
                    new ColumnMetadata("members_can_create_pages", BOOLEAN)))
            .put(GithubTable.USERS, ImmutableList.of(
                    new ColumnMetadata("login", VARCHAR),
                    new ColumnMetadata("id", BIGINT),
                    new ColumnMetadata("avatar_url", VARCHAR),
                    new ColumnMetadata("gravatar_id", VARCHAR),
                    new ColumnMetadata("type", VARCHAR),
                    new ColumnMetadata("site_admin", BOOLEAN),
                    new ColumnMetadata("name", VARCHAR),
                    new ColumnMetadata("company", VARCHAR),
                    new ColumnMetadata("blog", VARCHAR),
                    new ColumnMetadata("location", VARCHAR),
                    new ColumnMetadata("email", VARCHAR),
                    new ColumnMetadata("hireable", BOOLEAN),
                    new ColumnMetadata("bio", VARCHAR),
                    new ColumnMetadata("twitter_username", VARCHAR),
                    new ColumnMetadata("public_repos", BIGINT),
                    new ColumnMetadata("public_gists", BIGINT),
                    new ColumnMetadata("followers", BIGINT),
                    new ColumnMetadata("following", BIGINT),
                    new ColumnMetadata("created_at", TimestampWithTimeZoneType.createTimestampWithTimeZoneType(3)),
                    new ColumnMetadata("updated_at", TimestampWithTimeZoneType.createTimestampWithTimeZoneType(3))))
            .put(GithubTable.REPOS, ImmutableList.of(
                    new ColumnMetadata("id", BIGINT),
                    new ColumnMetadata("name", VARCHAR),
                    new ColumnMetadata("full_name", VARCHAR),
                    new ColumnMetadata("owner_id", BIGINT),
                    new ColumnMetadata("owner_login", VARCHAR),
                    new ColumnMetadata("private", BOOLEAN),
                    new ColumnMetadata("description", VARCHAR),
                    new ColumnMetadata("fork", BOOLEAN),
                    new ColumnMetadata("homepage", VARCHAR),
                    new ColumnMetadata("url", VARCHAR),
                    new ColumnMetadata("forks_count", BIGINT),
                    new ColumnMetadata("stargazers_count", BIGINT),
                    new ColumnMetadata("watchers_count", BIGINT),
                    new ColumnMetadata("size", BIGINT),
                    new ColumnMetadata("default_branch", VARCHAR),
                    new ColumnMetadata("open_issues_count", BIGINT),
                    new ColumnMetadata("is_template", BOOLEAN),
                    new ColumnMetadata("topics", new ArrayType(VARCHAR)),
                    new ColumnMetadata("has_issues", BOOLEAN),
                    new ColumnMetadata("has_projects", BOOLEAN),
                    new ColumnMetadata("has_wiki", BOOLEAN),
                    new ColumnMetadata("has_pages", BOOLEAN),
                    new ColumnMetadata("has_downloads", BOOLEAN),
                    new ColumnMetadata("archived", BOOLEAN),
                    new ColumnMetadata("disabled", BOOLEAN),
                    new ColumnMetadata("visibility", VARCHAR),
                    new ColumnMetadata("pushed_at", TimestampWithTimeZoneType.createTimestampWithTimeZoneType(3)),
                    new ColumnMetadata("created_at", TimestampWithTimeZoneType.createTimestampWithTimeZoneType(3)),
                    new ColumnMetadata("updated_at", TimestampWithTimeZoneType.createTimestampWithTimeZoneType(3)),
                    new ColumnMetadata("permissions", new MapType(VARCHAR, BOOLEAN, new TypeOperators()))))
            .put(GithubTable.PULLS, ImmutableList.of(
                    new ColumnMetadata("owner", VARCHAR),
                    new ColumnMetadata("repo", VARCHAR),
                    new ColumnMetadata("id", BIGINT),
                    new ColumnMetadata("number", BIGINT),
                    new ColumnMetadata("state", VARCHAR),
                    new ColumnMetadata("locked", BOOLEAN),
                    new ColumnMetadata("title", VARCHAR),
                    new ColumnMetadata("user_id", BIGINT),
                    new ColumnMetadata("user_login", VARCHAR),
                    new ColumnMetadata("body", VARCHAR),
                    new ColumnMetadata("label_ids", new ArrayType(BIGINT)),
                    new ColumnMetadata("label_names", new ArrayType(VARCHAR)),
                    new ColumnMetadata("milestone_id", BIGINT),
                    new ColumnMetadata("milestone_title", VARCHAR),
                    new ColumnMetadata("active_lock_reason", VARCHAR),
                    new ColumnMetadata("created_at", TimestampWithTimeZoneType.createTimestampWithTimeZoneType(3)),
                    new ColumnMetadata("updated_at", TimestampWithTimeZoneType.createTimestampWithTimeZoneType(3)),
                    new ColumnMetadata("closed_at", TimestampWithTimeZoneType.createTimestampWithTimeZoneType(3)),
                    new ColumnMetadata("merged_at", TimestampWithTimeZoneType.createTimestampWithTimeZoneType(3)),
                    new ColumnMetadata("merge_commit_sha", VARCHAR),
                    new ColumnMetadata("assignee_id", BIGINT),
                    new ColumnMetadata("assignee_login", VARCHAR),
                    new ColumnMetadata("requested_reviewer_ids", new ArrayType(BIGINT)),
                    new ColumnMetadata("requested_reviewer_logins", new ArrayType(VARCHAR)),
                    new ColumnMetadata("head_ref", VARCHAR),
                    new ColumnMetadata("head_sha", VARCHAR),
                    new ColumnMetadata("base_ref", VARCHAR),
                    new ColumnMetadata("base_sha", VARCHAR),
                    new ColumnMetadata("author_association", VARCHAR),
                    new ColumnMetadata("draft", BOOLEAN)))
            .put(GithubTable.PULL_COMMITS, ImmutableList.of(
                    new ColumnMetadata("owner", VARCHAR),
                    new ColumnMetadata("repo", VARCHAR),
                    new ColumnMetadata("sha", VARCHAR),
                    // this column is filled in from request params, it is not returned by the api
                    new ColumnMetadata("pull_number", BIGINT),
                    new ColumnMetadata("commit_message", VARCHAR),
                    new ColumnMetadata("commit_tree_sha", VARCHAR),
                    new ColumnMetadata("commit_comments_count", BIGINT),
                    new ColumnMetadata("commit_verified", BOOLEAN),
                    new ColumnMetadata("commit_verification_reason", VARCHAR),
                    new ColumnMetadata("author_name", VARCHAR),
                    new ColumnMetadata("author_email", VARCHAR),
                    new ColumnMetadata("author_date", TimestampWithTimeZoneType.createTimestampWithTimeZoneType(3)),
                    new ColumnMetadata("author_id", BIGINT),
                    new ColumnMetadata("author_login", VARCHAR),
                    new ColumnMetadata("committer_name", VARCHAR),
                    new ColumnMetadata("committer_email", VARCHAR),
                    new ColumnMetadata("committer_date", TimestampWithTimeZoneType.createTimestampWithTimeZoneType(3)),
                    new ColumnMetadata("committer_id", BIGINT),
                    new ColumnMetadata("committer_login", VARCHAR),
                    new ColumnMetadata("parent_shas", new ArrayType(VARCHAR))))
            .put(GithubTable.REVIEWS, ImmutableList.of(
                    new ColumnMetadata("owner", VARCHAR),
                    new ColumnMetadata("repo", VARCHAR),
                    new ColumnMetadata("id", BIGINT),
                    // this column is filled in from request params, it is not returned by the api
                    new ColumnMetadata("pull_number", BIGINT),
                    new ColumnMetadata("user_id", BIGINT),
                    new ColumnMetadata("user_login", VARCHAR),
                    new ColumnMetadata("body", VARCHAR),
                    new ColumnMetadata("state", VARCHAR),
                    new ColumnMetadata("submitted_at", TimestampWithTimeZoneType.createTimestampWithTimeZoneType(3)),
                    new ColumnMetadata("commit_id", VARCHAR),
                    new ColumnMetadata("author_association", VARCHAR)))
            .put(GithubTable.REVIEW_COMMENTS, ImmutableList.of(
                    new ColumnMetadata("owner", VARCHAR),
                    new ColumnMetadata("repo", VARCHAR),
                    new ColumnMetadata("pull_request_review_id", BIGINT),
                    new ColumnMetadata("id", BIGINT),
                    new ColumnMetadata("diff_hunk", VARCHAR),
                    new ColumnMetadata("path", VARCHAR),
                    new ColumnMetadata("position", BIGINT),
                    new ColumnMetadata("original_position", BIGINT),
                    new ColumnMetadata("commit_id", VARCHAR),
                    new ColumnMetadata("original_commit_id", VARCHAR),
                    new ColumnMetadata("in_reply_to_id", BIGINT),
                    new ColumnMetadata("user_id", BIGINT),
                    new ColumnMetadata("user_login", VARCHAR),
                    new ColumnMetadata("body", VARCHAR),
                    new ColumnMetadata("created_at", TimestampWithTimeZoneType.createTimestampWithTimeZoneType(3)),
                    new ColumnMetadata("updated_at", TimestampWithTimeZoneType.createTimestampWithTimeZoneType(3)),
                    new ColumnMetadata("author_association", VARCHAR),
                    new ColumnMetadata("start_line", BIGINT),
                    new ColumnMetadata("original_start_line", BIGINT),
                    new ColumnMetadata("start_side", VARCHAR),
                    new ColumnMetadata("line", BIGINT),
                    new ColumnMetadata("original_line", BIGINT),
                    new ColumnMetadata("side", VARCHAR)))
            .put(GithubTable.ISSUES, ImmutableList.of(
                    new ColumnMetadata("owner", VARCHAR),
                    new ColumnMetadata("repo", VARCHAR),
                    new ColumnMetadata("id", BIGINT),
                    new ColumnMetadata("number", BIGINT),
                    new ColumnMetadata("state", VARCHAR),
                    new ColumnMetadata("title", VARCHAR),
                    new ColumnMetadata("body", VARCHAR),
                    new ColumnMetadata("user_id", BIGINT),
                    new ColumnMetadata("user_login", VARCHAR),
                    new ColumnMetadata("label_ids", new ArrayType(BIGINT)),
                    new ColumnMetadata("label_names", new ArrayType(VARCHAR)),
                    new ColumnMetadata("assignee_id", BIGINT),
                    new ColumnMetadata("assignee_login", VARCHAR),
                    new ColumnMetadata("milestone_id", BIGINT),
                    new ColumnMetadata("milestone_title", VARCHAR),
                    new ColumnMetadata("locked", BOOLEAN),
                    new ColumnMetadata("active_lock_reason", VARCHAR),
                    new ColumnMetadata("comments", BIGINT),
                    new ColumnMetadata("closed_at", TimestampWithTimeZoneType.createTimestampWithTimeZoneType(3)),
                    new ColumnMetadata("created_at", TimestampWithTimeZoneType.createTimestampWithTimeZoneType(3)),
                    new ColumnMetadata("updated_at", TimestampWithTimeZoneType.createTimestampWithTimeZoneType(3)),
                    new ColumnMetadata("author_association", VARCHAR)))
            .put(GithubTable.ISSUE_COMMENTS, ImmutableList.of(
                    new ColumnMetadata("owner", VARCHAR),
                    new ColumnMetadata("repo", VARCHAR),
                    new ColumnMetadata("id", BIGINT),
                    new ColumnMetadata("body", VARCHAR),
                    new ColumnMetadata("user_id", BIGINT),
                    new ColumnMetadata("user_login", VARCHAR),
                    new ColumnMetadata("created_at", TimestampWithTimeZoneType.createTimestampWithTimeZoneType(3)),
                    new ColumnMetadata("updated_at", TimestampWithTimeZoneType.createTimestampWithTimeZoneType(3)),
                    new ColumnMetadata("author_association", VARCHAR)))
            .put(GithubTable.RUNS, ImmutableList.of(
                    new ColumnMetadata("owner", VARCHAR),
                    new ColumnMetadata("repo", VARCHAR),
                    new ColumnMetadata("id", BIGINT),
                    new ColumnMetadata("name", VARCHAR),
                    new ColumnMetadata("node_id", VARCHAR),
                    new ColumnMetadata("head_branch", VARCHAR),
                    new ColumnMetadata("head_sha", VARCHAR),
                    new ColumnMetadata("run_number", BIGINT),
                    new ColumnMetadata("event", VARCHAR),
                    new ColumnMetadata("status", VARCHAR),
                    new ColumnMetadata("conclusion", VARCHAR),
                    new ColumnMetadata("workflow_id", BIGINT),
                    new ColumnMetadata("created_at", TimestampWithTimeZoneType.createTimestampWithTimeZoneType(3)),
                    new ColumnMetadata("updated_at", TimestampWithTimeZoneType.createTimestampWithTimeZoneType(3))))
            .put(GithubTable.JOBS, ImmutableList.of(
                    new ColumnMetadata("owner", VARCHAR),
                    new ColumnMetadata("repo", VARCHAR),
                    new ColumnMetadata("id", BIGINT),
                    new ColumnMetadata("run_id", BIGINT),
                    new ColumnMetadata("node_id", VARCHAR),
                    new ColumnMetadata("head_sha", VARCHAR),
                    new ColumnMetadata("status", VARCHAR),
                    new ColumnMetadata("conclusion", VARCHAR),
                    new ColumnMetadata("started_at", TimestampWithTimeZoneType.createTimestampWithTimeZoneType(3)),
                    new ColumnMetadata("completed_at", TimestampWithTimeZoneType.createTimestampWithTimeZoneType(3)),
                    new ColumnMetadata("name", VARCHAR)))
            .put(GithubTable.JOB_LOGS, ImmutableList.of(
                    new ColumnMetadata("owner", VARCHAR),
                    new ColumnMetadata("repo", VARCHAR),
                    new ColumnMetadata("job_id", BIGINT),
                    new ColumnMetadata("size_in_bytes", BIGINT),
                    new ColumnMetadata("part_number", INTEGER),
                    new ColumnMetadata("contents", VARBINARY)))
            .put(GithubTable.STEPS, ImmutableList.of(
                    new ColumnMetadata("owner", VARCHAR),
                    new ColumnMetadata("repo", VARCHAR),
                    new ColumnMetadata("run_id", BIGINT),
                    new ColumnMetadata("job_id", BIGINT),
                    new ColumnMetadata("name", VARCHAR),
                    new ColumnMetadata("status", VARCHAR),
                    new ColumnMetadata("conclusion", VARCHAR),
                    new ColumnMetadata("number", BIGINT),
                    new ColumnMetadata("started_at", TimestampWithTimeZoneType.createTimestampWithTimeZoneType(3)),
                    new ColumnMetadata("completed_at", TimestampWithTimeZoneType.createTimestampWithTimeZoneType(3))))
            .put(GithubTable.ARTIFACTS, ImmutableList.of(
                    new ColumnMetadata("owner", VARCHAR),
                    new ColumnMetadata("repo", VARCHAR),
                    new ColumnMetadata("run_id", BIGINT),
                    new ColumnMetadata("id", BIGINT),
                    new ColumnMetadata("size_in_bytes", BIGINT),
                    new ColumnMetadata("name", VARCHAR),
                    new ColumnMetadata("url", VARCHAR),
                    new ColumnMetadata("archive_download_url", VARCHAR),
                    new ColumnMetadata("expired", BOOLEAN),
                    new ColumnMetadata("created_at", TimestampWithTimeZoneType.createTimestampWithTimeZoneType(3)),
                    new ColumnMetadata("expires_at", TimestampWithTimeZoneType.createTimestampWithTimeZoneType(3)),
                    new ColumnMetadata("updated_at", TimestampWithTimeZoneType.createTimestampWithTimeZoneType(3)),
                    new ColumnMetadata("filename", VARCHAR),
                    new ColumnMetadata("path", VARCHAR),
                    new ColumnMetadata("mimetype", VARCHAR),
                    new ColumnMetadata("file_size_in_bytes", BIGINT),
                    new ColumnMetadata("part_number", INTEGER),
                    new ColumnMetadata("contents", VARBINARY)))
            .put(GithubTable.RUNNERS, ImmutableList.of(
                    new ColumnMetadata("org", VARCHAR),
                    new ColumnMetadata("owner", VARCHAR),
                    new ColumnMetadata("repo", VARCHAR),
                    new ColumnMetadata("id", BIGINT),
                    new ColumnMetadata("name", VARCHAR),
                    new ColumnMetadata("os", VARCHAR),
                    new ColumnMetadata("status", VARCHAR),
                    new ColumnMetadata("busy", BOOLEAN),
                    new ColumnMetadata("label_ids", new ArrayType(BIGINT)),
                    new ColumnMetadata("label_names", new ArrayType(VARCHAR))))
            .put(GithubTable.CHECK_RUNS, ImmutableList.of(
                    new ColumnMetadata("owner", VARCHAR),
                    new ColumnMetadata("repo", VARCHAR),
                    new ColumnMetadata("ref", VARCHAR),
                    new ColumnMetadata("id", BIGINT),
                    new ColumnMetadata("head_sha", VARCHAR),
                    new ColumnMetadata("external_id", VARCHAR),
                    new ColumnMetadata("url", VARCHAR),
                    new ColumnMetadata("html_url", VARCHAR),
                    new ColumnMetadata("details_url", VARCHAR),
                    new ColumnMetadata("status", VARCHAR),
                    new ColumnMetadata("conclusion", VARCHAR),
                    new ColumnMetadata("started_at", TimestampWithTimeZoneType.createTimestampWithTimeZoneType(3)),
                    new ColumnMetadata("completed_at", TimestampWithTimeZoneType.createTimestampWithTimeZoneType(3)),
                    new ColumnMetadata("output_title", VARCHAR),
                    new ColumnMetadata("output_summary", VARCHAR),
                    new ColumnMetadata("output_text", VARCHAR),
                    new ColumnMetadata("annotations_count", BIGINT),
                    new ColumnMetadata("annotations_url", VARCHAR),
                    new ColumnMetadata("name", VARCHAR),
                    new ColumnMetadata("check_suite_id", BIGINT),
                    new ColumnMetadata("app_id", BIGINT),
                    new ColumnMetadata("app_slug", VARCHAR),
                    new ColumnMetadata("app_name", VARCHAR)))
            .put(GithubTable.CHECK_RUN_ANNOTATIONS, ImmutableList.of(
                    new ColumnMetadata("owner", VARCHAR),
                    new ColumnMetadata("repo", VARCHAR),
                    new ColumnMetadata("check_run_id", BIGINT),
                    new ColumnMetadata("path", VARCHAR),
                    new ColumnMetadata("start_line", INTEGER),
                    new ColumnMetadata("end_line", INTEGER),
                    new ColumnMetadata("start_column", INTEGER),
                    new ColumnMetadata("end_column", INTEGER),
                    new ColumnMetadata("annotation_level", VARCHAR),
                    new ColumnMetadata("title", VARCHAR),
                    new ColumnMetadata("message", VARCHAR),
                    new ColumnMetadata("raw_details", VARCHAR),
                    new ColumnMetadata("blob_href", VARCHAR)))
            .build();

    private final Map<GithubTable, Function<RestTableHandle, Collection<? extends List<?>>>> rowGetters = new ImmutableMap.Builder<GithubTable, Function<RestTableHandle, Collection<? extends List<?>>>>()
            .put(GithubTable.ORGS, this::getOrgs)
            .put(GithubTable.USERS, this::getUsers)
            .put(GithubTable.REPOS, this::getRepos)
            .put(GithubTable.PULLS, this::getPulls)
            .put(GithubTable.PULL_COMMITS, this::getPullCommits)
            .put(GithubTable.REVIEWS, this::getReviews)
            .put(GithubTable.REVIEW_COMMENTS, this::getReviewComments)
            .put(GithubTable.ISSUES, this::getIssues)
            .put(GithubTable.ISSUE_COMMENTS, this::getIssueComments)
            .put(GithubTable.RUNS, this::getRuns)
            .put(GithubTable.JOBS, this::getJobs)
            .put(GithubTable.JOB_LOGS, this::getJobLogs)
            .put(GithubTable.STEPS, this::getSteps)
            .put(GithubTable.ARTIFACTS, this::getArtifacts)
            .put(GithubTable.RUNNERS, this::getRunners)
            .put(GithubTable.CHECK_RUNS, this::getCheckRuns)
            .put(GithubTable.CHECK_RUN_ANNOTATIONS, this::getCheckRunAnnotations)
            .build();

    // The first sortItem is the default
    private final Map<GithubTable, List<SortItem>> supportedTableSort = new ImmutableMap.Builder<GithubTable, List<SortItem>>()
            .put(GithubTable.ORGS, ImmutableList.of(
                    new SortItem("created_at", SortOrder.ASC_NULLS_LAST)))
            .put(GithubTable.REPOS, ImmutableList.of(
                    new SortItem("full_name", SortOrder.ASC_NULLS_LAST),
                    new SortItem("full_name", SortOrder.DESC_NULLS_LAST),
                    new SortItem("created_at", SortOrder.DESC_NULLS_LAST),
                    new SortItem("created_at", SortOrder.ASC_NULLS_LAST),
                    new SortItem("updated_at", SortOrder.DESC_NULLS_LAST),
                    new SortItem("updated_at", SortOrder.ASC_NULLS_LAST),
                    new SortItem("pushed_at", SortOrder.ASC_NULLS_LAST),
                    new SortItem("pushed_at", SortOrder.DESC_NULLS_LAST)))
            .put(GithubTable.PULLS, ImmutableList.of(
                    new SortItem("created_at", SortOrder.DESC_NULLS_LAST),
                    new SortItem("created_at", SortOrder.ASC_NULLS_LAST),
                    new SortItem("updated_at", SortOrder.ASC_NULLS_LAST),
                    new SortItem("updated_at", SortOrder.DESC_NULLS_LAST)))
            .put(GithubTable.PULL_COMMITS, ImmutableList.of(
                    new SortItem("created_at", SortOrder.ASC_NULLS_LAST)))
            .put(GithubTable.REVIEWS, ImmutableList.of(
                    new SortItem("created_at", SortOrder.ASC_NULLS_LAST)))
            .put(GithubTable.REVIEW_COMMENTS, ImmutableList.of(
                    new SortItem("id", SortOrder.ASC_NULLS_LAST),
                    new SortItem("id", SortOrder.DESC_NULLS_LAST),
                    new SortItem("created_at", SortOrder.DESC_NULLS_LAST),
                    new SortItem("created_at", SortOrder.ASC_NULLS_LAST)))
            .put(GithubTable.ISSUES, ImmutableList.of(
                    new SortItem("created_at", SortOrder.DESC_NULLS_LAST),
                    new SortItem("created_at", SortOrder.ASC_NULLS_LAST),
                    new SortItem("updated_at", SortOrder.DESC_NULLS_LAST),
                    new SortItem("updated_at", SortOrder.ASC_NULLS_LAST),
                    new SortItem("comments", SortOrder.DESC_NULLS_LAST),
                    new SortItem("comments", SortOrder.ASC_NULLS_LAST)))
            .put(GithubTable.ISSUE_COMMENTS, ImmutableList.of(
                    new SortItem("id", SortOrder.ASC_NULLS_LAST),
                    new SortItem("id", SortOrder.DESC_NULLS_LAST),
                    new SortItem("created_at", SortOrder.ASC_NULLS_LAST),
                    new SortItem("created_at", SortOrder.DESC_NULLS_LAST)))
            .put(GithubTable.RUNS, ImmutableList.of(
                    new SortItem("created_at", SortOrder.DESC_NULLS_LAST)))
            .put(GithubTable.JOBS, ImmutableList.of(
                    new SortItem("started_at", SortOrder.ASC_NULLS_LAST)))
            .put(GithubTable.STEPS, ImmutableList.of(
                    new SortItem("job_id", SortOrder.ASC_NULLS_LAST)))
            .put(GithubTable.ARTIFACTS, ImmutableList.of(
                    new SortItem("id", SortOrder.ASC_NULLS_LAST)))
            .build();

    // TODO add tests that would verify this using getSqlType(), print the expected string so its easy to copy&paste
    // TODO consider moving to a separate class
    public static final String ORG_ROW_TYPE = "row(" +
            "login varchar, " +
            "id bigint, " +
            "description varchar, " +
            "name varchar, " +
            "company varchar, " +
            "blog varchar, " +
            "location varchar, " +
            "email varchar, " +
            "twitter_username varchar, " +
            "is_verified boolean, " +
            "has_organization_projects boolean, " +
            "has_repository_projects boolean, " +
            "public_repos bigint, " +
            "public_gists bigint, " +
            "followers bigint, " +
            "following bigint, " +
            "created_at timestamp(3) with time zone, " +
            "updated_at timestamp(3) with time zone, " +
            "type varchar, " +
            "total_private_repos bigint, " +
            "owned_private_repos bigint, " +
            "private_gists bigint, " +
            "disk_usage bigint, " +
            "collaborators bigint, " +
            "billing_email varchar, " +
            "default_repository_permission varchar, " +
            "members_can_create_repositories boolean, " +
            "two_factor_requirement_enabled boolean, " +
            "members_allowed_repository_creation_type varchar, " +
            "members_can_create_public_repositories boolean, " +
            "members_can_create_private_repositories boolean, " +
            "members_can_create_internal_repositories boolean, " +
            "members_can_create_pages boolean" +
            ")";

    public static final String ORGS_TABLE_TYPE = "array(" + ORG_ROW_TYPE + ")";

    public static final String USER_ROW_TYPE = "row(" +
            "login varchar, " +
            "id bigint, " +
            "avatar_url varchar, " +
            "gravatar_id varchar, " +
            "type varchar, " +
            "site_admin boolean, " +
            "name varchar, " +
            "company varchar, " +
            "blog varchar, " +
            "location varchar, " +
            "email varchar, " +
            "hireable boolean, " +
            "bio varchar, " +
            "twitter_username varchar, " +
            "public_repos bigint, " +
            "public_gists bigint, " +
            "followers bigint, " +
            "following bigint, " +
            "created_at timestamp(3) with time zone, " +
            "updated_at timestamp(3) with time zone" +
            ")";

    public static final String USERS_TABLE_TYPE = "array(" + USER_ROW_TYPE + ")";

    public static final String REPOS_TABLE_TYPE = "array(row(" +
            "id bigint, " +
            "name varchar, " +
            "full_name varchar, " +
            "owner_id bigint, " +
            "owner_login varchar, " +
            "private boolean, " +
            "description varchar, " +
            "fork boolean, " +
            "homepage varchar, " +
            "url varchar, " +
            "forks_count bigint, " +
            "stargazers_count bigint, " +
            "watchers_count bigint, " +
            "size bigint, " +
            "default_branch varchar, " +
            "open_issues_count bigint, " +
            "is_template boolean, " +
            "topics array(varchar), " +
            "has_issues boolean, " +
            "has_projects boolean, " +
            "has_wiki boolean, " +
            "has_pages boolean, " +
            "has_downloads boolean, " +
            "archived boolean, " +
            "disabled boolean, " +
            "visibility varchar, " +
            "pushed_at timestamp(3) with time zone, " +
            "created_at timestamp(3) with time zone, " +
            "updated_at timestamp(3) with time zone, " +
            "permissions map(varchar, boolean)" +
            "))";

    public static final String PULLS_TABLE_TYPE = "array(row(" +
            "owner varchar, " +
            "repo varchar, " +
            "id bigint, " +
            "number bigint, " +
            "state varchar, " +
            "locked boolean, " +
            "title varchar, " +
            "user_id bigint, " +
            "user_login varchar, " +
            "body varchar, " +
            "label_ids array(bigint), " +
            "label_names array(varchar), " +
            "milestone_id bigint, " +
            "milestone_title varchar, " +
            "active_lock_reason varchar, " +
            "created_at timestamp(3) with time zone, " +
            "updated_at timestamp(3) with time zone, " +
            "closed_at timestamp(3) with time zone, " +
            "merged_at timestamp(3) with time zone, " +
            "merge_commit_sha varchar, " +
            "assignee_id bigint, " +
            "assignee_login varchar, " +
            "requested_reviewer_ids array(bigint), " +
            "requested_reviewer_logins array(varchar), " +
            "head_ref varchar, " +
            "head_sha varchar, " +
            "base_ref varchar, " +
            "base_sha varchar, " +
            "author_association varchar, " +
            "draft boolean" +
            "))";

    public static final String PULL_COMMITS_TABLE_TYPE = "array(row(" +
            "owner varchar, " +
            "repo varchar, " +
            "sha varchar, " +
            "pull_number bigint, " +
            "commit_message varchar, " +
            "commit_tree_sha varchar, " +
            "commit_comments_count bigint, " +
            "commit_verified boolean, " +
            "commit_verification_reason varchar, " +
            "author_name varchar, " +
            "author_email varchar, " +
            "author_date timestamp(3) with time zone, " +
            "author_id bigint, " +
            "author_login varchar, " +
            "committer_name varchar, " +
            "committer_email varchar, " +
            "committer_date timestamp(3) with time zone, " +
            "committer_id bigint, " +
            "committer_login varchar, " +
            "parent_shas array(varchar)" +
            "))";

    public static final String REVIEWS_TABLE_TYPE = "array(row(" +
            "owner varchar, " +
            "repo varchar, " +
            "id bigint, " +
            "pull_number bigint, " +
            "user_id bigint, " +
            "user_login varchar, " +
            "body varchar, " +
            "state varchar, " +
            "submitted_at timestamp(3) with time zone, " +
            "commit_id varchar, " +
            "author_association varchar" +
            "))";

    public static final String REVIEW_COMMENTS_TABLE_TYPE = "array(row(" +
            "owner varchar, " +
            "repo varchar, " +
            "pull_request_review_id bigint, " +
            "id bigint, " +
            "diff_hunk varchar, " +
            "path varchar, " +
            "position bigint, " +
            "original_position bigint, " +
            "commit_id varchar, " +
            "original_commit_id varchar, " +
            "in_reply_to_id bigint, " +
            "user_id bigint, " +
            "user_login varchar, " +
            "body varchar, " +
            "created_at timestamp(3) with time zone, " +
            "updated_at timestamp(3) with time zone, " +
            "author_association varchar, " +
            "start_line bigint, " +
            "original_start_line bigint, " +
            "start_side varchar, " +
            "line bigint, " +
            "original_line bigint, " +
            "side varchar" +
            "))";

    public static final String ISSUES_TABLE_TYPE = "array(row(" +
            "owner varchar, " +
            "repo varchar, " +
            "id bigint, " +
            "number bigint, " +
            "state varchar, " +
            "title varchar, " +
            "body varchar, " +
            "user_id bigint, " +
            "user_login varchar, " +
            "label_ids array(bigint), " +
            "label_names array(varchar), " +
            "assignee_id bigint, " +
            "assignee_login varchar, " +
            "milestone_id bigint, " +
            "milestone_title varchar, " +
            "locked boolean, " +
            "active_lock_reason varchar, " +
            "comments bigint, " +
            "closed_at timestamp(3) with time zone, " +
            "created_at timestamp(3) with time zone, " +
            "updated_at timestamp(3) with time zone, " +
            "author_association varchar" +
            "))";

    public static final String ISSUE_COMMENTS_TABLE_TYPE = "array(row(" +
            "owner varchar, " +
            "repo varchar, " +
            "id bigint, " +
            "body varchar, " +
            "user_id bigint, " +
            "user_login varchar, " +
            "created_at timestamp(3) with time zone, " +
            "updated_at timestamp(3) with time zone, " +
            "author_association varchar" +
            "))";

    public static final String RUNS_TABLE_TYPE = "array(row(" +
            "owner varchar, " +
            "repo varchar, " +
            "id bigint, " +
            "name varchar, " +
            "node_id varchar, " +
            "head_branch varchar, " +
            "head_sha varchar, " +
            "run_number bigint, " +
            "event varchar, " +
            "status varchar, " +
            "conclusion varchar, " +
            "workflow_id bigint, " +
            "created_at timestamp(3) with time zone, " +
            "updated_at timestamp(3) with time zone" +
            "))";

    public static final String JOBS_TABLE_TYPE = "array(row(" +
            "owner varchar, " +
            "repo varchar, " +
            "id bigint, " +
            "run_id bigint, " +
            "node_id varchar, " +
            "head_sha varchar, " +
            "status varchar, " +
            "conclusion varchar, " +
            "created_at timestamp(3) with time zone, " +
            "updated_at timestamp(3) with time zone, " +
            "name varchar" +
            "))";

    public static final String JOB_LOGS_TABLE_TYPE = "array(row(" +
            "owner varchar, " +
            "repo varchar, " +
            "job_id bigint, " +
            "size_in_bytes bigint, " +
            "part_number integer, " +
            "contents varbinary" +
            "))";

    public static final String STEPS_TABLE_TYPE = "array(row(" +
            "owner varchar, " +
            "repo varchar, " +
            "run_id bigint, " +
            "job_id bigint, " +
            "name varchar, " +
            "status varchar, " +
            "conclusion varchar, " +
            "number bigint, " +
            "created_at timestamp(3) with time zone, " +
            "updated_at timestamp(3) with time zone" +
            "))";

    public static final String ARTIFACTS_TABLE_TYPE = "array(row(" +
            "owner varchar, " +
            "repo varchar, " +
            "run_id bigint, " +
            "id bigint, " +
            "size_in_bytes bigint, " +
            "name varchar, " +
            "url varchar, " +
            "archive_download_url varchar, " +
            "expired boolean, " +
            "created_at timestamp(3) with time zone, " +
            "expires_at timestamp(3) with time zone, " +
            "updated_at timestamp(3) with time zone, " +
            "filename varchar, " +
            "path varchar, " +
            "mimetype varchar, " +
            "file_size_in_bytes bigint, " +
            "part_number integer, " +
            "contents varbinary" +
            "))";

    public static final String RUNNERS_TABLE_TYPE = "array(row(" +
            "org varchar, " +
            "owner varchar, " +
            "repo varchar, " +
            "id bigint, " +
            "name varchar, " +
            "os varchar, " +
            "status varchar, " +
            "busy boolean, " +
            "label_ids array(bigint), " +
            "label_names array(varchar)" +
            "))";

    /* there are no functions that would use these
    public static final String CHECK_RUNS_TABLE_TYPE = "array(row(" +
            "owner varchar, " +
            "repo varchar, " +
            "ref varchar, " +
            "id bigint, " +
            "head_sha varchar, " +
            "external_id varchar, " +
            "url varchar, " +
            "html_url varchar, " +
            "details_url varchar, " +
            "status varchar, " +
            "conclusion varchar, " +
            "started_at timestamp(3) with time zone, " +
            "completed_at timestamp(3) with time zone, " +
            "output_title varchar, " +
            "output_summary varchar, " +
            "output_text varchar, " +
            "annotations_count bigint, " +
            "annotations_url varchar, " +
            "check_suite_id bigint, " +
            "app_id bigint, " +
            "app_slug varchar, " +
            "app_name varchar" +
        "))";

    public static final String CHECK_RUN_ANNOTATIONS_TABLE_TYPE = "array(row(" +
        "owner varchar, " +
        "repo varchar, " +
        "check_run_id bigint, " +
        "path varchar, " +
        "start_line integer, " +
        "end_line integer, " +
        "start_column integer, " +
        "end_column integer, " +
        "annotation_level varchar, " +
        "title varchar, " +
        "message varchar, " +
        "raw_details varchar, " +
        "blob_href varchar" +
        "))";
     */

    private final Map<GithubTable, Map<String, ColumnHandle>> columnHandles;

    private final Map<GithubTable, ? extends FilterApplier> filterAppliers = new ImmutableMap.Builder<GithubTable, FilterApplier>()
            .put(GithubTable.PULLS, new PullFilter())
            .put(GithubTable.PULL_COMMITS, new PullCommitFilter())
            .put(GithubTable.REVIEWS, new ReviewFilter())
            .put(GithubTable.REVIEW_COMMENTS, new ReviewCommentFilter())
            .put(GithubTable.ISSUES, new IssueFilter())
            .put(GithubTable.ISSUE_COMMENTS, new IssueCommentFilter())
            .put(GithubTable.RUNS, new RunFilter())
            .put(GithubTable.JOBS, new JobFilter())
            .put(GithubTable.JOB_LOGS, new JobLogFilter())
            .put(GithubTable.STEPS, new StepFilter())
            .put(GithubTable.ARTIFACTS, new ArtifactFilter())
            .put(GithubTable.RUNNERS, new RunnerFilter())
            .put(GithubTable.ORGS, new OrgFilter())
            .put(GithubTable.USERS, new UserFilter())
            .put(GithubTable.REPOS, new RepoFilter())
            .put(GithubTable.CHECK_RUNS, new CheckRunFilter())
            .put(GithubTable.CHECK_RUN_ANNOTATIONS, new CheckRunAnnotationFilter())
            .build();

    @Inject
    public GithubRest(RestConfig config)
    {
        requireNonNull(config, "config is null");
        GithubRest.token = config.getToken();

        columnHandles = columns.keySet()
                .stream()
                .collect(Collectors.toMap(
                        tableName -> tableName,
                        tableName -> columns.get(tableName)
                                .stream()
                                .collect(toMap(
                                        ColumnMetadata::getName,
                                        column -> new RestColumnHandle(column.getName(), column.getType())))));
    }

    public static String getToken()
    {
        return token;
    }

    public static <T> void checkServiceResponse(Response<T> response)
    {
        if (response.isSuccessful()) {
            return;
        }
        ResponseBody error = response.errorBody();
        String message = "Unable to read: ";
        if (error != null) {
            try {
                // TODO unserialize the JSON in error: https://github.com/nineinchnick/trino-rest/issues/33
                message += error.string();
            }
            catch (IOException e) {
                // pass
            }
        }
        throw new TrinoException(GENERIC_INTERNAL_ERROR, message);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(SchemaTableName schemaTableName)
    {
        GithubTable tableName = GithubTable.valueOf(schemaTableName);
        return new ConnectorTableMetadata(
                schemaTableName,
                columns.get(tableName));
    }

    @Override
    public List<String> listSchemas()
    {
        return ImmutableList.of(SCHEMA_NAME);
    }

    @Override
    public List<SchemaTableName> listTables(String schema)
    {
        return columns
                .keySet()
                .stream()
                .map(table -> new SchemaTableName(SCHEMA_NAME, table.getName()))
                .collect(toList());
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(SchemaTablePrefix schemaTablePrefix)
    {
        return columns.entrySet()
                .stream()
                .collect(Collectors.toMap(
                        e -> new SchemaTableName(schemaTablePrefix.getSchema().orElse(""), e.getKey().getName()),
                        Map.Entry::getValue));
    }

    @Override
    public Collection<? extends List<?>> getRows(RestTableHandle table)
    {
        GithubTable tableName = GithubTable.valueOf(table);
        return rowGetters.get(tableName).apply(table);
    }

    private Collection<? extends List<?>> getOrgs(RestTableHandle table)
    {
        if (table.getLimit() == 0) {
            return ImmutableList.of();
        }
        GithubTable tableName = GithubTable.valueOf(table);
        Map<String, ColumnHandle> columns = columnHandles.get(tableName);
        FilterApplier filter = filterAppliers.get(tableName);

        String login = (String) filter.getFilter((RestColumnHandle) columns.get("login"), table.getConstraint());
        return getRow(() -> service.getOrg("Bearer " + token, login), Organization::toRow);
    }

    private Collection<? extends List<?>> getUsers(RestTableHandle table)
    {
        if (table.getLimit() == 0) {
            return ImmutableList.of();
        }
        GithubTable tableName = GithubTable.valueOf(table);
        Map<String, ColumnHandle> columns = columnHandles.get(tableName);
        FilterApplier filter = filterAppliers.get(tableName);

        String login = (String) filter.getFilter((RestColumnHandle) columns.get("login"), table.getConstraint());
        return getRow(() -> service.getUser("Bearer " + token, login), User::toRow);
    }

    private Collection<? extends List<?>> getRepos(RestTableHandle table)
    {
        GithubTable tableName = GithubTable.valueOf(table);
        Map<String, ColumnHandle> columns = columnHandles.get(tableName);
        FilterApplier filter = filterAppliers.get(tableName);

        String owner = (String) filter.getFilter((RestColumnHandle) columns.get("owner_login"), table.getConstraint());
        SortItem sortOrder = getSortItem(table);
        return getRowsFromPages(
                page -> service.listUserRepos(
                        "Bearer " + token,
                        owner,
                        PER_PAGE,
                        page,
                        sortOrder.getName(),
                        sortOrder.getSortOrder().isAscending() ? "asc" : "desc"),
                Repository::toRow,
                table.getLimit());
    }

    private Collection<? extends List<?>> getPulls(RestTableHandle table)
    {
        GithubTable tableName = GithubTable.valueOf(table);
        TupleDomain<ColumnHandle> constraint = table.getConstraint();
        Map<String, ColumnHandle> columns = columnHandles.get(tableName);
        FilterApplier filter = filterAppliers.get(tableName);

        String owner = (String) filter.getFilter((RestColumnHandle) columns.get("owner"), constraint);
        String repo = (String) filter.getFilter((RestColumnHandle) columns.get("repo"), constraint);
        requirePredicate(owner, "owner");
        requirePredicate(repo, "repo");
        // TODO allow filtering by state (many, comma separated, or all), requires https://github.com/nineinchnick/trino-rest/issues/30
        SortItem sortOrder = getSortItem(table);
        return getRowsFromPages(
                page -> service.listPulls(
                        "Bearer " + token,
                        owner,
                        repo,
                        PER_PAGE,
                        page,
                        sortOrder.getName(),
                        sortOrder.getSortOrder().isAscending() ? "asc" : "desc",
                        "all"),
                item -> {
                    item.setOwner(owner);
                    item.setRepo(repo);
                    return item.toRow();
                },
                table.getLimit());
    }

    private void requirePredicate(Object value, String name)
    {
        if (value == null) {
            throw new TrinoException(INVALID_ROW_FILTER, "Missing required constraint for " + name);
        }
    }

    private SortItem getSortItem(RestTableHandle table)
    {
        GithubTable tableName = GithubTable.valueOf(table);
        List<SortItem> sortOrder = table.getSortOrder().orElseGet(() -> supportedTableSort.get(tableName));
        String sortName = sortOrder.get(0).getName();
        switch (sortOrder.get(0).getName()) {
            case "created_at":
                sortName = "created";
                break;
            case "updated_at":
                sortName = "updated";
                break;
        }
        return new SortItem(sortName, sortOrder.get(0).getSortOrder());
    }

    private Collection<? extends List<?>> getPullCommits(RestTableHandle table)
    {
        GithubTable tableName = GithubTable.valueOf(table);
        TupleDomain<ColumnHandle> constraint = table.getConstraint();
        Map<String, ColumnHandle> columns = columnHandles.get(tableName);
        FilterApplier filter = filterAppliers.get(tableName);

        String owner = (String) filter.getFilter((RestColumnHandle) columns.get("owner"), constraint);
        String repo = (String) filter.getFilter((RestColumnHandle) columns.get("repo"), constraint);
        requirePredicate(owner, "owner");
        requirePredicate(repo, "repo");
        long pullNumber = (long) filter.getFilter((RestColumnHandle) columns.get("pull_number"), constraint);
        // TODO allow filtering by state (many, comma separated, or all), requires https://github.com/nineinchnick/trino-rest/issues/30
        return getRowsFromPages(
                page -> service.listPullCommits("Bearer " + token, owner, repo, pullNumber, PER_PAGE, page),
                item -> {
                    item.setOwner(owner);
                    item.setRepo(repo);
                    item.setPullNumber(pullNumber);
                    return item.toRow();
                },
                table.getLimit());
    }

    private Collection<? extends List<?>> getReviews(RestTableHandle table)
    {
        GithubTable tableName = GithubTable.valueOf(table);
        TupleDomain<ColumnHandle> constraint = table.getConstraint();
        Map<String, ColumnHandle> columns = columnHandles.get(tableName);
        FilterApplier filter = filterAppliers.get(tableName);

        String owner = (String) filter.getFilter((RestColumnHandle) columns.get("owner"), constraint);
        String repo = (String) filter.getFilter((RestColumnHandle) columns.get("repo"), constraint);
        requirePredicate(owner, "owner");
        requirePredicate(repo, "repo");
        long pullNumber = (long) filter.getFilter((RestColumnHandle) columns.get("pull_number"), constraint);
        return getRowsFromPages(
                page -> service.listPullReviews("Bearer " + token, owner, repo, pullNumber, PER_PAGE, page),
                item -> {
                    item.setOwner(owner);
                    item.setRepo(repo);
                    return item.toRow();
                },
                table.getLimit());
    }

    private Collection<? extends List<?>> getReviewComments(RestTableHandle table)
    {
        GithubTable tableName = GithubTable.valueOf(table);
        TupleDomain<ColumnHandle> constraint = table.getConstraint();
        Map<String, ColumnHandle> columns = columnHandles.get(tableName);
        FilterApplier filter = filterAppliers.get(tableName);

        String owner = (String) filter.getFilter((RestColumnHandle) columns.get("owner"), constraint);
        String repo = (String) filter.getFilter((RestColumnHandle) columns.get("repo"), constraint);
        requirePredicate(owner, "owner");
        requirePredicate(repo, "repo");
        String since = (String) filter.getFilter((RestColumnHandle) columns.get("updated_at"), constraint);
        // TODO allow filtering by pull number, this would require using a different endpoint
        SortItem sortOrder = getSortItem(table);
        return getRowsFromPages(
                page -> service.listReviewComments(
                        "Bearer " + token,
                        owner,
                        repo,
                        PER_PAGE,
                        page,
                        sortOrder.getName(),
                        sortOrder.getSortOrder().isAscending() ? "asc" : "desc",
                        since),
                item -> {
                    item.setOwner(owner);
                    item.setRepo(repo);
                    return item.toRow();
                },
                table.getLimit());
    }

    private Collection<? extends List<?>> getIssues(RestTableHandle table)
    {
        GithubTable tableName = GithubTable.valueOf(table);
        TupleDomain<ColumnHandle> constraint = table.getConstraint();
        Map<String, ColumnHandle> columns = columnHandles.get(tableName);
        FilterApplier filter = filterAppliers.get(tableName);

        String owner = (String) filter.getFilter((RestColumnHandle) columns.get("owner"), constraint);
        String repo = (String) filter.getFilter((RestColumnHandle) columns.get("repo"), constraint);
        requirePredicate(owner, "owner");
        requirePredicate(repo, "repo");
        String since = (String) filter.getFilter((RestColumnHandle) columns.get("updated_at"), constraint);
        SortItem sortOrder = getSortItem(table);
        return getRowsFromPages(
                page -> service.listIssues(
                        "Bearer " + token,
                        owner,
                        repo,
                        PER_PAGE,
                        page,
                        sortOrder.getName(),
                        sortOrder.getSortOrder().isAscending() ? "asc" : "desc",
                        since),
                item -> {
                    item.setOwner(owner);
                    item.setRepo(repo);
                    return item.toRow();
                },
                table.getLimit());
    }

    private Collection<? extends List<?>> getIssueComments(RestTableHandle table)
    {
        GithubTable tableName = GithubTable.valueOf(table);
        TupleDomain<ColumnHandle> constraint = table.getConstraint();
        Map<String, ColumnHandle> columns = columnHandles.get(tableName);
        FilterApplier filter = filterAppliers.get(tableName);

        String owner = (String) filter.getFilter((RestColumnHandle) columns.get("owner"), constraint);
        String repo = (String) filter.getFilter((RestColumnHandle) columns.get("repo"), constraint);
        requirePredicate(owner, "owner");
        requirePredicate(repo, "repo");
        String since = (String) filter.getFilter((RestColumnHandle) columns.get("updated_at"), constraint);
        SortItem sortOrder = getSortItem(table);
        return getRowsFromPages(
                page -> service.listIssueComments(
                        "Bearer " + token,
                        owner,
                        repo,
                        PER_PAGE,
                        page,
                        sortOrder.getName(),
                        sortOrder.getSortOrder().isAscending() ? "asc" : "desc",
                        since),
                item -> {
                    item.setOwner(owner);
                    item.setRepo(repo);
                    return item.toRow();
                },
                table.getLimit());
    }

    private Collection<? extends List<?>> getRuns(RestTableHandle table)
    {
        GithubTable tableName = GithubTable.valueOf(table);
        TupleDomain<ColumnHandle> constraint = table.getConstraint();
        Map<String, ColumnHandle> columns = columnHandles.get(tableName);
        FilterApplier filter = filterAppliers.get(tableName);

        String owner = (String) filter.getFilter((RestColumnHandle) columns.get("owner"), constraint);
        String repo = (String) filter.getFilter((RestColumnHandle) columns.get("repo"), constraint);
        requirePredicate(owner, "owner");
        requirePredicate(repo, "repo");
        return getRowsFromPagesEnvelope(
                page -> service.listRuns("Bearer " + token, owner, repo, PER_PAGE, page),
                item -> {
                    item.setOwner(owner);
                    item.setRepo(repo);
                    return Stream.of(item.toRow());
                },
                table.getLimit());
    }

    private Collection<? extends List<?>> getJobs(RestTableHandle table)
    {
        GithubTable tableName = GithubTable.valueOf(table);
        TupleDomain<ColumnHandle> constraint = table.getConstraint();
        Map<String, ColumnHandle> columns = columnHandles.get(tableName);
        FilterApplier filter = filterAppliers.get(tableName);

        String owner = (String) filter.getFilter((RestColumnHandle) columns.get("owner"), constraint);
        String repo = (String) filter.getFilter((RestColumnHandle) columns.get("repo"), constraint);
        requirePredicate(owner, "owner");
        requirePredicate(repo, "repo");
        Long runId = (Long) filter.getFilter((RestColumnHandle) columns.get("run_id"), constraint);
        if (runId == null) {
            throw new TrinoException(INVALID_ROW_FILTER, "Missing required constraint for run_id");
        }
        // TODO this needs to allow pushing down multiple run_id values and make a separate request for each: https://github.com/nineinchnick/trino-rest/issues/30
        return getRowsFromPagesEnvelope(
                page -> service.listRunJobs("Bearer " + token, owner, repo, runId, "all", PER_PAGE, page),
                item -> {
                    item.setOwner(owner);
                    item.setRepo(repo);
                    return Stream.of(item.toRow());
                },
                table.getLimit());
    }

    private Collection<? extends List<?>> getJobLogs(RestTableHandle table)
    {
        GithubTable tableName = GithubTable.valueOf(table);
        TupleDomain<ColumnHandle> constraint = table.getConstraint();
        Map<String, ColumnHandle> columns = columnHandles.get(tableName);
        FilterApplier filter = filterAppliers.get(tableName);

        String owner = (String) filter.getFilter((RestColumnHandle) columns.get("owner"), constraint);
        String repo = (String) filter.getFilter((RestColumnHandle) columns.get("repo"), constraint);
        requirePredicate(owner, "owner");
        requirePredicate(repo, "repo");
        Long jobId = (Long) filter.getFilter((RestColumnHandle) columns.get("job_id"), constraint);
        if (jobId == null) {
            throw new TrinoException(INVALID_ROW_FILTER, "Missing required constraint for job_id");
        }

        Response<ResponseBody> response;
        try {
            response = service.jobLogs(
                    "Bearer " + token,
                    owner,
                    repo,
                    jobId).execute();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        if (response.code() == HTTP_NOT_FOUND) {
            return List.of();
        }
        checkServiceResponse(response);
        ResponseBody body = requireNonNull(response.body());
        String size = response.headers().get("Content-Length");
        long sizeBytes = size != null ? Long.parseLong(size) : 0;

        ImmutableList.Builder<List<?>> result = new ImmutableList.Builder<>();
        int i = 1;
        try {
            for (Slice slice : JobLogs.getParts(body.byteStream())) {
                result.add(List.of(
                        owner,
                        repo,
                        jobId,
                        sizeBytes,
                        i++,
                        slice));
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        return result.build();
    }

    private Collection<? extends List<?>> getSteps(RestTableHandle table)
    {
        GithubTable tableName = GithubTable.valueOf(table);
        TupleDomain<ColumnHandle> constraint = table.getConstraint();
        Map<String, ColumnHandle> columns = columnHandles.get(tableName);
        FilterApplier filter = filterAppliers.get(tableName);

        String owner = (String) filter.getFilter((RestColumnHandle) columns.get("owner"), constraint);
        String repo = (String) filter.getFilter((RestColumnHandle) columns.get("repo"), constraint);
        requirePredicate(owner, "owner");
        requirePredicate(repo, "repo");
        Long runId = (Long) filter.getFilter((RestColumnHandle) columns.get("run_id"), constraint);
        if (runId == null) {
            throw new TrinoException(INVALID_ROW_FILTER, "Missing required constraint for run_id");
        }

        ImmutableList.Builder<Job> jobs = new ImmutableList.Builder<>();

        int page = 1;
        while (true) {
            Response<JobsList> response;
            try {
                response = service.listRunJobs("Bearer " + token, owner, repo, runId, "all", PER_PAGE, page++).execute();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            if (response.code() == HTTP_NOT_FOUND) {
                break;
            }
            checkServiceResponse(response);
            List<Job> items = requireNonNull(response.body()).getItems();
            if (items.size() == 0) {
                break;
            }
            items.forEach(i -> i.setOwner(owner));
            items.forEach(i -> i.setRepo(repo));
            jobs.addAll(items);
            if (items.size() < PER_PAGE) {
                break;
            }
        }

        ImmutableList.Builder<List<?>> result = new ImmutableList.Builder<>();
        int resultSize = 0;
        for (Job job : jobs.build()) {
            List<List<?>> steps = job.getSteps().stream().map(Step::toRow).collect(toList());
            int stepsToUse = Math.max(0, table.getLimit() - resultSize);
            if (steps.size() > stepsToUse) {
                steps = steps.subList(0, stepsToUse);
            }
            else {
                stepsToUse = steps.size();
            }
            result.addAll(steps);
            resultSize += stepsToUse;
        }
        return result.build();
    }

    private Collection<? extends List<?>> getArtifacts(RestTableHandle table)
    {
        GithubTable tableName = GithubTable.valueOf(table);
        TupleDomain<ColumnHandle> constraint = table.getConstraint();
        Map<String, ColumnHandle> columns = columnHandles.get(tableName);
        FilterApplier filter = filterAppliers.get(tableName);

        String owner = (String) filter.getFilter((RestColumnHandle) columns.get("owner"), constraint);
        String repo = (String) filter.getFilter((RestColumnHandle) columns.get("repo"), constraint);
        requirePredicate(owner, "owner");
        requirePredicate(repo, "repo");
        Long runId = (Long) filter.getFilter((RestColumnHandle) columns.get("run_id"), constraint);

        IntFunction<Call<ArtifactsList>> fetcher = page -> service.listArtifacts("Bearer " + token, owner, repo, PER_PAGE, page);
        if (runId != null) {
            fetcher = page -> service.listRunArtifacts("Bearer " + token, owner, repo, runId, PER_PAGE, page);
        }
        else {
            log.warning(format("Missing filter on run_id, will try to fetch all artifacts for %s/%s", owner, repo));
        }
        // TODO this needs to allow pushing down multiple run_id values and make a separate request for each: https://github.com/nineinchnick/trino-rest/issues/30
        return getRowsFromPagesEnvelope(
                fetcher,
                item -> {
                    Stream.Builder<List<?>> result = Stream.builder();
                    item.setOwner(owner);
                    item.setRepo(repo);
                    if (runId != null) {
                        item.setRunId(runId);
                    }
                    try {
                        for (Artifact artifact : download(service, token, item)) {
                            result.add(artifact.toRow());
                        }
                    }
                    catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    return result.build();
                },
                table.getLimit());
    }

    private Collection<? extends List<?>> getRunners(RestTableHandle table)
    {
        GithubTable tableName = GithubTable.valueOf(table);
        TupleDomain<ColumnHandle> constraint = table.getConstraint();
        Map<String, ColumnHandle> columns = columnHandles.get(tableName);
        FilterApplier filter = filterAppliers.get(tableName);

        String org = (String) filter.getFilter((RestColumnHandle) columns.get("org"), constraint);
        String owner = (String) filter.getFilter((RestColumnHandle) columns.get("owner"), constraint);
        String repo = (String) filter.getFilter((RestColumnHandle) columns.get("repo"), constraint);
        if (owner == null && repo == null) {
            requirePredicate(org, "org");
        }
        if (org == null) {
            requirePredicate(owner, "owner");
            requirePredicate(repo, "repo");
        }
        IntFunction<Call<RunnersList>> fetcher = page -> service.listRunners("Bearer " + token, owner, repo, PER_PAGE, page);
        if (org != null) {
            fetcher = page -> service.listOrgRunners("Bearer " + token, org, PER_PAGE, page);
        }
        return getRowsFromPagesEnvelope(
                fetcher,
                item -> {
                    item.setOrg(org);
                    item.setOwner(owner);
                    item.setRepo(repo);
                    return Stream.of(item.toRow());
                },
                table.getLimit());
    }

    private Collection<? extends List<?>> getCheckRuns(RestTableHandle table)
    {
        GithubTable tableName = GithubTable.valueOf(table);
        TupleDomain<ColumnHandle> constraint = table.getConstraint();
        Map<String, ColumnHandle> columns = columnHandles.get(tableName);
        FilterApplier filter = filterAppliers.get(tableName);

        String owner = (String) filter.getFilter((RestColumnHandle) columns.get("owner"), constraint);
        String repo = (String) filter.getFilter((RestColumnHandle) columns.get("repo"), constraint);
        String ref = (String) filter.getFilter((RestColumnHandle) columns.get("ref"), constraint);
        requirePredicate(owner, "owner");
        requirePredicate(repo, "repo");
        requirePredicate(repo, "ref");
        return getRowsFromPagesEnvelope(
                page -> service.listCheckRuns("Bearer " + token, owner, repo, ref, PER_PAGE, page),
                item -> {
                    item.setOwner(owner);
                    item.setRepo(repo);
                    item.setRef(ref);
                    return Stream.of(item.toRow());
                },
                table.getLimit());
    }

    private Collection<? extends List<?>> getCheckRunAnnotations(RestTableHandle table)
    {
        GithubTable tableName = GithubTable.valueOf(table);
        TupleDomain<ColumnHandle> constraint = table.getConstraint();
        Map<String, ColumnHandle> columns = columnHandles.get(tableName);
        FilterApplier filter = filterAppliers.get(tableName);

        String owner = (String) filter.getFilter((RestColumnHandle) columns.get("owner"), constraint);
        String repo = (String) filter.getFilter((RestColumnHandle) columns.get("repo"), constraint);
        Long checkRunId = (Long) filter.getFilter((RestColumnHandle) columns.get("check_run_id"), constraint);
        requirePredicate(owner, "owner");
        requirePredicate(repo, "repo");
        requirePredicate(checkRunId, "check_run_id");
        return getRowsFromPages(
                page -> service.listCheckRunAnnotations(
                        "Bearer " + token,
                        owner,
                        repo,
                        checkRunId,
                        PER_PAGE,
                        page),
                item -> {
                    item.setOwner(owner);
                    item.setRepo(repo);
                    item.setCheckRunId(checkRunId);
                    return item.toRow();
                },
                table.getLimit());
    }

    private <T> Collection<? extends List<?>> getRow(Supplier<Call<T>> fetcher, Function<T, List<?>> mapper)
    {
        Response<T> response;
        try {
            response = fetcher.get().execute();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        if (response.code() == HTTP_NOT_FOUND) {
            return ImmutableList.of();
        }
        checkServiceResponse(response);
        return ImmutableList.of(mapper.apply(response.body()));
    }

    // TODO this abomination should be in a base class implementing a cursor
    private <T> Collection<? extends List<?>> getRowsFromPages(IntFunction<Call<List<T>>> fetcher, Function<T, List<?>> mapper, int limit)
    {
        ImmutableList.Builder<List<?>> result = new ImmutableList.Builder<>();
        int resultSize = 0;

        int page = 1;
        while (true) {
            Response<List<T>> response;
            try {
                response = fetcher.apply(page++).execute();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            if (response.code() == HTTP_NOT_FOUND) {
                break;
            }
            checkServiceResponse(response);
            List<T> items = requireNonNull(response.body());
            if (items.size() == 0) {
                break;
            }
            int itemsToUse = Math.min(
                    Math.max(0, limit - resultSize),
                    items.size());
            List<List<?>> rows = items.subList(0, itemsToUse).stream().map(mapper).collect(toList());

            result.addAll(rows);
            resultSize += itemsToUse;
            if (resultSize >= limit || items.size() < PER_PAGE) {
                break;
            }
        }

        return result.build();
    }

    // TODO this abomination is even worse
    private <T, E extends Envelope<T>> Collection<? extends List<?>> getRowsFromPagesEnvelope(IntFunction<Call<E>> fetcher, Function<T, Stream<List<?>>> mapper, int limit)
    {
        ImmutableList.Builder<List<?>> result = new ImmutableList.Builder<>();
        int resultSize = 0;

        int page = 1;
        while (true) {
            Response<E> response;
            try {
                response = fetcher.apply(page++).execute();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            if (response.code() == HTTP_NOT_FOUND) {
                break;
            }
            checkServiceResponse(response);
            E envelope = requireNonNull(response.body());
            List<T> items = envelope.getItems();
            if (items.size() == 0) {
                break;
            }
            int itemsToUse = Math.min(
                    Math.max(0, limit - resultSize),
                    items.size());
            List<List<?>> rows = items.subList(0, itemsToUse).stream().flatMap(mapper).collect(toList());

            // mapper can produce 1 or more rows per item, so subList them again
            result.addAll(rows.subList(0, itemsToUse));
            resultSize += itemsToUse;
            if (resultSize >= limit || resultSize >= envelope.getTotalCount()) {
                break;
            }
        }

        return result.build();
    }

    @Override
    public Consumer<List> createRowSink(SchemaTableName schemaTableName)
    {
        throw new IllegalStateException("This connector does not support write");
    }

    public static String getSqlType(String tableName)
    {
        return columns.get(GithubTable.valueOf(tableName.toUpperCase()))
                .stream()
                .map(column -> column.getName() + " " + column.getType().getDisplayName())
                .collect(Collectors.joining(", ", "ARRAY(ROW(", "))"));
    }

    public static RowType getRowType(GithubTable tableName)
    {
        List<RowType.Field> fields = GithubRest.columns.get(tableName)
                .stream()
                .map(columnMetadata -> RowType.field(
                        columnMetadata.getName(),
                        columnMetadata.getType()))
                .collect(Collectors.toList());
        return RowType.from(fields);
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(
            ConnectorSession session,
            ConnectorTableHandle table,
            long limit)
    {
        RestTableHandle restTable = (RestTableHandle) table;
        if (restTable.getLimit() == limit) {
            return Optional.empty();
        }
        return Optional.of(new LimitApplicationResult<>(
                new RestTableHandle(
                        restTable.getSchemaTableName(),
                        restTable.getConstraint(),
                        (int) Math.min(limit, Integer.MAX_VALUE),
                        restTable.getSortOrder().isPresent() ? restTable.getSortOrder().get() : null),
                false,
                true));
    }

    @Override
    public Optional<TopNApplicationResult<ConnectorTableHandle>> applyTopN(
            ConnectorSession session,
            ConnectorTableHandle table,
            long topNCount,
            List<SortItem> sortItems,
            Map<String, ColumnHandle> assignments)
    {
        verify(!sortItems.isEmpty(), "sortItems are empty");
        RestTableHandle restTable = (RestTableHandle) table;
        GithubTable tableName = GithubTable.valueOf(restTable);
        List<SortItem> supportedItems = supportedTableSort.get(tableName);
        if (supportedItems == null) {
            return Optional.empty();
        }

        if (!supportedItems.contains(sortItems.get(0))) {
            throw new TrinoException(INVALID_ORDER_BY,
                    format("When using LIMIT or FETCH, first expression in ORDER BY must be one of: %s",
                            supportedItems
                                    .stream()
                                    .map(s -> s.getName() + " " + s.getSortOrder().toString())
                                    .collect(Collectors.joining(", "))));
        }

        int limit = (int) Math.min(topNCount, Integer.MAX_VALUE);
        sortItems = sortItems.subList(0, 1);
        if (restTable.getSortOrder().isPresent()) {
            if (restTable.getLimit() == limit && restTable.getSortOrder().equals(Optional.of(sortItems))) {
                return Optional.empty();
            }
        }

        RestTableHandle sortedTableHandle = new RestTableHandle(
                restTable.getSchemaTableName(),
                restTable.getConstraint(),
                limit,
                sortItems);

        return Optional.of(new TopNApplicationResult<>(
                sortedTableHandle,
                false,
                true));
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
            ConnectorSession session,
            ConnectorTableHandle table,
            Constraint constraint)
    {
        RestTableHandle restTable = (RestTableHandle) table;
        GithubTable tableName = GithubTable.valueOf(restTable);

        FilterApplier filterApplier = filterAppliers.get(tableName);
        if (filterApplier == null) {
            return Optional.empty();
        }
        return filterApplier.applyFilter(
                restTable,
                columnHandles.get(tableName),
                filterApplier.getSupportedFilters(),
                constraint.getSummary());
    }

    public ConnectorSplitSource getSplitSource(
            NodeManager nodeManager,
            ConnectorTableHandle handle,
            ConnectorSplitManager.SplitSchedulingStrategy splitSchedulingStrategy,
            DynamicFilter dynamicFilter)
    {
        if (splitSchedulingStrategy != ConnectorSplitManager.SplitSchedulingStrategy.UNGROUPED_SCHEDULING) {
            throw new IllegalArgumentException("Unknown splitSchedulingStrategy: " + splitSchedulingStrategy);
        }

        RestTableHandle table = (RestTableHandle) handle;
        TupleDomain<ColumnHandle> constraint = table.getConstraint();

        List<HostAddress> addresses = nodeManager.getRequiredWorkerNodes().stream()
                .map(Node::getHostAndPort)
                .collect(toList());

        GithubTable tableName = GithubTable.valueOf(table);
        FilterApplier filterApplier = filterAppliers.get(tableName);
        if (filterApplier == null || constraint.getDomains().isEmpty()) {
            List<RestConnectorSplit> splits = List.of(new RestConnectorSplit(table, addresses));
            return new FixedSplitSource(splits);
        }

        /*
        Generate splits based on the cartesian product of all multi-valued domains.
        Example, given predicates such as: `job_id IN (12, 34) AND conclusion IN ('canceled', 'failure')`
        the cartesian product will yield:
        * job_id:12, conclusion:canceled
        * job_id:34, conclusion:canceled
        * job_id:12, conclusion:failure
        * job_id:34, conclusion:failure
         */
        Map<ColumnHandle, Domain> originalDomains = constraint.getDomains().get();
        // first build a list of lists of tuples with the column and single-valued domain, for every value of a multi-valued domain
        ImmutableList.Builder<List<Map.Entry<ColumnHandle, Domain>>> singleDomains = new ImmutableList.Builder<>();
        for (Map.Entry<ColumnHandle, Domain> entry : originalDomains.entrySet()) {
            RestColumnHandle column = (RestColumnHandle) entry.getKey();
            Domain domain = entry.getValue();

            FilterType supportedFilter = filterApplier.getSupportedFilters().get(column.getName());
            if (domain.isSingleValue() || supportedFilter != FilterType.EQUAL) {
                continue;
            }
            List<Object> values;
            if (domain.getValues().isDiscreteSet()) {
                values = domain.getValues().getDiscreteSet();
            } else {
                values = domain.getValues().getRanges().getOrderedRanges()
                        .stream()
                        .map(Range::getSingleValue)
                        .collect(toList());
            }
            ImmutableList.Builder<Map.Entry<ColumnHandle, Domain>> splitDomains = new ImmutableList.Builder<>();
            for (Object value : values) {
                splitDomains.add(new AbstractMap.SimpleImmutableEntry<>(column, Domain.create(
                        ValueSet.of(domain.getType(), value),
                        domain.isNullAllowed())));
            }
            singleDomains.add(splitDomains.build());
        }
        // then create copies of the original constraints, with every multi-valued domain replaced with single-value sets
        ImmutableList.Builder<RestConnectorSplit> splits = new ImmutableList.Builder<>();
        for (List<Map.Entry<ColumnHandle, Domain>> splitDomains : Lists.cartesianProduct(singleDomains.build())) {
            Map<ColumnHandle, Domain> newDomains = new HashMap<>(originalDomains);
            for (Map.Entry<ColumnHandle, Domain> entry : splitDomains) {
                newDomains.put(entry.getKey(), entry.getValue());
            }
            splits.add(new RestConnectorSplit(
                    new RestTableHandle(
                            table.getSchemaTableName(),
                            TupleDomain.withColumnDomains(newDomains),
                            table.getLimit(),
                            table.getSortOrder().isPresent() ? table.getSortOrder().get() : null),
                    addresses));
        }
        return new FixedSplitSource(splits.build());
    }
}
