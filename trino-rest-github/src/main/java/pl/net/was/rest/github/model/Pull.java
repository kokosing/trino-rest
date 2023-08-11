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

package pl.net.was.rest.github.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.block.BlockBuilder;

import java.time.ZonedDateTime;
import java.util.List;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;

@SuppressWarnings("unused")
public class Pull
        extends BaseBlockWriter
{
    private String owner;
    private String repo;
    private final String url;
    private final long id;
    private final String nodeId;
    private final String htmlUrl;
    private final String diffUrl;
    private final String patchUrl;
    private final String issueUrl;
    private final long number;
    private final String state;
    private final boolean locked;
    private final String title;
    private final User user;
    private final String body;
    private final ZonedDateTime createdAt;
    private final ZonedDateTime updatedAt;
    private final ZonedDateTime closedAt;
    private final ZonedDateTime mergedAt;
    private final String mergeCommitSha;
    private final User assignee;
    private final List<User> assignees;
    private final List<User> requestedReviewers;
    private final List<Team> requestedTeams;
    private final List<Label> labels;
    private final Milestone milestone;
    private final boolean draft;
    private final String commitsUrl;
    private final String reviewCommentsUrl;
    private final String reviewCommentUrl;
    private final String commentsUrl;
    private final String statusesUrl;
    private final String headRef;
    private final String headSha;
    private final String baseRef;
    private final String baseSha;
    private final String authorAssociation;
    private final AutoMerge autoMerge;
    private final String activeLockReason;

    public Pull(
            @JsonProperty("url") String url,
            @JsonProperty("id") long id,
            @JsonProperty("node_id") String nodeId,
            @JsonProperty("html_url") String htmlUrl,
            @JsonProperty("diff_url") String diffUrl,
            @JsonProperty("patch_url") String patchUrl,
            @JsonProperty("issue_url") String issueUrl,
            @JsonProperty("number") long number,
            @JsonProperty("state") String state,
            @JsonProperty("locked") boolean locked,
            @JsonProperty("title") String title,
            @JsonProperty("user") User user,
            @JsonProperty("body") String body,
            @JsonProperty("created_at") ZonedDateTime createdAt,
            @JsonProperty("updated_at") ZonedDateTime updatedAt,
            @JsonProperty("closed_at") ZonedDateTime closedAt,
            @JsonProperty("merged_at") ZonedDateTime mergedAt,
            @JsonProperty("merge_commit_sha") String mergeCommitSha,
            @JsonProperty("assignee") User assignee,
            @JsonProperty("assignees") List<User> assignees,
            @JsonProperty("requested_reviewers") List<User> requestedReviewers,
            @JsonProperty("requested_teams") List<Team> requestedTeams,
            @JsonProperty("labels") List<Label> labels,
            @JsonProperty("milestone") Milestone milestone,
            @JsonProperty("draft") boolean draft,
            @JsonProperty("commits_url") String commitsUrl,
            @JsonProperty("review_comments_url") String reviewCommentsUrl,
            @JsonProperty("review_comment_url") String reviewCommentUrl,
            @JsonProperty("comments_url") String commentsUrl,
            @JsonProperty("statuses_url") String statusesUrl,
            @JsonProperty("head") Ref head,
            @JsonProperty("base") Ref base,
            @JsonProperty("_links") Object unusedLinks,
            @JsonProperty("author_association") String authorAssociation,
            @JsonProperty("auto_merge") AutoMerge autoMerge,
            @JsonProperty("active_lock_reason") String activeLockReason)
    {
        this.url = url;
        this.id = id;
        this.nodeId = nodeId;
        this.htmlUrl = htmlUrl;
        this.diffUrl = diffUrl;
        this.patchUrl = patchUrl;
        this.issueUrl = issueUrl;
        this.number = number;
        this.state = state;
        this.locked = locked;
        this.title = title;
        this.user = user;
        this.body = body;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.closedAt = closedAt;
        this.mergedAt = mergedAt;
        this.mergeCommitSha = mergeCommitSha;
        this.assignee = assignee;
        this.assignees = assignees;
        this.requestedReviewers = requestedReviewers;
        this.requestedTeams = requestedTeams;
        this.labels = labels;
        this.milestone = milestone;
        this.draft = draft;
        this.commitsUrl = commitsUrl;
        this.reviewCommentsUrl = reviewCommentsUrl;
        this.reviewCommentUrl = reviewCommentUrl;
        this.commentsUrl = commentsUrl;
        this.statusesUrl = statusesUrl;
        if (head != null) {
            this.headRef = head.getRef();
            this.headSha = head.getSha();
        }
        else {
            this.headRef = "";
            this.headSha = "";
        }
        if (base != null) {
            this.baseRef = base.getRef();
            this.baseSha = base.getSha();
        }
        else {
            this.baseRef = "";
            this.baseSha = "";
        }
        this.authorAssociation = authorAssociation;
        this.autoMerge = autoMerge;
        this.activeLockReason = activeLockReason;
    }

    public void setOwner(String owner)
    {
        this.owner = owner;
    }

    public void setRepo(String repo)
    {
        this.repo = repo;
    }

    public long getNumber()
    {
        return number;
    }

    public List<?> toRow()
    {
        BlockBuilder labelIds = BIGINT.createBlockBuilder(null, labels.size());
        BlockBuilder labelNames = VARCHAR.createBlockBuilder(null, labels.size());
        for (Label label : labels) {
            BIGINT.writeLong(labelIds, label.getId());
            VARCHAR.writeString(labelNames, label.getName());
        }

        BlockBuilder requestedReviewerIds = BIGINT.createBlockBuilder(null, requestedReviewers.size());
        BlockBuilder requestedReviewerLogins = VARCHAR.createBlockBuilder(null, requestedReviewers.size());
        for (User requestedReviewer : requestedReviewers) {
            BIGINT.writeLong(requestedReviewerIds, requestedReviewer.getId());
            VARCHAR.writeString(requestedReviewerLogins, requestedReviewer.getLogin());
        }

        return ImmutableList.of(
                owner,
                repo,
                url,
                id,
                nodeId,
                htmlUrl,
                diffUrl,
                patchUrl,
                issueUrl,
                commitsUrl,
                reviewCommentsUrl,
                reviewCommentUrl,
                commentsUrl,
                statusesUrl,
                number,
                state,
                locked,
                title != null ? title : "",
                user != null ? user.getId() : 0,
                user != null ? user.getLogin() : "",
                body != null ? body : "",
                labelIds.build(),
                labelNames.build(),
                milestone != null ? milestone.getId() : 0,
                milestone != null ? milestone.getTitle() : "",
                activeLockReason != null ? activeLockReason : "",
                packTimestamp(createdAt),
                packTimestamp(updatedAt),
                packTimestamp(closedAt),
                packTimestamp(mergedAt),
                mergeCommitSha != null ? mergeCommitSha : "",
                assignee != null ? assignee.getId() : 0,
                assignee != null ? assignee.getLogin() : "",
                requestedReviewerIds.build(),
                requestedReviewerLogins.build(),
                headRef,
                headSha,
                baseRef,
                baseSha,
                authorAssociation,
                draft,
                autoMerge != null ? autoMerge.getEnabledBy().getId() : 0,
                autoMerge != null ? autoMerge.getEnabledBy().getLogin() : "",
                autoMerge != null ? autoMerge.getMergeMethod() : "",
                autoMerge != null ? autoMerge.getCommitTitle() : "",
                autoMerge != null ? autoMerge.getCommitMessage() : "");
    }

    @Override
    public void writeTo(List<BlockBuilder> fieldBuilders)
    {
        int i = 0;
        // TODO this should be a map of column names to value getters and types should be fetched from GithubRest.columns
        writeString(fieldBuilders.get(i++), owner);
        writeString(fieldBuilders.get(i++), repo);
        writeString(fieldBuilders.get(i++), url);
        BIGINT.writeLong(fieldBuilders.get(i++), id);
        writeString(fieldBuilders.get(i++), nodeId);
        writeString(fieldBuilders.get(i++), htmlUrl);
        writeString(fieldBuilders.get(i++), diffUrl);
        writeString(fieldBuilders.get(i++), patchUrl);
        writeString(fieldBuilders.get(i++), issueUrl);
        writeString(fieldBuilders.get(i++), commitsUrl);
        writeString(fieldBuilders.get(i++), reviewCommentsUrl);
        writeString(fieldBuilders.get(i++), reviewCommentUrl);
        writeString(fieldBuilders.get(i++), commentsUrl);
        writeString(fieldBuilders.get(i++), statusesUrl);
        BIGINT.writeLong(fieldBuilders.get(i++), number);
        writeString(fieldBuilders.get(i++), state);
        BOOLEAN.writeBoolean(fieldBuilders.get(i++), locked);
        writeString(fieldBuilders.get(i++), title);
        BIGINT.writeLong(fieldBuilders.get(i++), user.getId());
        writeString(fieldBuilders.get(i++), user.getLogin());
        writeString(fieldBuilders.get(i++), body);

        if (labels == null) {
            fieldBuilders.get(i++).appendNull();
            fieldBuilders.get(i++).appendNull();
        }
        else {
            // labels array
            BlockBuilder labelIds = BIGINT.createBlockBuilder(null, labels.size());
            for (Label label : labels) {
                BIGINT.writeLong(labelIds, label.getId());
            }
            ARRAY_BIGINT.writeObject(fieldBuilders.get(i++), labelIds.build());

            BlockBuilder labelNames = VARCHAR.createBlockBuilder(null, labels.size());
            for (Label label : labels) {
                writeString(labelNames, label.getName());
            }
            ARRAY_VARCHAR.writeObject(fieldBuilders.get(i++), labelNames.build());
        }

        if (milestone == null) {
            fieldBuilders.get(i++).appendNull();
            fieldBuilders.get(i++).appendNull();
        }
        else {
            BIGINT.writeLong(fieldBuilders.get(i++), milestone.getId());
            writeString(fieldBuilders.get(i++), milestone.getTitle());
        }
        writeString(fieldBuilders.get(i++), activeLockReason);
        writeTimestamp(fieldBuilders.get(i++), createdAt);
        writeTimestamp(fieldBuilders.get(i++), updatedAt);
        writeTimestamp(fieldBuilders.get(i++), closedAt);
        writeTimestamp(fieldBuilders.get(i++), mergedAt);
        writeString(fieldBuilders.get(i++), mergeCommitSha);
        if (assignee == null) {
            fieldBuilders.get(i++).appendNull();
            fieldBuilders.get(i++).appendNull();
        }
        else {
            BIGINT.writeLong(fieldBuilders.get(i++), assignee.getId());
            writeString(fieldBuilders.get(i++), assignee.getLogin());
        }

        if (requestedReviewers == null) {
            fieldBuilders.get(i++).appendNull();
            fieldBuilders.get(i++).appendNull();
        }
        else {
            BlockBuilder reviewerIds = BIGINT.createBlockBuilder(null, requestedReviewers.size());
            for (User reviewer : requestedReviewers) {
                BIGINT.writeLong(reviewerIds, reviewer.getId());
            }
            ARRAY_BIGINT.writeObject(fieldBuilders.get(i++), reviewerIds.build());

            BlockBuilder reviewerLogins = VARCHAR.createBlockBuilder(null, requestedReviewers.size());
            for (User reviewer : requestedReviewers) {
                writeString(reviewerLogins, reviewer.getLogin());
            }
            ARRAY_VARCHAR.writeObject(fieldBuilders.get(i++), reviewerLogins.build());
        }

        writeString(fieldBuilders.get(i++), headRef);
        writeString(fieldBuilders.get(i++), headSha);
        writeString(fieldBuilders.get(i++), baseRef);
        writeString(fieldBuilders.get(i++), baseSha);
        writeString(fieldBuilders.get(i++), authorAssociation);
        BOOLEAN.writeBoolean(fieldBuilders.get(i++), draft);
        if (autoMerge != null) {
            BIGINT.writeLong(fieldBuilders.get(i++), autoMerge.getEnabledBy().getId());
            writeString(fieldBuilders.get(i++), autoMerge.getEnabledBy().getLogin());
            writeString(fieldBuilders.get(i++), autoMerge.getMergeMethod());
            writeString(fieldBuilders.get(i++), autoMerge.getCommitTitle());
            writeString(fieldBuilders.get(i), autoMerge.getCommitMessage());
        }
        else {
            fieldBuilders.get(i++).appendNull();
            fieldBuilders.get(i++).appendNull();
            fieldBuilders.get(i++).appendNull();
            fieldBuilders.get(i++).appendNull();
            fieldBuilders.get(i).appendNull();
        }
    }
}
