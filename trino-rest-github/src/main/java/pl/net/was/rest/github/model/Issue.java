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
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;

@SuppressWarnings("unused")
public class Issue
        extends BaseBlockWriter
{
    private String owner;
    private String repo;
    private final long id;
    private final String nodeId;
    private final String url;
    private final String repositoryUrl;
    private final String labelsUrl;
    private final String commentsUrl;
    private final String eventsUrl;
    private final String htmlUrl;
    private final String timelineUrl;
    private final long number;
    private final String state;
    private final String title;
    private final String body;
    private final User user;
    private final List<Label> labels;
    private final User assignee;
    private final List<User> assignees;
    private final Milestone milestone;
    private final boolean locked;
    private final String activeLockReason;
    private final long comments;
    private final Pull pullRequest;
    private final ZonedDateTime closedAt;
    private final ZonedDateTime createdAt;
    private final ZonedDateTime updatedAt;
    private final User closedBy;
    private final String authorAssociation;
    private final boolean draft;
    private final Reactions reactions;
    private final App performedViaGithubApp;

    public Issue(
            @JsonProperty("id") long id,
            @JsonProperty("node_id") String nodeId,
            @JsonProperty("url") String url,
            @JsonProperty("repository_url") String repositoryUrl,
            @JsonProperty("labels_url") String labelsUrl,
            @JsonProperty("comments_url") String commentsUrl,
            @JsonProperty("events_url") String eventsUrl,
            @JsonProperty("html_url") String htmlUrl,
            @JsonProperty("timeline_url") String timelineUrl,
            @JsonProperty("number") long number,
            @JsonProperty("state") String state,
            @JsonProperty("title") String title,
            @JsonProperty("body") String body,
            @JsonProperty("user") User user,
            @JsonProperty("labels") List<Label> labels,
            @JsonProperty("assignee") User assignee,
            @JsonProperty("assignees") List<User> assignees,
            @JsonProperty("milestone") Milestone milestone,
            @JsonProperty("locked") boolean locked,
            @JsonProperty("active_lock_reason") String activeLockReason,
            @JsonProperty("comments") long comments,
            @JsonProperty("pull_request") Pull pullRequest,
            @JsonProperty("closed_at") ZonedDateTime closedAt,
            @JsonProperty("created_at") ZonedDateTime createdAt,
            @JsonProperty("updated_at") ZonedDateTime updatedAt,
            @JsonProperty("closed_by") User closedBy,
            @JsonProperty("author_association") String authorAssociation,
            @JsonProperty("draft") boolean draft,
            @JsonProperty("reactions") Reactions reactions,
            @JsonProperty("performed_via_github_app") App performedViaGithubApp)
    {
        this.id = id;
        this.nodeId = nodeId;
        this.url = url;
        this.repositoryUrl = repositoryUrl;
        this.labelsUrl = labelsUrl;
        this.commentsUrl = commentsUrl;
        this.eventsUrl = eventsUrl;
        this.htmlUrl = htmlUrl;
        this.timelineUrl = timelineUrl;
        this.number = number;
        this.state = state;
        this.title = title;
        this.body = body;
        this.user = user;
        this.labels = labels;
        this.assignee = assignee;
        this.assignees = assignees;
        this.milestone = milestone;
        this.locked = locked;
        this.activeLockReason = activeLockReason;
        this.comments = comments;
        this.pullRequest = pullRequest;
        this.closedAt = closedAt;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.closedBy = closedBy;
        this.authorAssociation = authorAssociation;
        this.draft = draft;
        this.reactions = reactions;
        this.performedViaGithubApp = performedViaGithubApp;
    }

    public void setOwner(String owner)
    {
        this.owner = owner;
    }

    public void setRepo(String repo)
    {
        this.repo = repo;
    }

    public List<?> toRow()
    {
        // TODO allow nulls

        BlockBuilder labelIds = BIGINT.createBlockBuilder(null, labels.size());
        BlockBuilder labelNames = VARCHAR.createBlockBuilder(null, labels.size());
        for (Label label : labels) {
            BIGINT.writeLong(labelIds, label.getId());
            VARCHAR.writeString(labelNames, label.getName());
        }

        return ImmutableList.of(
                owner,
                repo,
                id,
                nodeId,
                url,
                repositoryUrl,
                labelsUrl,
                commentsUrl,
                eventsUrl,
                htmlUrl,
                timelineUrl,
                number,
                state,
                title != null ? title : "",
                body != null ? body : "",
                user.getId(),
                user.getLogin(),
                labelIds.build(),
                labelNames.build(),
                assignee != null ? assignee.getId() : 0,
                assignee != null ? assignee.getLogin() : "",
                milestone != null ? milestone.getId() : 0,
                milestone != null ? milestone.getTitle() : "",
                locked,
                activeLockReason != null ? activeLockReason : "",
                comments,
                packTimestamp(closedAt),
                packTimestamp(createdAt),
                packTimestamp(updatedAt),
                closedBy != null ? closedBy.getId() : 0,
                closedBy != null ? closedBy.getLogin() : "",
                authorAssociation,
                draft,
                reactions != null ? reactions.getUrl() : "",
                reactions != null ? reactions.getTotalCount() : 0,
                performedViaGithubApp != null ? performedViaGithubApp.getId() : 0,
                performedViaGithubApp != null ? performedViaGithubApp.getSlug() : "",
                performedViaGithubApp != null ? performedViaGithubApp.getName() : "");
    }

    @Override
    public void writeTo(List<BlockBuilder> fieldBuilders)
    {
        // TODO this should be a map of column names to value getters and types should be fetched from GithubRest.columns
        int i = 0;
        writeString(fieldBuilders.get(i++), owner);
        writeString(fieldBuilders.get(i++), repo);
        BIGINT.writeLong(fieldBuilders.get(i++), id);
        writeString(fieldBuilders.get(i++), nodeId);
        writeString(fieldBuilders.get(i++), url);
        writeString(fieldBuilders.get(i++), repositoryUrl);
        writeString(fieldBuilders.get(i++), labelsUrl);
        writeString(fieldBuilders.get(i++), commentsUrl);
        writeString(fieldBuilders.get(i++), eventsUrl);
        writeString(fieldBuilders.get(i++), htmlUrl);
        writeString(fieldBuilders.get(i++), timelineUrl);
        BIGINT.writeLong(fieldBuilders.get(i++), number);
        writeString(fieldBuilders.get(i++), state);
        writeString(fieldBuilders.get(i++), title);
        writeString(fieldBuilders.get(i++), body);
        BIGINT.writeLong(fieldBuilders.get(i++), user.getId());
        writeString(fieldBuilders.get(i++), user.getLogin());

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

        if (assignee == null) {
            fieldBuilders.get(i++).appendNull();
            fieldBuilders.get(i++).appendNull();
        }
        else {
            BIGINT.writeLong(fieldBuilders.get(i++), assignee.getId());
            writeString(fieldBuilders.get(i++), assignee.getLogin());
        }
        if (milestone == null) {
            fieldBuilders.get(i++).appendNull();
            fieldBuilders.get(i++).appendNull();
        }
        else {
            BIGINT.writeLong(fieldBuilders.get(i++), milestone.getId());
            writeString(fieldBuilders.get(i++), milestone.getTitle());
        }
        BOOLEAN.writeBoolean(fieldBuilders.get(i++), locked);
        writeString(fieldBuilders.get(i++), activeLockReason);
        BIGINT.writeLong(fieldBuilders.get(i++), comments);
        writeTimestamp(fieldBuilders.get(i++), closedAt);
        writeTimestamp(fieldBuilders.get(i++), createdAt);
        writeTimestamp(fieldBuilders.get(i++), updatedAt);
        if (closedBy == null) {
            fieldBuilders.get(i++).appendNull();
            fieldBuilders.get(i++).appendNull();
        }
        else {
            BIGINT.writeLong(fieldBuilders.get(i++), closedBy.getId());
            writeString(fieldBuilders.get(i++), closedBy.getLogin());
        }
        writeString(fieldBuilders.get(i++), authorAssociation);
        BOOLEAN.writeBoolean(fieldBuilders.get(i++), draft);
        if (reactions == null) {
            fieldBuilders.get(i++).appendNull();
            fieldBuilders.get(i++).appendNull();
        }
        else {
            writeString(fieldBuilders.get(i++), reactions.getUrl());
            INTEGER.writeLong(fieldBuilders.get(i++), reactions.getTotalCount());
        }
        if (performedViaGithubApp == null) {
            fieldBuilders.get(i++).appendNull();
            fieldBuilders.get(i++).appendNull();
            fieldBuilders.get(i).appendNull();
        }
        else {
            BIGINT.writeLong(fieldBuilders.get(i++), performedViaGithubApp.getId());
            writeString(fieldBuilders.get(i++), performedViaGithubApp.getSlug());
            writeString(fieldBuilders.get(i), performedViaGithubApp.getName());
        }
    }
}
