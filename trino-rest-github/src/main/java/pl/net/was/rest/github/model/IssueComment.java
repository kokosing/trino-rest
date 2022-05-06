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
import static io.trino.spi.type.IntegerType.INTEGER;

@SuppressWarnings("unused")
public class IssueComment
        extends BaseBlockWriter
{
    private String owner;
    private String repo;
    // this is only set when fetching comments for a specific issue or pull request
    private long number;
    private final long id;
    private final String nodeId;
    private final String url;
    private final String htmlUrl;
    private final String body;
    private final User user;
    private final ZonedDateTime createdAt;
    private final ZonedDateTime updatedAt;
    private final String issueUrl;
    private final String authorAssociation;
    private final Reactions reactions;
    private final App performedViaGithubApp;

    public IssueComment(
            @JsonProperty("id") long id,
            @JsonProperty("node_id") String nodeId,
            @JsonProperty("url") String url,
            @JsonProperty("html_url") String htmlUrl,
            @JsonProperty("body") String body,
            @JsonProperty("user") User user,
            @JsonProperty("created_at") ZonedDateTime createdAt,
            @JsonProperty("updated_at") ZonedDateTime updatedAt,
            @JsonProperty("issue_url") String issueUrl,
            @JsonProperty("author_association") String authorAssociation,
            @JsonProperty("reactions") Reactions reactions,
            @JsonProperty("performed_via_github_app") App performedViaGithubApp)
    {
        this.id = id;
        this.nodeId = nodeId;
        this.url = url;
        this.htmlUrl = htmlUrl;
        this.body = body;
        this.user = user;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.issueUrl = issueUrl;
        this.authorAssociation = authorAssociation;
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

    public void setIssueNumber(long number)
    {
        this.number = number;
    }

    public List<?> toRow()
    {
        return ImmutableList.of(
                owner,
                repo,
                number,
                id,
                nodeId,
                url,
                htmlUrl,
                body,
                user.getId(),
                user.getLogin(),
                packTimestamp(createdAt),
                packTimestamp(updatedAt),
                issueUrl,
                authorAssociation,
                reactions != null ? reactions.getUrl() : "",
                reactions != null ? reactions.getTotalCount() : 0,
                performedViaGithubApp != null ? performedViaGithubApp.getId() : 0,
                performedViaGithubApp != null ? performedViaGithubApp.getSlug() : "",
                performedViaGithubApp != null ? performedViaGithubApp.getName() : "");
    }

    @Override
    public void writeTo(BlockBuilder rowBuilder)
    {
        // TODO this should be a map of column names to value getters and types should be fetched from GithubRest.columns
        writeString(rowBuilder, owner);
        writeString(rowBuilder, repo);
        BIGINT.writeLong(rowBuilder, number);
        BIGINT.writeLong(rowBuilder, id);
        writeString(rowBuilder, nodeId);
        writeString(rowBuilder, url);
        writeString(rowBuilder, htmlUrl);
        writeString(rowBuilder, body);
        BIGINT.writeLong(rowBuilder, user.getId());
        writeString(rowBuilder, user.getLogin());
        writeTimestamp(rowBuilder, createdAt);
        writeTimestamp(rowBuilder, updatedAt);
        writeString(rowBuilder, issueUrl);
        writeString(rowBuilder, authorAssociation);
        if (reactions == null) {
            rowBuilder.appendNull();
            rowBuilder.appendNull();
        }
        else {
            writeString(rowBuilder, reactions.getUrl());
            INTEGER.writeLong(rowBuilder, reactions.getTotalCount());
        }
        if (performedViaGithubApp == null) {
            rowBuilder.appendNull();
            rowBuilder.appendNull();
            rowBuilder.appendNull();
        }
        else {
            BIGINT.writeLong(rowBuilder, performedViaGithubApp.getId());
            writeString(rowBuilder, performedViaGithubApp.getSlug());
            writeString(rowBuilder, performedViaGithubApp.getName());
        }
    }
}
