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

@SuppressWarnings("unused")
public class ReviewComment
        extends BaseBlockWriter
{
    private String owner;
    private String repo;
    // this is only set when fetching comments for a specific PR
    private long pullNumber;
    private final String url;
    private final long pullRequestReviewId;
    private final long id;
    private final String nodeId;
    private final String diffHunk;
    private final String path;
    private final long position;
    private final long originalPosition;
    private final String commitId;
    private final String originalCommitId;
    private final long inReplyToId;
    private final User user;
    private final String body;
    private final ZonedDateTime createdAt;
    private final ZonedDateTime updatedAt;
    private final String htmlUrl;
    private final String pullRequestUrl;
    private final String authorAssociation;
    private final Reactions reactions;
    private final long startLine;
    private final long originalStartLine;
    private final String startSide;
    private final long line;
    private final long originalLine;
    private final String side;

    public ReviewComment(
            @JsonProperty("url") String url,
            @JsonProperty("pull_request_review_id") long pullRequestReviewId,
            @JsonProperty("id") long id,
            @JsonProperty("node_id") String nodeId,
            @JsonProperty("diff_hunk") String diffHunk,
            @JsonProperty("path") String path,
            @JsonProperty("position") long position,
            @JsonProperty("original_position") long originalPosition,
            @JsonProperty("commit_id") String commitId,
            @JsonProperty("original_commit_id") String originalCommitId,
            @JsonProperty("in_reply_to_id") long inReplyToId,
            @JsonProperty("user") User user,
            @JsonProperty("body") String body,
            @JsonProperty("created_at") ZonedDateTime createdAt,
            @JsonProperty("updated_at") ZonedDateTime updatedAt,
            @JsonProperty("html_url") String htmlUrl,
            @JsonProperty("pull_request_url") String pullRequestUrl,
            @JsonProperty("author_association") String authorAssociation,
            @JsonProperty("_links") Object unusedLinks,
            @JsonProperty("reactions") Reactions reactions,
            @JsonProperty("start_line") long startLine,
            @JsonProperty("original_start_line") long originalStartLine,
            @JsonProperty("start_side") String startSide,
            @JsonProperty("line") long line,
            @JsonProperty("original_line") long originalLine,
            @JsonProperty("side") String side)
    {
        this.url = url;
        this.pullRequestReviewId = pullRequestReviewId;
        this.id = id;
        this.nodeId = nodeId;
        this.diffHunk = diffHunk;
        this.path = path;
        this.position = position;
        this.originalPosition = originalPosition;
        this.commitId = commitId;
        this.originalCommitId = originalCommitId;
        this.inReplyToId = inReplyToId;
        this.user = user;
        this.body = body;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.htmlUrl = htmlUrl;
        this.pullRequestUrl = pullRequestUrl;
        this.authorAssociation = authorAssociation;
        this.reactions = reactions;
        this.startLine = startLine;
        this.originalStartLine = originalStartLine;
        this.startSide = startSide;
        this.line = line;
        this.originalLine = originalLine;
        this.side = side;
    }

    public void setOwner(String owner)
    {
        this.owner = owner;
    }

    public void setRepo(String repo)
    {
        this.repo = repo;
    }

    public void setPullNumber(long pullNumber)
    {
        this.pullNumber = pullNumber;
    }

    public List<?> toRow()
    {
        return ImmutableList.of(
                owner,
                repo,
                pullNumber,
                pullRequestUrl,
                pullRequestReviewId,
                id,
                diffHunk,
                path,
                position,
                originalPosition,
                commitId,
                originalCommitId,
                inReplyToId,
                user.getId(),
                user.getLogin(),
                body != null ? body : "",
                packTimestamp(createdAt),
                packTimestamp(updatedAt),
                authorAssociation != null ? authorAssociation : "",
                startLine,
                originalStartLine,
                startSide != null ? startSide : "",
                line,
                originalLine,
                side != null ? side : "");
    }

    @Override
    public void writeTo(List<BlockBuilder> fieldBuilders)
    {
        int i = 0;
        writeString(fieldBuilders.get(i++), owner);
        writeString(fieldBuilders.get(i++), repo);
        BIGINT.writeLong(fieldBuilders.get(i++), pullNumber);
        writeString(fieldBuilders.get(i++), pullRequestUrl);
        BIGINT.writeLong(fieldBuilders.get(i++), pullRequestReviewId);
        BIGINT.writeLong(fieldBuilders.get(i++), id);
        writeString(fieldBuilders.get(i++), diffHunk);
        writeString(fieldBuilders.get(i++), path);
        BIGINT.writeLong(fieldBuilders.get(i++), position);
        BIGINT.writeLong(fieldBuilders.get(i++), originalPosition);
        writeString(fieldBuilders.get(i++), commitId);
        writeString(fieldBuilders.get(i++), originalCommitId);
        BIGINT.writeLong(fieldBuilders.get(i++), inReplyToId);
        BIGINT.writeLong(fieldBuilders.get(i++), user.getId());
        writeString(fieldBuilders.get(i++), user.getLogin());
        writeString(fieldBuilders.get(i++), body);
        writeTimestamp(fieldBuilders.get(i++), createdAt);
        writeTimestamp(fieldBuilders.get(i++), updatedAt);
        writeString(fieldBuilders.get(i++), authorAssociation);
        BIGINT.writeLong(fieldBuilders.get(i++), startLine);
        BIGINT.writeLong(fieldBuilders.get(i++), originalStartLine);
        writeString(fieldBuilders.get(i++), startSide);
        BIGINT.writeLong(fieldBuilders.get(i++), line);
        BIGINT.writeLong(fieldBuilders.get(i++), originalLine);
        writeString(fieldBuilders.get(i), side);
    }
}
