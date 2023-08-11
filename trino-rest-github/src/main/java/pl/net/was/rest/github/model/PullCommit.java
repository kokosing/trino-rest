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

import java.util.List;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;

@SuppressWarnings("unused")
public class PullCommit
        extends BaseBlockWriter
{
    private String owner;
    private String repo;
    private long pullNumber;
    private final String sha;
    private final String nodeId;
    private final Commit commit;
    private final String url;
    private final String htmlUrl;
    private final String commentsUrl;
    private final User author;
    private final User committer;
    private final List<Ref> parents;

    public PullCommit(
            @JsonProperty("sha") String sha,
            @JsonProperty("node_id") String nodeId,
            @JsonProperty("commit") Commit commit,
            @JsonProperty("url") String url,
            @JsonProperty("html_url") String htmlUrl,
            @JsonProperty("comments_url") String commentsUrl,
            @JsonProperty("author") User author,
            @JsonProperty("committer") User committer,
            @JsonProperty("parents") List<Ref> parents)
    {
        this.sha = sha;
        this.nodeId = nodeId;
        this.commit = commit;
        this.url = url;
        this.htmlUrl = htmlUrl;
        this.commentsUrl = commentsUrl;
        this.author = author;
        this.committer = committer;
        this.parents = parents;
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
        BlockBuilder parentShas = VARCHAR.createBlockBuilder(null, parents.size());
        for (Ref parent : parents) {
            VARCHAR.writeString(parentShas, parent.getSha());
        }

        return ImmutableList.of(
                owner,
                repo,
                sha,
                pullNumber,
                commit.getMessage(),
                commit.getTree() != null ? commit.getTree().getSha() : "",
                commit.getCommentCount(),
                commit.getVerification() != null && commit.getVerification().getVerified(),
                commit.getVerification() != null ? commit.getVerification().getReason() : "",
                commit.getAuthor().getName(),
                commit.getAuthor().getEmail(),
                packTimestamp(commit.getAuthor().getDate()),
                author != null ? author.getId() : 0,
                author != null ? author.getLogin() : "",
                commit.getCommitter().getName(),
                commit.getCommitter().getEmail(),
                packTimestamp(commit.getCommitter().getDate()),
                committer != null ? committer.getId() : 0,
                committer != null ? committer.getLogin() : "",
                parentShas.build());
    }

    @Override
    public void writeTo(List<BlockBuilder> fieldBuilders)
    {
        int i = 0;
        writeString(fieldBuilders.get(i++), owner);
        writeString(fieldBuilders.get(i++), repo);
        writeString(fieldBuilders.get(i++), sha);
        BIGINT.writeLong(fieldBuilders.get(i++), pullNumber);
        writeString(fieldBuilders.get(i++), commit.getMessage());
        writeString(fieldBuilders.get(i++), commit.getTree() != null ? commit.getTree().getSha() : "");
        BIGINT.writeLong(fieldBuilders.get(i++), commit.getCommentCount());
        BOOLEAN.writeBoolean(fieldBuilders.get(i++), commit.getVerification() != null && commit.getVerification().getVerified());
        writeString(fieldBuilders.get(i++), commit.getVerification() != null ? commit.getVerification().getReason() : "");
        writeString(fieldBuilders.get(i++), commit.getAuthor().getName());
        writeString(fieldBuilders.get(i++), commit.getAuthor().getEmail());
        writeTimestamp(fieldBuilders.get(i++), commit.getAuthor().getDate());
        BIGINT.writeLong(fieldBuilders.get(i++), author != null ? author.getId() : 0);
        writeString(fieldBuilders.get(i++), author != null ? author.getLogin() : "");
        writeString(fieldBuilders.get(i++), commit.getCommitter().getName());
        writeString(fieldBuilders.get(i++), commit.getCommitter().getEmail());
        writeTimestamp(fieldBuilders.get(i++), commit.getCommitter().getDate());
        BIGINT.writeLong(fieldBuilders.get(i++), committer != null ? committer.getId() : 0);
        writeString(fieldBuilders.get(i++), committer != null ? committer.getLogin() : "");

        // parents array
        BlockBuilder parentShas = VARCHAR.createBlockBuilder(null, parents.size());
        for (Ref parent : parents) {
            writeString(parentShas, parent.getSha());
        }
        ARRAY_VARCHAR.writeObject(fieldBuilders.get(i), parentShas.build());
    }
}
