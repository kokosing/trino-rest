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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ColumnMetadata;
import pl.net.was.rest.github.GithubRest;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PullCommit
        extends BaseBlockWriter
{
    private long pullNumber;
    private final String url;
    private final String sha;
    private final String htmlUrl;
    private final String commentsUrl;
    private final Commit commit;
    private final User author;
    private final User committer;
    private final List<Ref> parents;

    public PullCommit(
            @JsonProperty("url") String url,
            @JsonProperty("sha") String sha,
            @JsonProperty("html_url") String htmlUrl,
            @JsonProperty("comments_url") String commentsUrl,
            @JsonProperty("commit") Commit commit,
            @JsonProperty("author") User author,
            @JsonProperty("committer") User committer,
            @JsonProperty("parents") List<Ref> parents)
    {
        this.url = url;
        this.sha = sha;
        this.htmlUrl = htmlUrl;
        this.commentsUrl = commentsUrl;
        this.commit = commit;
        this.author = author;
        this.committer = committer;
        this.parents = parents;
    }

    public List<?> toRow()
    {
        return ImmutableList.of(
                sha,
                pullNumber,
                commit,
                author,
                committer,
                parents);
    }

    @Override
    public void writeTo(BlockBuilder rowBuilder)
    {
        Map<String, ColumnMetadata> columns = GithubRest.columns.get("issues").stream()
                .collect(Collectors.toMap(ColumnMetadata::getName, columnMetadata -> columnMetadata));

        writeString(rowBuilder, sha);
        BIGINT.writeLong(rowBuilder, pullNumber);
        writeString(rowBuilder, commit.getMessage());
        writeString(rowBuilder, commit.getTree().getSha());
        BIGINT.writeLong(rowBuilder, commit.getCommentsCount());
        BOOLEAN.writeBoolean(rowBuilder, commit.getVerification().getVerified());
        writeString(rowBuilder, commit.getVerification().getReason());
        writeString(rowBuilder, commit.getAuthor().getName());
        writeString(rowBuilder, commit.getAuthor().getEmail());
        writeTimestamp(rowBuilder, commit.getAuthor().getDate());
        BIGINT.writeLong(rowBuilder, author.getId());
        writeString(rowBuilder, author.getLogin());
        writeString(rowBuilder, commit.getCommitter().getName());
        writeString(rowBuilder, commit.getCommitter().getEmail());
        writeTimestamp(rowBuilder, commit.getCommitter().getDate());
        BIGINT.writeLong(rowBuilder, committer.getId());
        writeString(rowBuilder, committer.getLogin());

        // parents array
        BlockBuilder parentShas = columns.get("label_names").getType().createBlockBuilder(null, parents.size());
        for (Ref parent : parents) {
            writeString(parentShas, parent.getSha());
        }
        rowBuilder.appendStructure(parentShas.build());
    }

    public void setPullNumber(long pullNumber)
    {
        this.pullNumber = pullNumber;
    }
}
