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

@SuppressWarnings("unused")
public class Team
        extends BaseBlockWriter
{
    private String org;
    private final long id;
    private final String nodeId;
    private final String url;
    private final String htmlUrl;
    private final String name;
    private final String slug;
    private final String description;
    private final String privacy;
    private final String permission;
    private final String membersUrl;
    private final String repositoriesUrl;
    private final Team parent;

    public Team(
            @JsonProperty("id") long id,
            @JsonProperty("node_id") String nodeId,
            @JsonProperty("url") String url,
            @JsonProperty("html_url") String htmlUrl,
            @JsonProperty("name") String name,
            @JsonProperty("slug") String slug,
            @JsonProperty("description") String description,
            @JsonProperty("privacy") String privacy,
            @JsonProperty("permission") String permission,
            @JsonProperty("members_url") String membersUrl,
            @JsonProperty("repositories_url") String repositoriesUrl,
            @JsonProperty("parent") Team parent)
    {
        this.id = id;
        this.nodeId = nodeId;
        this.url = url;
        this.htmlUrl = htmlUrl;
        this.name = name;
        this.slug = slug;
        this.description = description;
        this.privacy = privacy;
        this.permission = permission;
        this.membersUrl = membersUrl;
        this.repositoriesUrl = repositoriesUrl;
        this.parent = parent;
    }

    public void setOrg(String value)
    {
        this.org = value;
    }

    public List<?> toRow()
    {
        return ImmutableList.of(
                org,
                id,
                nodeId,
                url,
                htmlUrl,
                name,
                slug,
                description != null ? description : "",
                privacy != null ? privacy : "",
                permission != null ? permission : "",
                membersUrl,
                repositoriesUrl,
                parent != null ? parent.id : 0,
                parent != null ? parent.slug : "");
    }

    @Override
    public void writeTo(BlockBuilder rowBuilder)
    {
        writeString(rowBuilder, org);
        BIGINT.writeLong(rowBuilder, id);
        writeString(rowBuilder, nodeId);
        writeString(rowBuilder, url);
        writeString(rowBuilder, htmlUrl);
        writeString(rowBuilder, name);
        writeString(rowBuilder, slug);
        writeString(rowBuilder, description);
        writeString(rowBuilder, privacy);
        writeString(rowBuilder, permission);
        writeString(rowBuilder, membersUrl);
        writeString(rowBuilder, repositoriesUrl);
        if (parent == null) {
            rowBuilder.appendNull();
            rowBuilder.appendNull();
        }
        else {
            BIGINT.writeLong(rowBuilder, parent.id);
            writeString(rowBuilder, parent.slug);
        }
    }
}
