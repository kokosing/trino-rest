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

@SuppressWarnings("unused")
public class Member
        extends BaseBlockWriter
{
    private String org;
    private String teamSlug;
    private final String login;
    private final long id;
    private final String nodeId;
    private final String avatarUrl;
    private final String gravatarId;
    private final String url;
    private final String htmlUrl;
    private final String followersUrl;
    private final String followingUrl;
    private final String gistsUrl;
    private final String starredUrl;
    private final String subscriptionsUrl;
    private final String organizationsUrl;
    private final String reposUrl;
    private final String eventsUrl;
    private final String receivedEventsUrl;
    private final String type;
    private final boolean siteAdmin;

    public Member(
            @JsonProperty("login") String login,
            @JsonProperty("id") long id,
            @JsonProperty("node_id") String nodeId,
            @JsonProperty("avatar_url") String avatarUrl,
            @JsonProperty("gravatar_id") String gravatarId,
            @JsonProperty("url") String url,
            @JsonProperty("html_url") String htmlUrl,
            @JsonProperty("followers_url") String followersUrl,
            @JsonProperty("following_url") String followingUrl,
            @JsonProperty("gists_url") String gistsUrl,
            @JsonProperty("starred_url") String starredUrl,
            @JsonProperty("subscriptions_url") String subscriptionsUrl,
            @JsonProperty("organizations_url") String organizationsUrl,
            @JsonProperty("repos_url") String reposUrl,
            @JsonProperty("events_url") String eventsUrl,
            @JsonProperty("received_events_url") String receivedEventsUrl,
            @JsonProperty("type") String type,
            @JsonProperty("site_admin") boolean siteAdmin)
    {
        this.login = login;
        this.id = id;
        this.nodeId = nodeId;
        this.avatarUrl = avatarUrl;
        this.gravatarId = gravatarId;
        this.url = url;
        this.htmlUrl = htmlUrl;
        this.followersUrl = followersUrl;
        this.followingUrl = followingUrl;
        this.gistsUrl = gistsUrl;
        this.starredUrl = starredUrl;
        this.subscriptionsUrl = subscriptionsUrl;
        this.organizationsUrl = organizationsUrl;
        this.reposUrl = reposUrl;
        this.eventsUrl = eventsUrl;
        this.receivedEventsUrl = receivedEventsUrl;
        this.type = type;
        this.siteAdmin = siteAdmin;
    }

    public long getId()
    {
        return id;
    }

    public String getLogin()
    {
        return login;
    }

    public void setOrg(String value)
    {
        this.org = value;
    }

    public void setTeamSlug(String value)
    {
        this.teamSlug = value;
    }

    public List<?> toRow()
    {
        return ImmutableList.of(
                org,
                teamSlug != null ? teamSlug : "",
                login,
                id,
                avatarUrl != null ? avatarUrl : "",
                gravatarId != null ? gravatarId : "",
                type,
                siteAdmin);
    }

    @Override
    public void writeTo(List<BlockBuilder> fieldBuilders)
    {
        int i = 0;
        writeString(fieldBuilders.get(i++), org);
        writeString(fieldBuilders.get(i++), teamSlug);
        writeString(fieldBuilders.get(i++), login);
        BIGINT.writeLong(fieldBuilders.get(i++), id);
        writeString(fieldBuilders.get(i++), avatarUrl);
        writeString(fieldBuilders.get(i++), gravatarId);
        writeString(fieldBuilders.get(i++), type);
        BOOLEAN.writeBoolean(fieldBuilders.get(i), siteAdmin);
    }
}
