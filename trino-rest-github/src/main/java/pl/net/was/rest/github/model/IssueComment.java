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

import java.time.ZonedDateTime;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class IssueComment
{
    private final long id;
    private final String url;
    private final String htmlUrl;
    private final String body;
    private final User user;
    private final ZonedDateTime createdAt;
    private final ZonedDateTime updatedAt;
    private final String issueUrl;
    private final String authorAssociation;

    public IssueComment(
            @JsonProperty("id") long id,
            @JsonProperty("url") String url,
            @JsonProperty("html_url") String htmlUrl,
            @JsonProperty("body") String body,
            @JsonProperty("user") User user,
            @JsonProperty("created_at") ZonedDateTime createdAt,
            @JsonProperty("updated_at") ZonedDateTime updatedAt,
            @JsonProperty("issue_url") String issueUrl,
            @JsonProperty("author_association") String authorAssociation)
    {
        this.id = id;
        this.url = url;
        this.htmlUrl = htmlUrl;
        this.body = body;
        this.user = user;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.issueUrl = issueUrl;
        this.authorAssociation = authorAssociation;
    }

    public List<?> toRow()
    {
        return ImmutableList.of(
                id,
                url,
                htmlUrl,
                body,
                user.getId(),
                user.getLogin(),
                body,
                createdAt,
                updatedAt,
                issueUrl,
                authorAssociation);
    }
}
