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
import java.util.stream.Collectors;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Review
{
    private final long id;
    private final User user;
    private final String body;
    private final String state;
    private final String htmlUrl;
    private final String pullRequestUrl;
    private final ZonedDateTime submittedAt;
    private final String commitId;
    private final String authorAssociation;

    public Review(
            @JsonProperty("id") long id,
            @JsonProperty("user") User user,
            @JsonProperty("body") String body,
            @JsonProperty("state") String state,
            @JsonProperty("html_url") String htmlUrl,
            @JsonProperty("pull_request_url") String pullRequestUrl,
            @JsonProperty("submitted_at") ZonedDateTime submittedAt,
            @JsonProperty("commit_id") String commitId,
            @JsonProperty("author_association") String authorAssociation)
    {
        this.id = id;
        this.user = user;
        this.body = body;
        this.state = state;
        this.htmlUrl = htmlUrl;
        this.pullRequestUrl = pullRequestUrl;
        this.submittedAt = submittedAt;
        this.commitId = commitId;
        this.authorAssociation = authorAssociation;
    }

    public List<?> toRow()
    {
        return ImmutableList.of(
                id,
                user.getId(),
                user.getLogin(),
                body,
                state,
                htmlUrl,
                pullRequestUrl,
                submittedAt,
                commitId,
                authorAssociation);
    }
}
