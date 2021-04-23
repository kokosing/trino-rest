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
public class PullCommit
{
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
                url,
                sha,
                htmlUrl,
                commentsUrl,
                commit,
                author,
                committer,
                parents);
    }
}
