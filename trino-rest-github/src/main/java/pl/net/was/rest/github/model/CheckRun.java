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
public class CheckRun
        extends BaseBlockWriter
{
    private String owner;
    private String repo;
    private String ref;
    private final long id;
    private final String nodeId;
    private final String headSha;
    private final String externalId;
    private final String url;
    private final String htmlUrl;
    private final String detailsUrl;
    private final String status;
    private final String conclusion;
    private final ZonedDateTime startedAt;
    private final ZonedDateTime completedAt;
    private final CheckOutput output;
    private final String name;
    private final Long checkSuiteId;
    private final App app;
    private final List<Pull> pullRequests;

    public CheckRun(
            @JsonProperty("id") long id,
            @JsonProperty("node_id") String nodeId,
            @JsonProperty("head_sha") String headSha,
            @JsonProperty("external_id") String externalId,
            @JsonProperty("url") String url,
            @JsonProperty("html_url") String htmlUrl,
            @JsonProperty("details_url") String detailsUrl,
            @JsonProperty("status") String status,
            @JsonProperty("conclusion") String conclusion,
            @JsonProperty("started_at") ZonedDateTime startedAt,
            @JsonProperty("completed_at") ZonedDateTime completedAt,
            @JsonProperty("output") CheckOutput output,
            @JsonProperty("name") String name,
            @JsonProperty("check_suite") CheckSuite checkSuite,
            @JsonProperty("app") App app,
            @JsonProperty("pull_requests") List<Pull> pullRequests)
    {
        this.id = id;
        this.nodeId = nodeId;
        this.headSha = headSha;
        this.externalId = externalId;
        this.url = url;
        this.htmlUrl = htmlUrl;
        this.detailsUrl = detailsUrl;
        this.status = status;
        this.conclusion = conclusion;
        this.startedAt = startedAt;
        this.completedAt = completedAt;
        this.output = output;
        this.name = name;
        this.checkSuiteId = checkSuite != null ? checkSuite.getId() : null;
        this.app = app;
        this.pullRequests = pullRequests;
    }

    public void setOwner(String owner)
    {
        this.owner = owner;
    }

    public void setRepo(String repo)
    {
        this.repo = repo;
    }

    public void setRef(String ref)
    {
        this.ref = ref;
    }

    public List<?> toRow()
    {
        return ImmutableList.of(
                owner,
                repo,
                ref != null ? ref : "",
                id,
                headSha,
                externalId,
                url,
                htmlUrl,
                detailsUrl,
                status != null ? status : "",
                conclusion != null ? conclusion : "",
                packTimestamp(startedAt),
                packTimestamp(completedAt),
                output != null && output.getTitle() != null ? output.getTitle() : "",
                output != null && output.getSummary() != null ? output.getSummary() : "",
                output != null && output.getText() != null ? output.getText() : "",
                output != null ? output.getAnnotationsCount() : 0,
                output != null && output.getAnnotationsUrl() != null ? output.getAnnotationsUrl() : "",
                name != null ? name : "",
                checkSuiteId != null ? checkSuiteId : 0,
                app != null ? app.getId() : 0,
                app != null ? app.getSlug() : "",
                app != null ? app.getName() : "");
    }

    @Override
    public void writeTo(List<BlockBuilder> fieldBuilders)
    {
        int i = 0;
        writeString(fieldBuilders.get(i++), owner);
        writeString(fieldBuilders.get(i++), repo);
        writeString(fieldBuilders.get(i++), ref);
        BIGINT.writeLong(fieldBuilders.get(i++), id);
        writeString(fieldBuilders.get(i++), headSha);
        writeString(fieldBuilders.get(i++), externalId);
        writeString(fieldBuilders.get(i++), url);
        writeString(fieldBuilders.get(i++), htmlUrl);
        writeString(fieldBuilders.get(i++), detailsUrl);
        writeString(fieldBuilders.get(i++), status);
        writeString(fieldBuilders.get(i++), conclusion);
        writeTimestamp(fieldBuilders.get(i++), startedAt);
        writeTimestamp(fieldBuilders.get(i++), completedAt);
        writeString(fieldBuilders.get(i++), output.getTitle());
        writeString(fieldBuilders.get(i++), output.getSummary());
        writeString(fieldBuilders.get(i++), output.getText());
        BIGINT.writeLong(fieldBuilders.get(i++), output.getAnnotationsCount());
        writeString(fieldBuilders.get(i++), output.getAnnotationsUrl());
        writeString(fieldBuilders.get(i++), name);
        BIGINT.writeLong(fieldBuilders.get(i++), checkSuiteId);
        BIGINT.writeLong(fieldBuilders.get(i++), app.getId());
        writeString(fieldBuilders.get(i++), app.getSlug());
        writeString(fieldBuilders.get(i), app.getName());
    }
}
