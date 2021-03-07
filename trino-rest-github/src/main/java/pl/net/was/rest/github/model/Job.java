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
import io.trino.spi.type.DateTimeEncoding;

import java.time.ZonedDateTime;
import java.util.List;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_SECONDS;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.roundDiv;
import static io.trino.spi.type.VarcharType.VARCHAR;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Job
        implements BlockWriter
{
    private final long id;
    private final long runId;
    private final String runUrl;
    private final String nodeId;
    private final String headSha;
    private final String url;
    private final String htmlUrl;
    private final String status;
    private final String conclusion;
    private final ZonedDateTime startedAt;
    private final ZonedDateTime completedAt;
    private final String name;
    private final List<Step> steps;
    private final String checkRunUrl;

    public Job(
            @JsonProperty("id") long id,
            @JsonProperty("run_id") long runId,
            @JsonProperty("run_url") String runUrl,
            @JsonProperty("node_id") String nodeId,
            @JsonProperty("head_sha") String headSha,
            @JsonProperty("url") String url,
            @JsonProperty("html_url") String htmlUrl,
            @JsonProperty("status") String status,
            @JsonProperty("conclusion") String conclusion,
            @JsonProperty("started_at") ZonedDateTime startedAt,
            @JsonProperty("completed_at") ZonedDateTime completedAt,
            @JsonProperty("name") String name,
            @JsonProperty("steps") List<Step> steps,
            @JsonProperty("check_run_url") String checkRunUrl)
    {
        this.id = id;
        this.runId = runId;
        this.runUrl = runUrl;
        this.nodeId = nodeId;
        this.headSha = headSha;
        this.url = url;
        this.htmlUrl = htmlUrl;
        this.status = status;
        this.conclusion = conclusion;
        this.startedAt = startedAt;
        this.completedAt = completedAt;
        this.name = name;
        steps.forEach(s -> s.setJobId(id));
        this.steps = steps;
        this.checkRunUrl = checkRunUrl;
    }

    public List<?> toRow()
    {
        return ImmutableList.of(
                id,
                runId,
                runUrl,
                nodeId,
                headSha,
                url,
                htmlUrl,
                status,
                conclusion,
                startedAt,
                completedAt,
                name,
                steps,
                checkRunUrl);
    }

    public void writeTo(BlockBuilder rowBuilder)
    {
        BIGINT.writeLong(rowBuilder, id);
        BIGINT.writeLong(rowBuilder, runId);
        if (runUrl == null) {
            rowBuilder.appendNull();
        }
        else {
            VARCHAR.writeString(rowBuilder, runUrl);
        }
        if (nodeId == null) {
            rowBuilder.appendNull();
        }
        else {
            VARCHAR.writeString(rowBuilder, nodeId);
        }
        if (headSha == null) {
            rowBuilder.appendNull();
        }
        else {
            VARCHAR.writeString(rowBuilder, headSha);
        }
        if (url == null) {
            rowBuilder.appendNull();
        }
        else {
            VARCHAR.writeString(rowBuilder, url);
        }
        if (htmlUrl == null) {
            rowBuilder.appendNull();
        }
        else {
            VARCHAR.writeString(rowBuilder, htmlUrl);
        }
        if (status == null) {
            rowBuilder.appendNull();
        }
        else {
            VARCHAR.writeString(rowBuilder, status);
        }
        if (conclusion == null) {
            rowBuilder.appendNull();
        }
        else {
            VARCHAR.writeString(rowBuilder, conclusion);
        }
        if (startedAt == null) {
            rowBuilder.appendNull();
        }
        else {
            TIMESTAMP_TZ_SECONDS.writeLong(rowBuilder, packTimestamp(startedAt));
        }
        if (completedAt == null) {
            rowBuilder.appendNull();
        }
        else {
            TIMESTAMP_TZ_SECONDS.writeLong(rowBuilder, packTimestamp(completedAt));
        }
        if (name == null) {
            rowBuilder.appendNull();
        }
        else {
            VARCHAR.writeString(rowBuilder, name);
        }
        // not writing steps
        if (checkRunUrl == null) {
            rowBuilder.appendNull();
        }
        else {
            VARCHAR.writeString(rowBuilder, checkRunUrl);
        }
    }

    private static long packTimestamp(ZonedDateTime timestamp)
    {
        if (timestamp == null) {
            return 0;
        }
        return DateTimeEncoding.packDateTimeWithZone(
                timestamp.toEpochSecond() * MILLISECONDS_PER_SECOND + roundDiv(timestamp.toLocalTime().getNano(), NANOSECONDS_PER_MILLISECOND),
                timestamp.getZone().getId());
    }

    public List<Step> getSteps()
    {
        return steps;
    }
}
