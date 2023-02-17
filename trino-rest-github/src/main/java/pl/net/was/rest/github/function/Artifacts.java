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

package pl.net.was.rest.github.function;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import okhttp3.ResponseBody;
import org.apache.tika.Tika;
import pl.net.was.rest.Rest;
import pl.net.was.rest.github.GithubTable;
import pl.net.was.rest.github.model.Artifact;
import pl.net.was.rest.github.model.ArtifactsList;
import pl.net.was.rest.github.model.ClientError;
import pl.net.was.rest.github.service.ArtifactService;
import retrofit2.Response;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.type.StandardTypes.BIGINT;
import static io.trino.spi.type.StandardTypes.VARCHAR;
import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.util.Objects.requireNonNull;
import static pl.net.was.rest.github.GithubRest.ARTIFACTS_TABLE_TYPE;
import static pl.net.was.rest.github.GithubRest.getMaxBinaryDownloadSizeBytes;
import static pl.net.was.rest.github.GithubRest.getRowType;

@ScalarFunction(value = "artifacts", deterministic = false)
@Description("Get workflow run artifacts")
public class Artifacts
        extends BaseFunction
{
    // as defined in io.trino.operator.project.PageProcessor
    private static final int MAX_PAGE_SIZE_IN_BYTES = 4 * 1024 * 1024;
    private static final int MAX_ROW_SIZE_IN_BYTES = 100 * 1024;
    private static final Tika tika = new Tika();
    private static final String FAIL_ARTIFACT_URL = "Failed to generate URL to download artifact";

    public Artifacts()
    {
        RowType rowType = getRowType(GithubTable.ARTIFACTS);
        arrayType = new ArrayType(rowType);
        pageBuilder = new PageBuilder(ImmutableList.of(arrayType));
    }

    @SqlType(ARTIFACTS_TABLE_TYPE)
    public Block getPage(
            @SqlType(VARCHAR) Slice owner,
            @SqlType(VARCHAR) Slice repo,
            @SqlType(BIGINT) long runId)
            throws IOException
    {
        // there should not be more than a few pages worth of artifacts, so try to get all of them
        List<Artifact> result = new ArrayList<>();
        long total = Long.MAX_VALUE;
        int page = 1;
        while (result.size() < total) {
            Response<ArtifactsList> response = service.listRunArtifacts(
                    "Bearer " + token,
                    owner.toStringUtf8(),
                    repo.toStringUtf8(),
                    runId,
                    PER_PAGE,
                    page++).execute();
            if (response.code() == HTTP_NOT_FOUND) {
                break;
            }
            Rest.checkServiceResponse(response);
            ArtifactsList envelope = response.body();
            total = requireNonNull(envelope, "response body is null").getTotalCount();
            List<Artifact> items = envelope.getItems();
            if (items.size() == 0) {
                break;
            }
            for (Artifact artifact : items) {
                artifact.setOwner(owner.toStringUtf8());
                artifact.setRepo(repo.toStringUtf8());
                artifact.setRunId(runId);

                if (artifact.getSizeInBytes() > getMaxBinaryDownloadSizeBytes()) {
                    Logger.getLogger(Artifacts.class.getName()).warning(format("Skipping downloading artifact %s because its size %d is greater than max of %d",
                            artifact.getId(), artifact.getSizeInBytes(), getMaxBinaryDownloadSizeBytes()));
                    result.add(artifact.clone());
                    continue;
                }
                if (artifact.getExpired()) {
                    Logger.getLogger(Artifacts.class.getName()).warning(format("Skipping downloading expired artifact %s", artifact.getId()));
                    result.add(artifact.clone());
                    continue;
                }
                result.addAll(download(service, token, artifact));
            }
            if (items.size() < PER_PAGE) {
                break;
            }
        }
        return buildBlock(result);
    }

    public static List<Artifact> download(ArtifactService service, String token, Artifact artifact)
            throws IOException
    {
        Response<ResponseBody> response = service.getArtifact(
                "Bearer " + token,
                artifact.getOwner(),
                artifact.getRepo(),
                artifact.getId()).execute();
        if (response.code() == HTTP_NOT_FOUND) {
            return ImmutableList.of(artifact.clone());
        }
        if (!checkResponse(response)) {
            // some artifacts might not be available anymore for download and nothing can be done about it
            return ImmutableList.of(artifact.clone());
        }
        ResponseBody body = requireNonNull(response.body(), "response body is null");
        InputStream zipContents = body.byteStream();

        ImmutableList.Builder<Artifact> result = new ImmutableList.Builder<>();

        ZipInputStream zis = new ZipInputStream(zipContents);
        ZipEntry entry;
        while ((entry = zis.getNextEntry()) != null) {
            if (entry.isDirectory()) {
                continue;
            }
            String name = entry.getName();
            Artifact artifactFile = artifact.clone();
            artifactFile.setFilename(new File(name).getName());
            artifactFile.setPath(name);
            BufferedInputStream is = new BufferedInputStream(zis);
            artifactFile.setMimetype(tika.detect(is, name));
            artifactFile.setFileSizeInBytes(entry.getSize());
            long remaining = entry.getSize();
            if (remaining == 0) {
                artifactFile.setPartNumber(1);
                result.add(artifactFile);
                continue;
            }
            else if (remaining == -1) {
                // size is not know, keep reading until no more content is available
                remaining = Integer.MAX_VALUE;
            }
            int i = 1;
            while (remaining > 0) {
                Artifact chunk = artifactFile.clone();
                byte[] contents = new byte[Math.min((int) remaining, MAX_PAGE_SIZE_IN_BYTES - MAX_ROW_SIZE_IN_BYTES)];
                long read = is.readNBytes(contents, 0, contents.length);
                if (read == 0) {
                    remaining = 0;
                }
                else {
                    remaining -= read;
                }
                chunk.setContents(contents);
                chunk.setPartNumber(i++);
                result.add(chunk);
            }
        }
        zis.close();
        zipContents.close();
        body.close();

        return result.build();
    }

    private static boolean checkResponse(Response<ResponseBody> response)
    {
        if (response.isSuccessful()) {
            return true;
        }
        ResponseBody errorBody = response.errorBody();
        String message = "Unable to read: ";
        if (errorBody != null) {
            try {
                String errorJson = errorBody.string();
                try {
                    ClientError error = new ObjectMapper()
                            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                            .readerFor(ClientError.class)
                            .readValue(errorJson);
                    if (error.getMessage().equals(FAIL_ARTIFACT_URL)) {
                        return false;
                    }
                    message += errorJson;
                }
                catch (JsonProcessingException e) {
                    message += errorJson;
                }
            }
            catch (IOException e) {
                message += e.getMessage();
            }
        }
        throw new TrinoException(GENERIC_INTERNAL_ERROR, message);
    }
}
