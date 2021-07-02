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

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.VarcharType;
import okhttp3.ResponseBody;
import pl.net.was.rest.github.GithubTable;
import retrofit2.Response;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.StandardTypes.BIGINT;
import static io.trino.spi.type.StandardTypes.VARCHAR;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.util.Objects.requireNonNull;
import static pl.net.was.rest.github.GithubRest.JOBS_TABLE_TYPE;
import static pl.net.was.rest.github.GithubRest.checkServiceResponse;
import static pl.net.was.rest.github.GithubRest.getRowType;

@ScalarFunction(value = "job_logs", deterministic = false)
@Description("Get workflow job logs")
public class JobLogs
        extends BaseFunction
{
    // as defined in io.trino.operator.project.PageProcessor
    private static final int MAX_PAGE_SIZE_IN_BYTES = 4 * 1024 * 1024;
    private static final int MAX_ROW_SIZE_IN_BYTES = 100 * 1024;

    public JobLogs()
    {
        RowType rowType = getRowType(GithubTable.JOB_LOGS);
        arrayType = new ArrayType(rowType);
        pageBuilder = new PageBuilder(ImmutableList.of(arrayType));
    }

    @SqlType(JOBS_TABLE_TYPE)
    public Block getLog(@SqlType(VARCHAR) Slice owner, @SqlType(VARCHAR) Slice repo, @SqlType(BIGINT) long jobId)
            throws IOException
    {
        Response<ResponseBody> response = service.jobLogs(
                "Bearer " + token,
                owner.toStringUtf8(),
                repo.toStringUtf8(),
                jobId).execute();
        if (response.code() == HTTP_NOT_FOUND) {
            return null;
        }
        checkServiceResponse(response);
        ResponseBody body = requireNonNull(response.body());
        String size = response.headers().get("Content-Length");
        return buildBytesBlock(owner.toStringUtf8(),
                repo.toStringUtf8(),
                jobId,
                size != null ? Long.parseLong(size) : null,
                body.byteStream());
    }

    protected Block buildBytesBlock(String owner, String repo, long jobId, Long size, InputStream inputStream)
            throws IOException
    {
        if (pageBuilder.isFull()) {
            pageBuilder.reset();
        }

        BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(0);
        BlockBuilder entryBuilder = blockBuilder.beginBlockEntry();

        int i = 1;
        for (Slice slice : getParts(inputStream)) {
            BlockBuilder rowBuilder = entryBuilder.beginBlockEntry();

            VarcharType.VARCHAR.writeString(rowBuilder, owner);
            VarcharType.VARCHAR.writeString(rowBuilder, repo);
            BigintType.BIGINT.writeLong(rowBuilder, jobId);
            if (size == null) {
                rowBuilder.appendNull();
            }
            else {
                BigintType.BIGINT.writeLong(rowBuilder, size);
            }
            INTEGER.writeLong(rowBuilder, i++);
            VARBINARY.writeSlice(rowBuilder, slice);

            entryBuilder.closeEntry();
        }

        blockBuilder.closeEntry();
        pageBuilder.declarePosition();
        return arrayType.getObject(blockBuilder, blockBuilder.getPositionCount() - 1);
    }

    public static List<Slice> getParts(InputStream inputStream)
            throws IOException
    {
        ImmutableList.Builder<Slice> result = new ImmutableList.Builder<>();

        byte[] contents = new byte[MAX_PAGE_SIZE_IN_BYTES - MAX_ROW_SIZE_IN_BYTES];
        int n;
        while ((n = inputStream.readNBytes(contents, 0, contents.length)) != 0) {
            result.add(Slices.wrappedBuffer(contents, 0, n));
        }
        return result.build();
    }
}
