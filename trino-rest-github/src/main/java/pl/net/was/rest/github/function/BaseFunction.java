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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.ArrayType;
import okhttp3.Cache;
import okhttp3.OkHttpClient;
import okhttp3.logging.HttpLoggingInterceptor;
import pl.net.was.rest.github.GithubService;
import pl.net.was.rest.github.model.BlockWriter;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.time.ZoneOffset.UTC;

public abstract class BaseFunction
{
    protected ArrayType arrayType;
    protected PageBuilder pageBuilder;

    protected int cacheSize = 10 * 1024 * 1024; // 10 MB

    protected final GithubService service;

    public BaseFunction()
    {
        OkHttpClient.Builder clientBuilder = new OkHttpClient.Builder();

        if (cacheSize > 0) {
            Path cacheDir = Paths.get(System.getProperty("java.io.tmpdir"), "trino-rest-cache");
            clientBuilder.cache(new Cache(cacheDir.toFile(), cacheSize));
        }

        if (getLogLevel().intValue() <= Level.FINE.intValue()) {
            HttpLoggingInterceptor interceptor = new HttpLoggingInterceptor();
            interceptor.setLevel(HttpLoggingInterceptor.Level.BODY);
            clientBuilder.addInterceptor(interceptor);
        }

        // TODO this is duplicated in GithubRest.service, figure out if DI can be used with functions
        service = new Retrofit.Builder()
                .baseUrl("https://api.github.com/")
                .client(clientBuilder.build())
                .addConverterFactory(JacksonConverterFactory.create(
                        new ObjectMapper()
                                .registerModule(new Jdk8Module())
                                .registerModule(new JavaTimeModule())))
                .build()
                .create(GithubService.class);
    }

    private Level getLogLevel()
    {
        String loggerName = BaseFunction.class.getName();
        Logger logger = Logger.getLogger(loggerName);
        Level level = logger.getLevel();
        while (level == null) {
            Logger parent = logger.getParent();
            if (parent == null) {
                return Level.OFF;
            }
            level = parent.getLevel();
        }
        return level;
    }

    protected Block buildBlock(List<? extends BlockWriter> writers)
    {
        if (pageBuilder.isFull()) {
            pageBuilder.reset();
        }

        BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(0);
        BlockBuilder entryBuilder = blockBuilder.beginBlockEntry();

        for (BlockWriter writer : writers) {
            BlockBuilder rowBuilder = entryBuilder.beginBlockEntry();
            writer.writeTo(rowBuilder);
            entryBuilder.closeEntry();
        }

        blockBuilder.closeEntry();
        pageBuilder.declarePosition();
        return arrayType.getObject(blockBuilder, blockBuilder.getPositionCount() - 1);
    }

    protected static LocalDateTime fromTrinoTimestamp(long epochMicros)
    {
        long epochSecond = floorDiv(epochMicros, MICROSECONDS_PER_SECOND);
        int nanoFraction = floorMod(epochMicros, MICROSECONDS_PER_SECOND) * NANOSECONDS_PER_MICROSECOND;
        Instant instant = Instant.ofEpochSecond(epochSecond, nanoFraction);
        return LocalDateTime.ofInstant(instant, UTC);
    }
}
