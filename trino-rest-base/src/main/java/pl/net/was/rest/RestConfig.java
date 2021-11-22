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

package pl.net.was.rest;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import okhttp3.Cache;
import okhttp3.OkHttpClient;

import javax.validation.constraints.NotNull;

import java.io.File;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.google.common.base.Verify.verify;

public class RestConfig
{
    private String customerKey;
    private String customerSecret;
    private String secret;
    private String token;
    private String clientCachePath = Paths.get(System.getProperty("java.io.tmpdir"), "trino-rest-cache").toString();
    private DataSize clientCacheMaxSize = DataSize.of(10, DataSize.Unit.MEGABYTE);
    private Duration clientConnectTimeout = Duration.succinctDuration(10, TimeUnit.SECONDS);
    private Duration clientReadTimeout = Duration.succinctDuration(10, TimeUnit.SECONDS);
    private int minSplits = 1;
    private List<String> minSplitTables = List.of();

    public String getCustomerKey()
    {
        return customerKey;
    }

    @Config("customer_key")
    public RestConfig setCustomerKey(String key)
    {
        this.customerKey = key;
        return this;
    }

    public String getCustomerSecret()
    {
        return customerSecret;
    }

    @Config("customer_secret")
    @ConfigSecuritySensitive
    public RestConfig setCustomerSecret(String secret)
    {
        this.customerSecret = secret;
        return this;
    }

    public String getSecret()
    {
        return secret;
    }

    @Config("secret")
    @ConfigSecuritySensitive
    public RestConfig setSecret(String secret)
    {
        this.secret = secret;
        return this;
    }

    @NotNull
    public String getToken()
    {
        return token;
    }

    @Config("token")
    @ConfigSecuritySensitive
    public RestConfig setToken(String token)
    {
        this.token = token;
        return this;
    }

    @NotNull
    public String getClientCachePath()
    {
        return clientCachePath;
    }

    @Config("client-cache-path")
    public RestConfig setClientCachePath(String clientCachePath)
    {
        this.clientCachePath = clientCachePath;
        return this;
    }

    @NotNull
    public DataSize getClientCacheMaxSize()
    {
        return clientCacheMaxSize;
    }

    @NotNull
    public long getClientCacheMaxSizeBytes()
    {
        return clientCacheMaxSize.toBytes();
    }

    @Config("client-cache-max-size")
    public RestConfig setClientCacheMaxSize(DataSize clientCacheMaxSize)
    {
        this.clientCacheMaxSize = clientCacheMaxSize;
        return this;
    }

    @NotNull
    @MinDuration("1ms")
    public Duration getClientConnectTimeout()
    {
        return clientConnectTimeout;
    }

    @Config("client-connect-timeout")
    public RestConfig setClientConnectTimeout(Duration clientConnectTimeout)
    {
        this.clientConnectTimeout = clientConnectTimeout;
        return this;
    }

    @NotNull
    @MinDuration("1ms")
    public Duration getClientReadTimeout()
    {
        return clientReadTimeout;
    }

    @Config("client-read-timeout")
    public RestConfig setClientReadTimeout(Duration clientReadTimeout)
    {
        this.clientReadTimeout = clientReadTimeout;
        return this;
    }

    @NotNull
    public OkHttpClient.Builder getClientBuilder()
    {
        OkHttpClient.Builder clientBuilder = new OkHttpClient.Builder();

        if (!clientCachePath.isEmpty() && clientCacheMaxSize.toBytes() != 0) {
            clientBuilder.cache(new Cache(new File(clientCachePath), clientCacheMaxSize.toBytes()));
        }
        clientBuilder.connectTimeout(clientConnectTimeout.toMillis(), TimeUnit.MILLISECONDS);
        clientBuilder.readTimeout(clientReadTimeout.toMillis(), TimeUnit.MILLISECONDS);

        return clientBuilder;
    }

    public int getMinSplits()
    {
        return minSplits;
    }

    @Config("min_splits")
    public RestConfig setMinSplits(String minSplits)
    {
        int value = Integer.parseInt(minSplits);
        verify(value > 0, "min_splits must be greater than zero");
        this.minSplits = value;
        return this;
    }

    public List<String> getMinSplitTables()
    {
        return minSplitTables;
    }

    @Config("min_split_tables")
    public RestConfig setMinSplitTables(String minSplitTables)
    {
        this.minSplitTables = Arrays.stream(minSplitTables.split(",")).map(String::trim).collect(Collectors.toList());
        return this;
    }
}
