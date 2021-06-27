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

import javax.validation.constraints.NotNull;

import static com.google.common.base.Verify.verify;

public class RestConfig
{
    private String customerKey;
    private String customerSecret;
    private String secret;
    private String token;
    private int minSplits = 1;

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
}
