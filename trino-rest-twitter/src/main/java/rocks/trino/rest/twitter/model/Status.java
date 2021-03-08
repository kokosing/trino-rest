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

package rocks.trino.rest.twitter.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Status
{
    private final String id;
    private final String text;
    private final long retweetCount;
    private final User user;

    public Status(
            @JsonProperty("id_str") String id,
            @JsonProperty("text") String text,
            @JsonProperty("retweet_count") long retweetCount,
            @JsonProperty("user") User user)
    {
        this.id = id;
        this.text = text;
        this.retweetCount = retweetCount;
        this.user = user;
    }

    public String getId()
    {
        return id;
    }

    public String getText()
    {
        return text;
    }

    public long getRetweetCount()
    {
        return retweetCount;
    }

    public User getUser()
    {
        return user;
    }
}
