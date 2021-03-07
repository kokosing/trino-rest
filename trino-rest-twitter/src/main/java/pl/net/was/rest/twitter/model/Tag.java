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

package pl.net.was.rest.twitter.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;

public class Tag
{
    private final String value;
    private final String creator;
    private final Date lastSet;

    private Tag(
            @JsonProperty("value") String value,
            @JsonProperty("creator") String creator,
            @JsonProperty("last_set") Date lastSet)
    {
        this.value = value;
        this.creator = creator;
        this.lastSet = lastSet;
    }

    @JsonProperty("value")
    public String getValue()
    {
        return value;
    }

    @JsonProperty("creator")

    public String getCreator()
    {
        return creator;
    }

    @JsonProperty("last_set")
    public Date getLastSet()
    {
        return lastSet;
    }
}
