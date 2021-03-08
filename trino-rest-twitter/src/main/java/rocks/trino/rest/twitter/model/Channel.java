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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Channel
{
    private final String id;
    private final String name;
    private final boolean isMember;
    private final boolean isArchived;

    @JsonCreator
    public Channel(
            @JsonProperty("id") String id,
            @JsonProperty("name") String name,
            @JsonProperty("is_member") boolean isMember,
            @JsonProperty("is_archived") boolean isArchived)
    {
        this.id = id;
        this.name = name;
        this.isMember = isMember;
        this.isArchived = isArchived;
    }

    public String getName()
    {
        return name;
    }

    public boolean isMember()
    {
        return isMember;
    }

    public boolean isArchived()
    {
        return isArchived;
    }

    public String getId()
    {
        return id;
    }
}
