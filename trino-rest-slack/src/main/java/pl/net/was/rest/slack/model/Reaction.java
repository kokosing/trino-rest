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

package pl.net.was.rest.slack.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class Reaction
{
    private final String name;
    private final int count;
    private final List<String> users;

    private Reaction(
            @JsonProperty("name") String name,
            @JsonProperty("count") int count,
            @JsonProperty("users") List<String> users)
    {
        this.name = name;
        this.count = count;
        this.users = users;
    }

    public String getName()
    {
        return name;
    }

    public int getCount()
    {
        return count;
    }

    public List<String> getUsers()
    {
        return users;
    }
}
