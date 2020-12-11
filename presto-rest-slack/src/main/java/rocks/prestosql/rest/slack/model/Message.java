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

package rocks.prestosql.rest.slack.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Message
{
    private final String type;
    private final String user;
    private final String text;

    public Message(
            @JsonProperty("type") String type,
            @JsonProperty("user") String user,
            @JsonProperty("text") String text)
    {
        this.type = type;
        this.user = user;
        this.text = text;
    }

    public String getType()
    {
        return type;
    }

    public String getUser()
    {
        return user;
    }

    public String getText()
    {
        return text;
    }
}
