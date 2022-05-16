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

package pl.net.was.rest.github.model;

import com.fasterxml.jackson.annotation.JsonProperty;

@SuppressWarnings("unused")
public class RepoPermissions
{
    private final Boolean push;
    private final Boolean triage;
    private final Boolean pull;
    private final Boolean maintain;
    private final Boolean admin;

    public RepoPermissions(
            @JsonProperty("push") Boolean push,
            @JsonProperty("triage") Boolean triage,
            @JsonProperty("pull") Boolean pull,
            @JsonProperty("maintain") Boolean maintain,
            @JsonProperty("admin") Boolean admin)
    {
        this.push = push;
        this.triage = triage;
        this.pull = pull;
        this.maintain = maintain;
        this.admin = admin;
    }

    public Boolean getPush()
    {
        return push;
    }

    public Boolean getTriage()
    {
        return triage;
    }

    public Boolean getPull()
    {
        return pull;
    }

    public Boolean getMaintain()
    {
        return maintain;
    }

    public Boolean getAdmin()
    {
        return admin;
    }
}
