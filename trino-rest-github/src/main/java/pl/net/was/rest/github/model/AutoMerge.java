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
public class AutoMerge
{
    private final User enabledBy;
    private final String mergeMethod;
    private final String commitTitle;
    private final String commitMessage;

    public AutoMerge(
            @JsonProperty("enabled_by") User enabledBy,
            @JsonProperty("merge_method") String mergeMethod,
            @JsonProperty("commit_title") String commitTitle,
            @JsonProperty("commit_message") String commitMessage)
    {
        this.enabledBy = enabledBy;
        this.mergeMethod = mergeMethod;
        this.commitTitle = commitTitle;
        this.commitMessage = commitMessage;
    }

    public User getEnabledBy()
    {
        return enabledBy;
    }

    public String getMergeMethod()
    {
        return mergeMethod;
    }

    public String getCommitTitle()
    {
        return commitTitle;
    }

    public String getCommitMessage()
    {
        return commitMessage;
    }
}
