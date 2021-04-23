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

package pl.net.was.rest.github;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import pl.net.was.rest.RestConnectorFactory;
import pl.net.was.rest.github.function.JobLogs;
import pl.net.was.rest.github.function.Jobs;
import pl.net.was.rest.github.function.Runs;
import pl.net.was.rest.github.function.Steps;

import java.util.Set;

public class GithubPlugin
        implements Plugin
{
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new RestConnectorFactory(
                "github",
                config -> new GithubRest(
                        config.get("token"),
                        config.get("owner"),
                        config.get("repo"))));
    }

    @Override
    public Set<Class<?>> getFunctions()
    {
        return ImmutableSet.<Class<?>>builder()
                .add(Runs.class)
                .add(Jobs.class)
                .add(Steps.class)
                .add(JobLogs.class)
                .build();
    }
}
