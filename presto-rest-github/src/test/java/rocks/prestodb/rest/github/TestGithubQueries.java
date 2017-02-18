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
package rocks.prestodb.rest.github;

import com.facebook.presto.Session;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class TestGithubQueries
        extends AbstractTestQueryFramework
{
    protected TestGithubQueries()
            throws Exception
    {
        super(createLocalQueryRunner());
    }

    public static QueryRunner createLocalQueryRunner()
            throws Exception
    {
        Session defaultSession = testSessionBuilder()
                .setCatalog("github")
                .setSchema("default")
                .build();

        QueryRunner queryRunner = new DistributedQueryRunner(defaultSession, 1);
        queryRunner.installPlugin(new GithubPlugin());

        queryRunner.createCatalog(
                "github",
                "github",
                ImmutableMap.of("token", System.getenv("GITHUB_TOKEN")));

        return queryRunner;
    }

    @Test
    public void showTables()
    {
        assertQuery("SHOW SCHEMAS FROM github", "VALUES 'default', 'information_schema'");
        assertQuery("SHOW TABLES FROM github.default", "VALUES 'prestodb_issues'");
    }

    @Test
    public void selectFromGeneral()
    {
        computeActual("SELECT * FROM prestodb_issues");
    }

}
