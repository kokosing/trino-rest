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
package rocks.prestodb.rest.twitter;

import com.facebook.presto.Session;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.testing.TestingSession;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

public class TestTwitterQueries
        extends AbstractTestQueryFramework
{
    protected TestTwitterQueries()
            throws Exception
    {
        super(createLocalQueryRunner());
    }

    private static QueryRunner createLocalQueryRunner()
            throws Exception
    {
        Session defaultSession = TestingSession.testSessionBuilder()
                .setCatalog("twitter")
                .setSchema("default")
                .build();

        QueryRunner queryRunner = new DistributedQueryRunner(defaultSession, 1);
        queryRunner.installPlugin(new TwitterPlugin());

        queryRunner.createCatalog(
                "twitter",
                "twitter",
                ImmutableMap.of(
                        "customer_key", System.getenv("TWITTER_CUSTOMER_KEY"),
                        "customer_secret", System.getenv("TWITTER_CUSTOMER_SECRET"),
                        "token", System.getenv("TWITTER_TOKEN"),
                        "secret", System.getenv("TWITTER_SECRET")));

        return queryRunner;
    }

    @Test
    public void showTables()
    {
        assertQuery("SHOW SCHEMAS FROM twitter", "VALUES 'default', 'information_schema'");
        assertQuery("SHOW TABLES FROM twitter.default", "VALUES 'whug', 'prestodb', 'teradata', 'hive'");
    }

    @Test
    public void selectFromWhugTweets()
    {
        computeActual("SELECT * FROM twitter.default.whug");
        computeActual("SELECT * FROM twitter.default.prestodb");
        computeActual("SELECT * FROM twitter.default.teradata");
        computeActual("SELECT * FROM twitter.default.hive");
    }
}
