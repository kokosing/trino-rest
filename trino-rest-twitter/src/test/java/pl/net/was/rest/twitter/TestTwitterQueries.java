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
package pl.net.was.rest.twitter;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingSession;
import org.testng.annotations.Test;

public class TestTwitterQueries
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session defaultSession = TestingSession.testSessionBuilder()
                .setCatalog("twitter")
                .setSchema("default")
                .build();

        QueryRunner queryRunner = DistributedQueryRunner.builder(defaultSession)
                .setNodeCount(1)
                .build();
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
        assertQuery("SHOW TABLES FROM twitter.default", "VALUES 'whug', 'trino', 'teradata', 'hive'");
    }

    @Test
    public void selectFromWhugTweets()
    {
        computeActual("SELECT * FROM twitter.default.whug");
        computeActual("SELECT * FROM twitter.default.trino");
        computeActual("SELECT * FROM twitter.default.teradata");
        computeActual("SELECT * FROM twitter.default.hive");
    }
}
