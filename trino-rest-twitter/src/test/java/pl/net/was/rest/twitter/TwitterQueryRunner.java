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
import io.airlift.log.Level;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNullElse;

public class TwitterQueryRunner
{
    private TwitterQueryRunner() {}

    public static QueryRunner createQueryRunner()
            throws Exception
    {
        Logging logger = Logging.initialize();
        logger.setLevel("pl.net.was", Level.DEBUG);
        logger.setLevel("io.trino", Level.INFO);

        Session defaultSession = testSessionBuilder()
                .setCatalog("twitter")
                .setSchema("default")
                .build();

        QueryRunner queryRunner = DistributedQueryRunner.builder(defaultSession)
                .setNodeCount(1)
                .build();
        queryRunner.installPlugin(new TwitterPlugin());

        String key = requireNonNullElse(
                System.getenv("TWITTER_CUSTOMER_KEY"),
                requireNonNullElse(
                        System.getenv("TWITTER_CONSUMER_KEY"),
                        ""));
        String secret = requireNonNullElse(
                System.getenv("TWITTER_CUSTOMER_SECRET"),
                requireNonNullElse(
                        System.getenv("TWITTER_CONSUMER_SECRET"),
                        ""));
        queryRunner.createCatalog(
                "twitter",
                "twitter",
                ImmutableMap.of(
                        "customer_key", key,
                        "customer_secret", secret,
                        "token", System.getenv("TWITTER_TOKEN"),
                        "secret", System.getenv("TWITTER_SECRET")));

        return queryRunner;
    }

    public static void main(String[] args)
            throws Exception
    {
        QueryRunner queryRunner = createQueryRunner();

        Logger log = Logger.get(TwitterQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", ((DistributedQueryRunner) queryRunner).getCoordinator().getBaseUrl());
    }
}
