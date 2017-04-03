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

package rocks.prestodb.rest;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import java.util.Map;

public class RestConnectorFactory
        implements ConnectorFactory
{
    private final RestFactory restFactory;
    private final String name;

    public RestConnectorFactory(String name, RestFactory restFactory)
    {
        this.restFactory = restFactory;
        this.name = name;
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public Connector create(String s, Map<String, String> config, ConnectorContext context)
    {
        NodeManager nodeManager = context.getNodeManager();

        return new RestConnector(nodeManager, restFactory.create(config));
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new ConnectorHandleResolver()
        {
            public Class<? extends ConnectorTableHandle> getTableHandleClass()
            {
                return RestTableHandle.class;
            }

            public Class<? extends ColumnHandle> getColumnHandleClass()
            {
                return RestColumnHandle.class;
            }

            public Class<? extends ConnectorSplit> getSplitClass()
            {
                return RestConnectorSplit.class;
            }

            public Class<? extends ConnectorTableLayoutHandle> getTableLayoutHandleClass()
            {
                return RestConnectorTableLayoutHandle.class;
            }

            @Override
            public Class<? extends ConnectorTransactionHandle> getTransactionHandleClass()
            {
                return RestTransactionHandle.class;
            }

            @Override
            public Class<? extends ConnectorInsertTableHandle> getInsertTableHandleClass()
            {
                return RestInsertTableHandle.class;
            }
        };
    }
}
