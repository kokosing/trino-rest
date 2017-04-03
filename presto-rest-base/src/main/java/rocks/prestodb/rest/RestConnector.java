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

import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorRecordSinkProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.transaction.IsolationLevel;

public class RestConnector
        implements Connector
{
    private final NodeManager nodeManager;
    private final Rest rest;

    public RestConnector(NodeManager nodeManager, Rest rest)
    {
        this.nodeManager = nodeManager;
        this.rest = rest;
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
    {
        return new RestTransactionHandle(0);
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transaction)
    {
        return new RestMetadata(rest);
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return new RestSplitManager(nodeManager);
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider()
    {
        return new RestRecordSetProvider(rest);
    }

    @Override
    public ConnectorRecordSinkProvider getRecordSinkProvider()
    {
        return new RestRecordSinkProvider(rest);
    }
}
