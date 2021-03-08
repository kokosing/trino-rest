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

package rocks.trino.rest;

import com.google.common.collect.ImmutableList;
import io.trino.spi.HostAddress;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableLayoutHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.FixedSplitSource;

import java.util.List;

import static java.util.stream.Collectors.toList;

public class RestSplitManager
        implements ConnectorSplitManager
{
    private final NodeManager nodeManager;

    public RestSplitManager(NodeManager nodeManager)
    {
        this.nodeManager = nodeManager;
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout, SplitSchedulingStrategy splitSchedulingStrategy)
    {
        RestConnectorTableLayoutHandle layoutHandle = Types.checkType(layout, RestConnectorTableLayoutHandle.class, "layout");

        List<HostAddress> addresses = nodeManager.getRequiredWorkerNodes().stream()
                .map(Node::getHostAndPort)
                .collect(toList());

        return new FixedSplitSource(ImmutableList.of(
                new RestConnectorSplit(layoutHandle.getTableHandle(), addresses)));
    }
}
