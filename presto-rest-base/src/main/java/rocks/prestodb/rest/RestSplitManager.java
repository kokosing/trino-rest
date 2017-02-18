package rocks.prestodb.rest;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;

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
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout)
    {
        RestConnectorTableLayoutHandle layoutHandle = Types.checkType(layout, RestConnectorTableLayoutHandle.class, "layout");

        List<HostAddress> addresses = nodeManager.getRequiredWorkerNodes().stream()
                .map(Node::getHostAndPort)
                .collect(toList());

        return new FixedSplitSource(ImmutableList.of(
                new RestConnectorSplit(layoutHandle.getTableHandle(), addresses)));
    }
}
