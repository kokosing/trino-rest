package rocks.prestodb.rest;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.InMemoryRecordSet;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.type.Type;

import java.util.Collection;
import java.util.List;

import static java.util.stream.Collectors.toList;

public class RestRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final Rest rest;

    public RestRecordSetProvider(Rest rest)
    {
        this.rest = rest;
    }

    @Override
    public RecordSet getRecordSet(
            ConnectorTransactionHandle connectorTransactionHandle,
            ConnectorSession connectorSession,
            ConnectorSplit connectorSplit,
            List<? extends ColumnHandle> list)
    {
        RestConnectorSplit split = Types.checkType(connectorSplit, RestConnectorSplit.class, "split");
        // TODO fix below cast
        List<RestColumnHandle> restColumnHandles = (List<RestColumnHandle>) list;

        SchemaTableName schemaTableName = split.getTableHandle().getSchemaTableName();
        Collection<? extends List<?>> rows = rest.getRows(schemaTableName);
        ConnectorTableMetadata tableMetadata = rest.getTableMetadata(schemaTableName);

        List<Integer> columnIndexes = restColumnHandles.stream()
                .map(column -> {
                    int index = 0;
                    for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
                        if (columnMetadata.getName().equalsIgnoreCase(column.getName())) {
                            return index;
                        }
                        index++;
                    }
                    throw new IllegalStateException("Unknown column: " + column.getName());
                })
                .collect(toList());

        Collection<? extends List<?>> mappedRows = rows.stream()
                .map(row -> columnIndexes.stream()
                        .map(index -> row.get(index))
                        .collect(toList()))
                .collect(toList());

        List<Type> mappedTypes = restColumnHandles.stream()
                .map(RestColumnHandle::getType)
                .collect(toList());
        return new InMemoryRecordSet(mappedTypes, mappedRows);
    }
}
