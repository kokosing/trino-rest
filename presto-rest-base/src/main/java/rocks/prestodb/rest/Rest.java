package rocks.prestodb.rest;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.Type;

import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

import static java.util.stream.Collectors.toList;

public interface Rest
{
    ConnectorTableMetadata getTableMetadata(SchemaTableName schemaTableName);

    List<String> listSchemas();

    default List<SchemaTableName> listTables()
    {
        return listSchemas().stream()
                .flatMap(schema -> listTables(schema).stream())
                .collect(toList());
    }

    List<SchemaTableName> listTables(String schema);

    Collection<? extends List<?>> getRows(SchemaTableName schemaTableName);

    Consumer<List> createRowSink(SchemaTableName schemaTableName);

    default List<Type> getTypes(SchemaTableName schemaTableName)
    {
        return getTableMetadata(schemaTableName).getColumns().stream()
                .map(ColumnMetadata::getType)
                .collect(toList());
    }
}
