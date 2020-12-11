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

package rocks.prestosql.rest;

import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.Type;

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
