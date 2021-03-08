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
import io.airlift.slice.Slice;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableLayoutHandle;
import io.trino.spi.connector.ConnectorTableLayoutResult;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.ComputedStatistics;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.stream.Collectors.toMap;

public class RestMetadata
        implements ConnectorMetadata
{
    private final Rest rest;

    public RestMetadata(Rest rest)
    {
        this.rest = rest;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession connectorSession)
    {
        return rest.listSchemas();
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession connectorSession, SchemaTableName schemaTableName)
    {
        if (rest.listTables().contains(schemaTableName)) {
            return new RestTableHandle(schemaTableName);
        }
        return null;
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        RestTableHandle tableHandle = Types.checkType(table, RestTableHandle.class, "tableHandle");
        return ImmutableList.of(
                new ConnectorTableLayoutResult(
                        getTableLayout(session, new RestConnectorTableLayoutHandle(tableHandle)),
                        TupleDomain.all()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession connectorSession, ConnectorTableLayoutHandle connectorTableLayoutHandle)
    {
        RestConnectorTableLayoutHandle tableLayoutHandle = Types.checkType(connectorTableLayoutHandle, RestConnectorTableLayoutHandle.class, "tableLayoutHandle");
        return new ConnectorTableLayout(tableLayoutHandle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession connectorSession, ConnectorTableHandle connectorTableHandle)
    {
        RestTableHandle tableHandle = Types.checkType(connectorTableHandle, RestTableHandle.class, "tableHandle");
        return rest.getTableMetadata(tableHandle.getSchemaTableName());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return rest.listTables();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession connectorSession, ConnectorTableHandle connectorTableHandle)
    {
        return getTableMetadata(connectorSession, connectorTableHandle).getColumns().stream()
                .collect(toMap(column -> column.getName(), column -> new RestColumnHandle(column.getName(), column.getType())));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession connectorSession, ConnectorTableHandle connectorTableHandle, ColumnHandle columnHandle)
    {
        RestColumnHandle restColumnHandle = Types.checkType(columnHandle, RestColumnHandle.class, "columnHandle");
        return new ColumnMetadata(restColumnHandle.getName(), restColumnHandle.getType());
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession connectorSession, SchemaTablePrefix schemaTablePrefix)
    {
        return null;
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle connectorTableHandle)
    {
        RestTableHandle tableHandle = Types.checkType(connectorTableHandle, RestTableHandle.class, "tableHandle");
        return new RestInsertTableHandle(tableHandle);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return Optional.empty();
    }
}
