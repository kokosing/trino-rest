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

import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.RecordSink;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorRecordSinkProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

public class RestRecordSinkProvider
        implements ConnectorRecordSinkProvider
{
    private final Rest rest;

    public RestRecordSinkProvider(Rest rest)
    {
        this.rest = rest;
    }

    @Override
    public RecordSink getRecordSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle tableHandle)
    {
        return null;
    }

    @Override
    public RecordSink getRecordSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle connectorTableHandle)
    {
        RestInsertTableHandle insertTableHandle = Types.checkType(connectorTableHandle, RestInsertTableHandle.class, "tableHandle");

        RestTableHandle tableHandle = insertTableHandle.getTableHandle();

        SchemaTableName schemaTableName = tableHandle.getSchemaTableName();
        Consumer<List> rowSink = rest.createRowSink(schemaTableName);
        List<Type> types = rest.getTypes(schemaTableName);

        return new InMemoryObjectRecordSink(types, rowSink);
    }

    private static class InMemoryObjectRecordSink<T>
            implements RecordSink
    {
        private final List<Type> types;
        private final Consumer<List> rowSink;
        private final List<List> objects = new ArrayList();
        private final List rowBuilder;

        private InMemoryObjectRecordSink(List<Type> types, Consumer<List> rowSink)
        {
            this.types = types;
            this.rowSink = rowSink;
            this.rowBuilder = new ArrayList(types.size());
        }

        @Override
        public void beginRecord()
        {
            rowBuilder.clear();
        }

        @Override
        public void finishRecord()
        {
            objects.add(new ArrayList(rowBuilder));
        }

        @Override
        public void appendNull()
        {
            appendObject(null);
        }

        @Override
        public void appendBoolean(boolean value)
        {
            appendObject(value);
        }

        @Override
        public void appendLong(long value)
        {
            appendObject(value);
        }

        @Override
        public void appendDouble(double value)
        {
            appendObject(value);
        }

        @Override
        public void appendString(byte[] value)
        {
            appendObject(Slices.wrappedBuffer(value).toStringUtf8());
        }

        @Override
        public void appendObject(Object value)
        {
            rowBuilder.add(value);
        }

        @Override
        public Collection<Slice> commit()
        {
            objects.forEach(rowSink::accept);
            return ImmutableList.of(Slices.wrappedIntArray(objects.size()));
        }

        @Override
        public void rollback()
        {
            objects.clear();
        }

        @Override
        public List<Type> getColumnTypes()
        {
            return types;
        }
    }
}
