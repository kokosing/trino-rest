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

package pl.net.was.rest;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SortItem;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Optional;

public class RestTableHandle
        implements ConnectorTableHandle
{
    private final SchemaTableName schemaTableName;
    private final TupleDomain<ColumnHandle> constraint;
    private final int limit;
    private final List<SortItem> sortOrder;

    @JsonCreator
    public RestTableHandle(
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint,
            @JsonProperty("limit") int limit,
            @JsonProperty("sortOrder") List<SortItem> sortOrder)
    {
        this.schemaTableName = schemaTableName;
        this.constraint = constraint;
        this.limit = limit;
        this.sortOrder = sortOrder;
    }

    @JsonProperty("schemaTableName")
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @JsonProperty("constraint")
    public TupleDomain<ColumnHandle> getConstraint()
    {
        return constraint;
    }

    @JsonProperty("limit")
    public int getLimit()
    {
        return limit;
    }

    @JsonProperty("sortOrder")
    public Optional<List<SortItem>> getSortOrder()
    {
        return sortOrder == null ? Optional.empty() : Optional.of(sortOrder);
    }
}
