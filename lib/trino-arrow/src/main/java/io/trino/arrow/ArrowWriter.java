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
package io.trino.arrow;

import com.google.common.collect.ImmutableList;
import io.trino.spi.Page;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;

import java.io.IOException;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class ArrowWriter
        implements AutoCloseable
{
    private final VectorSchemaRoot schema;
    private final List<ArrowOutputColumn> columns;

    public ArrowWriter(VectorSchemaRoot schema, List<ArrowOutputColumn> columns)
    {
        this.schema = requireNonNull(schema, "schema is null");
        this.columns = ImmutableList.copyOf(columns);
    }

    public void write(ArrowStreamWriter streamWriter, Page page)
            throws IOException
    {
        for (ArrowOutputColumn column : columns) {
            FieldVector vector = schema.getVector(column.field());
            vector.allocateNew();
            column.columnWriter().write(page.getBlock(column.channel()));
        }
        schema.setRowCount(page.getPositionCount());
        streamWriter.writeBatch();
    }

    @Override
    public void close()
            throws IOException
    {
    }
}
