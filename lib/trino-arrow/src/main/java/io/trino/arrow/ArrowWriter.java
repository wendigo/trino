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

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.arrow.ArrowTypeConverter.createArrowField;
import static java.util.function.UnaryOperator.identity;

public class ArrowWriter
        implements AutoCloseable
{
    private final BufferAllocator allocator;
    private final VectorSchemaRoot root;
    private final Map<OutputColumn, Field> columnToField;
    private final OutputStream outputStream;

    public ArrowWriter(List<OutputColumn> columns, OutputStream output)
    {
        this.allocator = new RootAllocator();
        this.outputStream = output;

        // Convert OutputColumns to Arrow Fields
        this.columnToField = columns.stream().collect(toImmutableMap(identity(), ArrowWriter::toArrowField));

        // Create Arrow Schema
        Schema schema = new Schema(columnToField.values());

        // Create VectorSchemaRoot
        this.root = VectorSchemaRoot.create(schema, allocator);
    }

    private static Field toArrowField(OutputColumn column)
    {
        return createArrowField(column.columnName(), column.type());
    }

    public void write(Page page)
            throws IOException
    {
        try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(outputStream))) {
            writer.start();
            for (OutputColumn outputColumn : columnToField.keySet()) {
                Field field = columnToField.get(outputColumn);
                FieldVector vector = root.getVector(field);
                vector.allocateNew();
                Block block = page.getBlock(outputColumn.sourcePageChannel());
                ArrowColumnWriter columnWriter = ArrowWriters.createWriter(vector, outputColumn.type());
                columnWriter.write(block);
            }
            root.setRowCount(page.getPositionCount());
            writer.writeBatch();
            writer.end();
        }
    }

    @Override
    public void close()
            throws IOException
    {
        if (outputStream != null) {
            outputStream.close();
        }
        if (root != null) {
            root.close();
        }
        if (allocator != null) {
            allocator.close();
        }
    }
}
