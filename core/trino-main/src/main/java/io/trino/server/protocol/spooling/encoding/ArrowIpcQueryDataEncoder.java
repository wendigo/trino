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
package io.trino.server.protocol.spooling.encoding;

import com.google.common.collect.ImmutableList;
import com.google.common.io.CountingOutputStream;
import com.google.inject.Inject;
import io.trino.Session;
import io.trino.arrow.ArrowOutputColumn;
import io.trino.arrow.ArrowWriter;
import io.trino.client.spooling.DataAttributes;
import io.trino.server.protocol.OutputColumn;
import io.trino.server.protocol.spooling.QueryDataEncoder;
import io.trino.server.protocol.spooling.QueryDataEncodingConfig;
import io.trino.spi.Page;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.List;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.arrow.ArrowTypeConverter.createArrowField;
import static io.trino.arrow.ArrowWriters.createArrowWriter;
import static io.trino.client.spooling.DataAttribute.SEGMENT_SIZE;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class ArrowIpcQueryDataEncoder
        implements QueryDataEncoder
{
    private static final String ENCODING = "arrow";

    private final List<Field> fields;
    private final BufferAllocator allocator;
    private final List<OutputColumn> columns;

    public ArrowIpcQueryDataEncoder(BufferAllocator allocator, List<OutputColumn> columns)
    {
        this.allocator = requireNonNull(allocator, "allocator is null");
        this.columns = columns;
        this.fields = columns.stream()
                .map(column -> createArrowField(column.columnName(), column.type()))
                .collect(toImmutableList());
    }

    @Override
    public DataAttributes encodeTo(OutputStream output, List<Page> pages)
            throws IOException
    {
        VectorSchemaRoot schema = VectorSchemaRoot.create(new Schema(fields), requireNonNull(allocator, "allocator is null"));
        try (CountingOutputStream wrapper = new CountingOutputStream(output); ArrowStreamWriter writer = new ArrowStreamWriter(schema, null, Channels.newChannel(wrapper))) {
            try (ArrowWriter arrowWriter = new ArrowWriter(schema, buildArrowOutputColumns(columns, fields, schema))) {
                for (Page page : pages) {
                    arrowWriter.write(writer, page);
                }
                return DataAttributes.builder()
                        .set(SEGMENT_SIZE, toIntExact(wrapper.getCount()))
                        .build();
            }
        }
    }

    @Override
    public String encoding()
    {
        return ENCODING;
    }

    private static List<ArrowOutputColumn> buildArrowOutputColumns(List<OutputColumn> columns, List<Field> fields, VectorSchemaRoot schema)
    {
        ImmutableList.Builder<ArrowOutputColumn> outputColumns = ImmutableList
                .builderWithExpectedSize(columns.size());

        verify(columns.size() == fields.size(), "Expected columns size to be equal to fields size");

        for (int i = 0; i < columns.size(); i++) {
            outputColumns.add(new ArrowOutputColumn(
                    columns.get(i).sourcePageChannel(),
                    fields.get(i),
                    createArrowWriter(schema.getVector(i), columns.get(i).type())));
        }

        return outputColumns.build();
    }

    public static class Factory
            implements QueryDataEncoder.Factory
    {
        private final BufferAllocator allocator;

        @Inject
        public Factory(BufferAllocator rootAllocator)
        {
            this.allocator = requireNonNull(rootAllocator, "allocator is null");
        }

        @Override
        public QueryDataEncoder create(Session session, List<OutputColumn> columns)
        {
            return new ArrowIpcQueryDataEncoder(
                    allocator.newChildAllocator(session.getQueryId().toString(), Integer.MAX_VALUE, Integer.MAX_VALUE),
                    columns);
        }

        @Override
        public String encoding()
        {
            return ENCODING;
        }
    }

    public static class ZstdFactory
            extends ArrowIpcQueryDataEncoder.Factory
    {
        private final int compressionThreshold;

        @Inject
        public ZstdFactory(BufferAllocator rootAllocator, QueryDataEncodingConfig config)
        {
            super(rootAllocator);
            this.compressionThreshold = toIntExact(config.getCompressionThreshold().toBytes());
        }

        @Override
        public QueryDataEncoder create(Session session, List<OutputColumn> columns)
        {
            return new ZstdQueryDataEncoder(super.create(session, columns), compressionThreshold);
        }

        @Override
        public String encoding()
        {
            return super.encoding() + "+zstd";
        }
    }

    public static class Lz4Factory
            extends ArrowIpcQueryDataEncoder.Factory
    {
        private final int compressionThreshold;

        @Inject
        public Lz4Factory(BufferAllocator rootAllocator, QueryDataEncodingConfig config)
        {
            super(rootAllocator);
            this.compressionThreshold = toIntExact(config.getCompressionThreshold().toBytes());
        }

        @Override
        public QueryDataEncoder create(Session session, List<OutputColumn> columns)
        {
            return new Lz4QueryDataEncoder(super.create(session, columns), compressionThreshold);
        }

        @Override
        public String encoding()
        {
            return super.encoding() + "+lz4";
        }
    }
}
