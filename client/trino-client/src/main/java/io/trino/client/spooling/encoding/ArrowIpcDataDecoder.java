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
package io.trino.client.spooling.encoding;

import com.google.common.collect.ImmutableList;
import io.trino.client.ArrowDecodingUtils.TypeDecoder;
import io.trino.client.Column;
import io.trino.client.QueryDataDecoder;
import io.trino.client.ResultRows;
import io.trino.client.spooling.DataAttributes;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static io.trino.client.ArrowDecodingUtils.createTypeDecoders;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;

public class ArrowIpcDataDecoder
        implements QueryDataDecoder
{
    private static final String ENCODING = "arrow-ipc";

    private static final BufferAllocator ROOT_ALLOCATOR = new RootAllocator();
    private final List<Column> columns;

    public ArrowIpcDataDecoder(List<Column> columns)
    {
        this.columns = ImmutableList.copyOf(columns);
    }

    @Override
    public ResultRows decode(InputStream input, DataAttributes segmentAttributes)
            throws IOException
    {
        BufferAllocator allocator = ROOT_ALLOCATOR.newChildAllocator(randomUUID().toString(), Integer.MAX_VALUE, Integer.MAX_VALUE);
        ArrowStreamReader streamReader = new ArrowStreamReader(input, allocator);
        return new ResultRows() {
            @Override
            public void close()
            {
                try {
                    streamReader.close();
                    allocator.close();
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public Iterator<List<Object>> iterator()
            {
                return new ArrowRowIterator(allocator, streamReader, createTypeDecoders(columns));
            }
        };
    }

    public static class ArrowRowIterator
            implements Iterator<List<Object>>
    {
        private final BufferAllocator allocator;
        private final ArrowReader reader;
        private final TypeDecoder[] decoders;

        private int currentRow;

        public ArrowRowIterator(BufferAllocator allocator, ArrowReader reader, TypeDecoder[] decoders)
        {
            this.allocator = requireNonNull(allocator, "allocator is null");
            this.reader = requireNonNull(reader, "reader is null");
            this.decoders = requireNonNull(decoders, "decoders is null");
        }

        private boolean advance()
        {
            try {
                if (reader.loadNextBatch()) {
                    currentRow = 0;
                    return true;
                }
                return false;
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public boolean hasNext()
        {
            try {
                return currentRow < reader.getVectorSchemaRoot().getRowCount() || advance();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public List<Object> next()
        {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            try {
                List<Object> row = getRow(reader.getVectorSchemaRoot());
                currentRow++;
                return row;
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        public List<Object> getRow(VectorSchemaRoot vectorSchemaRoot)
        {
            ArrayList<Object> row = new ArrayList<>();

            for (int i = 0; i < decoders.length; i++) {
                FieldVector vector = vectorSchemaRoot.getVector(i);
                Object value = vector.getObject(currentRow);
                if (value == null) {
                    row.add(null);
                }
                else {
                    row.add(decoders[i].decode(value));
                }
            }
            return row;
        }
    }

    private static boolean isRowNull(VectorSchemaRoot root, int rowIndex)
    {
        for (ValueVector vector : root.getFieldVectors()) {
            if (!vector.isNull(rowIndex)) {
                return false;
            }
        }
        return true;
    }

    public static class Factory
            implements QueryDataDecoder.Factory
    {
        @Override
        public QueryDataDecoder create(List<Column> columns, DataAttributes queryAttributes)
        {
            return new ArrowIpcDataDecoder(columns);
        }

        @Override
        public String encoding()
        {
            return ENCODING;
        }
    }

    @Override
    public String encoding()
    {
        return ENCODING;
    }
}
