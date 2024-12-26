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
import io.trino.client.CloseableIterator;
import io.trino.client.Column;
import io.trino.client.QueryDataDecoder;
import io.trino.client.spooling.DataAttributes;
import io.trino.client.spooling.encoding.ArrowDecodingUtils.TypeDecoder;
import okhttp3.OkHttpClient;
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
import java.util.List;
import java.util.NoSuchElementException;
import java.util.logging.Level;
import java.util.logging.Logger;

import static io.trino.client.spooling.encoding.ArrowDecodingUtils.createTypeDecoders;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;

public class ArrowIpcQueryDataDecoder
        implements QueryDataDecoder
{
    static {
        Logger.getLogger(OkHttpClient.class.getName()).setLevel(Level.FINE);
    }

    private static final String ENCODING = "arrow";

    private static final BufferAllocator ROOT_ALLOCATOR = new RootAllocator();
    private final List<Column> columns;

    public ArrowIpcQueryDataDecoder(List<Column> columns)
    {
        this.columns = ImmutableList.copyOf(columns);
    }

    @Override
    public CloseableIterator<List<Object>> decode(InputStream input, DataAttributes segmentAttributes)
            throws IOException
    {
        BufferAllocator allocator = ROOT_ALLOCATOR.newChildAllocator(randomUUID().toString(), Integer.MAX_VALUE, Integer.MAX_VALUE);
        ArrowStreamReader streamReader = new ArrowStreamReader(input, allocator);
        return new ArrowRowIterator(allocator, streamReader, createTypeDecoders(columns));
    }

    public static class ArrowRowIterator
            implements CloseableIterator<List<Object>>
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

        @Override
        public String toString()
        {
            return "ArrowRowIterator{reader=" + reader + '}';
        }

        @Override
        public void close()
                throws IOException
        {
            reader.close();
            allocator.close();
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
            return new ArrowIpcQueryDataDecoder(columns);
        }

        @Override
        public String encoding()
        {
            return ENCODING;
        }
    }

    public static class ZstdFactory
            extends ArrowIpcQueryDataDecoder.Factory
    {
        @Override
        public QueryDataDecoder create(List<Column> columns, DataAttributes queryAttributes)
        {
            return new ZstdQueryDataDecoder(super.create(columns, queryAttributes));
        }

        @Override
        public String encoding()
        {
            return super.encoding() + "+zstd";
        }
    }

    public static class Lz4Factory
            extends ArrowIpcQueryDataDecoder.Factory
    {
        @Override
        public QueryDataDecoder create(List<Column> columns, DataAttributes queryAttributes)
        {
            return new Lz4QueryDataDecoder(super.create(columns, queryAttributes));
        }

        @Override
        public String encoding()
        {
            return super.encoding() + "+lz4";
        }
    }

    @Override
    public String encoding()
    {
        return ENCODING;
    }
}
