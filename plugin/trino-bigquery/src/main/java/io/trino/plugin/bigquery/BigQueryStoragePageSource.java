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
package io.trino.plugin.bigquery;

import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.protobuf.ByteString;
import io.airlift.log.Logger;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.type.Type;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.arrow.VectorToPageConverter.convert;
import static java.util.Objects.requireNonNull;

public class BigQueryStoragePageSource
        implements ConnectorPageSource
{
    private static final Logger log = Logger.get(BigQueryStoragePageSource.class);
    private static final BufferAllocator allocator = new RootAllocator(RootAllocator
            .configBuilder()
            .from(RootAllocator.defaultConfig())
            .maxAllocation(Integer.MAX_VALUE)
            .build());

    private final BigQueryReadClient bigQueryReadClient;
    private final BigQuerySplit split;
    private final List<String> columnNames;
    private final List<Type> columnTypes;
    private final AtomicLong readBytes;
    private final Iterator<ReadRowsResponse> responses;
    private final Schema schema;

    public BigQueryStoragePageSource(
            BigQueryReadClient bigQueryReadClient,
            int maxReadRowsRetries,
            BigQuerySplit split,
            List<BigQueryColumnHandle> columns)
    {
        this.bigQueryReadClient = requireNonNull(bigQueryReadClient, "bigQueryReadClient is null");
        this.split = requireNonNull(split, "split is null");
        this.readBytes = new AtomicLong();
        requireNonNull(columns, "columns is null");
        this.columnNames = columns.stream()
                .map(BigQueryColumnHandle::getName)
                .collect(toImmutableList());
        this.columnTypes = columns.stream()
                .map(BigQueryColumnHandle::getTrinoType)
                .collect(toImmutableList());
        this.schema = getSchema(split.getArrowSchema());

        log.debug("Starting to read from %s", split.getStreamName());
        responses = new ReadRowsHelper(bigQueryReadClient, split.getStreamName(), maxReadRowsRetries).readRows();
    }

    private static Schema getSchema(String schema)
    {
        try {
            return Schema.fromJSON(schema);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return readBytes.get();
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        return !responses.hasNext();
    }

    @Override
    public Page getNextPage()
    {
        ReadRowsResponse response = responses.next();

        try (BufferAllocator childAllocator = allocator.newChildAllocator(split.getStreamName(), response.getSerializedSize(), Long.MAX_VALUE)) {
            try (ArrowRecordBatch batch = parse(childAllocator, response)) {
                return convert(childAllocator, schema, columnNames, columnTypes, batch);
            }
        }
    }
//
//    private void appendTo(Type type, Object value, BlockBuilder output)
//    {
//        if (value == null) {
//            output.appendNull();
//            return;
//        }
//
//        Class<?> javaType = type.getJavaType();
//        try {
//            if (javaType == boolean.class) {
//                type.writeBoolean(output, (Boolean) value);
//            }
//            else if (javaType == long.class) {
//                if (type.equals(BIGINT)) {
//                    type.writeLong(output, ((Number) value).longValue());
//                }
//                else if (type.equals(INTEGER)) {
//                    type.writeLong(output, ((Number) value).intValue());
//                }
//                else if (type instanceof DecimalType) {
//                    verify(isShortDecimal(type), "The type should be short decimal");
//                    DecimalType decimalType = (DecimalType) type;
//                    BigDecimal decimal = DECIMAL_CONVERTER.convert(decimalType.getPrecision(), decimalType.getScale(), value);
//                    type.writeLong(output, encodeShortScaledValue(decimal, decimalType.getScale()));
//                }
//                else if (type.equals(DATE)) {
//                    type.writeLong(output, ((Number) value).intValue());
//                }
//                else if (type.equals(TIMESTAMP_MICROS)) {
//                    type.writeLong(output, toTrinoTimestamp(((Utf8) value).toString()));
//                }
//                else if (type.equals(TIME_MICROS)) {
//                    type.writeLong(output, (long) value * PICOSECONDS_PER_MICROSECOND);
//                }
//                else {
//                    throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Unhandled type for %s: %s", javaType.getSimpleName(), type));
//                }
//            }
//            else if (javaType == double.class) {
//                type.writeDouble(output, ((Number) value).doubleValue());
//            }
//            else if (type.getJavaType() == Int128.class) {
//                writeObject(output, type, value);
//            }
//            else if (javaType == Slice.class) {
//                writeSlice(output, type, value);
//            }
//            else if (javaType == LongTimestampWithTimeZone.class) {
//                verify(type.equals(TIMESTAMP_TZ_MICROS));
//                long epochMicros = (long) value;
//                int picosOfMillis = toIntExact(floorMod(epochMicros, MICROSECONDS_PER_MILLISECOND)) * PICOSECONDS_PER_MICROSECOND;
//                type.writeObject(output, fromEpochMillisAndFraction(floorDiv(epochMicros, MICROSECONDS_PER_MILLISECOND), picosOfMillis, UTC_KEY));
//            }
//            else if (javaType == Block.class) {
//                writeBlock(output, type, value);
//            }
//            else {
//                throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Unhandled type for %s: %s", javaType.getSimpleName(), type));
//            }
//        }
//        catch (ClassCastException ignore) {
//            // returns null instead of raising exception
//            output.appendNull();
//        }
//    }
//
//    private static void writeSlice(BlockBuilder output, Type type, Object value)
//    {
//        if (type instanceof VarcharType) {
//            type.writeSlice(output, utf8Slice(((Utf8) value).toString()));
//        }
//        else if (type instanceof VarbinaryType) {
//            if (value instanceof ByteBuffer) {
//                type.writeSlice(output, Slices.wrappedBuffer((ByteBuffer) value));
//            }
//            else {
//                output.appendNull();
//            }
//        }
//        else {
//            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unhandled type for Slice: " + type.getTypeSignature());
//        }
//    }
//
//    private static void writeObject(BlockBuilder output, Type type, Object value)
//    {
//        if (type instanceof DecimalType) {
//            verify(isLongDecimal(type), "The type should be long decimal");
//            DecimalType decimalType = (DecimalType) type;
//            BigDecimal decimal = DECIMAL_CONVERTER.convert(decimalType.getPrecision(), decimalType.getScale(), value);
//            type.writeObject(output, Decimals.encodeScaledValue(decimal, decimalType.getScale()));
//        }
//        else {
//            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unhandled type for Object: " + type.getTypeSignature());
//        }
//    }
//
//    private void writeBlock(BlockBuilder output, Type type, Object value)
//    {
//        if (type instanceof ArrayType && value instanceof List<?>) {
//            BlockBuilder builder = output.beginBlockEntry();
//
//            for (Object element : (List<?>) value) {
//                appendTo(type.getTypeParameters().get(0), element, builder);
//            }
//
//            output.closeEntry();
//            return;
//        }
//        if (type instanceof RowType && value instanceof GenericRecord) {
//            GenericRecord record = (GenericRecord) value;
//            BlockBuilder builder = output.beginBlockEntry();
//
//            List<String> fieldNames = new ArrayList<>();
//            for (int i = 0; i < type.getTypeSignature().getParameters().size(); i++) {
//                TypeSignatureParameter parameter = type.getTypeSignature().getParameters().get(i);
//                fieldNames.add(parameter.getNamedTypeSignature().getName().orElse("field" + i));
//            }
//            checkState(fieldNames.size() == type.getTypeParameters().size(), "fieldName doesn't match with type size : %s", type);
//            for (int index = 0; index < type.getTypeParameters().size(); index++) {
//                appendTo(type.getTypeParameters().get(index), record.get(fieldNames.get(index)), builder);
//            }
//            output.closeEntry();
//            return;
//        }
//        throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unhandled type for Block: " + type.getTypeSignature());
//    }

    @Override
    public long getMemoryUsage()
    {
        return allocator.getAllocatedMemory();
    }

    @Override
    public void close()
    {
        bigQueryReadClient.close();
    }

    org.apache.arrow.vector.ipc.message.ArrowRecordBatch parse(BufferAllocator allocator, ReadRowsResponse response)
    {
        readBytes.addAndGet(response.getArrowRecordBatch().getSerializedSize());
        log.debug("Read %d bytes (total %d) from %s", response.getArrowRecordBatch().getSerializedSize(), readBytes.get(), split.getStreamName());

        try {
            return MessageSerializer.deserializeRecordBatch(readChannelForByteString(response.getArrowRecordBatch().getSerializedRecordBatch()), allocator);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static ReadChannel readChannelForByteString(ByteString input)
    {
        return new ReadChannel(new ByteArrayReadableSeekableByteChannel(input.toByteArray()));
    }
}
