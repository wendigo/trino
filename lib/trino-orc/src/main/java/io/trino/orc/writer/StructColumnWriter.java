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
package io.trino.orc.writer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.trino.orc.checkpoint.BooleanStreamCheckpoint;
import io.trino.orc.metadata.ColumnEncoding;
import io.trino.orc.metadata.CompressedMetadataWriter;
import io.trino.orc.metadata.CompressionKind;
import io.trino.orc.metadata.OrcColumnId;
import io.trino.orc.metadata.RowGroupIndex;
import io.trino.orc.metadata.Stream;
import io.trino.orc.metadata.Stream.StreamKind;
import io.trino.orc.metadata.statistics.ColumnStatistics;
import io.trino.orc.stream.PresentOutputStream;
import io.trino.orc.stream.StreamDataOutput;
import io.trino.spi.block.Block;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.orc.metadata.ColumnEncoding.ColumnEncodingKind.DIRECT;
import static io.trino.orc.metadata.CompressionKind.NONE;
import static io.trino.spi.block.RowBlock.getNullSuppressedRowFieldsFromBlock;
import static java.util.Objects.requireNonNull;

public class StructColumnWriter
        implements ColumnWriter
{
    private static final int INSTANCE_SIZE = instanceSize(StructColumnWriter.class);
    private static final ColumnEncoding COLUMN_ENCODING = new ColumnEncoding(DIRECT, 0);

    private final OrcColumnId columnId;
    private final boolean compressed;
    private final PresentOutputStream presentStream;
    private final List<ColumnWriter> structFields;

    private final List<ColumnStatistics> rowGroupColumnStatistics = new ArrayList<>();

    private int nonNullValueCount;

    private boolean closed;

    public StructColumnWriter(OrcColumnId columnId, CompressionKind compression, int bufferSize, List<ColumnWriter> structFields)
    {
        this.columnId = columnId;
        this.compressed = requireNonNull(compression, "compression is null") != NONE;
        this.structFields = ImmutableList.copyOf(requireNonNull(structFields, "structFields is null"));
        this.presentStream = new PresentOutputStream(compression, bufferSize);
    }

    @Override
    public List<ColumnWriter> getNestedColumnWriters()
    {
        ImmutableList.Builder<ColumnWriter> nestedColumnWriters = ImmutableList.builder();
        for (ColumnWriter structField : structFields) {
            nestedColumnWriters
                    .add(structField)
                    .addAll(structField.getNestedColumnWriters());
        }
        return nestedColumnWriters.build();
    }

    @Override
    public Map<OrcColumnId, ColumnEncoding> getColumnEncodings()
    {
        ImmutableMap.Builder<OrcColumnId, ColumnEncoding> encodings = ImmutableMap.builder();
        encodings.put(columnId, COLUMN_ENCODING);
        structFields.stream()
                .map(ColumnWriter::getColumnEncodings)
                .forEach(encodings::putAll);
        return encodings.buildOrThrow();
    }

    @Override
    public void beginRowGroup()
    {
        presentStream.recordCheckpoint();

        structFields.forEach(ColumnWriter::beginRowGroup);
    }

    @Override
    public void writeBlock(Block block)
    {
        checkState(!closed);
        checkArgument(block.getPositionCount() > 0, "Block is empty");

        // record nulls
        for (int position = 0; position < block.getPositionCount(); position++) {
            boolean present = !block.isNull(position);
            presentStream.writeBoolean(present);
            if (present) {
                nonNullValueCount++;
            }
        }

        // write null-suppressed field values
        List<Block> fields = getNullSuppressedRowFieldsFromBlock(block);
        if (fields.get(0).getPositionCount() > 0) {
            for (int i = 0; i < structFields.size(); i++) {
                structFields.get(i).writeBlock(fields.get(i));
            }
        }
    }

    @Override
    public Map<OrcColumnId, ColumnStatistics> finishRowGroup()
    {
        checkState(!closed);
        ColumnStatistics statistics = new ColumnStatistics((long) nonNullValueCount, 0, null, null, null, null, null, null, null, null, null, null);
        rowGroupColumnStatistics.add(statistics);
        nonNullValueCount = 0;

        ImmutableMap.Builder<OrcColumnId, ColumnStatistics> columnStatistics = ImmutableMap.builder();
        columnStatistics.put(columnId, statistics);
        structFields.stream()
                .map(ColumnWriter::finishRowGroup)
                .forEach(columnStatistics::putAll);
        return columnStatistics.buildOrThrow();
    }

    @Override
    public void close()
    {
        closed = true;
        structFields.forEach(ColumnWriter::close);
        presentStream.close();
    }

    @Override
    public Map<OrcColumnId, ColumnStatistics> getColumnStripeStatistics()
    {
        checkState(closed);
        ImmutableMap.Builder<OrcColumnId, ColumnStatistics> columnStatistics = ImmutableMap.builder();
        columnStatistics.put(columnId, ColumnStatistics.mergeColumnStatistics(rowGroupColumnStatistics));
        structFields.stream()
                .map(ColumnWriter::getColumnStripeStatistics)
                .forEach(columnStatistics::putAll);
        return columnStatistics.buildOrThrow();
    }

    @Override
    public List<StreamDataOutput> getIndexStreams(CompressedMetadataWriter metadataWriter)
            throws IOException
    {
        checkState(closed);

        ImmutableList.Builder<RowGroupIndex> rowGroupIndexes = ImmutableList.builder();

        Optional<List<BooleanStreamCheckpoint>> presentCheckpoints = presentStream.getCheckpoints();
        for (int i = 0; i < rowGroupColumnStatistics.size(); i++) {
            int groupId = i;
            ColumnStatistics columnStatistics = rowGroupColumnStatistics.get(groupId);
            Optional<BooleanStreamCheckpoint> presentCheckpoint = presentCheckpoints.map(checkpoints -> checkpoints.get(groupId));
            List<Integer> positions = createStructColumnPositionList(compressed, presentCheckpoint);
            rowGroupIndexes.add(new RowGroupIndex(positions, columnStatistics));
        }

        Slice slice = metadataWriter.writeRowIndexes(rowGroupIndexes.build());
        Stream stream = new Stream(columnId, StreamKind.ROW_INDEX, slice.length(), false);

        ImmutableList.Builder<StreamDataOutput> indexStreams = ImmutableList.builder();
        indexStreams.add(new StreamDataOutput(slice, stream));
        for (ColumnWriter structField : structFields) {
            indexStreams.addAll(structField.getIndexStreams(metadataWriter));
            indexStreams.addAll(structField.getBloomFilters(metadataWriter));
        }
        return indexStreams.build();
    }

    @Override
    public List<StreamDataOutput> getBloomFilters(CompressedMetadataWriter metadataWriter)
    {
        return ImmutableList.of();
    }

    private static List<Integer> createStructColumnPositionList(
            boolean compressed,
            Optional<BooleanStreamCheckpoint> presentCheckpoint)
    {
        ImmutableList.Builder<Integer> positionList = ImmutableList.builder();
        presentCheckpoint.ifPresent(booleanStreamCheckpoint -> positionList.addAll(booleanStreamCheckpoint.toPositionList(compressed)));
        return positionList.build();
    }

    @Override
    public List<StreamDataOutput> getDataStreams()
    {
        checkState(closed);

        ImmutableList.Builder<StreamDataOutput> outputDataStreams = ImmutableList.builder();
        presentStream.getStreamDataOutput(columnId).ifPresent(outputDataStreams::add);
        for (ColumnWriter structField : structFields) {
            outputDataStreams.addAll(structField.getDataStreams());
        }
        return outputDataStreams.build();
    }

    @Override
    public long getBufferedBytes()
    {
        long bufferedBytes = presentStream.getBufferedBytes();
        for (ColumnWriter structField : structFields) {
            bufferedBytes += structField.getBufferedBytes();
        }
        return bufferedBytes;
    }

    @Override
    public long getRetainedBytes()
    {
        long retainedBytes = INSTANCE_SIZE + presentStream.getRetainedBytes();
        for (ColumnWriter structField : structFields) {
            retainedBytes += structField.getRetainedBytes();
        }
        for (ColumnStatistics statistics : rowGroupColumnStatistics) {
            retainedBytes += statistics.getRetainedSizeInBytes();
        }
        return retainedBytes;
    }

    @Override
    public void reset()
    {
        closed = false;
        presentStream.reset();
        structFields.forEach(ColumnWriter::reset);
        rowGroupColumnStatistics.clear();
        nonNullValueCount = 0;
    }
}
