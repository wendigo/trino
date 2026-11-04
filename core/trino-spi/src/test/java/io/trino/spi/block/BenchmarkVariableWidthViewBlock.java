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
package io.trino.spi.block;

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.slice.XxHash64;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Optional;
import java.util.Random;

import static io.trino.jmh.Benchmarks.benchmark;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Thread;

/// Compares [VariableWidthBlock] with [VariableWidthViewBlock] for the operations VARCHAR uses.
@OutputTimeUnit(MICROSECONDS)
@Fork(2)
@Warmup(iterations = 3, time = 1000, timeUnit = MILLISECONDS)
@Measurement(iterations = 5, time = 1000, timeUnit = MILLISECONDS)
@BenchmarkMode(AverageTime)
public class BenchmarkVariableWidthViewBlock
{
    private static final int POSITIONS = 8192;
    private static final int SELECTED_POSITIONS = 4096;

    @Benchmark
    public Block build(BenchmarkData data)
    {
        Slice[] values = data.values;
        if (data.layout == Layout.VIEW) {
            VariableWidthViewBlockBuilder builder = new VariableWidthViewBlockBuilder(null, values.length, data.totalBytes);
            for (Slice value : values) {
                builder.writeEntry(value);
            }
            return builder.build();
        }
        VariableWidthBlockBuilder builder = new VariableWidthBlockBuilder(null, values.length, data.totalBytes);
        for (Slice value : values) {
            builder.writeEntry(value);
        }
        return builder.build();
    }

    @Benchmark
    public Block buildWithoutSizeHint(BenchmarkData data)
    {
        Slice[] values = data.values;
        if (data.layout == Layout.VIEW) {
            VariableWidthViewBlockBuilder builder = new VariableWidthViewBlockBuilder(null, values.length, 0);
            for (Slice value : values) {
                builder.writeEntry(value);
            }
            return builder.build();
        }
        VariableWidthBlockBuilder builder = new VariableWidthBlockBuilder(null, values.length, 0);
        for (Slice value : values) {
            builder.writeEntry(value);
        }
        return builder.build();
    }

    @Benchmark
    public long hash(BenchmarkData data)
    {
        long result = 0;
        if (data.layout == Layout.VIEW) {
            VariableWidthViewBlock block = data.viewBlock;
            for (int position = 0; position < POSITIONS; position++) {
                result += block.hashValue(position);
            }
            return result;
        }
        VariableWidthBlock block = data.variableWidthBlock;
        Slice rawSlice = block.getRawSlice();
        for (int position = 0; position < POSITIONS; position++) {
            result += XxHash64.hash(rawSlice, block.getRawSliceOffset(position), block.getSliceLength(position));
        }
        return result;
    }

    @Benchmark
    public int equalValues(BenchmarkData data)
    {
        int result = 0;
        if (data.layout == Layout.VIEW) {
            VariableWidthViewBlock left = data.viewBlock;
            VariableWidthViewBlock right = data.otherViewBlock;
            for (int position = 0; position < POSITIONS; position++) {
                result += left.valueEquals(position, right, position) ? 1 : 0;
            }
            return result;
        }
        VariableWidthBlock left = data.variableWidthBlock;
        VariableWidthBlock right = data.otherVariableWidthBlock;
        Slice leftSlice = left.getRawSlice();
        Slice rightSlice = right.getRawSlice();
        for (int position = 0; position < POSITIONS; position++) {
            boolean equal = leftSlice.equals(
                    left.getRawSliceOffset(position),
                    left.getSliceLength(position),
                    rightSlice,
                    right.getRawSliceOffset(position),
                    right.getSliceLength(position));
            result += equal ? 1 : 0;
        }
        return result;
    }

    @Benchmark
    public long compareAdjacent(BenchmarkData data)
    {
        long result = 0;
        if (data.layout == Layout.VIEW) {
            VariableWidthViewBlock block = data.viewBlock;
            for (int position = 1; position < POSITIONS; position++) {
                result += Integer.signum(block.compareValue(position - 1, block, position));
            }
            return result;
        }
        VariableWidthBlock block = data.variableWidthBlock;
        Slice rawSlice = block.getRawSlice();
        for (int position = 1; position < POSITIONS; position++) {
            int comparison = rawSlice.compareTo(
                    block.getRawSliceOffset(position - 1),
                    block.getSliceLength(position - 1),
                    rawSlice,
                    block.getRawSliceOffset(position),
                    block.getSliceLength(position));
            result += Integer.signum(comparison);
        }
        return result;
    }

    @Benchmark
    public void getSlice(BenchmarkData data, Blackhole blackhole)
    {
        if (data.layout == Layout.VIEW) {
            VariableWidthViewBlock block = data.viewBlock;
            for (int position = 0; position < POSITIONS; position++) {
                blackhole.consume(block.getSlice(position));
            }
            return;
        }
        VariableWidthBlock block = data.variableWidthBlock;
        for (int position = 0; position < POSITIONS; position++) {
            blackhole.consume(block.getSlice(position));
        }
    }

    @Benchmark
    public Block copyPositions(BenchmarkData data)
    {
        return data.block().copyPositions(data.selectedPositions, 0, SELECTED_POSITIONS);
    }

    @Benchmark
    public Block appendPositions(BenchmarkData data)
    {
        BlockBuilder builder = data.layout == Layout.VIEW
                ? new VariableWidthViewBlockBuilder(null, SELECTED_POSITIONS, data.totalBytes / 2)
                : new VariableWidthBlockBuilder(null, SELECTED_POSITIONS, data.totalBytes / 2);
        builder.appendPositions(data.block(), data.selectedPositions, 0, SELECTED_POSITIONS);
        return builder.build();
    }

    @Benchmark
    public Block serializeAndDeserialize(BenchmarkData data)
    {
        BlockEncoding encoding = data.layout == Layout.VIEW ? new VariableWidthViewBlockEncoding() : new VariableWidthBlockEncoding();
        // large enough for either layout, so the output never grows
        DynamicSliceOutput output = new DynamicSliceOutput(data.totalBytes + POSITIONS * VariableWidthViewBlock.VIEW_SIZE + 1024);
        encoding.writeBlock(null, output, data.block());
        return encoding.readBlock(null, output.slice().getInput());
    }

    /// Wraps contiguous value bytes and offsets, as the ORC and Parquet readers do.
    @Benchmark
    public Block createFromReaderOffsets(BenchmarkData data)
    {
        if (data.layout == Layout.VIEW) {
            return VariableWidthViewBlock.fromOffsets(POSITIONS, data.readerSlice, data.readerOffsets, Optional.empty());
        }
        return new VariableWidthBlock(POSITIONS, data.readerSlice, data.readerOffsets, Optional.empty());
    }

    public enum Layout
    {
        VARIABLE_WIDTH,
        VIEW,
    }

    /// How the view blocks that the benchmarks read are created: written one value at a time to a builder without a
    /// size hint, or wrapped around the contiguous value bytes of a reader.
    public enum DataLayout
    {
        BUILT,
        READER,
    }

    public enum Values
    {
        /// all values are stored inline in the view
        SHORT_0_TO_12(0, 12, 0),
        MEDIUM_13_TO_32(13, 32, 0),
        /// long values that differ only in the last 4 bytes, so the view prefix does not help
        LONG_100_SHARED_PREFIX(100, 100, 96),
        MIXED_0_TO_64(0, 64, 0);

        private final int minLength;
        private final int maxLength;
        private final int sharedPrefixLength;

        Values(int minLength, int maxLength, int sharedPrefixLength)
        {
            this.minLength = minLength;
            this.maxLength = maxLength;
            this.sharedPrefixLength = sharedPrefixLength;
        }

        Slice[] generate(Random random, int count)
        {
            byte[] prefix = new byte[sharedPrefixLength];
            for (int i = 0; i < prefix.length; i++) {
                prefix[i] = (byte) ('a' + random.nextInt(26));
            }
            Slice[] values = new Slice[count];
            for (int i = 0; i < count; i++) {
                byte[] bytes = new byte[minLength + random.nextInt(maxLength - minLength + 1)];
                System.arraycopy(prefix, 0, bytes, 0, Math.min(prefix.length, bytes.length));
                for (int j = prefix.length; j < bytes.length; j++) {
                    bytes[j] = (byte) ('a' + random.nextInt(26));
                }
                values[i] = Slices.wrappedBuffer(bytes);
            }
            return values;
        }
    }

    @State(Thread)
    public static class BenchmarkData
    {
        @Param
        private Layout layout = Layout.VIEW;

        @Param
        private Values valueShape = Values.MIXED_0_TO_64;

        @Param
        private DataLayout dataLayout = DataLayout.BUILT;

        private Slice[] values;
        private int totalBytes;
        private VariableWidthBlock variableWidthBlock;
        private VariableWidthBlock otherVariableWidthBlock;
        private VariableWidthViewBlock viewBlock;
        private VariableWidthViewBlock otherViewBlock;
        private int[] selectedPositions;
        private Slice readerSlice;
        private int[] readerOffsets;

        public BenchmarkData() {}

        public BenchmarkData(Layout layout, Values valueShape)
        {
            this.layout = layout;
            this.valueShape = valueShape;
        }

        @Setup
        public void setup()
        {
            Random random = new Random(831);
            values = valueShape.generate(random, POSITIONS);
            for (Slice value : values) {
                totalBytes += value.length();
            }

            variableWidthBlock = buildVariableWidthBlock(values);
            otherVariableWidthBlock = buildVariableWidthBlock(copy(values));
            if (dataLayout == DataLayout.READER) {
                viewBlock = wrapReaderBytes(variableWidthBlock);
                otherViewBlock = wrapReaderBytes(otherVariableWidthBlock);
            }
            else {
                viewBlock = buildViewBlock(values);
                otherViewBlock = buildViewBlock(copy(values));
            }

            selectedPositions = random.ints(SELECTED_POSITIONS, 0, POSITIONS).toArray();

            readerSlice = variableWidthBlock.getRawSlice();
            readerOffsets = variableWidthBlock.getRawOffsets();
        }

        private ValueBlock block()
        {
            return layout == Layout.VIEW ? viewBlock : variableWidthBlock;
        }

        private static Slice[] copy(Slice[] values)
        {
            Slice[] copies = new Slice[values.length];
            for (int i = 0; i < values.length; i++) {
                copies[i] = Slices.wrappedBuffer(values[i].getBytes());
            }
            return copies;
        }

        private static VariableWidthBlock buildVariableWidthBlock(Slice[] values)
        {
            VariableWidthBlockBuilder builder = new VariableWidthBlockBuilder(null, values.length, 0);
            for (Slice value : values) {
                builder.writeEntry(value);
            }
            return builder.buildValueBlock();
        }

        private static VariableWidthViewBlock wrapReaderBytes(VariableWidthBlock block)
        {
            return VariableWidthViewBlock.fromOffsets(block.getPositionCount(), block.getRawSlice(), block.getRawOffsets(), Optional.empty());
        }

        private static VariableWidthViewBlock buildViewBlock(Slice[] values)
        {
            VariableWidthViewBlockBuilder builder = new VariableWidthViewBlockBuilder(null, values.length, 0);
            for (Slice value : values) {
                builder.writeEntry(value);
            }
            return builder.buildValueBlock();
        }
    }

    @Test
    public void testBenchmarks()
    {
        BenchmarkVariableWidthViewBlock benchmark = new BenchmarkVariableWidthViewBlock();
        for (Values values : Values.values()) {
            BenchmarkData variableWidth = new BenchmarkData(Layout.VARIABLE_WIDTH, values);
            variableWidth.setup();
            BenchmarkData view = new BenchmarkData(Layout.VIEW, values);
            view.setup();

            assertThat(benchmark.hash(view)).isEqualTo(benchmark.hash(variableWidth));
            assertThat(benchmark.equalValues(view)).isEqualTo(POSITIONS).isEqualTo(benchmark.equalValues(variableWidth));
            assertThat(benchmark.compareAdjacent(view)).isEqualTo(benchmark.compareAdjacent(variableWidth));
            assertSameValues(benchmark.build(view), benchmark.build(variableWidth));
            assertSameValues(benchmark.buildWithoutSizeHint(view), benchmark.buildWithoutSizeHint(variableWidth));
            assertSameValues(benchmark.copyPositions(view), benchmark.copyPositions(variableWidth));
            assertSameValues(benchmark.appendPositions(view), benchmark.appendPositions(variableWidth));
            assertSameValues(benchmark.serializeAndDeserialize(view), benchmark.serializeAndDeserialize(variableWidth));
            assertSameValues(benchmark.createFromReaderOffsets(view), benchmark.createFromReaderOffsets(variableWidth));
        }
    }

    private static void assertSameValues(Block view, Block variableWidth)
    {
        assertThat(view).isInstanceOf(VariableWidthViewBlock.class);
        assertThat(variableWidth).isInstanceOf(VariableWidthBlock.class);
        assertThat(view.getPositionCount()).isEqualTo(variableWidth.getPositionCount());
        for (int position = 0; position < view.getPositionCount(); position++) {
            assertThat(((VariableWidthViewBlock) view).getSlice(position)).isEqualTo(((VariableWidthBlock) variableWidth).getSlice(position));
        }
    }

    static void main()
            throws Exception
    {
        benchmark(BenchmarkVariableWidthViewBlock.class).run();
    }
}
