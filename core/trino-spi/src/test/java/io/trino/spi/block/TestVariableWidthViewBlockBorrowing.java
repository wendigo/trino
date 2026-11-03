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
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.spi.block.BorrowedBufferTracker.MAX_RETAINED_TO_REFERENCED_RATIO;
import static io.trino.spi.block.BorrowedBufferTracker.MIN_UNREFERENCED_BYTES;
import static io.trino.spi.block.TestVariableWidthViewBlockBuilder.assertCompact;
import static io.trino.spi.block.VariableWidthViewBlock.INLINE_SIZE;
import static io.trino.spi.block.VariableWidthViewBlock.VIEW_SIZE;
import static io.trino.spi.block.VariableWidthViewBlockBuilder.MIN_BORROWED_BUFFER_SIZE;
import static org.assertj.core.api.Assertions.assertThat;

final class TestVariableWidthViewBlockBorrowing
{
    private final BlockEncodingSerde blockEncodingSerde = new TestingBlockEncodingSerde();

    @Test
    public void testAppendsMatchValues()
    {
        Random random = new Random(3);
        List<Slice> values = randomValues(random, 20_000);
        for (VariableWidthViewBlock source : sources(values)) {
            List<Slice> expected = values(source);
            for (boolean tracked : new boolean[] {false, true}) {
                BorrowedBufferTracker tracker = tracked ? new BorrowedBufferTracker() : null;
                VariableWidthViewBlockBuilder blockBuilder = new VariableWidthViewBlockBuilder(null, 1, 0, tracker);
                List<Slice> expectedAppended = new ArrayList<>();

                // whole block, which is borrowed with and without a tracker
                blockBuilder.appendRange(source, 0, source.getPositionCount());
                expectedAppended.addAll(expected);

                // a region in the middle, of a region with an array offset
                VariableWidthViewBlock region = source.getRegion(7, source.getPositionCount() - 20);
                blockBuilder.appendRange(region, 3, 500);
                expectedAppended.addAll(expected.subList(10, 510));

                // a few positions, which are copied without a tracker
                int[] positions = {5, 0, 5, 1, 2, 3, 13, 12, 11, 4, 99, 1000};
                blockBuilder.appendPositions(region, positions, 1, positions.length - 1);
                for (int i = 1; i < positions.length; i++) {
                    expectedAppended.add(expected.get(7 + positions[i]));
                }

                // most positions, in a different order
                int[] manyPositions = new int[region.getPositionCount()];
                for (int i = 0; i < manyPositions.length; i++) {
                    manyPositions[i] = (int) ((i * 7L) % manyPositions.length);
                }
                blockBuilder.appendPositions(region, manyPositions, 0, manyPositions.length);
                for (int position : manyPositions) {
                    expectedAppended.add(expected.get(7 + position));
                }

                for (int position = 0; position < 40; position++) {
                    blockBuilder.append(region, position);
                    expectedAppended.add(expected.get(7 + position));
                    blockBuilder.appendRepeated(region, position, 3);
                    expectedAppended.addAll(Collections.nCopies(3, expected.get(7 + position)));
                }

                VariableWidthViewBlock block = blockBuilder.buildValueBlock();
                assertBlock(block, expectedAppended);
                assertBlock(block.copyRegion(0, block.getPositionCount()), expectedAppended);
                assertBlock(block.copyRegion(1000, 777), expectedAppended.subList(1000, 1777));
                assertBlock(block.getRegion(13, 5000).copyPositions(positions, 0, positions.length), select(expectedAppended.subList(13, 5013), positions));
                assertBlock(roundTrip(block), expectedAppended);
                assertBlock(roundTrip(block.getRegion(source.getPositionCount() + 1, 600)), expectedAppended.subList(source.getPositionCount() + 1, source.getPositionCount() + 601));
                assertCompact(block.copyRegion(1000, 777));
                if (tracker != null) {
                    assertThat(tracker.getRetainedSizeInBytes()).isZero();
                    assertThat(tracker.getReferencedBytes()).isZero();
                }
            }
        }
    }

    @Test
    public void testEveryLengthAroundInlineLimit()
    {
        // values of 0 to 2 * INLINE_SIZE bytes, with nulls, followed by a filler to make the buffer large enough to borrow
        List<Slice> values = new ArrayList<>();
        Random random = new Random(11);
        for (int length = 0; length <= 2 * INLINE_SIZE + 1; length++) {
            byte[] bytes = new byte[length];
            random.nextBytes(bytes);
            values.add(Slices.wrappedBuffer(bytes));
            values.add(null);
        }
        values.add(Slices.allocate(MIN_BORROWED_BUFFER_SIZE));
        for (VariableWidthViewBlock source : sources(values)) {
            List<Slice> expected = values(source);
            VariableWidthViewBlockBuilder blockBuilder = new VariableWidthViewBlockBuilder(null, 1, 0, new BorrowedBufferTracker());
            for (int start = 0; start < 5; start++) {
                blockBuilder.appendRange(source, start, source.getPositionCount() - start);
            }
            List<Slice> expectedAppended = new ArrayList<>();
            for (int start = 0; start < 5; start++) {
                expectedAppended.addAll(expected.subList(start, expected.size()));
            }
            VariableWidthViewBlock block = blockBuilder.buildValueBlock();
            assertBlock(block, expectedAppended);
            assertBlock(roundTrip(block), expectedAppended);
            assertBlock(block.copyRegion(3, block.getPositionCount() - 3), expectedAppended.subList(3, expectedAppended.size()));
        }
    }

    @Test
    public void testBuffersAreBorrowed()
    {
        List<Slice> values = randomValues(new Random(1), 10_000);
        VariableWidthViewBlock source = fromOffsets(values);
        byte[] sourceBuffer = source.getRawBuffers()[0];
        assertThat(sourceBuffer.length).isGreaterThanOrEqualTo(MIN_BORROWED_BUFFER_SIZE);
        int[] fewPositions = outOfLinePositions(values, 9000, 3);

        VariableWidthViewBlockBuilder blockBuilder = new VariableWidthViewBlockBuilder(null, 1, 0);
        blockBuilder.appendRange(source, 0, 6000);
        blockBuilder.appendRange(source, 4000, 6000);
        long retainedSize = blockBuilder.getRetainedSizeInBytes();
        // a buffer is borrowed once
        blockBuilder.appendPositions(source, fewPositions, 0, 3);
        VariableWidthViewBlock block = blockBuilder.buildValueBlock();
        assertThat(block.getRawBuffers()).hasNumberOfRows(2);
        assertThat(block.getRawBuffers()[0]).isSameAs(sourceBuffer);
        assertThat(blockBuilder.getRetainedSizeInBytes()).isGreaterThanOrEqualTo(sizeOf(sourceBuffer));
        // the second buffer holds the copies of the values of the last append, which references little of the buffer
        assertThat(sizeOf(block.getRawBuffers()[1])).isLessThan(MIN_BORROWED_BUFFER_SIZE);
        assertThat(blockBuilder.getRetainedSizeInBytes() - retainedSize).isLessThan(2 * MIN_BORROWED_BUFFER_SIZE);

        // a few values of a block are copied
        VariableWidthViewBlockBuilder copyingBuilder = new VariableWidthViewBlockBuilder(null, 1, 0);
        copyingBuilder.appendPositions(source, fewPositions, 0, 3);
        copyingBuilder.appendRange(source, 100, 10);
        assertThat(copyingBuilder.getRetainedSizeInBytes()).isLessThan(sizeOf(sourceBuffer));
        for (byte[] buffer : copyingBuilder.buildValueBlock().getRawBuffers()) {
            assertThat(buffer).isNotSameAs(sourceBuffer);
        }

        // with a tracker, a few values of a block are borrowed
        BorrowedBufferTracker tracker = new BorrowedBufferTracker();
        VariableWidthViewBlockBuilder trackedBuilder = new VariableWidthViewBlockBuilder(null, 1, 0, tracker);
        trackedBuilder.appendPositions(source, fewPositions, 0, 3);
        assertThat(tracker.getRetainedSizeInBytes()).isEqualTo(sizeOf(sourceBuffer));
        assertThat(trackedBuilder.getRetainedSizeInBytes()).isLessThan(sizeOf(sourceBuffer));
        VariableWidthViewBlock trackedBlock = trackedBuilder.buildValueBlock();
        assertThat(Arrays.asList(trackedBlock.getRawBuffers())).containsExactly(sourceBuffer);
        // the block accounts for the buffer once it is built
        assertThat(tracker.getRetainedSizeInBytes()).isZero();
        assertThat(trackedBlock.getRetainedSizeInBytes()).isGreaterThan(sizeOf(sourceBuffer));
    }

    @Test
    public void testSmallBuffersAreCopied()
    {
        // a builder writes values to buffers of 1 KB, 2 KB, 4 KB, and so on
        List<Slice> values = randomValues(new Random(2), 30_000);
        VariableWidthViewBlock source = build(values);
        assertThat(source.getRawBuffers().length).isGreaterThan(4);

        VariableWidthViewBlockBuilder blockBuilder = new VariableWidthViewBlockBuilder(null, 1, 0, new BorrowedBufferTracker());
        blockBuilder.appendRange(source, 0, source.getPositionCount());
        VariableWidthViewBlock block = blockBuilder.buildValueBlock();
        assertBlock(block, values);
        Set<byte[]> sourceBuffers = Collections.newSetFromMap(new IdentityHashMap<>());
        sourceBuffers.addAll(Arrays.asList(source.getRawBuffers()));
        Set<byte[]> blockBuffers = Collections.newSetFromMap(new IdentityHashMap<>());
        blockBuffers.addAll(Arrays.asList(block.getRawBuffers()));
        for (byte[] buffer : source.getRawBuffers()) {
            assertThat(blockBuffers.contains(buffer)).isEqualTo(buffer.length >= MIN_BORROWED_BUFFER_SIZE);
        }
    }

    @Test
    public void testRetainedSizeOfBuildersSharingBuffers()
    {
        int partitionCount = 256;
        BorrowedBufferTracker tracker = new BorrowedBufferTracker();
        List<VariableWidthViewBlockBuilder> builders = new ArrayList<>();
        for (int partition = 0; partition < partitionCount; partition++) {
            builders.add(new VariableWidthViewBlockBuilder(null, 1, 0, tracker));
        }

        long sourceBufferBytes = 0;
        List<List<Slice>> expected = new ArrayList<>();
        for (int partition = 0; partition < partitionCount; partition++) {
            expected.add(new ArrayList<>());
        }
        Random random = new Random(9);
        for (int page = 0; page < 4; page++) {
            List<Slice> values = randomValues(random, 20_000);
            VariableWidthViewBlock source = fromOffsets(values);
            sourceBufferBytes += sizeOf(source.getRawBuffers()[0]);
            for (int partition = 0; partition < partitionCount; partition++) {
                int[] positions = new int[values.size() / partitionCount + 1];
                int count = 0;
                for (int position = partition; position < values.size(); position += partitionCount) {
                    positions[count++] = position;
                    expected.get(partition).add(values.get(position));
                }
                builders.get(partition).appendPositions(source, positions, 0, count);
            }
        }

        // the builders borrow every buffer, and the tracker accounts for each buffer once
        assertThat(tracker.getCompactionCount()).isZero();
        assertThat(tracker.getRetainedSizeInBytes()).isEqualTo(sourceBufferBytes);
        long buildersRetainedSize = 0;
        for (VariableWidthViewBlockBuilder builder : builders) {
            buildersRetainedSize += builder.getRetainedSizeInBytes();
        }
        // the builders hold the views, and do not account for the buffers
        for (VariableWidthViewBlockBuilder builder : builders) {
            assertThat(builder.getRetainedSizeInBytes()).isLessThan(sourceBufferBytes / 4 / 10);
        }

        // the same values copied to every builder would retain about the bytes of the source buffers in total, and a
        // builder that accounted for the buffers it borrows would account for every buffer
        long totalRetainedSize = buildersRetainedSize + tracker.getRetainedSizeInBytes();
        assertThat(totalRetainedSize).isLessThan(sourceBufferBytes + partitionCount * 20_000L);

        // built blocks account for the buffers, and the tracker no longer does
        for (int partition = 0; partition < partitionCount; partition++) {
            VariableWidthViewBlock block = builders.get(partition).buildValueBlock();
            assertBlock(block, expected.get(partition));
            if (partition < 3) {
                VariableWidthViewBlock deserialized = roundTrip(block);
                assertBlock(deserialized, expected.get(partition));
                // the blocks borrow few values of each buffer, so they are compacted while they are written
                assertThat(deserialized.getRawBuffers()).hasNumberOfRows(1);
            }
        }
        assertThat(tracker.getRetainedSizeInBytes()).isZero();
        assertThat(tracker.getReferencedBytes()).isZero();
    }

    @Test
    public void testBuildersWithoutTrackerAccountForBorrowedBuffers()
    {
        VariableWidthViewBlock source = fromOffsets(randomValues(new Random(4), 20_000));
        long sourceBufferSize = sizeOf(source.getRawBuffers()[0]);
        List<VariableWidthViewBlockBuilder> builders = new ArrayList<>();
        for (int i = 0; i < 16; i++) {
            VariableWidthViewBlockBuilder builder = new VariableWidthViewBlockBuilder(null, 40_000, 0);
            builder.appendRange(source, 0, source.getPositionCount());
            long retainedSize = builder.getRetainedSizeInBytes();
            assertThat(retainedSize).isGreaterThan(sourceBufferSize);
            builder.appendRange(source, 100, 15_000);
            builders.add(builder);
            // the buffer is accounted for once in every builder
            assertThat(builder.getRetainedSizeInBytes()).isEqualTo(retainedSize);
        }
        for (VariableWidthViewBlockBuilder builder : builders) {
            assertThat(Arrays.asList(builder.buildValueBlock().getRawBuffers())).containsExactly(source.getRawBuffers()[0]);
        }
    }

    @Test
    public void testSkewedBuilderCopiesBorrowedValues()
    {
        BorrowedBufferTracker tracker = new BorrowedBufferTracker();
        VariableWidthViewBlockBuilder skewed = new VariableWidthViewBlockBuilder(null, 1, 0, tracker);
        VariableWidthViewBlockBuilder even = new VariableWidthViewBlockBuilder(null, 1, 0, tracker);

        // a block that the builder built before compaction keeps its values
        VariableWidthViewBlock first = fromOffsets(randomValues(new Random(0), 20_000));
        skewed.appendRange(first, 0, 10);
        VariableWidthViewBlock builtBefore = skewed.buildValueBlock();
        List<Slice> expectedBuiltBefore = values(first).subList(0, 10);

        // the builder is used after it built a block
        List<Slice> expectedSkewed = new ArrayList<>(expectedBuiltBefore);
        List<Slice> expectedEven = new ArrayList<>();
        Random random = new Random(6);
        long maxRetainedSize = 0;
        for (int page = 0; page < 200; page++) {
            List<Slice> values = randomValues(random, 5_000);
            VariableWidthViewBlock source = fromOffsets(values);
            int skewedPosition = firstOutOfLinePosition(values);
            skewed.appendPositions(source, new int[] {skewedPosition}, 0, 1);
            expectedSkewed.add(values.get(skewedPosition));
            int[] positions = new int[10];
            for (int i = 0; i < positions.length; i++) {
                positions[i] = i * 3;
                expectedEven.add(values.get(i * 3));
            }
            even.appendPositions(source, positions, 0, positions.length);
            maxRetainedSize = Math.max(maxRetainedSize, tracker.getRetainedSizeInBytes());
            assertThat(tracker.getRetainedSizeInBytes()).isLessThanOrEqualTo(MAX_RETAINED_TO_REFERENCED_RATIO * tracker.getReferencedBytes() + MIN_UNREFERENCED_BYTES);
        }
        assertThat(tracker.getCompactionCount()).isGreaterThan(0);
        assertThat(maxRetainedSize).isGreaterThan(MIN_UNREFERENCED_BYTES / 2);

        assertBlock(builtBefore, expectedBuiltBefore);
        assertBlock(skewed.buildValueBlock(), expectedSkewed);
        assertBlock(even.buildValueBlock(), expectedEven);
        assertThat(tracker.getRetainedSizeInBytes()).isZero();
        assertThat(tracker.getReferencedBytes()).isZero();
    }

    @Test
    public void testCopyRegionSharesDenseBuffers()
    {
        List<Slice> values = randomValues(new Random(8), 50_000);
        VariableWidthViewBlock block = build(values);
        byte[][] buffers = block.getRawBuffers();
        VariableWidthViewBlock copy = block.copyRegion(0, block.getPositionCount());
        assertBlock(copy, values);
        assertCompact(copy);
        // the full buffers are shared, and the values of the last buffer, which is partly empty, are copied
        Set<byte[]> copyBuffers = Collections.newSetFromMap(new IdentityHashMap<>());
        copyBuffers.addAll(Arrays.asList(copy.getRawBuffers()));
        for (int i = 0; i < buffers.length - 1; i++) {
            assertThat(copyBuffers).contains(buffers[i]);
        }
        assertThat(copy.copyRegion(0, copy.getPositionCount())).isSameAs(copy);
        assertThat(Arrays.asList(copy.copyPositions(new int[] {3, 2, 1}, 0, 3).getRawBuffers())).noneMatch(copyBuffers::contains);

        // a small region is copied to a single buffer
        VariableWidthViewBlock region = block.copyRegion(30_000, 100);
        assertBlock(region, values.subList(30_000, 30_100));
        assertThat(region.getRawBuffers().length).isLessThanOrEqualTo(1);
        assertCompact(region);

        // a block that wraps the bytes of a reader is compact, since only its inline values are not referenced
        VariableWidthViewBlock wrapped = fromOffsets(values);
        assertThat(wrapped.copyRegion(0, wrapped.getPositionCount())).isSameAs(wrapped);
    }

    @Test
    public void testResetToUpdatesBorrowedBytes()
    {
        VariableWidthViewBlock source = fromOffsets(randomValues(new Random(12), 20_000));
        BorrowedBufferTracker tracker = new BorrowedBufferTracker();
        VariableWidthViewBlockBuilder blockBuilder = new VariableWidthViewBlockBuilder(null, 1, 0, tracker);
        blockBuilder.appendRange(source, 0, 100);
        long referencedBytes = tracker.getReferencedBytes();
        blockBuilder.appendRange(source, 100, 100);
        assertThat(tracker.getReferencedBytes()).isGreaterThan(referencedBytes);
        blockBuilder.resetTo(100);
        assertThat(tracker.getReferencedBytes()).isEqualTo(referencedBytes);
        assertBlock(blockBuilder.buildValueBlock(), values(source).subList(0, 100));
        assertThat(tracker.getReferencedBytes()).isZero();
    }

    private VariableWidthViewBlock roundTrip(VariableWidthViewBlock block)
    {
        DynamicSliceOutput output = new DynamicSliceOutput(1024);
        blockEncodingSerde.writeBlock(output, block);
        VariableWidthViewBlock actual = (VariableWidthViewBlock) blockEncodingSerde.readBlock(output.slice().getInput());
        assertThat(actual.getSizeInBytes()).isEqualTo(block.getSizeInBytes());
        return actual;
    }

    /// Asserts the values of `block`, that it is padded with zeros, and that its cached sizes match its views.
    static void assertBlock(VariableWidthViewBlock block, List<Slice> expected)
    {
        assertThat(values(block)).isEqualTo(expected);
        byte[] views = block.getRawViews();
        long valueBytes = 0;
        long outOfLineBytes = 0;
        for (int position = 0; position < block.getPositionCount(); position++) {
            int view = (block.getRawArrayBase() + position) * VIEW_SIZE;
            int length = VariableWidthViewBlock.length(views, view);
            valueBytes += length;
            if (block.isNull(position)) {
                assertThat(Arrays.copyOfRange(views, view, view + VIEW_SIZE)).containsOnly(0);
            }
            else if (length <= INLINE_SIZE) {
                for (int i = Integer.BYTES + length; i < VIEW_SIZE; i++) {
                    assertThat(views[view + i]).isZero();
                }
            }
            else {
                outOfLineBytes += length;
                int bufferIndex = VariableWidthViewBlock.bufferIndex(views, view);
                assertThat(VariableWidthViewBlock.bufferOffset(views, view) + length).isLessThanOrEqualTo(block.getRawBufferLengths()[bufferIndex]);
                Slice value = expected.get(position);
                assertThat(Slices.wrappedBuffer(views, view + Integer.BYTES, VariableWidthViewBlock.PREFIX_SIZE)).isEqualTo(value.slice(0, VariableWidthViewBlock.PREFIX_SIZE));
            }
        }
        assertThat(block.getValueBytes()).isEqualTo(valueBytes);
        assertThat(block.getOutOfLineBytes()).isEqualTo(outOfLineBytes);
    }

    private static List<Slice> values(VariableWidthViewBlock block)
    {
        List<Slice> values = new ArrayList<>(block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            values.add(block.isNull(position) ? null : block.getSlice(position));
        }
        return values;
    }

    private static List<Slice> select(List<Slice> values, int[] positions)
    {
        List<Slice> selected = new ArrayList<>();
        for (int position : positions) {
            selected.add(values.get(position));
        }
        return selected;
    }

    private static int[] outOfLinePositions(List<Slice> values, int start, int count)
    {
        int[] positions = new int[count];
        int found = 0;
        for (int position = start; found < count; position++) {
            if (values.get(position) != null && values.get(position).length() > INLINE_SIZE) {
                positions[found++] = position;
            }
        }
        return positions;
    }

    private static int firstOutOfLinePosition(List<Slice> values)
    {
        for (int position = 0; position < values.size(); position++) {
            if (values.get(position) != null && values.get(position).length() > INLINE_SIZE) {
                return position;
            }
        }
        throw new IllegalArgumentException("no out-of-line value");
    }

    /// Blocks with the values: built by a builder, wrapping contiguous bytes at an offset, a region with an array
    /// offset, and a deserialized block.
    private List<VariableWidthViewBlock> sources(List<Slice> values)
    {
        VariableWidthViewBlock built = build(values);
        List<Slice> padded = new ArrayList<>();
        padded.add(Slices.utf8Slice("a padding value"));
        padded.addAll(values);
        return List.of(
                built,
                fromOffsets(values),
                build(padded).getRegion(1, values.size()),
                roundTrip(built));
    }

    private static VariableWidthViewBlock build(List<Slice> values)
    {
        VariableWidthViewBlockBuilder blockBuilder = new VariableWidthViewBlockBuilder(null, values.size(), 0);
        for (Slice value : values) {
            if (value == null) {
                blockBuilder.appendNull();
            }
            else {
                blockBuilder.writeEntry(value);
            }
        }
        return blockBuilder.buildValueBlock();
    }

    /// Wraps the values written contiguously after 5 bytes of padding, like the values of a reader.
    private static VariableWidthViewBlock fromOffsets(List<Slice> values)
    {
        DynamicSliceOutput output = new DynamicSliceOutput(1024);
        output.writeBytes(new byte[5]);
        int[] offsets = new int[values.size() + 1];
        long[] valueIsValid = new long[Bitmap.wordsForBits(values.size())];
        boolean hasNull = false;
        for (int i = 0; i < values.size(); i++) {
            Slice value = values.get(i);
            if (value == null) {
                hasNull = true;
            }
            else {
                Bitmap.set(valueIsValid, 0, i);
                output.writeBytes(value);
            }
            offsets[i + 1] = output.size() - 5;
        }
        // the slice of an output can be shorter than the array that holds it
        Slice slice = Slices.wrappedBuffer(output.slice().getBytes(), 5, output.size() - 5);
        return VariableWidthViewBlock.fromOffsets(values.size(), slice, offsets, hasNull ? Optional.of(valueIsValid) : Optional.empty());
    }

    /// Values of 0 to 100 bytes, a few of them null.
    private static List<Slice> randomValues(Random random, int count)
    {
        List<Slice> values = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            if (random.nextInt(17) == 0) {
                values.add(null);
                continue;
            }
            int length = random.nextBoolean() ? random.nextInt(2 * INLINE_SIZE) : random.nextInt(101);
            byte[] bytes = new byte[length];
            random.nextBytes(bytes);
            values.add(Slices.wrappedBuffer(bytes));
        }
        return values;
    }
}
