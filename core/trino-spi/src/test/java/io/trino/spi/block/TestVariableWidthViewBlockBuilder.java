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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.slice.XxHash64;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.block.VariableWidthViewBlock.INLINE_SIZE;
import static io.trino.spi.block.VariableWidthViewBlockBuilder.MAX_BUFFER_SIZE;
import static org.assertj.core.api.Assertions.assertThat;

final class TestVariableWidthViewBlockBuilder
        extends AbstractTestBlockBuilder<String>
{
    @Test
    public void testComparisonsMatchSlice()
    {
        List<Slice> values = randomValues(new Random(42), 400);
        VariableWidthViewBlock block = build(values);
        VariableWidthViewBlock otherBlock = build(values.reversed());

        for (int left = 0; left < values.size(); left++) {
            Slice leftValue = values.get(left);
            assertThat(block.getSlice(left)).isEqualTo(leftValue);
            assertThat(block.getSliceLength(left)).isEqualTo(leftValue.length());
            assertThat(block.hashValue(left)).isEqualTo(XxHash64.hash(leftValue));
            for (int right = 0; right < values.size(); right++) {
                Slice rightValue = values.get(right);
                int otherPosition = values.size() - 1 - right;
                assertThat(block.valueEquals(left, otherBlock, otherPosition)).isEqualTo(leftValue.equals(rightValue));
                assertThat(block.valueEquals(left, rightValue)).isEqualTo(leftValue.equals(rightValue));
                assertThat(Integer.signum(block.compareValue(left, otherBlock, otherPosition))).isEqualTo(Integer.signum(leftValue.compareTo(rightValue)));
                assertThat(Integer.signum(block.compareValue(left, rightValue))).isEqualTo(Integer.signum(leftValue.compareTo(rightValue)));
            }
        }
    }

    @Test
    public void testResetToThenWriteShorterValues()
    {
        VariableWidthViewBlockBuilder blockBuilder = new VariableWidthViewBlockBuilder(null, 4, 0);
        blockBuilder.writeEntry(utf8Slice("a value that is stored out of line"));
        blockBuilder.writeEntry(utf8Slice("twelve bytes"));
        blockBuilder.appendNull();
        blockBuilder.resetTo(0);
        blockBuilder.writeEntry(utf8Slice("abc"));
        blockBuilder.writeEntry(utf8Slice("ab"));
        blockBuilder.writeEntry(utf8Slice(""));
        VariableWidthViewBlock block = blockBuilder.buildValueBlock();

        VariableWidthViewBlock expected = build(List.of(utf8Slice("abc"), utf8Slice("ab"), utf8Slice("")));
        for (int position = 0; position < 3; position++) {
            assertThat(block.isNull(position)).isFalse();
            assertThat(block.valueEquals(position, expected, position)).isTrue();
            assertThat(block.compareValue(position, expected, position)).isZero();
            assertThat(block.hashValue(position)).isEqualTo(expected.hashValue(position));
        }
        assertThat(block.getSizeInBytes()).isEqualTo(expected.getSizeInBytes());
    }

    @Test
    public void testFromOffsets()
    {
        List<Slice> values = randomValues(new Random(7), 100);
        VariableWidthBlockBuilder variableWidthBlockBuilder = new VariableWidthBlockBuilder(null, values.size(), 0);
        for (int i = 0; i < values.size(); i++) {
            if (i % 7 == 3) {
                variableWidthBlockBuilder.appendNull();
            }
            else {
                variableWidthBlockBuilder.writeEntry(values.get(i));
            }
        }
        VariableWidthBlock variableWidthBlock = variableWidthBlockBuilder.buildValueBlock();

        assertSameValues(VariableWidthViewBlock.fromVariableWidthBlock(variableWidthBlock), variableWidthBlock);
        VariableWidthBlock region = variableWidthBlock.getRegion(11, 50);
        assertSameValues(VariableWidthViewBlock.fromVariableWidthBlock(region), region);
        assertSameValues(
                VariableWidthViewBlock.fromOffsets(variableWidthBlock.getPositionCount(), variableWidthBlock.getRawSlice(), variableWidthBlock.getRawOffsets(), Optional.ofNullable(variableWidthBlock.getRawValueIsValid())),
                variableWidthBlock);
    }

    @Test
    public void testRegionsAndCopies()
    {
        List<Slice> values = randomValues(new Random(3), 64);
        VariableWidthViewBlockBuilder blockBuilder = new VariableWidthViewBlockBuilder(null, 1, 0);
        for (int i = 0; i < values.size(); i++) {
            if (i % 5 == 0) {
                blockBuilder.appendNull();
            }
            else {
                blockBuilder.writeEntry(values.get(i));
            }
        }
        VariableWidthViewBlock block = blockBuilder.buildValueBlock();
        List<String> expected = blockToValues(block);

        VariableWidthViewBlock region = block.getRegion(9, 40);
        assertThat(blockToValues(region)).isEqualTo(expected.subList(9, 49));
        assertThat(blockToValues(region.copyRegion(3, 20))).isEqualTo(expected.subList(12, 32));
        assertThat(blockToValues(block.copyRegion(0, block.getPositionCount()))).isEqualTo(expected);

        int[] positions = {40, 0, 3, 3, 17, 39};
        List<String> expectedPositions = new ArrayList<>();
        for (int position : positions) {
            expectedPositions.add(expected.get(9 + position));
        }
        VariableWidthViewBlock copied = region.copyPositions(positions, 1, 4);
        assertThat(blockToValues(copied)).isEqualTo(expectedPositions.subList(1, 5));
        assertThat(copied.getRawBuffers().length).isLessThanOrEqualTo(1);

        VariableWidthViewBlock withNull = region.copyWithAppendedNull();
        List<String> expectedWithNull = new ArrayList<>(expected.subList(9, 49));
        expectedWithNull.add(null);
        assertThat(blockToValues(withNull)).isEqualTo(expectedWithNull);

        for (int position = 0; position < region.getPositionCount(); position++) {
            assertThat(blockToValues(region.getSingleValueBlock(position))).containsExactly(expected.get(9 + position));
        }
    }

    @Test
    public void testBuffersAreNotCopied()
    {
        VariableWidthViewBlockBuilder blockBuilder = new VariableWidthViewBlockBuilder(null, 1, 0);
        Slice value = Slices.wrappedBuffer(new byte[1000]);
        for (int i = 0; i < 4000; i++) {
            blockBuilder.writeEntry(value);
        }
        VariableWidthViewBlock block = blockBuilder.buildValueBlock();
        byte[][] buffers = block.getRawBuffers();
        assertThat(buffers.length).isGreaterThan(1);
        long bufferBytes = Arrays.stream(buffers).mapToLong(buffer -> buffer.length).sum();
        // each buffer is at most one value short of full, and buffers stop growing at MAX_BUFFER_SIZE
        assertThat(bufferBytes).isLessThan(4000L * value.length() + MAX_BUFFER_SIZE);
        for (int position = 0; position < block.getPositionCount(); position++) {
            assertThat(block.getSlice(position)).isEqualTo(value);
        }
    }

    @Test
    public void testShortValuesDoNotAllocateBuffers()
    {
        VariableWidthViewBlockBuilder blockBuilder = new VariableWidthViewBlockBuilder(null, 100, 100_000);
        for (int i = 0; i < 100; i++) {
            blockBuilder.writeEntry(utf8Slice("x".repeat(i % (INLINE_SIZE + 1))));
        }
        assertThat(blockBuilder.buildValueBlock().getRawBuffers()).isEmpty();
    }

    @Test
    public void testSizeInBytesMatchesVariableWidthBlock()
    {
        List<Slice> values = randomValues(new Random(11), 200);
        VariableWidthBlockBuilder variableWidthBlockBuilder = new VariableWidthBlockBuilder(null, values.size(), 0);
        VariableWidthViewBlockBuilder viewBlockBuilder = new VariableWidthViewBlockBuilder(null, values.size(), 0);
        for (int i = 0; i < values.size(); i++) {
            if (i % 9 == 0) {
                variableWidthBlockBuilder.appendNull();
                viewBlockBuilder.appendNull();
            }
            else {
                variableWidthBlockBuilder.writeEntry(values.get(i));
                viewBlockBuilder.writeEntry(values.get(i));
            }
        }
        assertThat(viewBlockBuilder.getSizeInBytes()).isEqualTo(variableWidthBlockBuilder.getSizeInBytes());
        VariableWidthViewBlock viewBlock = viewBlockBuilder.buildValueBlock();
        VariableWidthBlock variableWidthBlock = variableWidthBlockBuilder.buildValueBlock();
        assertThat(viewBlock.getSizeInBytes()).isEqualTo(variableWidthBlock.getSizeInBytes());
        assertThat(viewBlock.getRegionSizeInBytes(17, 100)).isEqualTo(variableWidthBlock.getRegionSizeInBytes(17, 100));
    }

    @Test
    public void testEveryLengthAroundInlineLimit()
    {
        for (int length = 0; length <= INLINE_SIZE + 1; length++) {
            byte[] value = new byte[length];
            for (int i = 0; i < length; i++) {
                value[i] = (byte) (0xF0 + i);
            }
            // a value that ends at the end of its array, and one inside an array with non-zero bytes around it
            byte[] padded = new byte[length + 2 * INLINE_SIZE];
            Arrays.fill(padded, (byte) 0xFF);
            System.arraycopy(value, 0, padded, 3, length);
            Slice expected = Slices.wrappedBuffer(value);
            for (Slice slice : List.of(Slices.wrappedBuffer(value.clone()), Slices.wrappedBuffer(padded, 3, length))) {
                VariableWidthViewBlockBuilder blockBuilder = new VariableWidthViewBlockBuilder(null, 1, 0);
                blockBuilder.writeEntry(slice);
                VariableWidthViewBlock built = blockBuilder.buildValueBlock();
                VariableWidthViewBlock wrapped = VariableWidthViewBlock.fromOffsets(1, slice, new int[] {0, length}, Optional.empty());
                VariableWidthViewBlock wrappedAtOffset = VariableWidthViewBlock.fromOffsets(1, Slices.wrappedBuffer(padded), new int[] {3, 3 + length}, Optional.empty());
                for (VariableWidthViewBlock block : List.of(built, wrapped, wrappedAtOffset)) {
                    assertThat(block.getSlice(0)).isEqualTo(expected);
                    assertThat(block.hashValue(0)).isEqualTo(XxHash64.hash(expected));
                    assertThat(block.valueEquals(0, built, 0)).isTrue();
                    assertThat(block.compareValue(0, built, 0)).isZero();
                    if (length <= INLINE_SIZE) {
                        byte[] views = block.getRawViews();
                        for (int i = Integer.BYTES + length; i < VariableWidthViewBlock.VIEW_SIZE; i++) {
                            assertThat(views[i]).isZero();
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testOrderOfValuesThatDifferInTrailingBytes()
    {
        List<Slice> values = new ArrayList<>();
        for (String value : List.of("", "\0", "ab", "ab\0", "ab\0\0", "abcd", "abcd\0", "abcdefgh", "abcdefgh\0", "abcdefghijk", "abcdefghijk\0", "abcdefghijkl", "abcdefghijkl\0", "abcdefghijkla")) {
            values.add(utf8Slice(value));
        }
        values.add(Slices.wrappedBuffer(new byte[] {'a', 'b', 'c', 'd', (byte) 0xFF}));
        values.add(Slices.wrappedBuffer(new byte[] {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', (byte) 0xFF}));
        values.add(Slices.wrappedBuffer(new byte[] {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', (byte) 0xFF, 0}));
        VariableWidthViewBlock block = build(values);
        VariableWidthViewBlock otherBlock = build(values.reversed());
        for (int left = 0; left < values.size(); left++) {
            for (int right = 0; right < values.size(); right++) {
                int expected = Integer.signum(values.get(left).compareTo(values.get(right)));
                assertThat(Integer.signum(block.compareValue(left, otherBlock, values.size() - 1 - right))).isEqualTo(expected);
                assertThat(block.valueEquals(left, otherBlock, values.size() - 1 - right)).isEqualTo(expected == 0);
            }
        }
    }

    @Test
    public void testAppendRepeatedSharesValue()
    {
        Slice longValue = utf8Slice("a value that is stored out of line");
        VariableWidthViewBlockBuilder sourceBuilder = new VariableWidthViewBlockBuilder(null, 3, 0);
        sourceBuilder.writeEntry(utf8Slice("short"));
        sourceBuilder.writeEntry(longValue);
        sourceBuilder.appendNull();
        VariableWidthViewBlock source = sourceBuilder.buildValueBlock();

        VariableWidthViewBlockBuilder blockBuilder = new VariableWidthViewBlockBuilder(null, 1, 0);
        blockBuilder.appendRepeated(source, 1, 5);
        blockBuilder.appendRepeated(source, 0, 3);
        blockBuilder.appendRepeated(source, 2, 2);
        blockBuilder.appendRepeated(source, 1, 0);
        blockBuilder.writeEntry(longValue);
        VariableWidthViewBlock block = blockBuilder.buildValueBlock();

        List<String> expected = new ArrayList<>();
        expected.addAll(Collections.nCopies(5, longValue.toStringUtf8()));
        expected.addAll(Collections.nCopies(3, "short"));
        expected.addAll(Collections.nCopies(2, null));
        expected.add(longValue.toStringUtf8());
        assertThat(blockToValues(block)).isEqualTo(expected);
        assertThat(block.getSizeInBytes()).isEqualTo(6L * longValue.length() + 3 * 5 + 11L * (Integer.BYTES + Byte.BYTES));
        // the repeated value is stored once, and the value written after it follows it in the buffer
        byte[] views = block.getRawViews();
        for (int position = 1; position < 5; position++) {
            assertThat(VariableWidthViewBlock.bufferOffset(views, position * VariableWidthViewBlock.VIEW_SIZE)).isEqualTo(VariableWidthViewBlock.bufferOffset(views, 0));
        }
        assertThat(VariableWidthViewBlock.bufferOffset(views, 10 * VariableWidthViewBlock.VIEW_SIZE)).isEqualTo(VariableWidthViewBlock.bufferOffset(views, 0) + longValue.length());
        assertThat(blockToValues(block.copyRegion(0, block.getPositionCount()))).isEqualTo(expected);
        assertThat(blockToValues(block.copyRegion(2, 7))).isEqualTo(expected.subList(2, 9));
        assertThat(blockToValues(block.copyPositions(new int[] {10, 0, 9, 4, 5}, 0, 5)))
                .containsExactly(expected.get(10), expected.get(0), expected.get(9), expected.get(4), expected.get(5));
    }

    @Test
    public void testCopiesOfValuesInSeveralBuffers()
    {
        List<Slice> values = randomValues(new Random(5), 3000);
        VariableWidthViewBlockBuilder blockBuilder = new VariableWidthViewBlockBuilder(null, 1, 0);
        for (int i = 0; i < values.size(); i++) {
            if (i % 13 == 0) {
                blockBuilder.appendNull();
            }
            else {
                blockBuilder.writeEntry(values.get(i));
            }
        }
        VariableWidthViewBlock block = blockBuilder.buildValueBlock();
        assertThat(block.getRawBuffers().length).isGreaterThan(1);
        List<String> expected = blockToValues(block);

        VariableWidthViewBlock region = block.getRegion(101, 2500);
        VariableWidthViewBlock copiedRegion = region.copyRegion(7, 2400);
        assertThat(blockToValues(copiedRegion)).isEqualTo(expected.subList(108, 2508));
        assertCompact(copiedRegion);
        assertThat(copiedRegion.copyRegion(0, copiedRegion.getPositionCount())).isSameAs(copiedRegion);

        int[] positions = new int[region.getPositionCount()];
        for (int i = 0; i < positions.length; i++) {
            positions[i] = (i * 7) % positions.length;
        }
        List<String> expectedPositions = new ArrayList<>();
        for (int position : positions) {
            expectedPositions.add(expected.get(101 + position));
        }
        assertThat(blockToValues(region.copyPositions(positions, 0, positions.length))).isEqualTo(expectedPositions);

        VariableWidthViewBlockBuilder appended = new VariableWidthViewBlockBuilder(null, 1, 0);
        appended.appendRange(region, 5, 1000);
        appended.appendPositions(region, positions, 0, positions.length);
        List<String> expectedAppended = new ArrayList<>(expected.subList(106, 1106));
        expectedAppended.addAll(expectedPositions);
        assertThat(blockToValues(appended.buildValueBlock())).isEqualTo(expectedAppended);
    }

    @Test
    public void testBuildAllNulls()
    {
        VariableWidthViewBlockBuilder blockBuilder = new VariableWidthViewBlockBuilder(null, 3, 0);
        blockBuilder.appendNull();
        blockBuilder.appendNull();
        Block block = blockBuilder.build();
        assertThat(block).isInstanceOf(RunLengthEncodedBlock.class);
        assertThat(((RunLengthEncodedBlock) block).getValue()).isInstanceOf(VariableWidthViewBlock.class);
        assertThat(block.isNull(1)).isTrue();
    }

    /// Asserts that the buffers of `block` hold at most 1/8 more bytes than its out-of-line values, plus the bytes of
    /// its inline values.
    static void assertCompact(VariableWidthViewBlock block)
    {
        long bufferBytes = 0;
        for (byte[] buffer : block.getRawBuffers()) {
            bufferBytes += buffer.length;
        }
        assertThat(bufferBytes).isLessThanOrEqualTo(block.getValueBytes() + block.getOutOfLineBytes() / 8);
        assertThat(block.getRawViews()).hasSize(block.getPositionCount() * VariableWidthViewBlock.VIEW_SIZE);
    }

    private static void assertSameValues(VariableWidthViewBlock actual, VariableWidthBlock expected)
    {
        assertThat(actual.getPositionCount()).isEqualTo(expected.getPositionCount());
        for (int position = 0; position < expected.getPositionCount(); position++) {
            assertThat(actual.isNull(position)).isEqualTo(expected.isNull(position));
            if (!expected.isNull(position)) {
                assertThat(actual.getSlice(position)).isEqualTo(expected.getSlice(position));
            }
        }
        assertThat(actual.getSizeInBytes()).isEqualTo(expected.getSizeInBytes());
    }

    private static VariableWidthViewBlock build(List<Slice> values)
    {
        VariableWidthViewBlockBuilder blockBuilder = new VariableWidthViewBlockBuilder(null, values.size(), 0);
        values.forEach(blockBuilder::writeEntry);
        return blockBuilder.buildValueBlock();
    }

    /// Values of 0 to 40 bytes with shared prefixes, bytes above 0x7F, and lengths around the inline limit.
    private static List<Slice> randomValues(Random random, int count)
    {
        byte[] prefix = new byte[40];
        random.nextBytes(prefix);
        List<Slice> values = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            int length = switch (i % 4) {
                case 0 -> INLINE_SIZE - 1 + random.nextInt(3);
                case 1 -> random.nextInt(5);
                default -> random.nextInt(41);
            };
            byte[] bytes = new byte[length];
            random.nextBytes(bytes);
            int shared = random.nextInt(length + 1);
            System.arraycopy(prefix, 0, bytes, 0, shared);
            if (i % 11 == 0 && length > 0) {
                bytes[length - 1] = 0;
            }
            values.add(Slices.wrappedBuffer(bytes));
        }
        return values;
    }

    @Override
    protected BlockBuilder createBlockBuilder()
    {
        return new VariableWidthViewBlockBuilder(null, 1, 100);
    }

    @Override
    protected List<String> getTestValues()
    {
        return List.of("a", "bb", "twelve bytes", "thirteen byte", "a value that is stored out of line");
    }

    @Override
    protected String getUnusedTestValue()
    {
        return "an unused value that is stored out of line";
    }

    @Override
    protected ValueBlock blockFromValues(Iterable<String> values)
    {
        VariableWidthViewBlockBuilder blockBuilder = new VariableWidthViewBlockBuilder(null, 1, 100);
        for (String value : values) {
            if (value == null) {
                blockBuilder.appendNull();
            }
            else {
                blockBuilder.writeEntry(utf8Slice(value));
            }
        }
        return blockBuilder.buildValueBlock();
    }

    @Override
    protected List<String> blockToValues(ValueBlock valueBlock)
    {
        VariableWidthViewBlock block = (VariableWidthViewBlock) valueBlock;
        List<String> actualValues = new ArrayList<>(block.getPositionCount());
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (block.isNull(i)) {
                actualValues.add(null);
            }
            else {
                actualValues.add(block.getSlice(i).toStringUtf8());
            }
        }
        return actualValues;
    }
}
