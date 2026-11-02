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
        assertThat(copied.getRawBuffers()).hasSizeLessThanOrEqualTo(1);

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
        Slice[] buffers = block.getRawBuffers();
        assertThat(buffers.length).isGreaterThan(1);
        long bufferBytes = Arrays.stream(buffers).mapToLong(buffer -> buffer.byteArray().length).sum();
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
