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
package io.trino.block;

import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.VariableWidthViewBlock;
import io.trino.spi.block.VariableWidthViewBlockBuilder;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.String.format;
import static java.util.Arrays.copyOfRange;
import static org.assertj.core.api.Assertions.assertThat;

public class TestVariableWidthViewBlock
        extends AbstractTestBlock
{
    @Test
    public void test()
    {
        Slice[] expectedValues = createExpectedValues(100);
        assertVariableWithValues(expectedValues);
        assertVariableWithValues(alternatingNullValues(expectedValues));
    }

    @Test
    public void testCopyRegion()
    {
        Slice[] expectedValues = createExpectedValues(100);
        Block block = createBlockBuilderWithValues(expectedValues).build();
        Block actual = block.copyRegion(10, 10);
        Block expected = createBlockBuilderWithValues(copyOfRange(expectedValues, 10, 20)).build();
        assertThat(actual.getPositionCount()).isEqualTo(expected.getPositionCount());
        assertThat(actual.getSizeInBytes()).isEqualTo(expected.getSizeInBytes());
    }

    @Test
    public void testCopyPositions()
    {
        Slice[] expectedValues = alternatingNullValues(createExpectedValues(100));
        BlockBuilder blockBuilder = createBlockBuilderWithValues(expectedValues);
        assertBlockFilteredPositions(expectedValues, blockBuilder.build(), 0, 2, 4, 6, 7, 9, 10, 16);
        assertBlockFilteredPositions(expectedValues, blockBuilder.build());
        assertBlockFilteredPositions(expectedValues, blockBuilder.build(), 1, 2, 3, 7, 8, 9, 10, 11, 50);
        assertBlockFilteredPositions(expectedValues, blockBuilder.build(), 7, 6, 6, 50, 11, 50);
    }

    @Test
    public void testLazyBlockBuilderInitialization()
    {
        Slice[] expectedValues = createExpectedValues(100);
        BlockBuilder emptyBlockBuilder = new VariableWidthViewBlockBuilder(null, 0, 0);

        BlockBuilder blockBuilder = new VariableWidthViewBlockBuilder(null, expectedValues.length, 32 * expectedValues.length);
        assertThat(blockBuilder.getSizeInBytes()).isEqualTo(emptyBlockBuilder.getSizeInBytes());
        assertThat(blockBuilder.getRetainedSizeInBytes()).isEqualTo(emptyBlockBuilder.getRetainedSizeInBytes());

        writeValues(expectedValues, blockBuilder);
        assertThat(blockBuilder.getSizeInBytes() > emptyBlockBuilder.getSizeInBytes()).isTrue();
        assertThat(blockBuilder.getRetainedSizeInBytes() > emptyBlockBuilder.getRetainedSizeInBytes()).isTrue();

        blockBuilder = blockBuilder.newBlockBuilderLike(null);
        assertThat(blockBuilder.getSizeInBytes()).isEqualTo(emptyBlockBuilder.getSizeInBytes());
        assertThat(blockBuilder.getRetainedSizeInBytes()).isEqualTo(emptyBlockBuilder.getRetainedSizeInBytes());
    }

    @Test
    public void testGetSizeInBytes()
    {
        int numEntries = 1000;
        VariableWidthViewBlockBuilder blockBuilder = new VariableWidthViewBlockBuilder(null, numEntries, 20 * numEntries);
        for (int i = 0; i < numEntries; i++) {
            blockBuilder.writeEntry(utf8Slice(String.valueOf(ThreadLocalRandom.current().nextLong())));
        }
        Block block = blockBuilder.build();

        List<Block> splitQuarter = splitBlock(block, 4);
        long sizeInBytes = block.getSizeInBytes();
        long quarter1size = splitQuarter.get(0).getSizeInBytes();
        long quarter2size = splitQuarter.get(1).getSizeInBytes();
        long quarter3size = splitQuarter.get(2).getSizeInBytes();
        long quarter4size = splitQuarter.get(3).getSizeInBytes();
        double expectedQuarterSizeMin = sizeInBytes * 0.2;
        double expectedQuarterSizeMax = sizeInBytes * 0.3;
        assertThat(quarter1size > expectedQuarterSizeMin && quarter1size < expectedQuarterSizeMax)
                .describedAs(format("quarter1size is %s, should be between %s and %s", quarter1size, expectedQuarterSizeMin, expectedQuarterSizeMax))
                .isTrue();
        assertThat(quarter2size > expectedQuarterSizeMin && quarter2size < expectedQuarterSizeMax)
                .describedAs(format("quarter2size is %s, should be between %s and %s", quarter2size, expectedQuarterSizeMin, expectedQuarterSizeMax))
                .isTrue();
        assertThat(quarter3size > expectedQuarterSizeMin && quarter3size < expectedQuarterSizeMax)
                .describedAs(format("quarter3size is %s, should be between %s and %s", quarter3size, expectedQuarterSizeMin, expectedQuarterSizeMax))
                .isTrue();
        assertThat(quarter4size > expectedQuarterSizeMin && quarter4size < expectedQuarterSizeMax)
                .describedAs(format("quarter4size is %s, should be between %s and %s", quarter4size, expectedQuarterSizeMin, expectedQuarterSizeMax))
                .isTrue();
        assertThat(quarter1size + quarter2size + quarter3size + quarter4size).isEqualTo(sizeInBytes);
    }

    @Test
    public void testEstimatedDataSizeForStats()
    {
        Slice[] expectedValues = createExpectedValues(100);
        assertEstimatedDataSizeForStats(createBlockBuilderWithValues(expectedValues), expectedValues);
    }

    @Test
    public void testCompactBlock()
    {
        Slice[] values = {createExpectedValue(3), createExpectedValue(13), null, createExpectedValue(20)};
        Block compact = createBlockBuilderWithValues(values).build().copyRegion(0, values.length);
        testCompactBlock(compact);
        testCompactBlock(createBlockBuilderWithValues(new Slice[] {createExpectedValue(1), createExpectedValue(12)}).build().copyRegion(0, 2));

        // views reference a slice that also holds the inline values
        Slice slice = createExpectedValue(40);
        testNotCompactBlock(VariableWidthViewBlock.fromOffsets(3, slice, new int[] {0, 4, 20, 40}, Optional.empty()));
        // builder views have spare capacity
        testNotCompactBlock(createBlockBuilderWithValues(values).build());
    }

    private void assertVariableWithValues(Slice[] expectedValues)
    {
        Block block = createBlockBuilderWithValues(expectedValues).build();
        assertBlock(block, expectedValues);
    }

    private static BlockBuilder createBlockBuilderWithValues(Slice[] expectedValues)
    {
        BlockBuilder blockBuilder = new VariableWidthViewBlockBuilder(null, expectedValues.length, 32 * expectedValues.length);
        return writeValues(expectedValues, blockBuilder);
    }

    private static BlockBuilder writeValues(Slice[] expectedValues, BlockBuilder blockBuilder)
    {
        for (Slice expectedValue : expectedValues) {
            if (expectedValue == null) {
                blockBuilder.appendNull();
            }
            else {
                ((VariableWidthViewBlockBuilder) blockBuilder).writeEntry(expectedValue);
            }
        }
        return blockBuilder;
    }

    @Override
    protected <T> void assertPositionValue(Block block, int position, T expectedValue)
    {
        if (expectedValue == null) {
            assertThat(block.isNull(position)).isTrue();
            return;
        }

        assertThat(block.isNull(position)).isFalse();
        assertThat(((VariableWidthViewBlock) block).getSlice(position)).isEqualTo(expectedValue);
    }
}
