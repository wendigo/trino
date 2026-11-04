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
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Random;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

final class TestVariableWidthViewBlockEncoding
        extends BaseBlockEncodingTest<String>
{
    private final BlockEncodingSerde blockEncodingSerde = new TestingBlockEncodingSerde();

    @Override
    protected Type getType()
    {
        return VARCHAR;
    }

    @Override
    protected void write(BlockBuilder blockBuilder, String value)
    {
        VARCHAR.writeString(blockBuilder, value);
    }

    @Override
    protected String randomValue(Random random)
    {
        // mostly values around the inline limit, and a few values long enough to fill the scratch buffer of the encoder
        int length = random.nextInt(100) == 0 ? 200 + random.nextInt(5000) : random.nextInt(41);
        char[] value = new char[length];
        for (int i = 0; i < value.length; i++) {
            value[i] = (char) random.nextInt(Byte.MAX_VALUE);
        }
        return new String(value);
    }

    @Test
    public void testUnicode()
    {
        roundTrip(
                "\u0000",
                "Ní hé lá na gaoithe lá na scolb",
                "لولا اختلاف النظر، لبارت السلع",
                "△△▿▿◁▷◁▷BA",
                "Something in ASCII, latin ÿ, some İ and I, geometry ▦ and finally an emoji 😍");
    }

    @Test
    public void testRegionsCopiesAndWrappedBlocks()
    {
        Random random = new Random(17);
        VariableWidthViewBlockBuilder blockBuilder = new VariableWidthViewBlockBuilder(null, 1, 0);
        for (int i = 0; i < 5000; i++) {
            if (i % 17 == 0) {
                blockBuilder.appendNull();
            }
            else {
                blockBuilder.writeEntry(utf8Slice(randomValue(random)));
            }
        }
        VariableWidthViewBlock block = blockBuilder.buildValueBlock();
        assertThat(block.getRawBuffers().length).isGreaterThan(1);
        assertRoundTrip(block);
        assertRoundTrip(block.getRegion(333, 4000));
        assertRoundTrip(block.getRegion(333, 4000).copyRegion(10, 3000));
        assertRoundTrip(block.copyPositions(new int[] {4999, 0, 17, 17, 2500, 3}, 0, 6));

        VariableWidthViewBlockBuilder repeated = new VariableWidthViewBlockBuilder(null, 1, 0);
        repeated.appendRepeated(block, 1, 100);
        repeated.appendRepeated(block, 0, 3);
        repeated.appendRange(block, 1, 200);
        assertRoundTrip(repeated.buildValueBlock());

        // values that reference the bytes of a reader, which also hold the inline values
        byte[] bytes = new byte[1000];
        random.nextBytes(bytes);
        int[] offsets = {7, 7, 10, 30, 31, 600, 612, 613, 1000};
        assertRoundTrip(VariableWidthViewBlock.fromOffsets(offsets.length - 1, Slices.wrappedBuffer(bytes, 7, 993), shift(offsets, -7), Optional.empty()));
        assertRoundTrip(VariableWidthViewBlock.fromOffsets(offsets.length - 1, Slices.wrappedBuffer(bytes), offsets, Optional.of(new long[] {0b1011_0110})));
    }

    @Test
    public void testOnlyTheValuesOfARegionAreWritten()
    {
        Slice longValue = utf8Slice("x".repeat(1000));
        VariableWidthViewBlockBuilder blockBuilder = new VariableWidthViewBlockBuilder(null, 1, 0);
        for (int i = 0; i < 1000; i++) {
            blockBuilder.writeEntry(longValue);
            blockBuilder.writeEntry(utf8Slice("short"));
        }
        VariableWidthViewBlock block = blockBuilder.buildValueBlock();

        // a region references a small part of the buffers, and only its values are written
        VariableWidthViewBlock region = block.getRegion(1001, 10);
        assertThat(serializedSize(region)).isEqualTo(headerSize(region) + 5 * longValue.length() + 5 * 5);
        VariableWidthViewBlock actual = assertRoundTrip(region);
        assertThat(actual.getRawBuffers()).hasNumberOfRows(1);
        assertThat(actual.getRawBuffers()[0]).hasSize(5 * longValue.length() + 5 * 5);

        VariableWidthViewBlock inlineRegion = block.copyPositions(new int[] {1, 3, 5}, 0, 3);
        assertThat(serializedSize(inlineRegion)).isEqualTo(headerSize(inlineRegion) + 3 * 5);
    }

    @Test
    public void testUnusedEndOfBuffersIsNotWritten()
    {
        // buffers of a builder double in size, so the last one is partly empty
        Slice value = utf8Slice("a value of 24 bytes long");
        VariableWidthViewBlockBuilder blockBuilder = new VariableWidthViewBlockBuilder(null, 1, 0);
        for (int i = 0; i < 3000; i++) {
            blockBuilder.writeEntry(value);
        }
        VariableWidthViewBlock block = blockBuilder.buildValueBlock();
        long bufferBytes = 0;
        for (byte[] buffer : block.getRawBuffers()) {
            bufferBytes += buffer.length;
        }
        assertThat(bufferBytes).isGreaterThan(3000L * value.length() * 9 / 8);

        assertThat(serializedSize(block)).isEqualTo(headerSize(block) + 3000L * value.length());
        VariableWidthViewBlock actual = assertRoundTrip(block);
        assertThat(actual.getRawBuffers()).hasNumberOfRows(1);
        assertThat(actual.getRawBuffers()[0]).hasSize(3000 * value.length());
    }

    @Test
    public void testNullsAndInlineValuesAreWrittenCompactly()
    {
        VariableWidthViewBlockBuilder blockBuilder = new VariableWidthViewBlockBuilder(null, 1, 0);
        for (int i = 0; i < 1000; i++) {
            if (i % 3 == 0) {
                blockBuilder.appendNull();
            }
            else {
                blockBuilder.writeEntry(utf8Slice("v" + i));
            }
        }
        VariableWidthViewBlock block = blockBuilder.buildValueBlock();

        // a null costs its length, and an inline value its bytes; the views are not written
        assertThat(serializedSize(block)).isEqualTo(headerSize(block) + block.getValueBytes());
        assertThat(serializedSize(block)).isLessThan(1000L * VariableWidthViewBlock.VIEW_SIZE);
        // the values are inline, so the read block does not reference the bytes it was read from
        VariableWidthViewBlock actual = assertRoundTrip(block);
        assertThat(actual.getRawBuffers()).isEmpty();
        assertThat(actual.mayHaveNull()).isTrue();
    }

    @Test
    public void testRepeatedValuesAreWrittenForEveryPosition()
    {
        VariableWidthViewBlockBuilder source = new VariableWidthViewBlockBuilder(null, 1, 0);
        source.writeEntry(utf8Slice("y".repeat(10_000)));
        VariableWidthViewBlockBuilder blockBuilder = new VariableWidthViewBlockBuilder(null, 1, 0);
        blockBuilder.appendRepeated(source.buildValueBlock(), 0, 1000);
        VariableWidthViewBlock block = blockBuilder.buildValueBlock();

        // like a variable-width block, the encoding does not share values, which a run-length encoded block does
        assertThat(serializedSize(block)).isEqualTo(headerSize(block) + 1000L * 10_000);
        assertRoundTrip(block);
    }

    @Test
    public void testEstimatedWriteSize()
    {
        Random random = new Random(23);
        VariableWidthViewBlockBuilder blockBuilder = new VariableWidthViewBlockBuilder(null, 1, 0);
        for (int i = 0; i < 2000; i++) {
            if (i % 7 == 0) {
                blockBuilder.appendNull();
            }
            else {
                blockBuilder.writeEntry(utf8Slice(randomValue(random)));
            }
        }
        VariableWidthViewBlock block = blockBuilder.buildValueBlock();

        VariableWidthViewBlockBuilder shortValues = new VariableWidthViewBlockBuilder(null, 100, 0);
        for (int i = 0; i < 100; i++) {
            shortValues.writeEntry(utf8Slice("v" + i));
        }
        VariableWidthViewBlockBuilder repeated = new VariableWidthViewBlockBuilder(null, 1, 0);
        repeated.appendRepeated(block, 1, 500);

        for (Block value : List.of(block, block.getRegion(100, 300), block.copyRegion(0, 1000), shortValues.buildValueBlock(), repeated.buildValueBlock())) {
            assertEstimateMatches(value);
        }

        // blocks that contain the view block estimate it with its encoding
        ArrayBlock arrayBlock = ArrayBlock.fromElementBlock(2, Optional.empty(), new int[] {0, 700, 2000}, block);
        assertEstimateMatches(arrayBlock);
        assertEstimateMatches(RowBlock.fromFieldBlocks(2000, new Block[] {block, block}));
        assertEstimateMatches(DictionaryBlock.create(3, block.copyRegion(0, 10), new int[] {9, 0, 9}));
        assertEstimateMatches(RunLengthEncodedBlock.create(block.getRegion(1, 1), 1000));
    }

    private void assertEstimateMatches(Block block)
    {
        long estimate = blockEncodingSerde.estimatedWriteSize(block);
        DynamicSliceOutput output = new DynamicSliceOutput(1024);
        blockEncodingSerde.writeBlock(output, block);
        if (block instanceof VariableWidthViewBlock) {
            assertThat((long) output.size()).isEqualTo(estimate);
        }
        else {
            // a dictionary scales the estimate of its dictionary, including the fixed size of the header
            assertThat((long) output.size()).isBetween(estimate * 8 / 9 - 64, estimate * 9 / 8 + 32);
        }
    }

    /// The size of everything but the value bytes: the encoding name, position count, validity, total length and
    /// the lengths.
    private static long headerSize(VariableWidthViewBlock block)
    {
        return Integer.BYTES + VariableWidthViewBlockEncoding.NAME.length()
                + Integer.BYTES
                + EncoderUtil.encodedValiditySize(block.getRawValueIsValid(), block.getPositionCount())
                + Integer.BYTES
                + Integer.BYTES * (long) block.getPositionCount();
    }

    private long serializedSize(VariableWidthViewBlock block)
    {
        DynamicSliceOutput output = new DynamicSliceOutput(1024);
        blockEncodingSerde.writeBlock(output, block);
        return output.size();
    }

    private VariableWidthViewBlock assertRoundTrip(VariableWidthViewBlock block)
    {
        DynamicSliceOutput output = new DynamicSliceOutput(1024);
        blockEncodingSerde.writeBlock(output, block);
        Block actual = blockEncodingSerde.readBlock(output.slice().getInput());
        assertThat(actual.getPositionCount()).isEqualTo(block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            assertThat(actual.isNull(position)).isEqualTo(block.isNull(position));
            if (!block.isNull(position)) {
                Slice expected = block.getSlice(position);
                assertThat(((VariableWidthViewBlock) actual).getSlice(position)).isEqualTo(expected);
            }
        }
        return (VariableWidthViewBlock) actual;
    }

    private static int[] shift(int[] offsets, int delta)
    {
        int[] shifted = new int[offsets.length];
        for (int i = 0; i < offsets.length; i++) {
            shifted[i] = offsets[i] + delta;
        }
        return shifted;
    }
}
