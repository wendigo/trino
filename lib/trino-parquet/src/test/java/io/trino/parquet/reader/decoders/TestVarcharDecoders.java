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
package io.trino.parquet.reader.decoders;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.parquet.reader.SimpleSliceInputStream;
import io.trino.parquet.reader.decoders.VarcharDecoders.PlainVarcharDecoder;
import io.trino.spi.block.VariableWidthViewBlock;
import io.trino.spi.block.VariableWidthViewBuffer;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

final class TestVarcharDecoders
{
    // 100 inline values and 12 long ones, so the long values are more than a quarter of the page
    private static final int SHORT_COUNT = 100;
    private static final int LONG_COUNT = 12;
    private static final int LONG_LENGTH = 40;

    @Test
    public void testOwnedPageIsReferenced()
    {
        byte[] page = plainPage();
        VariableWidthViewBlock block = decode(page, true, SHORT_COUNT + LONG_COUNT);

        assertValues(block, 0, SHORT_COUNT + LONG_COUNT);
        // the block retains the page and its views
        assertThat(block.getRetainedSizeInBytes()).isGreaterThan(page.length + viewBytes(SHORT_COUNT + LONG_COUNT));
    }

    @Test
    public void testUnownedPageIsCopied()
    {
        byte[] page = plainPage();
        VariableWidthViewBlock block = decode(page, false, SHORT_COUNT + LONG_COUNT);

        // only the long values are copied, so the block retains much less than the page, besides its views
        assertThat(block.getRetainedSizeInBytes()).isLessThan(page.length / 2 + viewBytes(SHORT_COUNT + LONG_COUNT));
        // the values do not change with the page
        Arrays.fill(page, (byte) 0);
        assertValues(block, 0, SHORT_COUNT + LONG_COUNT);
    }

    @Test
    public void testSmallBatchOfAnOwnedPageIsCopied()
    {
        byte[] page = plainPage();
        // the first batch reads the short values and one long value, which is a small part of the page
        PlainVarcharDecoder decoder = new PlainVarcharDecoder();
        decoder.init(new SimpleSliceInputStream(Slices.wrappedBuffer(page), 0, true));
        VariableWidthViewBuffer first = new VariableWidthViewBuffer(SHORT_COUNT + 1);
        decoder.read(first, 0, SHORT_COUNT + 1);
        VariableWidthViewBuffer second = new VariableWidthViewBuffer(LONG_COUNT - 1);
        decoder.read(second, 0, LONG_COUNT - 1);
        VariableWidthViewBlock firstBlock = first.build(Optional.empty());
        VariableWidthViewBlock secondBlock = second.build(Optional.empty());

        assertValues(firstBlock, 0, SHORT_COUNT + 1);
        assertValues(secondBlock, SHORT_COUNT + 1, LONG_COUNT - 1);
        assertThat(firstBlock.getRetainedSizeInBytes()).isLessThan(page.length / 2 + viewBytes(SHORT_COUNT + 1));
        assertThat(secondBlock.getRetainedSizeInBytes()).isGreaterThan(page.length + viewBytes(LONG_COUNT - 1));
    }

    @Test
    public void testSkip()
    {
        byte[] page = plainPage();
        PlainVarcharDecoder decoder = new PlainVarcharDecoder();
        decoder.init(new SimpleSliceInputStream(Slices.wrappedBuffer(page), 0, true));
        decoder.skip(SHORT_COUNT - 3);
        VariableWidthViewBuffer buffer = new VariableWidthViewBuffer(LONG_COUNT + 3);
        decoder.read(buffer, 0, LONG_COUNT + 3);

        assertValues(buffer.build(Optional.empty()), SHORT_COUNT - 3, LONG_COUNT + 3);
    }

    private static long viewBytes(int positionCount)
    {
        return 16L * positionCount;
    }

    private static VariableWidthViewBlock decode(byte[] page, boolean dataOwned, int count)
    {
        PlainVarcharDecoder decoder = new PlainVarcharDecoder();
        decoder.init(new SimpleSliceInputStream(Slices.wrappedBuffer(page), 0, dataOwned));
        VariableWidthViewBuffer buffer = new VariableWidthViewBuffer(count);
        decoder.read(buffer, 0, count);
        return buffer.build(Optional.empty());
    }

    private static void assertValues(VariableWidthViewBlock block, int firstValue, int count)
    {
        assertThat(block.getPositionCount()).isEqualTo(count);
        for (int i = 0; i < count; i++) {
            assertThat(block.getSlice(i)).isEqualTo(value(firstValue + i));
        }
    }

    private static byte[] plainPage()
    {
        Slice page = Slices.allocate(SHORT_COUNT * (Integer.BYTES + 8) + LONG_COUNT * (Integer.BYTES + LONG_LENGTH));
        int offset = 0;
        for (int i = 0; i < SHORT_COUNT + LONG_COUNT; i++) {
            Slice value = value(i);
            page.setInt(offset, value.length());
            page.setBytes(offset + Integer.BYTES, value);
            offset += Integer.BYTES + value.length();
        }
        return page.byteArray();
    }

    private static Slice value(int index)
    {
        String value = index < SHORT_COUNT ? String.format("v%07d", index) : String.valueOf((char) ('a' + index % 26)).repeat(LONG_LENGTH);
        return Slices.wrappedBuffer(value.getBytes(UTF_8));
    }
}
