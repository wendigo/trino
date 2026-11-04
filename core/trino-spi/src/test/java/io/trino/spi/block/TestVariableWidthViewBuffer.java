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
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Random;

import static io.airlift.slice.Slices.utf8Slice;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestVariableWidthViewBuffer
{
    private static final int VALUE_COUNT = 300;

    // values of varying lengths, separated by gaps like the length prefixes of a Parquet page
    private final byte[] page = new byte[20_000];
    private final int[] offsets = new int[VALUE_COUNT];
    private final int[] lengths = new int[VALUE_COUNT];

    TestVariableWidthViewBuffer()
    {
        Random random = new Random(11);
        random.nextBytes(page);
        int offset = 7;
        for (int i = 0; i < VALUE_COUNT; i++) {
            int length = i % 5 == 0 ? random.nextInt(100) : random.nextInt(14);
            offset += 4;
            offsets[i] = offset;
            lengths[i] = length;
            offset += length;
        }
    }

    @Test
    public void testBorrowedValuesReferenceTheBytes()
    {
        VariableWidthViewBuffer buffer = new VariableWidthViewBuffer(VALUE_COUNT);
        buffer.setValues(0, page, offsets, lengths, VALUE_COUNT, true);
        VariableWidthViewBlock block = buffer.build(Optional.empty());

        assertValues(block, 0, VALUE_COUNT, 0);
        assertThat(block.getRawBuffers()).hasNumberOfRows(1);
        assertThat(block.getRawBuffers()[0]).isSameAs(page);
        int referencedEnd = 0;
        for (int i = 0; i < VALUE_COUNT; i++) {
            if (lengths[i] > VariableWidthViewBuffer.INLINE_SIZE) {
                referencedEnd = Math.max(referencedEnd, offsets[i] + lengths[i]);
            }
        }
        assertThat(block.getRawBufferLengths()[0]).isEqualTo(referencedEnd);
    }

    @Test
    public void testCopiedValuesDoNotReferenceTheBytes()
    {
        VariableWidthViewBuffer buffer = new VariableWidthViewBuffer(VALUE_COUNT);
        buffer.setValues(0, page, offsets, lengths, VALUE_COUNT, false);
        VariableWidthViewBlock block = buffer.build(Optional.empty());

        assertValues(block, 0, VALUE_COUNT, 0);
        assertThat(block.getRawBuffers()).hasNumberOfRows(1);
        assertThat(block.getRawBuffers()[0]).isNotSameAs(page);
        assertThat(block.getRawBuffers()[0].length).isEqualTo(block.getOutOfLineBytes());
    }

    @Test
    public void testInlineValuesReferenceNothing()
    {
        // the first values are short
        VariableWidthViewBuffer buffer = new VariableWidthViewBuffer(4);
        buffer.setValues(0, page, new int[] {offsets[1], offsets[2], offsets[3], offsets[4]}, new int[] {lengths[1], lengths[2], lengths[3], lengths[4]}, 4, true);
        VariableWidthViewBlock block = buffer.build(Optional.empty());

        assertValues(block, 0, 4, 1);
        assertThat(block.getRawBuffers()).isEmpty();
    }

    @Test
    public void testTemporaryViewsAreCopiedAroundNulls()
    {
        VariableWidthViewBuffer buffer = new VariableWidthViewBuffer(VALUE_COUNT);
        VariableWidthViewBuffer temporary = buffer.createTemporary(VALUE_COUNT / 2);
        temporary.setValues(0, page, offsets, lengths, VALUE_COUNT / 2, true);
        long[] valueIsValid = new long[Bitmap.wordsForBits(VALUE_COUNT)];
        for (int i = 0; i < VALUE_COUNT / 2; i++) {
            buffer.copyViews(temporary, i, i * 2, 1);
            Bitmap.set(valueIsValid, 0, i * 2);
        }
        VariableWidthViewBlock block = buffer.build(Optional.of(valueIsValid));

        for (int i = 0; i < VALUE_COUNT; i++) {
            if (i % 2 == 0) {
                assertThat(block.isNull(i)).isFalse();
                assertThat(block.getSlice(i)).isEqualTo(value(i / 2));
            }
            else {
                assertThat(block.isNull(i)).isTrue();
            }
        }
        assertThat(block.getRawBuffers()).hasNumberOfRows(1);
        assertThat(block.getRawBuffers()[0]).isSameAs(page);

        VariableWidthViewBuffer other = new VariableWidthViewBuffer(10);
        assertThatThrownBy(() -> buffer.copyViews(other, 0, 0, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not share");
    }

    @Test
    public void testDictionaryValuesReferenceTheDictionary()
    {
        VariableWidthViewBuffer dictionary = new VariableWidthViewBuffer(VALUE_COUNT);
        dictionary.setValues(0, page, offsets, lengths, VALUE_COUNT, true);

        // values of a page of their own come first, then dictionary ids, twice, then more page values
        byte[] otherPage = new byte[2000];
        new Random(3).nextBytes(otherPage);
        VariableWidthViewBuffer buffer = new VariableWidthViewBuffer(40);
        buffer.setValues(0, otherPage, new int[] {0, 100}, new int[] {50, 70}, 2, true);
        int[] ids = new int[36];
        for (int i = 0; i < ids.length; i++) {
            ids[i] = (i * 37) % VALUE_COUNT;
        }
        buffer.setDictionaryValues(2, dictionary, ids, 0, 18);
        buffer.setDictionaryValues(20, dictionary, ids, 18, 18);
        buffer.setValues(38, otherPage, new int[] {300, 400}, new int[] {5, 90}, 2, true);
        VariableWidthViewBlock block = buffer.build(Optional.empty());

        assertThat(block.getSlice(0)).isEqualTo(Slices.wrappedBuffer(otherPage, 0, 50));
        assertThat(block.getSlice(1)).isEqualTo(Slices.wrappedBuffer(otherPage, 100, 70));
        for (int i = 0; i < ids.length; i++) {
            assertThat(block.getSlice(2 + i)).isEqualTo(value(ids[i]));
        }
        assertThat(block.getSlice(38)).isEqualTo(Slices.wrappedBuffer(otherPage, 300, 5));
        assertThat(block.getSlice(39)).isEqualTo(Slices.wrappedBuffer(otherPage, 400, 90));
        assertThat(List.of(block.getRawBuffers())).containsExactly(otherPage, page);

        assertThatThrownBy(() -> buffer.setDictionaryValues(0, dictionary, new int[] {VALUE_COUNT}, 0, 1))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    public void testMerge()
    {
        VariableWidthViewBuffer first = new VariableWidthViewBuffer(100);
        first.setValues(0, page, offsets, lengths, 100, true);
        VariableWidthViewBuffer second = new VariableWidthViewBuffer(50);
        second.setValues(0, page, offsets, lengths, 50, false);
        VariableWidthViewBuffer third = new VariableWidthViewBuffer(150);
        // the same page is referenced by another buffer, like when a batch spans the pages of a reader
        third.setValues(0, page, offsets, lengths, 150, true);
        VariableWidthViewBlock block = VariableWidthViewBuffer.merge(List.of(first, second, third)).build(Optional.empty());

        assertValues(block, 0, 100, 0);
        assertValues(block, 100, 50, 0);
        assertValues(block, 150, 150, 0);
        assertThat(VariableWidthViewBuffer.merge(List.of(first))).isSameAs(first);
    }

    @Test
    public void testContiguousValues()
    {
        // the format of a variable-width block, in the middle of an array
        byte[] bytes = new byte[500];
        new Random(5).nextBytes(bytes);
        int[] valueOffsets = {0, 3, 3, 20, 100, 113, 400};
        VariableWidthViewBuffer buffer = new VariableWidthViewBuffer(6);
        buffer.setContiguousValues(0, bytes, 50, valueOffsets, 0, 6);
        VariableWidthViewBlock block = buffer.build(Optional.empty());

        for (int i = 0; i < 6; i++) {
            assertThat(block.getSlice(i)).isEqualTo(Slices.wrappedBuffer(bytes, 50 + valueOffsets[i], valueOffsets[i + 1] - valueOffsets[i]));
        }
        assertThat(block.getRawBuffers()[0]).isSameAs(bytes);
    }

    @Test
    public void testInvalidValues()
    {
        VariableWidthViewBuffer buffer = new VariableWidthViewBuffer(2);
        assertThatThrownBy(() -> buffer.setValues(0, page, new int[] {0, page.length - 5}, new int[] {3, 6}, 2, true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("outside of the bytes");
        assertThatThrownBy(() -> buffer.setValues(1, page, new int[] {0, 0}, new int[] {3, 6}, 2, true))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> buffer.setValues(0, page, new int[] {-1, 0}, new int[] {3, 6}, 2, false))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private void assertValues(VariableWidthViewBlock block, int position, int count, int firstValue)
    {
        for (int i = 0; i < count; i++) {
            assertThat(block.isNull(position + i)).isFalse();
            assertThat(block.getSlice(position + i)).isEqualTo(value(firstValue + i));
        }
    }

    private Slice value(int index)
    {
        return Slices.wrappedBuffer(page, offsets[index], lengths[index]);
    }

    @SuppressWarnings("unused")
    private static Slice utf8(String value)
    {
        return utf8Slice(value);
    }
}
