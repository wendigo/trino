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
import io.trino.parquet.reader.flat.BinaryBuffer;
import io.trino.spi.block.VariableWidthViewBuffer;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Decoders of VARCHAR values into {@link VariableWidthViewBuffer}, whose values reference the bytes they were decoded
 * from or into rather than being copied to a buffer of the batch.
 */
public final class VarcharDecoders
{
    private VarcharDecoders() {}

    /**
     * Decodes PLAIN values without copying them: values longer than the inline size of a view reference the page when
     * the page owns its bytes and the batch references a large enough part of it, and are copied to a buffer of their
     * own otherwise.
     */
    public static final class PlainVarcharDecoder
            implements ValueDecoder<VariableWidthViewBuffer>
    {
        // A page is referenced when the values of a batch that reference it are at least this fraction of its bytes,
        // so that a block retains at most this many times the bytes of its values. Batches of a few positions, like
        // the first batches of a reader, copy their values.
        private static final int MAX_PAGE_TO_REFERENCED_BYTES_RATIO = 4;

        private SimpleSliceInputStream input;
        private int[] offsets = new int[0];
        private int[] lengths = new int[0];

        @Override
        public void init(SimpleSliceInputStream input)
        {
            this.input = requireNonNull(input, "input is null");
        }

        @Override
        public void read(VariableWidthViewBuffer values, int offset, int length)
        {
            Slice inputSlice = input.asSlice();
            byte[] bytes = inputSlice.byteArray();
            int bytesOffset = inputSlice.byteArrayOffset();
            if (offsets.length < length) {
                offsets = new int[length];
                lengths = new int[length];
            }
            int currentInputOffset = 0;
            long outOfLineBytes = 0;
            for (int i = 0; i < length; i++) {
                int positionLength = inputSlice.getInt(currentInputOffset);
                currentInputOffset += Integer.BYTES;
                offsets[i] = bytesOffset + currentInputOffset;
                lengths[i] = positionLength;
                currentInputOffset += positionLength;
                outOfLineBytes += positionLength > VariableWidthViewBuffer.INLINE_SIZE ? positionLength : 0;
            }
            // the loop reads the length of every value, so all values are in the slice
            boolean borrow = input.isDataOwned() && outOfLineBytes * MAX_PAGE_TO_REFERENCED_BYTES_RATIO >= bytes.length;
            values.setValues(offset, bytes, offsets, lengths, length, borrow);
            input.skip(currentInputOffset);
        }

        @Override
        public void skip(int n)
        {
            for (int i = 0; i < n; i++) {
                int positionLength = input.readInt();
                input.skip(positionLength);
            }
        }
    }

    /**
     * Decodes values with a decoder that copies them to a {@link BinaryBuffer}, and references the copies.
     */
    public static final class CopyingVarcharDecoder
            implements ValueDecoder<VariableWidthViewBuffer>
    {
        private final ValueDecoder<BinaryBuffer> delegate;

        public CopyingVarcharDecoder(ValueDecoder<BinaryBuffer> delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        public void init(SimpleSliceInputStream input)
        {
            delegate.init(input);
        }

        @Override
        public void read(VariableWidthViewBuffer values, int offset, int length)
        {
            BinaryBuffer buffer = new BinaryBuffer(length);
            delegate.read(buffer, 0, length);
            setValues(values, offset, buffer);
        }

        @Override
        public void skip(int n)
        {
            delegate.skip(n);
        }
    }

    /**
     * Sets the values of {@code values} from {@code position} to the values of {@code buffer}, referencing its chunks,
     * which hold the values contiguously.
     */
    public static void setValues(VariableWidthViewBuffer values, int position, BinaryBuffer buffer)
    {
        int valueCount = buffer.getValueCount();
        int[] offsets = buffer.getOffsets();
        List<Slice> chunks = buffer.getChunks();
        if (chunks.size() <= 1) {
            Slice chunk = chunks.isEmpty() ? Slices.EMPTY_SLICE : chunks.getFirst();
            values.setContiguousValues(position, chunk.byteArray(), chunk.byteArrayOffset() - offsets[0], offsets, 0, valueCount);
            return;
        }
        // every value is in a single chunk, so a chunk holds the values whose offsets are within it
        int chunkStart = offsets[0];
        int firstValue = 0;
        for (Slice chunk : chunks) {
            int chunkEnd = chunkStart + chunk.length();
            int lastValue = firstValue;
            while (lastValue < valueCount && offsets[lastValue + 1] <= chunkEnd) {
                lastValue++;
            }
            values.setContiguousValues(position + firstValue, chunk.byteArray(), chunk.byteArrayOffset() - chunkStart, offsets, firstValue, lastValue - firstValue);
            firstValue = lastValue;
            chunkStart = chunkEnd;
        }
        if (firstValue != valueCount) {
            throw new IllegalArgumentException("values are not within the chunks");
        }
    }
}
