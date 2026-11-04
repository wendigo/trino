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

import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

import java.util.Optional;

import static io.trino.spi.block.EncoderUtil.decodeValidityAsLongs;
import static io.trino.spi.block.EncoderUtil.encodeValidityAsLongs;
import static io.trino.spi.block.EncoderUtil.encodedValiditySize;
import static io.trino.spi.block.VariableWidthViewBlock.INLINE_SIZE;
import static io.trino.spi.block.VariableWidthViewBlock.LONG_LE;
import static io.trino.spi.block.VariableWidthViewBlock.VIEW_SIZE;
import static io.trino.spi.block.VariableWidthViewBlock.bufferIndex;
import static io.trino.spi.block.VariableWidthViewBlock.bufferOffset;
import static io.trino.spi.block.VariableWidthViewBlock.length;
import static java.lang.Math.toIntExact;
import static java.lang.System.arraycopy;

/// Serializes [VariableWidthViewBlock] like [VariableWidthBlockEncoding] serializes [VariableWidthBlock]: the
/// validity, the length of every value, and the value bytes in position order. The views are not written, since they
/// hold 16 bytes per position and mostly zeros, which would triple the size of short values on the wire and the work
/// of compressing them; reading rebuilds the views from the lengths with [VariableWidthViewBlock#fromLengths], which
/// is vectorized. Null positions have a length of zero, which is written too, so the lengths are read without a branch.
public class VariableWidthViewBlockEncoding
        implements BlockEncoding
{
    public static final String NAME = "VARIABLE_WIDTH_VIEW";

    private static final int SCRATCH_SIZE = 4096;
    // the longest value gathered in the scratch buffer rather than written from its buffer
    private static final int MAX_GATHERED_LENGTH = 256;
    // gathered values are copied in words of this many bytes
    private static final int COPIED_WORD = 2 * Long.BYTES;

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public Class<? extends Block> getBlockClass()
    {
        return VariableWidthViewBlock.class;
    }

    @Override
    public void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput sliceOutput, Block block)
    {
        VariableWidthViewBlock viewBlock = (VariableWidthViewBlock) block;

        int positionCount = viewBlock.getPositionCount();
        sliceOutput.appendInt(positionCount);

        int arrayBase = viewBlock.getRawArrayBase();
        encodeValidityAsLongs(sliceOutput, viewBlock.getRawValueIsValid(), arrayBase, positionCount);

        byte[] views = viewBlock.getRawViews();
        int start = arrayBase * VIEW_SIZE;
        int end = start + positionCount * VIEW_SIZE;

        // the view of a null position is all zeros, so its length is zero
        int[] lengths = new int[positionCount];
        long totalLength = 0;
        for (int position = 0; position < positionCount; position++) {
            int length = length(views, start + position * VIEW_SIZE);
            lengths[position] = length;
            totalLength += length;
        }
        sliceOutput.appendInt(toIntExact(totalLength));
        sliceOutput.writeInts(lengths, 0, positionCount);

        writeValues(sliceOutput, views, start, end, viewBlock.getRawBuffers());
    }

    @Override
    public long estimatedWriteSize(BlockEncodingSerde blockEncodingSerde, Block block)
    {
        VariableWidthViewBlock viewBlock = (VariableWidthViewBlock) block;
        int positionCount = viewBlock.getPositionCount();
        return Integer.BYTES
                + encodedValiditySize(viewBlock.getRawValueIsValid(), positionCount)
                + Integer.BYTES
                + Integer.BYTES * (long) positionCount
                + viewBlock.getValueBytes();
    }

    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        int positionCount = sliceInput.readInt();
        long[] valueIsValid = decodeValidityAsLongs(sliceInput, positionCount);
        int totalLength = sliceInput.readInt();
        if (totalLength < 0) {
            throw new IllegalArgumentException("totalLength is negative");
        }
        int[] lengths = new int[positionCount];
        sliceInput.readInts(lengths, 0, positionCount);
        byte[] bytes = new byte[totalLength];
        sliceInput.readBytes(bytes, 0, totalLength);
        return VariableWidthViewBlock.fromLengths(positionCount, Slices.wrappedBuffer(bytes), lengths, Optional.ofNullable(valueIsValid));
    }

    /// Writes the value bytes in position order. Inline values are copied from their views, and short out-of-line
    /// values from their buffers, to a scratch buffer that is written when it fills up; longer values are written
    /// from their buffers.
    private static void writeValues(SliceOutput sliceOutput, byte[] views, int start, int end, byte[][] buffers)
    {
        // Values are copied in words, past their length, which is faster than a copy of an arbitrary length, so the
        // scratch buffer has room for a word after the longest gathered value. An inline value is copied with two
        // overlapping long stores of its 12 inline view bytes.
        byte[] scratch = new byte[SCRATCH_SIZE + COPIED_WORD];
        int scratchLength = 0;
        for (int view = start; view < end; view += VIEW_SIZE) {
            int length = length(views, view);
            if (length <= INLINE_SIZE) {
                if (scratchLength > SCRATCH_SIZE - INLINE_SIZE) {
                    sliceOutput.writeBytes(scratch, 0, scratchLength);
                    scratchLength = 0;
                }
                // the inline bytes follow the length; unused view bytes are zero
                LONG_LE.set(scratch, scratchLength, (long) LONG_LE.get(views, view + Integer.BYTES));
                LONG_LE.set(scratch, scratchLength + Integer.BYTES, (long) LONG_LE.get(views, view + Long.BYTES));
                scratchLength += length;
                continue;
            }
            byte[] buffer = buffers[bufferIndex(views, view)];
            int offset = bufferOffset(views, view);
            if (length <= MAX_GATHERED_LENGTH) {
                if (scratchLength + length > SCRATCH_SIZE) {
                    sliceOutput.writeBytes(scratch, 0, scratchLength);
                    scratchLength = 0;
                }
                if (offset <= buffer.length - length - COPIED_WORD) {
                    for (int copied = 0; copied < length; copied += COPIED_WORD) {
                        LONG_LE.set(scratch, scratchLength + copied, (long) LONG_LE.get(buffer, offset + copied));
                        LONG_LE.set(scratch, scratchLength + copied + Long.BYTES, (long) LONG_LE.get(buffer, offset + copied + Long.BYTES));
                    }
                }
                else {
                    // the value ends near the end of its buffer
                    arraycopy(buffer, offset, scratch, scratchLength, length);
                }
                scratchLength += length;
                continue;
            }
            if (scratchLength > 0) {
                sliceOutput.writeBytes(scratch, 0, scratchLength);
                scratchLength = 0;
            }
            sliceOutput.writeBytes(buffer, offset, length);
        }
        if (scratchLength > 0) {
            sliceOutput.writeBytes(scratch, 0, scratchLength);
        }
    }
}
