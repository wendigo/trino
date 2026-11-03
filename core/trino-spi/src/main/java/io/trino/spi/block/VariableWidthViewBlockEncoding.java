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

import static io.trino.spi.block.EncoderUtil.decodeValidityAsLongs;
import static io.trino.spi.block.EncoderUtil.encodeValidityAsLongs;
import static io.trino.spi.block.EncoderUtil.encodedValiditySize;
import static io.trino.spi.block.VariableWidthViewBlock.INLINE_SIZE;
import static io.trino.spi.block.VariableWidthViewBlock.LONG_LE;
import static io.trino.spi.block.VariableWidthViewBlock.MAX_UNREFERENCED_BYTES_DIVISOR;
import static io.trino.spi.block.VariableWidthViewBlock.NO_BUFFERS;
import static io.trino.spi.block.VariableWidthViewBlock.VIEW_SIZE;
import static io.trino.spi.block.VariableWidthViewBlock.bufferIndex;
import static io.trino.spi.block.VariableWidthViewBlock.bufferOffset;
import static io.trino.spi.block.VariableWidthViewBlock.length;
import static java.lang.Math.toIntExact;

/// Serializes [VariableWidthViewBlock] in its in-memory layout, like the variadic buffers of the Arrow `BinaryView`
/// IPC format: validity, the total length of the values and of the out-of-line values, the buffer count and lengths, the
/// views, and the buffers. Reading a block copies the views and buffers without looking at the values.
///
/// Each buffer is written up to the end of the bytes that views of the block may reference, which the block knows, so
/// a block with dense buffers is written without looking at its views. A block whose buffers would hold many bytes
/// that its views do not reference, such as a region in the middle of a larger block or a block that borrows a few
/// values of many other blocks, is compacted while it is written, so its views reference a single buffer that holds only
/// their values.
public class VariableWidthViewBlockEncoding
        implements BlockEncoding
{
    public static final String NAME = "VARIABLE_WIDTH_VIEW";

    private static final int SCRATCH_SIZE = 4096;
    private static final int MAX_GATHERED_RUN_LENGTH = 256;

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

        // the sizes of the values, which the reader would otherwise compute from the views
        long valueBytes = viewBlock.getValueBytes();
        long outOfLineBytes = viewBlock.getOutOfLineBytes();
        sliceOutput.appendLong(valueBytes);
        sliceOutput.appendLong(outOfLineBytes);

        byte[] views = viewBlock.getRawViews();
        byte[][] buffers = viewBlock.getRawBuffers();
        int start = arrayBase * VIEW_SIZE;
        int end = start + positionCount * VIEW_SIZE;

        if (outOfLineBytes == 0) {
            // only inline values, which have no buffer location to update
            sliceOutput.appendInt(0);
            sliceOutput.writeBytes(views, start, end - start);
            return;
        }

        // Only the bytes of a buffer that views of the block may reference are written, so the views stay valid and
        // the unused end of the last buffer of a builder is not written. A value repeated by a builder is counted once
        // for every view, so the block is not compacted, which would copy the value for every view.
        if (viewBlock.getUsedBufferBytes() - outOfLineBytes <= outOfLineBytes / MAX_UNREFERENCED_BYTES_DIVISOR) {
            int[] bufferLengths = viewBlock.getRawBufferLengths();
            sliceOutput.appendInt(buffers.length);
            for (int bufferLength : bufferLengths) {
                sliceOutput.appendInt(bufferLength);
            }
            sliceOutput.writeBytes(views, start, end - start);
            for (int i = 0; i < buffers.length; i++) {
                sliceOutput.writeBytes(buffers[i], 0, bufferLengths[i]);
            }
            return;
        }

        sliceOutput.appendInt(1);
        sliceOutput.appendInt(toIntExact(outOfLineBytes));
        byte[] scratch = new byte[Math.min(end - start, SCRATCH_SIZE)];
        writeCompactedViews(sliceOutput, views, start, end, scratch);
        writeReferencedBytes(sliceOutput, views, start, end, buffers, scratch);
    }

    @Override
    public long estimatedWriteSize(BlockEncodingSerde blockEncodingSerde, Block block)
    {
        VariableWidthViewBlock viewBlock = (VariableWidthViewBlock) block;
        int positionCount = viewBlock.getPositionCount();
        // a value that several views share is written once, so no more than the buffers hold is written
        return Integer.BYTES
                + encodedValiditySize(viewBlock.getRawValueIsValid(), positionCount)
                + Long.BYTES * 2L
                + Integer.BYTES * (1L + viewBlock.getRawBuffers().length)
                + VIEW_SIZE * (long) positionCount
                + Math.min(viewBlock.getOutOfLineBytes(), viewBlock.getUsedBufferBytes());
    }

    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        int positionCount = sliceInput.readInt();
        long[] valueIsValid = decodeValidityAsLongs(sliceInput, positionCount);
        long valueBytes = sliceInput.readLong();
        long outOfLineBytes = sliceInput.readLong();
        if (outOfLineBytes < 0 || valueBytes < outOfLineBytes) {
            throw new IllegalArgumentException("Invalid value sizes");
        }

        int bufferCount = sliceInput.readInt();
        if (bufferCount < 0) {
            throw new IllegalArgumentException("bufferCount is negative");
        }
        byte[][] buffers = bufferCount == 0 ? NO_BUFFERS : new byte[bufferCount][];
        for (int i = 0; i < bufferCount; i++) {
            buffers[i] = new byte[sliceInput.readInt()];
        }

        byte[] views = new byte[Math.multiplyExact(positionCount, VIEW_SIZE)];
        sliceInput.readBytes(views);
        for (byte[] buffer : buffers) {
            sliceInput.readBytes(buffer);
        }
        return new VariableWidthViewBlock(0, positionCount, views, buffers, null, valueIsValid, valueBytes, outOfLineBytes);
    }

    /// Writes the views, pointing the views of out-of-line values at consecutive offsets of a single buffer.
    private static void writeCompactedViews(SliceOutput sliceOutput, byte[] views, int start, int end, byte[] scratch)
    {
        int scratchLength = 0;
        int newOffset = 0;
        for (int view = start; view < end; view += VIEW_SIZE) {
            long lengthAndPrefix = (long) LONG_LE.get(views, view);
            long rest = (long) LONG_LE.get(views, view + Long.BYTES);
            int length = (int) lengthAndPrefix;
            if (length > INLINE_SIZE) {
                rest = (long) newOffset << Integer.SIZE;
                newOffset += length;
            }
            LONG_LE.set(scratch, scratchLength, lengthAndPrefix);
            LONG_LE.set(scratch, scratchLength + Long.BYTES, rest);
            scratchLength += VIEW_SIZE;
            if (scratchLength == scratch.length) {
                sliceOutput.writeBytes(scratch, 0, scratchLength);
                scratchLength = 0;
            }
        }
        if (scratchLength > 0) {
            sliceOutput.writeBytes(scratch, 0, scratchLength);
        }
    }

    /// Writes the out-of-line values in view order. Values that are adjacent in a buffer are written together, and
    /// short runs are gathered in `scratch`.
    private static void writeReferencedBytes(SliceOutput sliceOutput, byte[] views, int start, int end, byte[][] buffers, byte[] scratch)
    {
        int scratchLength = 0;
        byte[] run = null;
        int runStart = 0;
        int runEnd = 0;
        for (int view = start; view < end; view += VIEW_SIZE) {
            int length = length(views, view);
            if (length <= INLINE_SIZE) {
                continue;
            }
            byte[] buffer = buffers[bufferIndex(views, view)];
            int offset = bufferOffset(views, view);
            if (buffer == run && offset == runEnd) {
                runEnd += length;
                continue;
            }
            if (run != null) {
                scratchLength = writeRun(sliceOutput, run, runStart, runEnd - runStart, scratch, scratchLength);
            }
            run = buffer;
            runStart = offset;
            runEnd = offset + length;
        }
        scratchLength = writeRun(sliceOutput, run, runStart, runEnd - runStart, scratch, scratchLength);
        if (scratchLength > 0) {
            sliceOutput.writeBytes(scratch, 0, scratchLength);
        }
    }

    private static int writeRun(SliceOutput sliceOutput, byte[] run, int runStart, int runLength, byte[] scratch, int scratchLength)
    {
        if (runLength <= MAX_GATHERED_RUN_LENGTH && runLength <= scratch.length) {
            if (scratchLength + runLength > scratch.length) {
                sliceOutput.writeBytes(scratch, 0, scratchLength);
                scratchLength = 0;
            }
            System.arraycopy(run, runStart, scratch, scratchLength, runLength);
            return scratchLength + runLength;
        }
        if (scratchLength > 0) {
            sliceOutput.writeBytes(scratch, 0, scratchLength);
        }
        sliceOutput.writeBytes(run, runStart, runLength);
        return 0;
    }
}
