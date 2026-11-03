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
import jakarta.annotation.Nullable;

import java.util.Arrays;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.spi.block.Bitmap.clear;
import static io.trino.spi.block.Bitmap.copyBits;
import static io.trino.spi.block.Bitmap.hasSetBit;
import static io.trino.spi.block.Bitmap.hasUnsetBit;
import static io.trino.spi.block.Bitmap.set;
import static io.trino.spi.block.Bitmap.setBits;
import static io.trino.spi.block.BlockUtil.MAX_ARRAY_SIZE;
import static io.trino.spi.block.BlockUtil.calculateNewArraySize;
import static io.trino.spi.block.BlockUtil.checkValidArrayGrowth;
import static io.trino.spi.block.VariableWidthViewBlock.INLINE_SIZE;
import static io.trino.spi.block.VariableWidthViewBlock.LONG_LE;
import static io.trino.spi.block.VariableWidthViewBlock.NO_BUFFERS;
import static io.trino.spi.block.VariableWidthViewBlock.VIEW_SIZE;
import static io.trino.spi.block.VariableWidthViewBlock.length;
import static io.trino.spi.block.VariableWidthViewBlock.writeInlineView;
import static io.trino.spi.block.VariableWidthViewBlock.writeOutOfLineView;
import static java.lang.Math.clamp;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.checkIndex;

/// Builds [VariableWidthViewBlock]s. Values longer than [VariableWidthViewBlock#INLINE_SIZE] bytes are appended to
/// buffers that double in size up to [#MAX_BUFFER_SIZE]. When a value does not fit in the current buffer, a new
/// buffer is started, so data that was already written is never copied.
public class VariableWidthViewBlockBuilder
        implements BlockBuilder
{
    static final int MIN_BUFFER_SIZE = 1024;
    static final int MAX_BUFFER_SIZE = 1024 * 1024;

    private static final int INSTANCE_SIZE = instanceSize(VariableWidthViewBlockBuilder.class);
    private static final int SIZE_IN_BYTES_PER_POSITION = Integer.BYTES + Byte.BYTES;
    private static final Block NULL_VALUE_BLOCK = new VariableWidthViewBlock(0, 1, new byte[VIEW_SIZE], NO_BUFFERS, new long[] {0});

    private final BlockBuilderStatus blockBuilderStatus;

    private boolean initialized;
    private final int initialEntryCount;
    private final int initialBufferSize;

    private byte[] views = new byte[0];

    private byte[][] buffers = new byte[0][];
    private int[] bufferLengths = new int[0];
    private int bufferCount;
    private long buffersRetainedSizeInBytes;

    private boolean hasNullValue;
    private boolean hasNonNullValue;
    @Nullable
    private long[] valueIsValid;

    private int positionCount;
    private long valueBytes;
    private long outOfLineBytes;

    /// @param expectedBytes expected number of bytes in values longer than [VariableWidthViewBlock#INLINE_SIZE]
    ///         bytes, used to size the first buffer
    public VariableWidthViewBlockBuilder(@Nullable BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytes)
    {
        this.blockBuilderStatus = blockBuilderStatus;
        this.initialEntryCount = expectedEntries;
        this.initialBufferSize = clamp(expectedBytes, MIN_BUFFER_SIZE, MAX_BUFFER_SIZE);
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public long getSizeInBytes()
    {
        return valueBytes + SIZE_IN_BYTES_PER_POSITION * (long) positionCount;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        long size = INSTANCE_SIZE + sizeOf(views) + sizeOf(valueIsValid) + sizeOf(buffers) + sizeOf(bufferLengths) + buffersRetainedSizeInBytes;
        if (blockBuilderStatus != null) {
            size += BlockBuilderStatus.INSTANCE_SIZE;
        }
        return size;
    }

    public VariableWidthViewBlockBuilder writeEntry(Slice source)
    {
        return writeEntry(source, 0, source.length());
    }

    public VariableWidthViewBlockBuilder writeEntry(Slice source, int sourceIndex, int length)
    {
        checkFromIndexSize(sourceIndex, length, source.length());
        writeValue(source.byteArray(), source.byteArrayOffset() + sourceIndex, length);
        return this;
    }

    public VariableWidthViewBlockBuilder writeEntry(byte[] source, int sourceIndex, int length)
    {
        checkFromIndexSize(sourceIndex, length, source.length);
        writeValue(source, sourceIndex, length);
        return this;
    }

    private void writeValue(byte[] source, int sourceIndex, int length)
    {
        ensureCapacity(positionCount + 1);
        int view = positionCount * VIEW_SIZE;
        if (length <= INLINE_SIZE) {
            writeInlineView(views, view, source, sourceIndex, length);
        }
        else {
            int bufferOffset = reserve(length);
            System.arraycopy(source, sourceIndex, buffers[bufferCount - 1], bufferOffset, length);
            writeOutOfLineView(views, view, source, sourceIndex, length, bufferCount - 1, bufferOffset);
        }
        entryAdded(length, false);
    }

    @Override
    public void append(ValueBlock block, int position)
    {
        VariableWidthViewBlock viewBlock = (VariableWidthViewBlock) block;
        if (viewBlock.isNull(position)) {
            appendNull();
            return;
        }
        ensureCapacity(positionCount + 1);
        int length = copyValue(viewBlock, position, positionCount * VIEW_SIZE);
        entryAdded(length, false);
    }

    @Override
    public void appendRepeated(ValueBlock block, int position, int count)
    {
        VariableWidthViewBlock viewBlock = (VariableWidthViewBlock) block;
        if (count == 0) {
            return;
        }
        if (viewBlock.isNull(position)) {
            for (int i = 0; i < count; i++) {
                appendNull();
            }
            return;
        }
        ensureCapacity(positionCount + count);
        // the views share a single copy of an out-of-line value
        int firstView = positionCount * VIEW_SIZE;
        int length = copyValue(viewBlock, position, firstView);
        long lengthAndPrefix = (long) LONG_LE.get(views, firstView);
        long location = (long) LONG_LE.get(views, firstView + Long.BYTES);
        for (int i = 1; i < count; i++) {
            int view = firstView + i * VIEW_SIZE;
            LONG_LE.set(views, view, lengthAndPrefix);
            LONG_LE.set(views, view + Long.BYTES, location);
        }
        if (valueIsValid != null) {
            setBits(valueIsValid, 0, positionCount, count);
        }
        hasNonNullValue = true;
        entriesAdded(count, length * (long) count);
    }

    @Override
    public void appendRange(ValueBlock block, int offset, int length)
    {
        VariableWidthViewBlock viewBlock = (VariableWidthViewBlock) block;
        if (length == 0) {
            return;
        }
        ensureCapacity(positionCount + length);

        long[] rawValueIsValid = viewBlock.getRawValueIsValid();
        int rawArrayBase = viewBlock.getRawArrayBase();
        // null positions have empty views, so they are copied like any other value
        long addedBytes = 0;
        for (int i = 0; i < length; i++) {
            addedBytes += copyValue(viewBlock, offset + i, (positionCount + i) * VIEW_SIZE);
        }
        if (rawValueIsValid == null || !hasUnsetBit(rawValueIsValid, rawArrayBase + offset, length)) {
            if (valueIsValid != null) {
                setBits(valueIsValid, 0, positionCount, length);
            }
            hasNonNullValue = true;
        }
        else {
            initializeValidityForFirstNull();
            copyBits(rawValueIsValid, rawArrayBase + offset, valueIsValid, positionCount, length);
            hasNullValue = true;
            hasNonNullValue |= hasSetBit(rawValueIsValid, rawArrayBase + offset, length);
        }
        entriesAdded(length, addedBytes);
    }

    @Override
    public void appendPositions(ValueBlock block, int[] positions, int offset, int length)
    {
        VariableWidthViewBlock viewBlock = (VariableWidthViewBlock) block;
        if (length == 0) {
            return;
        }
        ensureCapacity(positionCount + length);

        long[] rawValueIsValid = viewBlock.getRawValueIsValid();
        int rawArrayBase = viewBlock.getRawArrayBase();
        // null positions have empty views, so they are copied like any other value
        long addedBytes = 0;
        for (int i = 0; i < length; i++) {
            addedBytes += copyValue(viewBlock, positions[offset + i], (positionCount + i) * VIEW_SIZE);
        }
        if (rawValueIsValid == null || !hasUnsetBit(rawValueIsValid, rawArrayBase, positions, offset, length)) {
            if (valueIsValid != null) {
                setBits(valueIsValid, 0, positionCount, length);
            }
            hasNonNullValue = true;
        }
        else {
            initializeValidityForFirstNull();
            copyBits(rawValueIsValid, rawArrayBase, positions, offset, valueIsValid, positionCount, length);
            hasNullValue = true;
            hasNonNullValue |= hasSetBit(rawValueIsValid, rawArrayBase, positions, offset, length);
        }
        entriesAdded(length, addedBytes);
    }

    @Override
    public BlockBuilder appendNull()
    {
        ensureCapacity(positionCount + 1);
        int view = positionCount * VIEW_SIZE;
        LONG_LE.set(views, view, 0L);
        LONG_LE.set(views, view + Long.BYTES, 0L);
        hasNullValue = true;
        entryAdded(0, true);
        return this;
    }

    @Override
    public void resetTo(int position)
    {
        checkIndex(position, positionCount + 1);
        positionCount = position;
        valueBytes = 0;
        outOfLineBytes = 0;
        for (int i = 0; i < positionCount; i++) {
            int length = length(views, i * VIEW_SIZE);
            valueBytes += length;
            if (length > INLINE_SIZE) {
                outOfLineBytes += length;
            }
        }
    }

    @Override
    public Block build()
    {
        if (!hasNonNullValue) {
            return RunLengthEncodedBlock.create(NULL_VALUE_BLOCK, positionCount);
        }
        return buildValueBlock();
    }

    @Override
    public VariableWidthViewBlock buildValueBlock()
    {
        byte[][] blockBuffers = bufferCount == 0 ? NO_BUFFERS : Arrays.copyOf(buffers, bufferCount);
        return new VariableWidthViewBlock(0, positionCount, views, blockBuffers, hasNullValue ? valueIsValid : null);
    }

    @Override
    public BlockBuilder newBlockBuilderLike(int expectedEntries, BlockBuilderStatus blockBuilderStatus)
    {
        long expectedBytes = positionCount == 0 ? 0 : outOfLineBytes * expectedEntries / positionCount;
        return new VariableWidthViewBlockBuilder(blockBuilderStatus, expectedEntries, (int) min(expectedBytes, MAX_BUFFER_SIZE));
    }

    @Override
    public String toString()
    {
        return "VariableWidthViewBlockBuilder{positionCount=" + positionCount + ", size=" + valueBytes + '}';
    }

    /// Copies the view of `position` to `view`, copying an out-of-line value to the buffers of this builder, and
    /// returns the length of the value.
    private int copyValue(VariableWidthViewBlock source, int position, int view)
    {
        byte[] sourceViews = source.getRawViews();
        int sourceView = (source.getRawArrayBase() + position) * VIEW_SIZE;
        long lengthAndPrefix = (long) LONG_LE.get(sourceViews, sourceView);
        long location = (long) LONG_LE.get(sourceViews, sourceView + Long.BYTES);
        int length = (int) lengthAndPrefix;
        if (length > INLINE_SIZE) {
            byte[] sourceBuffer = source.getRawBuffers()[(int) location];
            int bufferOffset = reserve(length);
            System.arraycopy(sourceBuffer, (int) (location >>> Integer.SIZE), buffers[bufferCount - 1], bufferOffset, length);
            location = ((bufferCount - 1) & 0xFFFF_FFFFL) | ((long) bufferOffset << Integer.SIZE);
        }
        LONG_LE.set(views, view, lengthAndPrefix);
        LONG_LE.set(views, view + Long.BYTES, location);
        return length;
    }

    private void entryAdded(int length, boolean isNull)
    {
        if (isNull) {
            initializeValidityForFirstNull();
            clear(valueIsValid, 0, positionCount);
        }
        else {
            if (valueIsValid != null) {
                set(valueIsValid, 0, positionCount);
            }
            hasNonNullValue = true;
        }
        entriesAdded(1, length);
    }

    private void entriesAdded(int count, long addedBytes)
    {
        positionCount += count;
        valueBytes += addedBytes;
        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(toIntExact(count * (long) SIZE_IN_BYTES_PER_POSITION + addedBytes));
        }
    }

    /// Reserves `length` bytes at the end of the buffers and returns the offset of the reserved bytes in the last
    /// buffer.
    private int reserve(int length)
    {
        if (bufferCount == 0 || buffers[bufferCount - 1].length - bufferLengths[bufferCount - 1] < length) {
            newBuffer(length);
        }
        int bufferIndex = bufferCount - 1;
        int bufferOffset = bufferLengths[bufferIndex];
        bufferLengths[bufferIndex] += length;
        outOfLineBytes += length;
        return bufferOffset;
    }

    private void newBuffer(int minimumSize)
    {
        int size = bufferCount == 0 ? initialBufferSize : min(buffers[bufferCount - 1].length * 2, MAX_BUFFER_SIZE);
        size = max(size, minimumSize);
        if (size > MAX_ARRAY_SIZE) {
            throw new IllegalArgumentException("Value is too large: " + minimumSize);
        }
        if (bufferCount == buffers.length) {
            int newLength = max(4, bufferCount * 2);
            buffers = Arrays.copyOf(buffers, newLength);
            bufferLengths = Arrays.copyOf(bufferLengths, newLength);
        }
        buffers[bufferCount] = new byte[size];
        bufferLengths[bufferCount] = 0;
        bufferCount++;
        buffersRetainedSizeInBytes += sizeOf(buffers[bufferCount - 1]);
    }

    private void ensureCapacity(int capacity)
    {
        if (views.length >= capacity * VIEW_SIZE) {
            return;
        }

        int newSize;
        if (initialized) {
            newSize = calculateNewArraySize(capacity);
        }
        else {
            checkValidArrayGrowth(capacity, initialEntryCount);
            newSize = initialEntryCount;
            initialized = true;
        }
        newSize = max(newSize, capacity);

        if (valueIsValid != null) {
            valueIsValid = Bitmap.ensureCapacity(valueIsValid, newSize);
        }
        views = Arrays.copyOf(views, Math.multiplyExact(newSize, VIEW_SIZE));
    }

    private void initializeValidityForFirstNull()
    {
        if (valueIsValid != null) {
            return;
        }
        valueIsValid = Bitmap.allocateWords(views.length / VIEW_SIZE, false);
        setBits(valueIsValid, 0, 0, positionCount);
    }
}
