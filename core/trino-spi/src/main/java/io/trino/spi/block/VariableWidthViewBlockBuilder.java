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
import static io.trino.spi.block.VariableWidthViewBlock.INT_LE;
import static io.trino.spi.block.VariableWidthViewBlock.NO_BUFFERS;
import static io.trino.spi.block.VariableWidthViewBlock.PREFIX_SIZE;
import static io.trino.spi.block.VariableWidthViewBlock.VIEW_SIZE;
import static io.trino.spi.block.VariableWidthViewBlock.bufferIndex;
import static io.trino.spi.block.VariableWidthViewBlock.bufferOffset;
import static io.trino.spi.block.VariableWidthViewBlock.length;
import static io.trino.spi.block.VariableWidthViewBlock.setBufferLocation;
import static java.lang.Math.clamp;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
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
        int view = startEntry();
        INT_LE.set(views, view, length);
        if (length <= INLINE_SIZE) {
            source.getBytes(sourceIndex, views, view + Integer.BYTES, length);
        }
        else {
            source.getBytes(sourceIndex, views, view + Integer.BYTES, PREFIX_SIZE);
            int bufferOffset = reserve(view, length);
            source.getBytes(sourceIndex, buffers[bufferCount - 1], bufferOffset, length);
        }
        entryAdded(length, false);
        return this;
    }

    public VariableWidthViewBlockBuilder writeEntry(byte[] source, int sourceIndex, int length)
    {
        int view = startEntry();
        INT_LE.set(views, view, length);
        if (length <= INLINE_SIZE) {
            System.arraycopy(source, sourceIndex, views, view + Integer.BYTES, length);
        }
        else {
            System.arraycopy(source, sourceIndex, views, view + Integer.BYTES, PREFIX_SIZE);
            int bufferOffset = reserve(view, length);
            System.arraycopy(source, sourceIndex, buffers[bufferCount - 1], bufferOffset, length);
        }
        entryAdded(length, false);
        return this;
    }

    @Override
    public void append(ValueBlock block, int position)
    {
        VariableWidthViewBlock viewBlock = (VariableWidthViewBlock) block;
        if (viewBlock.isNull(position)) {
            appendNull();
            return;
        }
        int view = startEntry();
        int length = copyValue(viewBlock, position, view);
        entryAdded(length, false);
    }

    @Override
    public void appendRepeated(ValueBlock block, int position, int count)
    {
        VariableWidthViewBlock viewBlock = (VariableWidthViewBlock) block;
        for (int i = 0; i < count; i++) {
            append(viewBlock, position);
        }
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
        long addedBytes = 0;
        for (int i = 0; i < length; i++) {
            int view = (positionCount + i) * VIEW_SIZE;
            Arrays.fill(views, view, view + VIEW_SIZE, (byte) 0);
            if (rawValueIsValid == null || Bitmap.isSet(rawValueIsValid, rawArrayBase, offset + i)) {
                addedBytes += copyValue(viewBlock, offset + i, view);
            }
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
        long addedBytes = 0;
        for (int i = 0; i < length; i++) {
            int view = (positionCount + i) * VIEW_SIZE;
            Arrays.fill(views, view, view + VIEW_SIZE, (byte) 0);
            int position = positions[offset + i];
            if (rawValueIsValid == null || Bitmap.isSet(rawValueIsValid, rawArrayBase, position)) {
                addedBytes += copyValue(viewBlock, position, view);
            }
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
        startEntry();
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
        Slice[] slices = NO_BUFFERS;
        if (bufferCount > 0) {
            slices = new Slice[bufferCount];
            for (int i = 0; i < bufferCount; i++) {
                slices[i] = Slices.wrappedBuffer(buffers[i], 0, bufferLengths[i]);
            }
        }
        return new VariableWidthViewBlock(0, positionCount, views, slices, hasNullValue ? valueIsValid : null);
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

    /// Copies the value at `position` into the zeroed view at `view` and returns its length.
    private int copyValue(VariableWidthViewBlock source, int position, int view)
    {
        byte[] sourceViews = source.getRawViews();
        int sourceView = (source.getRawArrayBase() + position) * VIEW_SIZE;
        System.arraycopy(sourceViews, sourceView, views, view, VIEW_SIZE);
        int length = length(sourceViews, sourceView);
        if (length > INLINE_SIZE) {
            Slice sourceBuffer = source.getRawBuffers()[bufferIndex(sourceViews, sourceView)];
            int bufferOffset = reserve(view, length);
            sourceBuffer.getBytes(bufferOffset(sourceViews, sourceView), buffers[bufferCount - 1], bufferOffset, length);
        }
        return length;
    }

    /// Ensures capacity for one more entry, zeroes its view and returns the view offset.
    private int startEntry()
    {
        ensureCapacity(positionCount + 1);
        int view = positionCount * VIEW_SIZE;
        Arrays.fill(views, view, view + VIEW_SIZE, (byte) 0);
        return view;
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

    /// Reserves `length` bytes in the current buffer for the value of `view`, records the location in the view,
    /// and returns the offset of the reserved bytes in the last buffer.
    private int reserve(int view, int length)
    {
        if (bufferCount == 0 || buffers[bufferCount - 1].length - bufferLengths[bufferCount - 1] < length) {
            newBuffer(length);
        }
        int bufferIndex = bufferCount - 1;
        int bufferOffset = bufferLengths[bufferIndex];
        bufferLengths[bufferIndex] += length;
        setBufferLocation(views, view, bufferIndex, bufferOffset);
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
