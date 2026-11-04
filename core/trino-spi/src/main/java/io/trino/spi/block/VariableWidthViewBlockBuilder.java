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
import java.util.IdentityHashMap;

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
import static io.trino.spi.block.VariableWidthViewBlock.NO_BUFFER_LENGTHS;
import static io.trino.spi.block.VariableWidthViewBlock.VIEW_SIZE;
import static io.trino.spi.block.VariableWidthViewBlock.bufferIndex;
import static io.trino.spi.block.VariableWidthViewBlock.bufferOffset;
import static io.trino.spi.block.VariableWidthViewBlock.copyValueBytes;
import static io.trino.spi.block.VariableWidthViewBlock.length;
import static io.trino.spi.block.VariableWidthViewBlock.setBufferIndex;
import static io.trino.spi.block.VariableWidthViewBlock.setBufferLocation;
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
///
/// [#appendRange] and [#appendPositions] can borrow the buffers of the appended block instead of copying its values:
/// only the views are copied, pointing at the buffers of the block, which become buffers of this builder and of the
/// blocks it builds. A buffer is borrowed once, however many values reference it. Appends of single values and
/// [#appendRepeated] always copy. The values in buffers smaller than [#MIN_BORROWED_BUFFER_SIZE] are copied, since
/// they are cheap to copy and many small buffers would make the builder and its blocks larger.
///
/// Without a [BorrowedBufferTracker], the buffers of a block are borrowed only when an append references at least
/// `1 / MAX_BORROWED_TO_REFERENCED_RATIO` of the bytes they hold, so the borrowed buffers retain at most
/// [#MAX_BORROWED_TO_REFERENCED_RATIO] times the bytes of the values that reference them. Later appends of the same
/// block borrow its buffers as well while they are borrowed, since that retains nothing more. The retained size of the
/// builder includes the borrowed buffers. With a tracker, the buffers of every appended block are borrowed, and the
/// tracker accounts for them instead of the builder. See [BorrowedBufferTracker].
public class VariableWidthViewBlockBuilder
        implements BlockBuilder
{
    static final int MIN_BUFFER_SIZE = 1024;
    static final int MAX_BUFFER_SIZE = 1024 * 1024;
    static final int MIN_BORROWED_BUFFER_SIZE = 64 * 1024;
    static final int MAX_BORROWED_TO_REFERENCED_RATIO = 2;

    private static final int INSTANCE_SIZE = instanceSize(VariableWidthViewBlockBuilder.class);
    private static final int SIZE_IN_BYTES_PER_POSITION = Integer.BYTES + Byte.BYTES;
    private static final Block NULL_VALUE_BLOCK = new VariableWidthViewBlock(0, 1, new byte[VIEW_SIZE], NO_BUFFERS, new long[] {0});
    // markers for the buffers of an appended block that are not mapped to a buffer of this builder
    private static final int NOT_BORROWED_YET = -2;
    private static final int COPIED = -1;

    private final BlockBuilderStatus blockBuilderStatus;
    @Nullable
    private final BorrowedBufferTracker tracker;

    private boolean initialized;
    private final int initialEntryCount;
    private final int initialBufferSize;

    private byte[] views = new byte[0];

    private byte[][] buffers = new byte[0][];
    // the number of leading bytes of each buffer that views may reference
    private int[] bufferLengths = new int[0];
    private boolean[] bufferIsBorrowed = new boolean[0];
    private int bufferCount;
    // the buffer that values are written to, which is never a borrowed buffer
    private int currentBuffer = -1;
    // The array and the length of the current buffer, which are kept in fields, so writing a value does not look up the
    // buffer. The length in bufferLengths is updated when the buffers are read.
    private byte[] writeBuffer = new byte[0];
    private int writeOffset;
    private int lastBufferSize;
    private long ownBuffersRetainedSizeInBytes;

    // the index of each borrowed buffer in buffers
    @Nullable
    private IdentityHashMap<byte[], Integer> borrowedBufferIndexes;
    private long borrowedBuffersRetainedSizeInBytes;
    // the total length of the values in borrowed buffers, counted for every view
    private long borrowedBytes;
    // maps the buffers of an appended block to buffers of this builder
    private int[] bufferMapping = new int[0];
    // the buffers of the last block whose values were borrowed, which `bufferMapping` still maps
    @Nullable
    private byte[][] mappedBuffers;

    private boolean hasNullValue;
    private boolean hasNonNullValue;
    @Nullable
    private long[] valueIsValid;

    private int positionCount;
    private long valueBytes;
    // the total length of the out-of-line values, counted for every view
    private long outOfLineBytes;

    /// @param expectedBytes expected number of bytes in values longer than [VariableWidthViewBlock#INLINE_SIZE]
    ///         bytes, used to size the first buffer
    public VariableWidthViewBlockBuilder(@Nullable BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytes)
    {
        this(blockBuilderStatus, expectedEntries, expectedBytes, null);
    }

    /// @param expectedBytes expected number of bytes in values longer than [VariableWidthViewBlock#INLINE_SIZE]
    ///         bytes, used to size the first buffer
    /// @param tracker accounts for the buffers that the builder borrows, which makes the builder borrow the buffers
    ///         of every appended block
    public VariableWidthViewBlockBuilder(@Nullable BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytes, @Nullable BorrowedBufferTracker tracker)
    {
        this.blockBuilderStatus = blockBuilderStatus;
        this.initialEntryCount = expectedEntries;
        this.initialBufferSize = clamp(expectedBytes, MIN_BUFFER_SIZE, MAX_BUFFER_SIZE);
        this.tracker = tracker;
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
        long size = INSTANCE_SIZE +
                sizeOf(views) +
                sizeOf(valueIsValid) +
                sizeOf(buffers) +
                sizeOf(bufferLengths) +
                sizeOf(bufferIsBorrowed) +
                sizeOf(bufferMapping) +
                ownBuffersRetainedSizeInBytes;
        if (tracker == null) {
            size += borrowedBuffersRetainedSizeInBytes;
        }
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
            System.arraycopy(source, sourceIndex, writeBuffer, bufferOffset, length);
            writeOutOfLineView(views, view, source, sourceIndex, length, currentBuffer, bufferOffset);
            outOfLineBytes += length;
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
        if (length > INLINE_SIZE) {
            outOfLineBytes += length;
        }
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
        if (length > INLINE_SIZE) {
            outOfLineBytes += length * (long) count;
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
        int firstView = positionCount * VIEW_SIZE;
        System.arraycopy(viewBlock.getRawViews(), (rawArrayBase + offset) * VIEW_SIZE, views, firstView, length * VIEW_SIZE);
        long addedBytes = 0;
        long addedOutOfLineBytes = 0;
        for (int view = firstView; view < firstView + length * VIEW_SIZE; view += VIEW_SIZE) {
            int valueLength = length(views, view);
            addedBytes += valueLength;
            addedOutOfLineBytes += valueLength > INLINE_SIZE ? valueLength : 0;
        }
        adoptValues(viewBlock, firstView, firstView + length * VIEW_SIZE, addedOutOfLineBytes);
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
        enforceBorrowLimit();
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
        byte[] sourceViews = viewBlock.getRawViews();
        // null positions have empty views, so they are copied like any other value
        int firstView = positionCount * VIEW_SIZE;
        long addedBytes = 0;
        long addedOutOfLineBytes = 0;
        for (int i = 0; i < length; i++) {
            int sourceView = (rawArrayBase + positions[offset + i]) * VIEW_SIZE;
            int view = firstView + i * VIEW_SIZE;
            long lengthAndPrefix = (long) LONG_LE.get(sourceViews, sourceView);
            LONG_LE.set(views, view, lengthAndPrefix);
            LONG_LE.set(views, view + Long.BYTES, (long) LONG_LE.get(sourceViews, sourceView + Long.BYTES));
            int valueLength = (int) lengthAndPrefix;
            addedBytes += valueLength;
            addedOutOfLineBytes += valueLength > INLINE_SIZE ? valueLength : 0;
        }
        adoptValues(viewBlock, firstView, firstView + length * VIEW_SIZE, addedOutOfLineBytes);
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
        enforceBorrowLimit();
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
        long newBorrowedBytes = 0;
        for (int i = 0; i < positionCount; i++) {
            int view = i * VIEW_SIZE;
            int length = length(views, view);
            valueBytes += length;
            if (length > INLINE_SIZE) {
                outOfLineBytes += length;
                if (bufferIsBorrowed[bufferIndex(views, view)]) {
                    newBorrowedBytes += length;
                }
            }
        }
        // the borrowed buffers stay borrowed
        if (tracker != null) {
            tracker.addReferencedBytes(newBorrowedBytes - borrowedBytes);
        }
        borrowedBytes = newBorrowedBytes;
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
        VariableWidthViewBlock block;
        if (bufferCount == 0) {
            block = new VariableWidthViewBlock(0, positionCount, views, NO_BUFFERS, NO_BUFFER_LENGTHS, hasNullValue ? valueIsValid : null, valueBytes, outOfLineBytes);
        }
        else {
            updateCurrentBufferLength();
            block = new VariableWidthViewBlock(
                    0,
                    positionCount,
                    views,
                    Arrays.copyOf(buffers, bufferCount),
                    Arrays.copyOf(bufferLengths, bufferCount),
                    hasNullValue ? valueIsValid : null,
                    valueBytes,
                    outOfLineBytes);
        }
        if (tracker != null) {
            // the block accounts for the buffers from now on
            handOffBorrowedBuffers();
        }
        return block;
    }

    @Override
    public BlockBuilder newBlockBuilderLike(int expectedEntries, BlockBuilderStatus blockBuilderStatus)
    {
        updateCurrentBufferLength();
        long ownBufferBytes = 0;
        for (int i = 0; i < bufferCount; i++) {
            if (!bufferIsBorrowed[i]) {
                ownBufferBytes += bufferLengths[i];
            }
        }
        long expectedBytes = positionCount == 0 ? 0 : ownBufferBytes * expectedEntries / positionCount;
        return new VariableWidthViewBlockBuilder(blockBuilderStatus, expectedEntries, (int) min(expectedBytes, MAX_BUFFER_SIZE), tracker);
    }

    @Override
    public String toString()
    {
        return "VariableWidthViewBlockBuilder{positionCount=" + positionCount + ", size=" + valueBytes + ", buffers=" + bufferCount + '}';
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
            copyValueBytes(sourceBuffer, (int) (location >>> Integer.SIZE), writeBuffer, bufferOffset, length);
            location = (currentBuffer & 0xFFFF_FFFFL) | ((long) bufferOffset << Integer.SIZE);
        }
        LONG_LE.set(views, view, lengthAndPrefix);
        LONG_LE.set(views, view + Long.BYTES, location);
        return length;
    }

    /// Points the views between `start` and `end`, which were copied from `source`, at buffers of this builder,
    /// borrowing or copying their out-of-line values, whose total length is `addedOutOfLineBytes`.
    private void adoptValues(VariableWidthViewBlock source, int start, int end, long addedOutOfLineBytes)
    {
        if (addedOutOfLineBytes == 0) {
            return;
        }
        outOfLineBytes += addedOutOfLineBytes;

        byte[][] sourceBuffers = source.getRawBuffers();
        int[] sourceBufferLengths = source.getRawBufferLengths();
        // Ranges of one block are often appended one after the other, like the elements of its arrays, so the mapping
        // of its buffers is kept while they are borrowed. It is dropped when the borrowed buffers are released.
        if (sourceBuffers != mappedBuffers) {
            // the inline values that a buffer of a reader may also hold are not counted, since copying them is not avoided
            boolean borrow = tracker != null ||
                    addedOutOfLineBytes * MAX_BORROWED_TO_REFERENCED_RATIO >= source.getUsedBufferBytes() - source.getInlineBytes();
            if (bufferMapping.length < sourceBuffers.length) {
                bufferMapping = new int[sourceBuffers.length];
            }
            for (int i = 0; i < sourceBuffers.length; i++) {
                bufferMapping[i] = borrow && sourceBuffers[i].length >= MIN_BORROWED_BUFFER_SIZE ? NOT_BORROWED_YET : COPIED;
            }
            mappedBuffers = borrow ? sourceBuffers : null;
        }
        int[] mapping = bufferMapping;

        long addedBorrowedBytes = 0;
        for (int view = start; view < end; view += VIEW_SIZE) {
            int length = length(views, view);
            if (length <= INLINE_SIZE) {
                continue;
            }
            int sourceIndex = bufferIndex(views, view);
            int index = mapping[sourceIndex];
            if (index == NOT_BORROWED_YET) {
                index = borrowBuffer(sourceBuffers[sourceIndex], sourceBufferLengths[sourceIndex]);
                mapping[sourceIndex] = index;
            }
            if (index == COPIED) {
                int bufferOffset = reserve(length);
                copyValueBytes(sourceBuffers[sourceIndex], bufferOffset(views, view), writeBuffer, bufferOffset, length);
                setBufferLocation(views, view, currentBuffer, bufferOffset);
            }
            else {
                setBufferIndex(views, view, index);
                addedBorrowedBytes += length;
            }
        }
        if (addedBorrowedBytes > 0) {
            borrowedBytes += addedBorrowedBytes;
            if (tracker != null) {
                tracker.addReferencedBytes(addedBorrowedBytes);
            }
        }
    }

    /// Adds `buffer` to the buffers of this builder unless it is already borrowed, and returns its index.
    private int borrowBuffer(byte[] buffer, int length)
    {
        if (borrowedBufferIndexes == null) {
            borrowedBufferIndexes = new IdentityHashMap<>();
        }
        Integer index = borrowedBufferIndexes.get(buffer);
        if (index != null) {
            bufferLengths[index] = max(bufferLengths[index], length);
            return index;
        }
        int newIndex = addBuffer(buffer, length, true);
        borrowedBufferIndexes.put(buffer, newIndex);
        borrowedBuffersRetainedSizeInBytes += sizeOf(buffer);
        if (tracker != null) {
            tracker.borrow(this, buffer);
        }
        return newIndex;
    }

    private void enforceBorrowLimit()
    {
        if (tracker != null && borrowedBufferIndexes != null) {
            tracker.enforceLimit();
        }
    }

    /// Stops accounting for the borrowed buffers in the tracker. Views that reference them stay valid, and later
    /// appends borrow the buffers again.
    private void handOffBorrowedBuffers()
    {
        if (borrowedBufferIndexes == null) {
            return;
        }
        for (Integer index : borrowedBufferIndexes.values()) {
            bufferIsBorrowed[index] = false;
        }
        releaseBorrowedBuffers();
    }

    private void releaseBorrowedBuffers()
    {
        if (tracker != null) {
            for (byte[] buffer : borrowedBufferIndexes.keySet()) {
                tracker.release(buffer);
            }
            tracker.addReferencedBytes(-borrowedBytes);
            tracker.builderReleased(this);
        }
        borrowedBufferIndexes = null;
        borrowedBuffersRetainedSizeInBytes = 0;
        borrowedBytes = 0;
        mappedBuffers = null;
    }

    /// Copies the values in borrowed buffers to buffers of this builder and releases the borrowed buffers.
    void copyBorrowedValues()
    {
        if (borrowedBufferIndexes == null) {
            return;
        }
        updateCurrentBufferLength();
        byte[][] oldBuffers = buffers;
        int[] oldBufferLengths = bufferLengths;
        boolean[] oldBufferIsBorrowed = bufferIsBorrowed;
        int oldBufferCount = bufferCount;
        int oldCurrentBuffer = currentBuffer;

        buffers = new byte[oldBuffers.length][];
        bufferLengths = new int[oldBuffers.length];
        bufferIsBorrowed = new boolean[oldBuffers.length];
        bufferCount = 0;
        int[] newIndexes = new int[oldBufferCount];
        for (int i = 0; i < oldBufferCount; i++) {
            newIndexes[i] = oldBufferIsBorrowed[i] ? COPIED : addBuffer(oldBuffers[i], oldBufferLengths[i], false);
        }
        currentBuffer = oldCurrentBuffer < 0 ? -1 : newIndexes[oldCurrentBuffer];

        // blocks built earlier share the views, so the views are updated in a copy
        views = Arrays.copyOf(views, views.length);
        for (int view = 0; view < positionCount * VIEW_SIZE; view += VIEW_SIZE) {
            int length = length(views, view);
            if (length <= INLINE_SIZE) {
                continue;
            }
            int oldIndex = bufferIndex(views, view);
            int newIndex = newIndexes[oldIndex];
            if (newIndex == COPIED) {
                int bufferOffset = reserve(length);
                System.arraycopy(oldBuffers[oldIndex], bufferOffset(views, view), writeBuffer, bufferOffset, length);
                setBufferLocation(views, view, currentBuffer, bufferOffset);
            }
            else {
                setBufferIndex(views, view, newIndex);
            }
        }
        releaseBorrowedBuffers();
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

    /// Reserves `length` bytes at the end of the current buffer and returns the offset of the reserved bytes.
    private int reserve(int length)
    {
        int bufferOffset = writeOffset;
        if (writeBuffer.length - bufferOffset < length) {
            newBuffer(length);
            bufferOffset = 0;
        }
        writeOffset = bufferOffset + length;
        return bufferOffset;
    }

    private void updateCurrentBufferLength()
    {
        if (currentBuffer >= 0) {
            bufferLengths[currentBuffer] = writeOffset;
        }
    }

    private void newBuffer(int minimumSize)
    {
        int size = lastBufferSize == 0 ? initialBufferSize : min(lastBufferSize * 2, MAX_BUFFER_SIZE);
        size = max(size, minimumSize);
        if (size > MAX_ARRAY_SIZE) {
            throw new IllegalArgumentException("Value is too large: " + minimumSize);
        }
        lastBufferSize = size;
        updateCurrentBufferLength();
        writeBuffer = new byte[size];
        writeOffset = 0;
        currentBuffer = addBuffer(writeBuffer, 0, false);
        ownBuffersRetainedSizeInBytes += sizeOf(writeBuffer);
    }

    private int addBuffer(byte[] buffer, int length, boolean borrowed)
    {
        if (bufferCount == buffers.length) {
            int newLength = max(4, bufferCount * 2);
            buffers = Arrays.copyOf(buffers, newLength);
            bufferLengths = Arrays.copyOf(bufferLengths, newLength);
            bufferIsBorrowed = Arrays.copyOf(bufferIsBorrowed, newLength);
        }
        buffers[bufferCount] = buffer;
        bufferLengths[bufferCount] = length;
        bufferIsBorrowed[bufferCount] = borrowed;
        return bufferCount++;
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
