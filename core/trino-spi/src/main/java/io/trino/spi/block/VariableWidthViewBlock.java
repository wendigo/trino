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
import io.airlift.slice.XxHash64;
import jakarta.annotation.Nullable;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.ObjLongConsumer;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.spi.block.Bitmap.checkBitRange;
import static io.trino.spi.block.Bitmap.compactBitmap;
import static io.trino.spi.block.Bitmap.copyBitmapAndAppendUnset;
import static io.trino.spi.block.Bitmap.set;
import static io.trino.spi.block.BlockUtil.checkArrayRange;
import static io.trino.spi.block.BlockUtil.checkValidPosition;
import static io.trino.spi.block.BlockUtil.checkValidRegion;
import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.nio.ByteOrder.LITTLE_ENDIAN;

/// A variable-width block that stores a fixed-size 16 byte view for every position.
///
/// A view starts with the value length as a little-endian int. Values of up to [#INLINE_SIZE] bytes are stored
/// inline in the remaining 12 bytes. Longer values store their first 4 bytes as a prefix, followed by the index of
/// the buffer holding the value and the offset of the value in that buffer. A value never spans buffers, so adding
/// data to a builder never copies data that was already written.
///
/// Unused view bytes are always zero. Equality and ordering compare the length and prefix of two views before
/// looking at the value bytes, which relies on this invariant.
public final class VariableWidthViewBlock
        implements ValueBlock
{
    static final int VIEW_SIZE = 16;
    static final int INLINE_SIZE = 12;
    static final int PREFIX_SIZE = 4;

    private static final int INSTANCE_SIZE = instanceSize(VariableWidthViewBlock.class);
    private static final int SIZE_IN_BYTES_PER_POSITION = Integer.BYTES + Byte.BYTES;
    private static final int PREFIX_OFFSET = 4;
    private static final int BUFFER_INDEX_OFFSET = 8;
    private static final int BUFFER_OFFSET_OFFSET = 12;

    static final VarHandle INT_LE = MethodHandles.byteArrayViewVarHandle(int[].class, LITTLE_ENDIAN);
    private static final VarHandle INT_BE = MethodHandles.byteArrayViewVarHandle(int[].class, BIG_ENDIAN);
    private static final VarHandle LONG_LE = MethodHandles.byteArrayViewVarHandle(long[].class, LITTLE_ENDIAN);

    static final Slice[] NO_BUFFERS = new Slice[0];

    private final int arrayOffset;
    private final int positionCount;
    private final byte[] views;
    private final Slice viewsSlice;
    private final Slice[] buffers;
    @Nullable
    private final long[] valueIsValid;

    private final long retainedSizeInBytes;
    private long sizeInBytes = -1;

    /// Creates a block from contiguous value bytes and offsets, in the format of [VariableWidthBlock].
    /// The value bytes are not copied. Values longer than [#INLINE_SIZE] bytes reference `slice`.
    public static VariableWidthViewBlock fromOffsets(int positionCount, Slice slice, int[] offsets, Optional<long[]> valueIsValid)
    {
        return fromOffsets(positionCount, slice, offsets, 0, valueIsValid.orElse(null), 0);
    }

    /// Creates a block with the same values as `block`. Value bytes are not copied.
    public static VariableWidthViewBlock fromVariableWidthBlock(VariableWidthBlock block)
    {
        return fromOffsets(
                block.getPositionCount(),
                block.getRawSlice(),
                block.getRawOffsets(),
                block.getRawArrayBase(),
                block.getRawValueIsValid(),
                block.getRawArrayBase());
    }

    private static VariableWidthViewBlock fromOffsets(int positionCount, Slice slice, int[] offsets, int offsetsBase, @Nullable long[] valueIsValid, int valueIsValidBase)
    {
        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }
        if (offsets.length - offsetsBase < positionCount + 1) {
            throw new IllegalArgumentException("offsets length is less than positionCount");
        }
        checkBitRange(valueIsValid, valueIsValidBase, positionCount);

        byte[] views = new byte[positionCount * VIEW_SIZE];
        boolean referencesSlice = false;
        for (int position = 0; position < positionCount; position++) {
            if (valueIsValid != null && !Bitmap.isSet(valueIsValid, valueIsValidBase, position)) {
                continue;
            }
            int offset = offsets[offsetsBase + position];
            int length = offsets[offsetsBase + position + 1] - offset;
            int view = position * VIEW_SIZE;
            INT_LE.set(views, view, length);
            if (length <= INLINE_SIZE) {
                slice.getBytes(offset, views, view + PREFIX_OFFSET, length);
            }
            else {
                slice.getBytes(offset, views, view + PREFIX_OFFSET, PREFIX_SIZE);
                INT_LE.set(views, view + BUFFER_INDEX_OFFSET, 0);
                INT_LE.set(views, view + BUFFER_OFFSET_OFFSET, offset);
                referencesSlice = true;
            }
        }
        return new VariableWidthViewBlock(
                0,
                positionCount,
                views,
                referencesSlice ? new Slice[] {slice} : NO_BUFFERS,
                compactBitmap(valueIsValid, valueIsValidBase, positionCount));
    }

    VariableWidthViewBlock(int arrayOffset, int positionCount, byte[] views, Slice[] buffers, @Nullable long[] valueIsValid)
    {
        if (arrayOffset < 0) {
            throw new IllegalArgumentException("arrayOffset is negative");
        }
        this.arrayOffset = arrayOffset;
        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }
        this.positionCount = positionCount;

        if ((long) views.length < (long) (arrayOffset + positionCount) * VIEW_SIZE) {
            throw new IllegalArgumentException("views length is less than positionCount");
        }
        this.views = views;
        this.viewsSlice = Slices.wrappedBuffer(views);

        if (buffers == null) {
            throw new IllegalArgumentException("buffers is null");
        }
        this.buffers = buffers;

        checkBitRange(valueIsValid, arrayOffset, positionCount);
        this.valueIsValid = valueIsValid;

        long retainedSize = INSTANCE_SIZE + sizeOf(views) + sizeOf(valueIsValid) + sizeOf(buffers);
        for (Slice buffer : buffers) {
            retainedSize += buffer.getRetainedSize();
        }
        this.retainedSizeInBytes = retainedSize;
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    public int getSliceLength(int position)
    {
        checkValidPosition(position, positionCount);
        return length(view(position));
    }

    public Slice getSlice(int position)
    {
        checkValidPosition(position, positionCount);
        int view = view(position);
        int length = length(view);
        return valueSlice(view, length).slice(valueOffset(view, length), length);
    }

    /// Returns whether the value at `position` equals the value at `otherPosition` in `other`.
    /// Null positions compare as empty values.
    public boolean valueEquals(int position, VariableWidthViewBlock other, int otherPosition)
    {
        checkValidPosition(position, positionCount);
        checkValidPosition(otherPosition, other.positionCount);
        int view = view(position);
        int otherView = other.view(otherPosition);

        // length and prefix
        if ((long) LONG_LE.get(views, view) != (long) LONG_LE.get(other.views, otherView)) {
            return false;
        }
        int length = length(view);
        if (length <= INLINE_SIZE) {
            return (long) LONG_LE.get(views, view + BUFFER_INDEX_OFFSET) == (long) LONG_LE.get(other.views, otherView + BUFFER_INDEX_OFFSET);
        }
        return buffer(view).equals(
                bufferOffset(view) + PREFIX_SIZE,
                length - PREFIX_SIZE,
                other.buffer(otherView),
                other.bufferOffset(otherView) + PREFIX_SIZE,
                length - PREFIX_SIZE);
    }

    public boolean valueEquals(int position, Slice value)
    {
        checkValidPosition(position, positionCount);
        int view = view(position);
        int length = length(view);
        if (length != value.length()) {
            return false;
        }
        return valueSlice(view, length).equals(valueOffset(view, length), length, value, 0, length);
    }

    public boolean valueEquals(int position, byte[] value, int valueOffset, int valueLength)
    {
        checkValidPosition(position, positionCount);
        int view = view(position);
        int length = length(view);
        if (length != valueLength) {
            return false;
        }
        return valueSlice(view, length).equals(valueOffset(view, length), length, value, valueOffset, valueLength);
    }

    /// Compares the value at `position` with the value at `otherPosition` in `other`, using the unsigned byte order
    /// of [Slice#compareTo(Slice)].
    public int compareValue(int position, VariableWidthViewBlock other, int otherPosition)
    {
        checkValidPosition(position, positionCount);
        checkValidPosition(otherPosition, other.positionCount);
        int view = view(position);
        int otherView = other.view(otherPosition);

        // Unused prefix bytes are zero, so when the prefixes differ the first difference is either a real byte
        // difference or the end of the shorter value, and the big-endian unsigned order of the prefixes is the order
        // of the values.
        int prefix = (int) INT_BE.get(views, view + PREFIX_OFFSET);
        int otherPrefix = (int) INT_BE.get(other.views, otherView + PREFIX_OFFSET);
        if (prefix != otherPrefix) {
            return Integer.compareUnsigned(prefix, otherPrefix);
        }

        int length = length(view);
        int otherLength = other.length(otherView);
        return valueSlice(view, length).compareTo(
                valueOffset(view, length),
                length,
                other.valueSlice(otherView, otherLength),
                other.valueOffset(otherView, otherLength),
                otherLength);
    }

    public int compareValue(int position, Slice value)
    {
        checkValidPosition(position, positionCount);
        int view = view(position);
        int length = length(view);
        return valueSlice(view, length).compareTo(valueOffset(view, length), length, value, 0, value.length());
    }

    public long hashValue(int position)
    {
        checkValidPosition(position, positionCount);
        int view = view(position);
        int length = length(view);
        return XxHash64.hash(valueSlice(view, length), valueOffset(view, length), length);
    }

    /// Copies the value at `position` to `destination` starting at `destinationOffset`.
    public void getValueBytes(int position, byte[] destination, int destinationOffset)
    {
        checkValidPosition(position, positionCount);
        int view = view(position);
        int length = length(view);
        valueSlice(view, length).getBytes(valueOffset(view, length), destination, destinationOffset, length);
    }

    @Override
    public long getSizeInBytes()
    {
        long sizeInBytes = this.sizeInBytes;
        if (sizeInBytes < 0) {
            sizeInBytes = getRegionSizeInBytes(0, positionCount);
            this.sizeInBytes = sizeInBytes;
        }
        return sizeInBytes;
    }

    @Override
    public long getRegionSizeInBytes(int position, int length)
    {
        checkValidRegion(positionCount, position, length);
        // same accounting as VariableWidthBlock, so page sizes do not depend on the layout
        long size = SIZE_IN_BYTES_PER_POSITION * (long) length;
        for (int i = position; i < position + length; i++) {
            size += length(view(i));
        }
        return size;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    @Override
    public long getEstimatedDataSizeForStats(int position)
    {
        return isNull(position) ? 0 : getSliceLength(position);
    }

    @Override
    public void retainedBytesForEachPart(ObjLongConsumer<Object> consumer)
    {
        consumer.accept(views, sizeOf(views));
        for (Slice buffer : buffers) {
            consumer.accept(buffer, buffer.getRetainedSize());
        }
        if (valueIsValid != null) {
            consumer.accept(valueIsValid, sizeOf(valueIsValid));
        }
        consumer.accept(this, INSTANCE_SIZE);
    }

    @Override
    public boolean mayHaveNull()
    {
        return valueIsValid != null;
    }

    @Override
    public boolean hasNull()
    {
        return Bitmap.hasUnsetBit(valueIsValid, arrayOffset, positionCount);
    }

    @Override
    public boolean isNull(int position)
    {
        if (!mayHaveNull()) {
            return false;
        }
        checkValidPosition(position, positionCount);
        return !Bitmap.isSet(valueIsValid, arrayOffset, position);
    }

    @Override
    public VariableWidthViewBlock getSingleValueBlock(int position)
    {
        if (isNull(position)) {
            return new VariableWidthViewBlock(0, 1, new byte[VIEW_SIZE], NO_BUFFERS, new long[] {0});
        }
        return copyPositions(new int[] {position}, 0, 1);
    }

    @Override
    public VariableWidthViewBlock copyPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);
        for (int i = offset; i < offset + length; i++) {
            checkValidPosition(positions[i], positionCount);
        }

        long[] newValueIsValid = null;
        if (valueIsValid != null) {
            newValueIsValid = new long[Bitmap.wordsForBits(length)];
            for (int i = 0; i < length; i++) {
                if (Bitmap.isSet(valueIsValid, arrayOffset, positions[offset + i])) {
                    set(newValueIsValid, 0, i);
                }
            }
            if (!Bitmap.hasUnsetBit(newValueIsValid, 0, length)) {
                newValueIsValid = null;
            }
        }

        int outOfLineBytes = 0;
        for (int i = 0; i < length; i++) {
            int valueLength = length(view(positions[offset + i]));
            if (valueLength > INLINE_SIZE) {
                outOfLineBytes = Math.addExact(outOfLineBytes, valueLength);
            }
        }

        byte[] newViews = new byte[length * VIEW_SIZE];
        byte[] newBuffer = new byte[outOfLineBytes];
        int newBufferOffset = 0;
        for (int i = 0; i < length; i++) {
            int view = view(positions[offset + i]);
            int newView = i * VIEW_SIZE;
            System.arraycopy(views, view, newViews, newView, VIEW_SIZE);
            int valueLength = length(view);
            if (valueLength > INLINE_SIZE) {
                buffer(view).getBytes(bufferOffset(view), newBuffer, newBufferOffset, valueLength);
                INT_LE.set(newViews, newView + BUFFER_INDEX_OFFSET, 0);
                INT_LE.set(newViews, newView + BUFFER_OFFSET_OFFSET, newBufferOffset);
                newBufferOffset += valueLength;
            }
        }
        Slice[] newBuffers = outOfLineBytes == 0 ? NO_BUFFERS : new Slice[] {Slices.wrappedBuffer(newBuffer)};
        return new VariableWidthViewBlock(0, length, newViews, newBuffers, newValueIsValid);
    }

    @Override
    public VariableWidthViewBlock getRegion(int positionOffset, int length)
    {
        checkValidRegion(positionCount, positionOffset, length);
        return new VariableWidthViewBlock(arrayOffset + positionOffset, length, views, buffers, valueIsValid);
    }

    @Override
    public VariableWidthViewBlock copyRegion(int positionOffset, int length)
    {
        checkValidRegion(positionCount, positionOffset, length);
        if (positionOffset == 0 && length == positionCount && isCompact()) {
            return this;
        }
        int[] positions = new int[length];
        for (int i = 0; i < length; i++) {
            positions[i] = positionOffset + i;
        }
        return copyPositions(positions, 0, length);
    }

    private boolean isCompact()
    {
        if (arrayOffset != 0 || views.length != positionCount * VIEW_SIZE || buffers.length > 1) {
            return false;
        }
        if (valueIsValid != null && valueIsValid.length != Bitmap.wordsForBits(positionCount)) {
            return false;
        }
        long outOfLineBytes = 0;
        for (int position = 0; position < positionCount; position++) {
            int length = length(view(position));
            if (length > INLINE_SIZE) {
                outOfLineBytes += length;
            }
        }
        if (buffers.length == 0) {
            return outOfLineBytes == 0;
        }
        Slice buffer = buffers[0];
        return buffer.length() == outOfLineBytes && buffer.byteArrayOffset() == 0 && buffer.byteArray().length == outOfLineBytes;
    }

    @Override
    public VariableWidthViewBlock copyWithAppendedNull()
    {
        long[] newValueIsValid = copyBitmapAndAppendUnset(valueIsValid, arrayOffset, positionCount);
        int end = (arrayOffset + positionCount) * VIEW_SIZE;
        byte[] newViews = Arrays.copyOf(views, end + VIEW_SIZE);
        Arrays.fill(newViews, end, end + VIEW_SIZE, (byte) 0);
        return new VariableWidthViewBlock(arrayOffset, positionCount + 1, newViews, buffers, newValueIsValid);
    }

    @Override
    public VariableWidthViewBlock getUnderlyingValueBlock()
    {
        return this;
    }

    @Override
    public Optional<Bitmap> getValidityBitmap()
    {
        if (valueIsValid == null) {
            return Optional.empty();
        }
        return Optional.of(new Bitmap(valueIsValid, arrayOffset, positionCount));
    }

    @Override
    public String toString()
    {
        return "VariableWidthViewBlock{positionCount=" + positionCount + ", buffers=" + buffers.length + '}';
    }

    int getRawArrayBase()
    {
        return arrayOffset;
    }

    byte[] getRawViews()
    {
        return views;
    }

    Slice[] getRawBuffers()
    {
        return buffers;
    }

    @Nullable
    long[] getRawValueIsValid()
    {
        return valueIsValid;
    }

    private int view(int position)
    {
        return (arrayOffset + position) * VIEW_SIZE;
    }

    static int length(byte[] views, int view)
    {
        return (int) INT_LE.get(views, view);
    }

    private int length(int view)
    {
        return length(views, view);
    }

    static int bufferIndex(byte[] views, int view)
    {
        return (int) INT_LE.get(views, view + BUFFER_INDEX_OFFSET);
    }

    static int bufferOffset(byte[] views, int view)
    {
        return (int) INT_LE.get(views, view + BUFFER_OFFSET_OFFSET);
    }

    static void setBufferLocation(byte[] views, int view, int bufferIndex, int bufferOffset)
    {
        INT_LE.set(views, view + BUFFER_INDEX_OFFSET, bufferIndex);
        INT_LE.set(views, view + BUFFER_OFFSET_OFFSET, bufferOffset);
    }

    private Slice buffer(int view)
    {
        return buffers[bufferIndex(views, view)];
    }

    private int bufferOffset(int view)
    {
        return bufferOffset(views, view);
    }

    private Slice valueSlice(int view, int length)
    {
        return length <= INLINE_SIZE ? viewsSlice : buffer(view);
    }

    private int valueOffset(int view, int length)
    {
        return length <= INLINE_SIZE ? view + PREFIX_OFFSET : bufferOffset(view);
    }
}
