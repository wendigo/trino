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
/// Unused view bytes are always zero, and the view of a null position is all zeros. Equality and ordering compare the
/// length and prefix of two views before looking at the value bytes, which relies on this invariant, and copying a
/// view copies a null as an empty value.
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
    static final VarHandle LONG_LE = MethodHandles.byteArrayViewVarHandle(long[].class, LITTLE_ENDIAN);
    private static final VarHandle LONG_BE = MethodHandles.byteArrayViewVarHandle(long[].class, BIG_ENDIAN);

    // masks selecting the first `length` bytes of an inline value read as a little-endian long and int
    private static final long[] LOW_WORD_MASKS = new long[INLINE_SIZE + 1];
    private static final long[] HIGH_WORD_MASKS = new long[INLINE_SIZE + 1];

    static {
        for (int length = 0; length <= INLINE_SIZE; length++) {
            int lowBytes = Math.min(length, Long.BYTES);
            int highBytes = length - lowBytes;
            LOW_WORD_MASKS[length] = lowBytes == Long.BYTES ? -1L : (1L << (lowBytes * Byte.SIZE)) - 1;
            HIGH_WORD_MASKS[length] = (1L << (highBytes * Byte.SIZE)) - 1;
        }
    }

    static final byte[][] NO_BUFFERS = new byte[0][];

    private final int arrayOffset;
    private final int positionCount;
    private final byte[] views;
    private final byte[][] buffers;
    @Nullable
    private final long[] valueIsValid;

    private final long retainedSizeInBytes;
    // computed on first use; racing threads compute the same values
    private long sizeInBytes = -1;
    private long outOfLineBytes = -1;

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
        if (offsets[offsetsBase] < 0 || offsets[offsetsBase + positionCount] > slice.length()) {
            throw new IllegalArgumentException("offsets are outside of the slice");
        }

        byte[] views = new byte[positionCount * VIEW_SIZE];
        byte[] bytes = slice.byteArray();
        int bytesOffset = slice.byteArrayOffset();
        boolean referencesSlice = false;
        for (int position = 0; position < positionCount; position++) {
            if (valueIsValid != null && !Bitmap.isSet(valueIsValid, valueIsValidBase, position)) {
                continue;
            }
            int start = bytesOffset + offsets[offsetsBase + position];
            int length = offsets[offsetsBase + position + 1] - offsets[offsetsBase + position];
            if (length < 0) {
                throw new IllegalArgumentException("offsets are not increasing");
            }
            if (length <= INLINE_SIZE) {
                writeInlineView(views, position * VIEW_SIZE, bytes, start, length);
            }
            else {
                writeOutOfLineView(views, position * VIEW_SIZE, bytes, start, length, 0, start);
                referencesSlice = true;
            }
        }
        return new VariableWidthViewBlock(
                0,
                positionCount,
                views,
                referencesSlice ? new byte[][] {bytes} : NO_BUFFERS,
                compactBitmap(valueIsValid, valueIsValidBase, positionCount));
    }

    VariableWidthViewBlock(int arrayOffset, int positionCount, byte[] views, byte[][] buffers, @Nullable long[] valueIsValid)
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

        if (buffers == null) {
            throw new IllegalArgumentException("buffers is null");
        }
        this.buffers = buffers;

        checkBitRange(valueIsValid, arrayOffset, positionCount);
        this.valueIsValid = valueIsValid;

        long retainedSize = INSTANCE_SIZE + sizeOf(views) + sizeOf(valueIsValid) + sizeOf(buffers);
        for (byte[] buffer : buffers) {
            retainedSize += sizeOf(buffer);
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
        return Slices.wrappedBuffer(valueArray(view, length), valueOffset(view, length), length);
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
        int offset = bufferOffset(view);
        int otherOffset = other.bufferOffset(otherView);
        return Arrays.equals(
                buffer(view),
                offset + PREFIX_SIZE,
                offset + length,
                other.buffer(otherView),
                otherOffset + PREFIX_SIZE,
                otherOffset + length);
    }

    public boolean valueEquals(int position, Slice value)
    {
        checkValidPosition(position, positionCount);
        int view = view(position);
        int length = length(view);
        if (length != value.length()) {
            return false;
        }
        return value.equals(0, length, valueArray(view, length), valueOffset(view, length), length);
    }

    public boolean valueEquals(int position, byte[] value, int valueOffset, int valueLength)
    {
        checkValidPosition(position, positionCount);
        int view = view(position);
        int length = length(view);
        if (length != valueLength) {
            return false;
        }
        int offset = valueOffset(view, length);
        return Arrays.equals(valueArray(view, length), offset, offset + length, value, valueOffset, valueOffset + valueLength);
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
        if (length <= INLINE_SIZE && otherLength <= INLINE_SIZE) {
            // the same argument applies to the remaining inline bytes; when they are equal, the shorter value is a
            // prefix of the longer one
            long suffix = (long) LONG_BE.get(views, view + BUFFER_INDEX_OFFSET);
            long otherSuffix = (long) LONG_BE.get(other.views, otherView + BUFFER_INDEX_OFFSET);
            if (suffix != otherSuffix) {
                return Long.compareUnsigned(suffix, otherSuffix);
            }
            return Integer.compare(length, otherLength);
        }
        int offset = valueOffset(view, length);
        int otherOffset = other.valueOffset(otherView, otherLength);
        return Arrays.compareUnsigned(
                valueArray(view, length),
                offset,
                offset + length,
                other.valueArray(otherView, otherLength),
                otherOffset,
                otherOffset + otherLength);
    }

    public int compareValue(int position, Slice value)
    {
        checkValidPosition(position, positionCount);
        int view = view(position);
        int length = length(view);
        int offset = valueOffset(view, length);
        int valueOffset = value.byteArrayOffset();
        return Arrays.compareUnsigned(valueArray(view, length), offset, offset + length, value.byteArray(), valueOffset, valueOffset + value.length());
    }

    public long hashValue(int position)
    {
        checkValidPosition(position, positionCount);
        int view = view(position);
        int length = length(view);
        return XxHash64.hash(valueArray(view, length), valueOffset(view, length), length);
    }

    /// Copies the value at `position` to `destination` starting at `destinationOffset`.
    public void getValueBytes(int position, byte[] destination, int destinationOffset)
    {
        checkValidPosition(position, positionCount);
        int view = view(position);
        int length = length(view);
        System.arraycopy(valueArray(view, length), valueOffset(view, length), destination, destinationOffset, length);
    }

    @Override
    public long getSizeInBytes()
    {
        long sizeInBytes = this.sizeInBytes;
        if (sizeInBytes < 0) {
            computeSizes();
            sizeInBytes = this.sizeInBytes;
        }
        return sizeInBytes;
    }

    /// Returns the total length of the values stored outside of the views. A value that several views share is
    /// counted for every view.
    long getOutOfLineBytes()
    {
        long outOfLineBytes = this.outOfLineBytes;
        if (outOfLineBytes < 0) {
            computeSizes();
            outOfLineBytes = this.outOfLineBytes;
        }
        return outOfLineBytes;
    }

    private void computeSizes()
    {
        long valueBytes = 0;
        long outOfLineBytes = 0;
        for (int position = 0; position < positionCount; position++) {
            int length = length(view(position));
            valueBytes += length;
            if (length > INLINE_SIZE) {
                outOfLineBytes += length;
            }
        }
        this.outOfLineBytes = outOfLineBytes;
        // same accounting as VariableWidthBlock, so page sizes do not depend on the layout
        this.sizeInBytes = SIZE_IN_BYTES_PER_POSITION * (long) positionCount + valueBytes;
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
        for (byte[] buffer : buffers) {
            consumer.accept(buffer, sizeOf(buffer));
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

        byte[] newViews = new byte[length * VIEW_SIZE];
        int outOfLineBytes = 0;
        for (int i = 0; i < length; i++) {
            int view = view(positions[offset + i]);
            int newView = i * VIEW_SIZE;
            long lengthAndPrefix = (long) LONG_LE.get(views, view);
            LONG_LE.set(newViews, newView, lengthAndPrefix);
            LONG_LE.set(newViews, newView + Long.BYTES, (long) LONG_LE.get(views, view + Long.BYTES));
            int valueLength = (int) lengthAndPrefix;
            if (valueLength > INLINE_SIZE) {
                outOfLineBytes = Math.addExact(outOfLineBytes, valueLength);
            }
        }
        return new VariableWidthViewBlock(0, length, newViews, moveOutOfLineValues(newViews, outOfLineBytes), newValueIsValid);
    }

    /// Copies the out-of-line values referenced by `newViews` to a single new buffer and points the views at it.
    /// Values that are adjacent in a buffer of this block are copied together.
    private byte[][] moveOutOfLineValues(byte[] newViews, int outOfLineBytes)
    {
        if (outOfLineBytes == 0) {
            return NO_BUFFERS;
        }
        byte[] newBuffer = new byte[outOfLineBytes];
        int newBufferOffset = 0;
        byte[] run = null;
        int runStart = 0;
        int runEnd = 0;
        int runDestination = 0;
        for (int newView = 0; newView < newViews.length; newView += VIEW_SIZE) {
            int valueLength = length(newViews, newView);
            if (valueLength <= INLINE_SIZE) {
                continue;
            }
            byte[] buffer = buffers[bufferIndex(newViews, newView)];
            int bufferOffset = bufferOffset(newViews, newView);
            if (buffer != run || bufferOffset != runEnd) {
                if (run != null) {
                    System.arraycopy(run, runStart, newBuffer, runDestination, runEnd - runStart);
                }
                run = buffer;
                runStart = bufferOffset;
                runDestination = newBufferOffset;
            }
            runEnd = bufferOffset + valueLength;
            setBufferLocation(newViews, newView, 0, newBufferOffset);
            newBufferOffset += valueLength;
        }
        System.arraycopy(run, runStart, newBuffer, runDestination, runEnd - runStart);
        return new byte[][] {newBuffer};
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
        int start = view(positionOffset);
        byte[] newViews = Arrays.copyOfRange(views, start, start + length * VIEW_SIZE);
        int outOfLineBytes = 0;
        for (int newView = 0; newView < newViews.length; newView += VIEW_SIZE) {
            int valueLength = length(newViews, newView);
            if (valueLength > INLINE_SIZE) {
                outOfLineBytes = Math.addExact(outOfLineBytes, valueLength);
            }
        }
        long[] newValueIsValid = compactBitmap(valueIsValid, arrayOffset + positionOffset, length);
        return new VariableWidthViewBlock(0, length, newViews, moveOutOfLineValues(newViews, outOfLineBytes), newValueIsValid);
    }

    /// Returns whether the block retains no more than its views and the bytes of its values. A block wrapping the
    /// value bytes of a reader also holds the bytes of the inline values in its buffer, which is bounded by the size of
    /// the views.
    private boolean isCompact()
    {
        if (arrayOffset != 0 || views.length != positionCount * VIEW_SIZE || buffers.length > 1) {
            return false;
        }
        if (valueIsValid != null && valueIsValid.length != Bitmap.wordsForBits(positionCount)) {
            return false;
        }
        long outOfLineBytes = 0;
        long valueBytes = 0;
        for (int position = 0; position < positionCount; position++) {
            int length = length(view(position));
            valueBytes += length;
            if (length > INLINE_SIZE) {
                outOfLineBytes += length;
            }
        }
        if (buffers.length == 0) {
            return outOfLineBytes == 0;
        }
        return buffers[0].length >= outOfLineBytes && buffers[0].length <= valueBytes;
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

    byte[][] getRawBuffers()
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

    /// Writes the complete view of a value of at most [#INLINE_SIZE] bytes stored at `source[sourceOffset]`.
    /// Only the first `length` bytes are read, so the unused view bytes are zero.
    static void writeInlineView(byte[] views, int view, byte[] source, int sourceOffset, int length)
    {
        long low;
        long high;
        if (source.length - sourceOffset >= INLINE_SIZE) {
            low = (long) LONG_LE.get(source, sourceOffset) & LOW_WORD_MASKS[length];
            high = ((int) INT_LE.get(source, sourceOffset + Long.BYTES) & 0xFFFF_FFFFL) & HIGH_WORD_MASKS[length];
        }
        else {
            // fewer than INLINE_SIZE bytes remain in the array
            low = readLowWord(source, sourceOffset, length);
            high = readHighWord(source, sourceOffset, length);
        }
        LONG_LE.set(views, view, (length & 0xFFFF_FFFFL) | (low << Integer.SIZE));
        LONG_LE.set(views, view + Long.BYTES, (low >>> Integer.SIZE) | (high << Integer.SIZE));
    }

    /// Reads bytes 0 to 7 of an inline value of `length` bytes as a little-endian long, without reading past it.
    private static long readLowWord(byte[] source, int sourceOffset, int length)
    {
        long word = 0;
        for (int i = 0; i < Math.min(length, Long.BYTES); i++) {
            word |= (source[sourceOffset + i] & 0xFFL) << (i * Byte.SIZE);
        }
        return word;
    }

    /// Reads bytes 8 to 11 of an inline value of `length` bytes as a little-endian int, without reading past it.
    private static long readHighWord(byte[] source, int sourceOffset, int length)
    {
        long word = 0;
        for (int i = Long.BYTES; i < length; i++) {
            word |= (source[sourceOffset + i] & 0xFFL) << ((i - Long.BYTES) * Byte.SIZE);
        }
        return word;
    }

    /// Writes the complete view of a value longer than [#INLINE_SIZE] bytes stored at `source[sourceOffset]`,
    /// pointing at `bufferOffset` in buffer `bufferIndex`.
    static void writeOutOfLineView(byte[] views, int view, byte[] source, int sourceOffset, int length, int bufferIndex, int bufferOffset)
    {
        long prefix = (int) INT_LE.get(source, sourceOffset) & 0xFFFF_FFFFL;
        LONG_LE.set(views, view, (length & 0xFFFF_FFFFL) | (prefix << Integer.SIZE));
        LONG_LE.set(views, view + Long.BYTES, (bufferIndex & 0xFFFF_FFFFL) | ((long) bufferOffset << Integer.SIZE));
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

    private byte[] buffer(int view)
    {
        return buffers[bufferIndex(views, view)];
    }

    private int bufferOffset(int view)
    {
        return bufferOffset(views, view);
    }

    private byte[] valueArray(int view, int length)
    {
        return length <= INLINE_SIZE ? views : buffer(view);
    }

    private int valueOffset(int view, int length)
    {
        return length <= INLINE_SIZE ? view + PREFIX_OFFSET : bufferOffset(view);
    }
}
