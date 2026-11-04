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

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.spi.block.VariableWidthViewBlock.INLINE_SIZE;
import static io.trino.spi.block.VariableWidthViewBlock.INT_LE;
import static io.trino.spi.block.VariableWidthViewBlock.LONG_LE;
import static io.trino.spi.block.VariableWidthViewBlock.VIEW_SIZE;
import static io.trino.spi.block.VariableWidthViewBlock.length;
import static io.trino.spi.block.VariableWidthViewBlock.writeOutOfLineView;
import static io.trino.spi.block.VariableWidthViewBlock.writeView;
import static io.trino.spi.block.VariableWidthViewBlock.writeViews;
import static java.lang.Math.multiplyExact;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/// The views of a fixed number of positions of a [VariableWidthViewBlock] that a reader builds, in any order, over
/// value bytes it decoded or over the buffers it decoded them from, which are referenced rather than copied. A
/// position whose value is not set is null or empty. A temporary buffer shares the referenced buffers, so its views
/// can be copied to the buffer it was created from, which a reader does to expand the values around nulls.
public final class VariableWidthViewBuffer
{
    /// Values of up to this many bytes are stored in their views, so they never reference a buffer.
    public static final int INLINE_SIZE = VariableWidthViewBlock.INLINE_SIZE;

    private static final int INSTANCE_SIZE = instanceSize(VariableWidthViewBuffer.class);

    private final int positionCount;
    private final byte[] views;
    private final Buffers buffers;

    public VariableWidthViewBuffer(int positionCount)
    {
        this(positionCount, new Buffers());
    }

    private VariableWidthViewBuffer(int positionCount, Buffers buffers)
    {
        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }
        this.positionCount = positionCount;
        this.views = new byte[multiplyExact(positionCount, VIEW_SIZE)];
        this.buffers = requireNonNull(buffers, "buffers is null");
    }

    /// Creates a buffer whose views can be copied to this buffer with [#copyViews], since they reference the same
    /// buffers.
    public VariableWidthViewBuffer createTemporary(int positionCount)
    {
        return new VariableWidthViewBuffer(positionCount, buffers);
    }

    public int getPositionCount()
    {
        return positionCount;
    }

    /// Sets the values of `count` positions from `position` to the bytes at `bytes[offsets[i]]`, `lengths[i]` bytes
    /// long, which need not be contiguous. Values longer than [VariableWidthViewBlock#INLINE_SIZE] bytes reference
    /// `bytes` when `borrow` is set, so `bytes` must not change afterward, and are copied to a new buffer otherwise.
    public void setValues(int position, byte[] bytes, int[] offsets, int[] lengths, int count, boolean borrow)
    {
        checkRange(position, count);
        long outOfLineBytes = 0;
        for (int i = 0; i < count; i++) {
            int length = lengths[i];
            outOfLineBytes += length > INLINE_SIZE ? length : 0;
        }
        if (outOfLineBytes == 0) {
            // nothing references the bytes
            writeViews(views, position * VIEW_SIZE, bytes, offsets, 0, lengths, 0, count, 0);
            return;
        }
        if (borrow) {
            int bufferIndex = buffers.add(bytes);
            writeViews(views, position * VIEW_SIZE, bytes, offsets, 0, lengths, 0, count, bufferIndex);
            buffers.reference(bufferIndex, referencedEnd(offsets, lengths, count));
            return;
        }

        // the out-of-line values are copied to a buffer of their own, where they are contiguous
        byte[] copy = new byte[toIntExact(outOfLineBytes)];
        int bufferIndex = buffers.add(copy);
        buffers.reference(bufferIndex, copy.length);
        int copyOffset = 0;
        for (int i = 0; i < count; i++) {
            int length = lengths[i];
            int view = (position + i) * VIEW_SIZE;
            if (length > INLINE_SIZE) {
                System.arraycopy(bytes, offsets[i], copy, copyOffset, length);
                writeOutOfLineView(views, view, copy, copyOffset, length, bufferIndex, copyOffset);
                copyOffset += length;
            }
            else {
                writeView(views, view, bytes, offsets[i], length, 0);
            }
        }
    }

    /// Sets the values of `count` positions from `position` to contiguous values stored in `bytes`, whose value `i`
    /// starts at `bytesOffset + offsets[offsetsIndex + i]` and ends where the next one starts, in the format of
    /// [VariableWidthBlock]. Values longer than [VariableWidthViewBlock#INLINE_SIZE] bytes reference `bytes`.
    public void setContiguousValues(int position, byte[] bytes, int bytesOffset, int[] offsets, int offsetsIndex, int count)
    {
        checkRange(position, count);
        int[] starts = new int[count];
        int[] lengths = new int[count];
        for (int i = 0; i < count; i++) {
            int start = offsets[offsetsIndex + i];
            starts[i] = bytesOffset + start;
            lengths[i] = offsets[offsetsIndex + i + 1] - start;
        }
        setValues(position, bytes, starts, lengths, count, true);
    }

    /// Copies the views of `count` positions from `sourcePosition` of `source`, which must share the buffers of this
    /// buffer, to `position`.
    public void copyViews(VariableWidthViewBuffer source, int sourcePosition, int position, int count)
    {
        if (source.buffers != buffers) {
            throw new IllegalArgumentException("source does not share the buffers of this buffer");
        }
        source.checkRange(sourcePosition, count);
        checkRange(position, count);
        System.arraycopy(source.views, sourcePosition * VIEW_SIZE, views, position * VIEW_SIZE, count * VIEW_SIZE);
    }

    /// Sets the values of `count` positions from `position` to the values of `dictionary` at `ids[idsOffset + i]`.
    /// The values are not copied; the buffers of the dictionary are referenced.
    public void setDictionaryValues(int position, VariableWidthViewBuffer dictionary, int[] ids, int idsOffset, int count)
    {
        checkRange(position, count);
        int[] indexes = buffers.indexesOf(dictionary.buffers);
        byte[] dictionaryViews = dictionary.views;
        int dictionarySize = dictionary.positionCount;
        for (int i = 0; i < count; i++) {
            int id = ids[idsOffset + i];
            if (id < 0 || id >= dictionarySize) {
                throw new IndexOutOfBoundsException("id is outside of the dictionary: " + id);
            }
            int source = id * VIEW_SIZE;
            int view = (position + i) * VIEW_SIZE;
            long lengthAndPrefix = (long) LONG_LE.get(dictionaryViews, source);
            long rest = (long) LONG_LE.get(dictionaryViews, source + Long.BYTES);
            if ((int) lengthAndPrefix > INLINE_SIZE) {
                // the low int of the second word is the buffer index
                rest = (rest & 0xFFFF_FFFF_0000_0000L) | indexes[(int) rest];
            }
            LONG_LE.set(views, view, lengthAndPrefix);
            LONG_LE.set(views, view + Long.BYTES, rest);
        }
    }

    /// Creates a block of the positions of this buffer. The buffer must not be changed afterward.
    public VariableWidthViewBlock build(Optional<long[]> valueIsValid)
    {
        return new VariableWidthViewBlock(0, positionCount, views, buffers.arrays(), buffers.referencedEnds(), valueIsValid.orElse(null), -1, -1);
    }

    /// Creates a buffer with the positions of all the buffers, in order.
    public static VariableWidthViewBuffer merge(List<VariableWidthViewBuffer> sources)
    {
        if (sources.size() == 1) {
            return sources.getFirst();
        }
        int positionCount = 0;
        for (VariableWidthViewBuffer source : sources) {
            positionCount = Math.addExact(positionCount, source.positionCount);
        }
        VariableWidthViewBuffer merged = new VariableWidthViewBuffer(positionCount);
        int position = 0;
        for (VariableWidthViewBuffer source : sources) {
            int[] indexes = merged.buffers.indexesOf(source.buffers);
            byte[] sourceViews = source.views;
            int sourceEnd = source.positionCount * VIEW_SIZE;
            int target = position * VIEW_SIZE;
            System.arraycopy(sourceViews, 0, merged.views, target, sourceEnd);
            for (int view = 0; view < sourceEnd; view += VIEW_SIZE) {
                if (length(sourceViews, view) > INLINE_SIZE) {
                    int indexOffset = target + view + Long.BYTES;
                    INT_LE.set(merged.views, indexOffset, indexes[(int) INT_LE.get(merged.views, indexOffset)]);
                }
            }
            position += source.positionCount;
        }
        return merged;
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + sizeOf(views) + buffers.getRetainedSizeInBytes();
    }

    private void checkRange(int position, int count)
    {
        if (position < 0 || count < 0 || position + count > positionCount) {
            throw new IndexOutOfBoundsException("positions " + position + " to " + (position + count) + " are outside of the buffer of " + positionCount + " positions");
        }
    }

    private static int referencedEnd(int[] offsets, int[] lengths, int count)
    {
        int end = 0;
        for (int i = 0; i < count; i++) {
            if (lengths[i] > INLINE_SIZE) {
                end = Math.max(end, offsets[i] + lengths[i]);
            }
        }
        return end;
    }

    /// The arrays that views reference, and the end of the referenced bytes of each.
    private static final class Buffers
    {
        private static final int INSTANCE_SIZE = instanceSize(Buffers.class);

        private byte[][] arrays = new byte[2][];
        private int[] referencedEnds = new int[2];
        private int count;
        // the buffers last added from another list, and the indexes of their arrays in this list
        private Buffers lastAdded;
        private int lastAddedCount;
        private int[] lastAddedIndexes;

        /// Returns the index of `array`, adding it when it is not listed.
        int add(byte[] array)
        {
            // a batch references a few arrays, and usually the one added last
            for (int i = count - 1; i >= 0; i--) {
                if (arrays[i] == array) {
                    return i;
                }
            }
            if (count == arrays.length) {
                arrays = Arrays.copyOf(arrays, count * 2);
                referencedEnds = Arrays.copyOf(referencedEnds, count * 2);
            }
            arrays[count] = array;
            return count++;
        }

        void reference(int index, int end)
        {
            referencedEnds[index] = Math.max(referencedEnds[index], end);
        }

        /// Returns the index in this list of every array of `other`, adding the arrays that were not added before.
        int[] indexesOf(Buffers other)
        {
            if (other == lastAdded && other.count == lastAddedCount) {
                return lastAddedIndexes;
            }
            int[] indexes = new int[other.count];
            for (int i = 0; i < other.count; i++) {
                int index = add(other.arrays[i]);
                reference(index, other.referencedEnds[i]);
                indexes[i] = index;
            }
            lastAdded = other;
            lastAddedCount = other.count;
            lastAddedIndexes = indexes;
            return indexes;
        }

        byte[][] arrays()
        {
            return count == 0 ? VariableWidthViewBlock.NO_BUFFERS : Arrays.copyOf(arrays, count);
        }

        int[] referencedEnds()
        {
            return count == 0 ? VariableWidthViewBlock.NO_BUFFER_LENGTHS : Arrays.copyOf(referencedEnds, count);
        }

        long getRetainedSizeInBytes()
        {
            long size = INSTANCE_SIZE + sizeOf(arrays) + sizeOf(referencedEnds);
            for (int i = 0; i < count; i++) {
                size += sizeOf(arrays[i]);
            }
            return size;
        }
    }
}
