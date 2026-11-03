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

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

import static io.airlift.slice.SizeOf.sizeOf;

/// Accounts for the value buffers that a group of [VariableWidthViewBlockBuilder]s borrow from the blocks appended to
/// them, such as the builders of all partitions of a partitioned output. The builders of a group borrow the buffers of
/// every block that is appended to them, and a buffer that several builders borrow is accounted once, by the tracker:
/// the retained size of the builders does not include borrowed buffers, and the owner of the builders adds
/// [#getRetainedSizeInBytes()] once.
///
/// A builder that receives a few values of every block keeps all of those blocks alive. When the borrowed buffers
/// exceed [#MAX_RETAINED_TO_REFERENCED_RATIO] times the length of the borrowed values that the builders reference, plus
/// [#MIN_UNREFERENCED_BYTES], every builder copies its borrowed values to its own buffers and releases the borrowed
/// buffers. When values are spread evenly over the builders, the buffers stay at about one or two times the referenced
/// values, so this bounds the retained memory without copying values in the common case.
///
/// A builder hands its borrowed buffers to the block that it builds, so the tracker accounts for the buffers only while
/// they are referenced by a builder.
///
/// This class is not thread safe. The builders of a group must be used by a single thread at a time.
public final class BorrowedBufferTracker
{
    static final int MAX_RETAINED_TO_REFERENCED_RATIO = 4;
    static final long MIN_UNREFERENCED_BYTES = 8 * 1024 * 1024;

    // the number of builders that borrow each buffer
    private final IdentityHashMap<byte[], int[]> borrowCounts = new IdentityHashMap<>();
    private final Set<VariableWidthViewBlockBuilder> builders = Collections.newSetFromMap(new IdentityHashMap<>());
    private long retainedSizeInBytes;
    private long referencedBytes;
    private int compactionCount;

    /// Returns the retained size of the distinct buffers that the builders borrow.
    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    /// Returns the total length of the borrowed values that the builders reference. A value that a builder references
    /// several times is counted every time.
    public long getReferencedBytes()
    {
        return referencedBytes;
    }

    /// Returns how many times the builders copied their borrowed values because the borrowed buffers were too large.
    public int getCompactionCount()
    {
        return compactionCount;
    }

    void borrow(VariableWidthViewBlockBuilder builder, byte[] buffer)
    {
        builders.add(builder);
        int[] count = borrowCounts.get(buffer);
        if (count == null) {
            borrowCounts.put(buffer, new int[] {1});
            retainedSizeInBytes += sizeOf(buffer);
        }
        else {
            count[0]++;
        }
    }

    void release(byte[] buffer)
    {
        int[] count = borrowCounts.get(buffer);
        if (count == null) {
            throw new IllegalStateException("buffer is not borrowed");
        }
        count[0]--;
        if (count[0] == 0) {
            borrowCounts.remove(buffer);
            retainedSizeInBytes -= sizeOf(buffer);
        }
    }

    void builderReleased(VariableWidthViewBlockBuilder builder)
    {
        builders.remove(builder);
    }

    void addReferencedBytes(long bytes)
    {
        referencedBytes += bytes;
    }

    /// Makes every builder copy its borrowed values when the borrowed buffers are too large.
    void enforceLimit()
    {
        if (retainedSizeInBytes <= MAX_RETAINED_TO_REFERENCED_RATIO * referencedBytes + MIN_UNREFERENCED_BYTES) {
            return;
        }
        compactionCount++;
        List<VariableWidthViewBlockBuilder> borrowers = new ArrayList<>(builders);
        for (VariableWidthViewBlockBuilder builder : borrowers) {
            builder.copyBorrowedValues();
        }
    }

    @Override
    public String toString()
    {
        return "BorrowedBufferTracker{buffers=" + borrowCounts.size() + ", retainedSizeInBytes=" + retainedSizeInBytes + ", referencedBytes=" + referencedBytes + '}';
    }
}
