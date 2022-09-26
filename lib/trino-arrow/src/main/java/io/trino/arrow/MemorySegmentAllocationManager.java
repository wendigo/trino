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
package io.trino.arrow;

import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import org.apache.arrow.memory.AllocationManager;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.ReferenceManager;

public class MemorySegmentAllocationManager
        extends AllocationManager
{
    public static final AllocationManager.Factory FACTORY = new AllocationManager.Factory() {
        public AllocationManager create(BufferAllocator accountingAllocator, long size)
        {
            return new MemorySegmentAllocationManager(accountingAllocator, size);
        }

        public ArrowBuf empty()
        {
            return new ArrowBuf(ReferenceManager.NO_OP,
                    null,
                    0,
                    MemorySegment.globalNativeSegment().address().toRawLongValue());
        }
    };

    private final MemorySegment memorySegment;
    private final ResourceScope resourceScope;

    protected MemorySegmentAllocationManager(BufferAllocator accountingAllocator)
    {
        this(accountingAllocator, 0);
    }

    public MemorySegmentAllocationManager(BufferAllocator accountingAllocator, long size)
    {
        super(accountingAllocator);
        this.resourceScope = ResourceScope.newImplicitScope();
        this.memorySegment = MemorySegment.allocateNative(size, resourceScope);
    }

    @Override
    public long getSize()
    {
        return memorySegment.byteSize();
    }

    @Override
    protected long memoryAddress()
    {
        return memorySegment.address().toRawLongValue();
    }

    @Override
    protected void release0()
    {
        resourceScope.close();
    }
}
