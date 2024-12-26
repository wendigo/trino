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
package io.trino.arrow.extension;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.hash.ArrowBufHasher;
import org.apache.arrow.vector.ExtensionTypeVector;
import org.apache.arrow.vector.complex.StructVector;

public class Timestamp12Vector
        extends ExtensionTypeVector<StructVector>
{
    public Timestamp12Vector(String name, BufferAllocator allocator, StructVector underlyingVector)
    {
        super(name, allocator, underlyingVector);
    }

    @Override
    public Object getObject(int index)
    {
        return getUnderlyingVector().getObject(index);
    }

    @Override
    public int hashCode(int index)
    {
        return getUnderlyingVector().hashCode(index);
    }

    @Override
    public int hashCode(int index, ArrowBufHasher hasher)
    {
        return getUnderlyingVector().hashCode(index, hasher);
    }
}
