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
package io.trino.parquet.reader.flat;

import io.trino.spi.block.Block;
import io.trino.spi.block.VariableWidthViewBuffer;

import java.util.List;
import java.util.Optional;

/**
 * Builds {@link io.trino.spi.block.VariableWidthViewBlock} over the bytes the values were decoded from, which are
 * referenced rather than copied. Null positions keep the zero views the buffer is created with.
 */
public final class VarcharColumnAdapter
        implements ColumnAdapter<VariableWidthViewBuffer>
{
    public static final VarcharColumnAdapter VARCHAR_ADAPTER = new VarcharColumnAdapter();

    @Override
    public VariableWidthViewBuffer createBuffer(int size)
    {
        return new VariableWidthViewBuffer(size);
    }

    @Override
    public VariableWidthViewBuffer createTemporaryBuffer(int currentOffset, int size, VariableWidthViewBuffer buffer)
    {
        return buffer.createTemporary(size);
    }

    @Override
    public void copyValue(VariableWidthViewBuffer source, int sourceIndex, VariableWidthViewBuffer destination, int destinationIndex)
    {
        destination.copyViews(source, sourceIndex, destinationIndex, 1);
    }

    @Override
    public void copyValues(VariableWidthViewBuffer source, int sourceIndex, VariableWidthViewBuffer destination, int destinationIndex, int length)
    {
        destination.copyViews(source, sourceIndex, destinationIndex, length);
    }

    @Override
    public Block createNullableBlock(long[] valueIsValid, VariableWidthViewBuffer values)
    {
        return values.build(Optional.of(valueIsValid));
    }

    @Override
    public Block createNonNullBlock(VariableWidthViewBuffer values)
    {
        return values.build(Optional.empty());
    }

    @Override
    public void decodeDictionaryIds(VariableWidthViewBuffer values, int offset, int length, int[] ids, VariableWidthViewBuffer dictionary)
    {
        values.setDictionaryValues(offset, dictionary, ids, 0, length);
    }

    @Override
    public long getSizeInBytes(VariableWidthViewBuffer values)
    {
        return values.getRetainedSizeInBytes();
    }

    @Override
    public VariableWidthViewBuffer merge(List<VariableWidthViewBuffer> buffers)
    {
        if (buffers.isEmpty()) {
            return new VariableWidthViewBuffer(0);
        }
        return VariableWidthViewBuffer.merge(buffers);
    }
}
