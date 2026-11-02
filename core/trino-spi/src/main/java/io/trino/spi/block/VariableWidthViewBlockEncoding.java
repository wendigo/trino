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
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

import java.util.Optional;

import static io.trino.spi.block.EncoderUtil.decodeValidityAsLongs;
import static io.trino.spi.block.EncoderUtil.encodeValidityAsLongs;
import static io.trino.spi.block.VariableWidthBlockEncoding.readOffsetsWithNullsCompacted;

/// Serializes [VariableWidthViewBlock] in the same format as [VariableWidthBlockEncoding]: validity, the end offsets
/// of the non-null values, and the value bytes. The views are rebuilt when the block is read.
public class VariableWidthViewBlockEncoding
        implements BlockEncoding
{
    public static final String NAME = "VARIABLE_WIDTH_VIEW";

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public Class<? extends Block> getBlockClass()
    {
        return VariableWidthViewBlock.class;
    }

    @Override
    public void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput sliceOutput, Block block)
    {
        VariableWidthViewBlock viewBlock = (VariableWidthViewBlock) block;

        int positionCount = viewBlock.getPositionCount();
        sliceOutput.appendInt(positionCount);

        long[] valueIsValid = viewBlock.getRawValueIsValid();
        int arrayBase = viewBlock.getRawArrayBase();
        encodeValidityAsLongs(sliceOutput, valueIsValid, arrayBase, positionCount);

        int[] nonNullOffsets = new int[positionCount];
        int nonNullCount = 0;
        int totalLength = 0;
        for (int position = 0; position < positionCount; position++) {
            if (valueIsValid == null || Bitmap.isSet(valueIsValid, arrayBase, position)) {
                totalLength += viewBlock.getSliceLength(position);
                nonNullOffsets[nonNullCount] = totalLength;
                nonNullCount++;
            }
        }
        sliceOutput
                .appendInt(nonNullCount)
                .writeInts(nonNullOffsets, 0, nonNullCount);

        byte[] bytes = new byte[totalLength];
        int offset = 0;
        for (int position = 0; position < positionCount; position++) {
            if (valueIsValid == null || Bitmap.isSet(valueIsValid, arrayBase, position)) {
                viewBlock.getValueBytes(position, bytes, offset);
                offset += viewBlock.getSliceLength(position);
            }
        }
        sliceOutput.writeBytes(bytes);
    }

    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        int positionCount = sliceInput.readInt();

        long[] valueIsValid = decodeValidityAsLongs(sliceInput, positionCount);

        int[] offsets = readOffsetsWithNullsCompacted(sliceInput, valueIsValid, positionCount);

        int sliceSize = offsets[offsets.length - 1];
        Slice slice = Slices.allocate(sliceSize);
        sliceInput.readBytes(slice);

        return VariableWidthViewBlock.fromOffsets(positionCount, slice, offsets, Optional.ofNullable(valueIsValid));
    }
}
