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
package io.trino.arrow.writer;

import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlock;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.StructVector;

import java.util.List;

import static io.trino.arrow.ArrowWriters.createArrowWriter;
import static java.util.Objects.requireNonNull;

public final class RowVectorWriter
        implements ArrowVectorWriter
{
    private final RowType type;
    private final StructVector vector;

    public RowVectorWriter(StructVector vector, RowType rowType)
    {
        this.type = requireNonNull(rowType, "rowType is null");
        this.vector = vector;
    }

    @Override
    public void write(Block block)
    {
        vector.setInitialCapacity(block.getPositionCount());
        vector.allocateNew();
        List<Block> fields = RowBlock.getRowFieldsFromBlock(block);
        List<FieldVector> children = vector.getChildrenFromFields();
        for (int i = 0; i < children.size(); i++) {
            Type childType = type.getFields().get(i).getType();
            Block childBlock = fields.get(i);
            ArrowVectorWriter columnWriter = createArrowWriter(children.get(i), childType);
            columnWriter.write(childBlock);
        }
    }
}
