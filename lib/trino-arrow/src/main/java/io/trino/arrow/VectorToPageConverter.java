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

import com.google.common.collect.ImmutableList;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.math.BigDecimal;
import java.util.List;

import static com.google.common.base.Verify.verify;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.spi.type.Decimals.encodeShortScaledValue;
import static io.trino.spi.type.Decimals.isShortDecimal;

public class VectorToPageConverter
{
    private VectorToPageConverter() {}

    public static Page convert(BufferAllocator allocator, Schema schema, List<String> columnNames, List<Type> columnTypes, ArrowRecordBatch batch)
    {
        ImmutableList.Builder<FieldVector> vectorBuilder = ImmutableList.builder();
        for (Field field : schema.getFields()) {
            vectorBuilder.add(field.createVector(allocator));
        }

        List<FieldVector> vectors = vectorBuilder.build();
        try (VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {
            VectorLoader loader = new VectorLoader(root, new CommonsCompressionFactory());
            PageBuilder pageBuilder = new PageBuilder(columnTypes);
            loader.load(batch);
            pageBuilder.declarePositions(root.getRowCount());

            for (int i = 0; i < vectors.size(); i++) {
                convertType(pageBuilder.getBlockBuilder(i), vectors.get(i), columnTypes.get(i));
            }

            root.clear();
            return pageBuilder.build();
        }
    }

    private static void convertType(BlockBuilder output, FieldVector vector, Type type)
    {
        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.isNull(i)) {
                output.appendNull();
            }
            else if (vector instanceof BigIntVector bigIntVector) {
                type.writeLong(output, bigIntVector.get(i));
            }
            else if (vector instanceof VarCharVector varCharVector) {
                type.writeSlice(output, wrappedBuffer(varCharVector.get(i)));
            }
            else if (vector instanceof Float8Vector float8Vector) {
                type.writeDouble(output, float8Vector.getValueAsDouble(i));
            }
            else if (vector instanceof DecimalVector decimalVector) {
                verify(isShortDecimal(type), "The type should be short decimal");
                DecimalType decimalType = (DecimalType) type;
                BigDecimal decimal = decimalVector.getObject(i);
                type.writeLong(output, encodeShortScaledValue(decimal, decimalType.getScale()));
            }
            else if (vector instanceof DateDayVector dateDayVector) {
                type.writeLong(output, dateDayVector.get(i));
            }
            else {
                throw new RuntimeException("Unrecognized minor type " + vector.getMinorType());
            }
        }
    }
}
