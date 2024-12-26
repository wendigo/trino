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

import io.trino.arrow.extension.Time12Vector;
import io.trino.arrow.extension.TimeWithValueTimezoneVector;
import io.trino.arrow.extension.Timestamp12Vector;
import io.trino.arrow.extension.TimestampWithValueTimezoneVector;
import io.trino.arrow.writer.ArrayVectorWriter;
import io.trino.arrow.writer.ArrowVectorWriter;
import io.trino.arrow.writer.BigIntVectorWriter;
import io.trino.arrow.writer.BooleanVectorWriter;
import io.trino.arrow.writer.CharVectorWriter;
import io.trino.arrow.writer.DateVectorWriter;
import io.trino.arrow.writer.DecimalVectorWriter;
import io.trino.arrow.writer.DoubleVectorWriter;
import io.trino.arrow.writer.IntegerVectorWriter;
import io.trino.arrow.writer.MapVectorWriter;
import io.trino.arrow.writer.PicosecondTimestampVectorWriter;
import io.trino.arrow.writer.RealVectorWriter;
import io.trino.arrow.writer.RowVectorWriter;
import io.trino.arrow.writer.SmallIntVectorWriter;
import io.trino.arrow.writer.Time0VectorWriter;
import io.trino.arrow.writer.Time12VectorWriter;
import io.trino.arrow.writer.Time3VectorWriter;
import io.trino.arrow.writer.Time6VectorWriter;
import io.trino.arrow.writer.Time9VectorWriter;
import io.trino.arrow.writer.TimeWithValueTimezoneWriter;
import io.trino.arrow.writer.Timestamp0VectorWriter;
import io.trino.arrow.writer.Timestamp3VectorWriter;
import io.trino.arrow.writer.Timestamp6VectorWriter;
import io.trino.arrow.writer.Timestamp9VectorWriter;
import io.trino.arrow.writer.TimestampWithValueTimezoneWriter;
import io.trino.arrow.writer.TinyIntVectorWriter;
import io.trino.arrow.writer.UuidVectorWriter;
import io.trino.arrow.writer.VarbinaryVectorWriter;
import io.trino.arrow.writer.VarcharVectorWriter;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;

public final class ArrowWriters
{
    private ArrowWriters() {}

    public static ArrowVectorWriter createArrowWriter(ValueVector valueVector, Type type)
    {
        return switch (valueVector) {
            case BitVector vector -> new BooleanVectorWriter(vector);
            case TinyIntVector vector -> new TinyIntVectorWriter(vector);
            case SmallIntVector vector -> new SmallIntVectorWriter(vector);
            case IntVector vector -> new IntegerVectorWriter(vector);
            case BigIntVector vector -> new BigIntVectorWriter(vector);
            case Float4Vector vector -> new RealVectorWriter(vector);
            case Float8Vector vector -> new DoubleVectorWriter(vector);
            case VarCharVector vector -> type instanceof CharType charType ? new CharVectorWriter(vector, charType) : new VarcharVectorWriter(vector);
            case VarBinaryVector vector -> new VarbinaryVectorWriter(vector);
            case DateDayVector vector -> new DateVectorWriter(vector);
            case DecimalVector vector -> new DecimalVectorWriter(vector, (DecimalType) type);
            case FixedSizeBinaryVector vector -> new UuidVectorWriter(vector);
            case TimeSecVector vector -> new Time0VectorWriter(vector);
            case TimeMilliVector vector -> new Time3VectorWriter(vector);
            case TimeMicroVector vector -> new Time6VectorWriter(vector);
            case TimeNanoVector vector -> new Time9VectorWriter(vector);
            case TimeStampSecVector vector -> new Timestamp0VectorWriter(vector);
            case TimeStampMilliVector vector -> new Timestamp3VectorWriter(vector);
            case TimeStampMicroVector vector -> new Timestamp6VectorWriter(vector);
            case TimeStampNanoVector vector -> new Timestamp9VectorWriter(vector);
            case MapVector vector -> new MapVectorWriter(vector, (MapType) type);
            case ListVector vector -> new ArrayVectorWriter(vector, (ArrayType) type);
            case Timestamp12Vector vector -> new PicosecondTimestampVectorWriter(vector);
            case Time12Vector vector -> new Time12VectorWriter(vector);
            case TimestampWithValueTimezoneVector vector -> new TimestampWithValueTimezoneWriter(vector, type);
            case TimeWithValueTimezoneVector vector -> new TimeWithValueTimezoneWriter(vector, type);
            case StructVector vector -> new RowVectorWriter(vector, (RowType) type);
            default -> throw new UnsupportedOperationException("Unsupported vector type: " + valueVector.getClass().getName());
        };
    }
}
