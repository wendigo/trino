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

import io.trino.arrow.extension.PicosecondTimeVector;
import io.trino.arrow.extension.PicosecondTimestampVector;
import io.trino.arrow.extension.TimeWithValueTimezoneVector;
import io.trino.arrow.extension.TimestampWithValueTimezoneVector;
import io.trino.arrow.writer.ArrayColumnWriter;
import io.trino.arrow.writer.BigIntColumnWriter;
import io.trino.arrow.writer.BooleanColumnWriter;
import io.trino.arrow.writer.CharColumnWriter;
import io.trino.arrow.writer.DateColumnWriter;
import io.trino.arrow.writer.DecimalColumnWriter;
import io.trino.arrow.writer.DoubleColumnWriter;
import io.trino.arrow.writer.IntegerColumnWriter;
import io.trino.arrow.writer.MapColumnWriter;
import io.trino.arrow.writer.PicosecondTimeColumnWriter;
import io.trino.arrow.writer.PicosecondTimestampColumnWriter;
import io.trino.arrow.writer.RealColumnWriter;
import io.trino.arrow.writer.SmallIntColumnWriter;
import io.trino.arrow.writer.StructColumnWriter;
import io.trino.arrow.writer.TimeMicroColumnWriter;
import io.trino.arrow.writer.TimeMilliColumnWriter;
import io.trino.arrow.writer.TimeNanoColumnWriter;
import io.trino.arrow.writer.TimeSecColumnWriter;
import io.trino.arrow.writer.TimeWithValueTimezoneWriter;
import io.trino.arrow.writer.Timestamp0ColumnWriter;
import io.trino.arrow.writer.Timestamp3ColumnWriter;
import io.trino.arrow.writer.Timestamp6ColumnWriter;
import io.trino.arrow.writer.Timestamp9ColumnWriter;
import io.trino.arrow.writer.TimestampWithValueTimezoneWriter;
import io.trino.arrow.writer.TinyIntColumnWriter;
import io.trino.arrow.writer.UuidColumnWriter;
import io.trino.arrow.writer.VarbinaryColumnWriter;
import io.trino.arrow.writer.VarcharColumnWriter;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
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

    public static ArrowColumnWriter createWriter(ValueVector valueVector, Type type)
    {
        return switch (valueVector) {
            case BitVector vector -> new BooleanColumnWriter(vector);
            case TinyIntVector vector -> new TinyIntColumnWriter(vector);
            case SmallIntVector vector -> new SmallIntColumnWriter(vector);
            case IntVector vector -> new IntegerColumnWriter(vector);
            case BigIntVector vector -> new BigIntColumnWriter(vector);
            case Float4Vector vector -> new RealColumnWriter(vector);
            case Float8Vector vector -> new DoubleColumnWriter(vector);
            case VarCharVector vector -> type instanceof CharType charType ? new CharColumnWriter(vector, charType) : new VarcharColumnWriter(vector);
            case VarBinaryVector vector -> new VarbinaryColumnWriter(vector);
            case DateDayVector vector -> new DateColumnWriter(vector);
            case DecimalVector vector -> new DecimalColumnWriter(vector, (DecimalType) type);
            case FixedSizeBinaryVector vector -> new UuidColumnWriter(vector);
            case TimeSecVector vector -> new TimeSecColumnWriter(vector, (TimeType) type);
            case TimeMilliVector vector -> new TimeMilliColumnWriter(vector, (TimeType) type);
            case TimeMicroVector vector -> new TimeMicroColumnWriter(vector, (TimeType) type);
            case TimeNanoVector vector -> new TimeNanoColumnWriter(vector, (TimeType) type);
            case TimeStampSecVector vector -> new Timestamp0ColumnWriter(vector, (TimestampType) type);
            case TimeStampMilliVector vector -> new Timestamp3ColumnWriter(vector, (TimestampType) type);
            case TimeStampMicroVector vector -> new Timestamp6ColumnWriter(vector, (TimestampType) type);
            case TimeStampNanoVector vector -> new Timestamp9ColumnWriter(vector, (TimestampType) type);
            case MapVector vector -> new MapColumnWriter(vector, (MapType) type);
            case ListVector vector -> new ArrayColumnWriter(vector, (ArrayType) type);
            case PicosecondTimestampVector vector -> new PicosecondTimestampColumnWriter(vector);
            case PicosecondTimeVector vector -> new PicosecondTimeColumnWriter(vector);
            case TimestampWithValueTimezoneVector vector -> new TimestampWithValueTimezoneWriter(vector, type);
            case TimeWithValueTimezoneVector vector -> new TimeWithValueTimezoneWriter(vector, type);
            case StructVector vector -> new StructColumnWriter(vector, type);
            default -> throw new UnsupportedOperationException("Unsupported vector type: " + valueVector.getClass().getName());
        };
    }
}
