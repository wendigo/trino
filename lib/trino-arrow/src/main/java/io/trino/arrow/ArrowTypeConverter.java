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

import io.trino.arrow.extension.PicosecondTimeType;
import io.trino.arrow.extension.PicosecondTimestampType;
import io.trino.arrow.extension.TimeWithValueTimezoneType;
import io.trino.arrow.extension.TimestampWithValueTimezoneType;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.HyperLogLogType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.apache.arrow.vector.types.pojo.FieldType.notNullable;
import static org.apache.arrow.vector.types.pojo.FieldType.nullable;

public final class ArrowTypeConverter
{
    private ArrowTypeConverter() {}

    public static Field createArrowField(String name, Type type)
    {
        return switch (type) {
            case ArrayType t -> new Field(name, new FieldType(true, new ArrowType.List(), null), List.of(
                    new Field("element", new FieldType(true, toArrowType(t.getElementType()), null), null)));
            case MapType mapType -> {
                Field entries = new Field("entries",
                        notNullable(ArrowType.Struct.INSTANCE),
                        List.of(createArrowField("key", mapType.getKeyType()), createArrowField("value", mapType.getValueType())));
                yield new Field(name, nullable(new ArrowType.Map(false)), List.of(entries));
            }
            case RowType rowType -> {
                List<Field> children = rowType.getFields().stream()
                        .map(field -> createArrowField(field.getName().orElse(""), field.getType()))
                        .collect(toImmutableList());
                yield new Field(name, nullable(ArrowType.Struct.INSTANCE), children);
            }
            default -> new Field(name, nullable(toArrowType(type)), null);
        };
    }

    private static ArrowType toArrowType(Type type)
    {
        return switch (type) {
            case BooleanType _ -> new ArrowType.Bool();
            case TinyintType _ -> new ArrowType.Int(8, true);
            case SmallintType _ -> new ArrowType.Int(16, true);
            case IntegerType _ -> new ArrowType.Int(32, true);
            case BigintType _ -> new ArrowType.Int(64, true);
            case RealType _ -> new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
            case DoubleType _ -> new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case VarcharType _, CharType _ -> new ArrowType.Utf8();
            case VarbinaryType _ -> new ArrowType.Binary();
            case DateType _ -> new ArrowType.Date(DateUnit.DAY);
            case TimeType time -> switch (time.getPrecision()) {
                case 0 -> new ArrowType.Time(TimeUnit.SECOND, 32);
                case 3 -> new ArrowType.Time(TimeUnit.MILLISECOND, 32);
                case 6 -> new ArrowType.Time(TimeUnit.MICROSECOND, 64);
                case 9 -> new ArrowType.Time(TimeUnit.NANOSECOND, 64);
                case 12 -> new PicosecondTimeType();
                default -> throw new UnsupportedOperationException("Unsupported timestamp precision: " + time.getPrecision());
            };
            case TimeWithTimeZoneType timestampWithTimezone -> new TimeWithValueTimezoneType(timestampWithTimezone.getPrecision());
            case TimestampType timestamp -> switch (timestamp.getPrecision()) {
                case 0 -> new ArrowType.Timestamp(TimeUnit.SECOND, null);
                case 3 -> new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
                case 6 -> new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
                case 9 -> new ArrowType.Timestamp(TimeUnit.NANOSECOND, null);
                case 12 -> new PicosecondTimestampType(null);
                default -> throw new UnsupportedOperationException("Unsupported timestamp precision: " + timestamp.getPrecision());
            };
            case TimestampWithTimeZoneType timestampWithTimeZone -> new TimestampWithValueTimezoneType(timestampWithTimeZone.getPrecision());
            case DecimalType decimal -> decimal.isShort() ?
                    new ArrowType.Decimal(decimal.getPrecision(), decimal.getScale(), 64) :
                    new ArrowType.Decimal(decimal.getPrecision(), decimal.getScale(), 128);
            case UuidType _ -> new ArrowType.FixedSizeBinary(16);
            case HyperLogLogType _ -> new ArrowType.Binary();
            case ArrayType _ -> new ArrowType.List();
            case MapType _ -> new ArrowType.Map(false);
            case RowType _ -> new ArrowType.Struct();
            default -> throw new UnsupportedOperationException("Unsupported type: " + type);
        };
    }
}
