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
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;

public class TimeWithValueTimezoneType
        extends ArrowType.ExtensionType
{
    private final int precision;

    public TimeWithValueTimezoneType(int precision)
    {
        this.precision = precision;
    }

    @Override
    public ArrowType storageType()
    {
        return ArrowType.Struct.INSTANCE;
    }

    @Override
    public String extensionName()
    {
        return "trino.timetz";
    }

    @Override
    public boolean extensionEquals(ExtensionType other)
    {
        if (other instanceof TimeWithValueTimezoneType timeWithValueTimezoneType) {
            return this.precision == timeWithValueTimezoneType.precision;
        }
        else {
            return false;
        }
    }

    @Override
    public String serialize()
    {
        return String.valueOf(precision);
    }

    @Override
    public ArrowType deserialize(ArrowType storageType, String serializedData)
    {
        return new TimeWithValueTimezoneType(Integer.parseInt(serializedData));
    }

    private record TimeHolder(Types.MinorType type, Class<? extends BaseFixedWidthVector> clazz) {}

    @Override
    public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator)
    {
        TimeHolder timeType = switch (precision) {
            case 0 -> new TimeHolder(Types.MinorType.TIMESEC, TimeSecVector.class);
            case 3 -> new TimeHolder(Types.MinorType.TIMEMILLI, TimeMilliVector.class);
            case 6, 12 -> new TimeHolder(Types.MinorType.TIMEMICRO, TimeMicroVector.class);
            case 9 -> new TimeHolder(Types.MinorType.TIMENANO, TimeNanoVector.class);
            default -> throw new IllegalArgumentException("Unsupported precision: " + precision);
        };
        FieldType timeField = new FieldType(false, timeType.type().getType(), null);
        StructVector structVector = new StructVector(name, allocator, fieldType, null);
        structVector.addOrGet("time", timeField, timeType.clazz());

        if (precision == 12) {
            FieldType picoAdjustmentField = new FieldType(false, new ArrowType.Int(32, false), null);
            structVector.addOrGet("pico_adjustment", picoAdjustmentField, IntVector.class);
        }

        structVector.addOrGet("offset_minutes", new FieldType(false, new ArrowType.Int(32, true), null), IntVector.class);
        return new TimeWithValueTimezoneVector(name, allocator, structVector);
    }
}
