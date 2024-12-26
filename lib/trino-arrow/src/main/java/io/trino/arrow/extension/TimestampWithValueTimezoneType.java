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
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;

public class TimestampWithValueTimezoneType
        extends ArrowType.ExtensionType
{
    private final int precision;

    public TimestampWithValueTimezoneType(int precision)
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
        return "trino.timestamptz";
    }

    @Override
    public boolean extensionEquals(ExtensionType other)
    {
        if (other instanceof TimestampWithValueTimezoneType timestampWithValueTimezoneType) {
            return this.precision == timestampWithValueTimezoneType.precision;
        }

        return false;
    }

    @Override
    public String serialize()
    {
        return String.valueOf(precision);
    }

    @Override
    public ArrowType deserialize(ArrowType storageType, String serializedData)
    {
        return new TimestampWithValueTimezoneType(Integer.parseInt(serializedData));
    }

    private record TimeStampHolder(Types.MinorType type, Class<? extends TimeStampVector> clazz)
    {
    }

    @Override
    public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator)
    {
        //for precision up to nano, use a struct of arrow timestamp (with appropriate precision) and zone id
        TimeStampHolder timestampType = switch (precision) {
            case 0 -> new TimeStampHolder(Types.MinorType.TIMESTAMPSEC, TimeStampSecVector.class);
            case 3, 12 -> new TimeStampHolder(Types.MinorType.TIMESTAMPMILLI, TimeStampMilliVector.class);
            case 6 -> new TimeStampHolder(Types.MinorType.TIMESTAMPMICRO, TimeStampMicroVector.class);
            case 9 -> new TimeStampHolder(Types.MinorType.TIMESTAMPNANO, TimeStampNanoVector.class);
            default -> throw new IllegalArgumentException("Unsupported precision: " + precision);
        };
        FieldType timeStampField = new FieldType(false, timestampType.type().getType(), null);
        StructVector structVector = new StructVector(name, allocator, fieldType, null);
        structVector.addOrGet("timestamp", timeStampField, timestampType.clazz);
        if (precision == 12) {
            FieldType picoAdjustmentField = new FieldType(false, new Int(32, false), null);
            structVector.addOrGet("pico_adjustment", picoAdjustmentField, IntVector.class);
        }
        structVector.addOrGet("zone_id", new FieldType(false, new ArrowType.Int(16, true), null), SmallIntVector.class); //TODO do we need a 4 byte int?
        return new TimestampWithValueTimezoneVector(name, allocator, structVector);
    }
}
