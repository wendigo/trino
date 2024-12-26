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
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class Timestamp12Type
        extends ArrowType.ExtensionType
{
    public static final String EXTENSION_NAME = "trino.timestamp.pico";
    private final String timezone;

    public Timestamp12Type(String timezone)
    {
        this.timezone = requireNonNull(timezone, "timezone is null");
    }

    @Override
    public String extensionName()
    {
        return EXTENSION_NAME;
    }

    @Override
    public ArrowType storageType()
    {
        return new ArrowType.Struct();
    }

    @Override
    public String serialize()
    {
        return timezone == null ? "" : timezone;
    }

    @Override
    public ArrowType deserialize(ArrowType storageType, String serializedData)
    {
        return new Timestamp12Type(serializedData.isEmpty() ? null : serializedData);
    }

    @Override
    public boolean extensionEquals(ExtensionType other)
    {
        if (!(other instanceof Timestamp12Type that)) {
            return false;
        }
        return Objects.equals(this.timezone, that.timezone);
    }

    @Override
    public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator)
    {
        StructVector vector = new StructVector(name, allocator, fieldType, null);
        vector.addOrGet("timestamp", FieldType.notNullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND, timezone)), BigIntVector.class);
        vector.addOrGet("pico_adjustment", FieldType.notNullable(new ArrowType.Int(32, true)), IntVector.class);
        return new Timestamp12Vector(name, allocator, vector);
    }

    public String getTimezone()
    {
        return timezone;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Timestamp12Type that = (Timestamp12Type) o;
        return Objects.equals(timezone, that.timezone);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(timezone);
    }
}
