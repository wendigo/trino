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

import io.trino.arrow.extension.TimestampWithValueTimezoneVector;
import io.trino.spi.block.Block;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.SqlTimestampWithTimeZone;
import io.trino.spi.type.Type;
import org.apache.arrow.vector.complex.writer.BaseWriter.StructWriter;
import org.apache.arrow.vector.complex.writer.IntWriter;
import org.apache.arrow.vector.complex.writer.SmallIntWriter;
import org.apache.arrow.vector.complex.writer.TimeStampMilliWriter;

import static java.util.Objects.requireNonNull;

public non-sealed class TimestampWithValueTimezoneWriter
        implements ArrowVectorWriter
{
    private final TimestampWithValueTimezoneVector vector;
    private final Type type;

    public TimestampWithValueTimezoneWriter(TimestampWithValueTimezoneVector vector, Type type)
    {
        this.vector = requireNonNull(vector, "vector is null");
        this.type = requireNonNull(type, "type is null");
    }

    @Override
    public void write(Block block)
    {
        StructWriter structWriter = vector.getUnderlyingVector().getWriter();
        TimeStampMilliWriter timeStampMilliWriter = structWriter.timeStampMilli("timestamp");
        IntWriter adjustmentWriter = structWriter.integer("pico_adjustment");
        SmallIntWriter zoneWriter = structWriter.smallInt("zone_id");
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (block.isNull(i)) {
                structWriter.writeNull();
                continue;
            }
            structWriter.start();
            long epochMillis;
            short zoneId;
            switch (type.getObject(block, i)) {
                case LongTimestampWithTimeZone timestamp -> {
                    epochMillis = timestamp.getEpochMillis();
                    zoneId = timestamp.getTimeZoneKey();
                    adjustmentWriter.writeInt(timestamp.getPicosOfMilli());
                }
                case SqlTimestampWithTimeZone timestamp -> {
                    epochMillis = timestamp.getEpochMillis();
                    zoneId = timestamp.getTimeZoneKey().getKey();
                }
                default -> throw new IllegalArgumentException("Unexpected value type: " + type.getClass());
            }
            timeStampMilliWriter.writeTimeStampMilli(epochMillis);
            zoneWriter.writeSmallInt(zoneId);
            structWriter.end();
        }
    }
}
