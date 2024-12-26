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

import io.trino.arrow.ArrowColumnWriter;
import io.trino.arrow.extension.TimeWithValueTimezoneVector;
import io.trino.spi.block.Block;
import io.trino.spi.type.LongTimeWithTimeZone;
import io.trino.spi.type.SqlTimeWithTimeZone;
import io.trino.spi.type.Type;
import org.apache.arrow.vector.complex.writer.BaseWriter.StructWriter;
import org.apache.arrow.vector.complex.writer.IntWriter;
import org.apache.arrow.vector.complex.writer.TimeMilliWriter;

import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MILLISECOND;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class TimeWithValueTimezoneWriter
        implements ArrowColumnWriter
{
    private final TimeWithValueTimezoneVector vector;
    private final Type type;

    public TimeWithValueTimezoneWriter(TimeWithValueTimezoneVector vector, Type type)
    {
        this.vector = requireNonNull(vector, "vector is null");
        this.type = requireNonNull(type, "type is null");
    }

    @Override
    public void write(Block block)
    {
        StructWriter structWriter = vector.getUnderlyingVector().getWriter();

        TimeMilliWriter timeMilliWriter = structWriter.timeMilli("time");
        IntWriter adjustmentWriter = structWriter.integer("pico_adjustment");
        IntWriter offsetWriter = structWriter.integer("offset_minutes");

        for (int i = 0; i < block.getPositionCount(); i++) {
            if (block.isNull(i)) {
                structWriter.writeNull();
                continue;
            }

            structWriter.start();
            switch (type.getObject(block, i)) {
                case LongTimeWithTimeZone time -> {
                    int millis = toIntExact(time.getPicoseconds() / PICOSECONDS_PER_MILLISECOND);
                    adjustmentWriter.writeInt(toIntExact(time.getPicoseconds() % PICOSECONDS_PER_MILLISECOND));
                    timeMilliWriter.writeTimeMilli(millis);
                    offsetWriter.writeInt(time.getOffsetMinutes());
                }
                case SqlTimeWithTimeZone time -> {
                    timeMilliWriter.writeTimeMilli(toIntExact(time.getPicos() / PICOSECONDS_PER_MILLISECOND));
                    offsetWriter.writeInt(time.getOffsetMinutes());
                }
                default -> throw new IllegalArgumentException("Unexpected value type: " + type.getClass());
            }
            structWriter.end();
        }
    }
}
