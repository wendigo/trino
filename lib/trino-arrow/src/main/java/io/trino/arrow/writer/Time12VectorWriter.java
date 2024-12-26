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

import io.trino.arrow.extension.Time12Vector;
import io.trino.spi.block.Block;
import io.trino.spi.type.SqlTime;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.complex.writer.IntWriter;
import org.apache.arrow.vector.complex.writer.TimeMicroWriter;

import static io.trino.spi.type.TimeType.TIME_PICOS;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static java.lang.Math.toIntExact;

public final class Time12VectorWriter
        implements ArrowVectorWriter
{
    private final Time12Vector vector;

    public Time12VectorWriter(Time12Vector vector)
    {
        this.vector = vector;
    }

    @Override
    public void write(Block block)
    {
        BaseWriter.StructWriter structWriter = vector.getUnderlyingVector().getWriter();
        TimeMicroWriter timeMicroWriter = structWriter.timeMicro("time");
        IntWriter intWriter = structWriter.integer("pico_adjustment");

        for (int i = 0; i < block.getPositionCount(); i++) {
            SqlTime timestamp = (SqlTime) TIME_PICOS.getObject(block, i);
            if (block.isNull(i)) {
                structWriter.writeNull();
                continue;
            }

            structWriter.start();
            long micros = timestamp.getPicos() / PICOSECONDS_PER_MICROSECOND;
            int picoAdjustment = toIntExact(timestamp.getPicos() - micros);
            timeMicroWriter.writeTimeMicro(micros);
            intWriter.writeInt(picoAdjustment);
            structWriter.end();
        }
    }
}
