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

import io.trino.spi.block.Block;
import org.apache.arrow.vector.TimeStampSecVector;

import static io.trino.spi.type.TimestampType.TIMESTAMP_SECONDS;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_SECOND;

public final class Timestamp0VectorWriter
        extends FixedWidthVectorWriter<TimeStampSecVector>
{
    public Timestamp0VectorWriter(TimeStampSecVector vector)
    {
        super(vector);
    }

    @Override
    protected void writeNull(int position)
    {
        vector.setNull(position);
    }

    @Override
    protected void writeValue(Block block, int position)
    {
        vector.set(position, TIMESTAMP_SECONDS.getLong(block, position) / PICOSECONDS_PER_SECOND);
    }
}
