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
package io.trino.orc.reader;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Random;

import static io.trino.orc.reader.ReaderUtils.unpackLengthNulls;
import static io.trino.spi.block.Bitmap.wordsForBits;
import static org.assertj.core.api.Assertions.assertThat;

class TestReaderUtils
{
    @Test
    void testUnpackLengthNulls()
    {
        Random random = new Random(42);
        for (int positionCount : new int[] {1, 2, 63, 64, 65, 127, 128, 129, 1000, 8192}) {
            // nulls at every position, at none, scattered, and in runs of both lengths
            assertUnpackLengthNulls(positionCount, _ -> false);
            assertUnpackLengthNulls(positionCount, _ -> true);
            for (double nullRate : new double[] {0.01, 0.1, 0.5, 0.9, 0.99}) {
                boolean[] valid = new boolean[positionCount];
                for (int i = 0; i < positionCount; i++) {
                    valid[i] = random.nextDouble() >= nullRate;
                }
                assertUnpackLengthNulls(positionCount, position -> valid[position]);
            }
            for (int runLength : new int[] {1, 2, 3, 7, 20, 64, 100}) {
                assertUnpackLengthNulls(positionCount, position -> (position / runLength) % 2 == 0);
                assertUnpackLengthNulls(positionCount, position -> (position / runLength) % 2 == 1);
                assertUnpackLengthNulls(positionCount, position -> position % runLength != 0);
                assertUnpackLengthNulls(positionCount, position -> position % runLength == 0);
            }
        }
    }

    private static void assertUnpackLengthNulls(int positionCount, java.util.function.IntPredicate isValid)
    {
        long[] valueIsValid = new long[wordsForBits(positionCount)];
        int[] expected = new int[positionCount];
        int[] values = new int[positionCount];
        int nonNullCount = 0;
        for (int position = 0; position < positionCount; position++) {
            if (isValid.test(position)) {
                valueIsValid[position / Long.SIZE] |= 1L << (position % Long.SIZE);
                int length = position + 1;
                expected[position] = length;
                values[nonNullCount] = length;
                nonNullCount++;
            }
        }
        // the lengths past the non-null ones are garbage the reader left behind
        Arrays.fill(values, nonNullCount, positionCount, -1);

        unpackLengthNulls(values, valueIsValid, positionCount, nonNullCount);

        assertThat(values).isEqualTo(expected);
    }
}
