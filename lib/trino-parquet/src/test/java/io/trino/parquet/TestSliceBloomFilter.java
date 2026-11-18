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
package io.trino.parquet;

import io.airlift.slice.Slice;
import org.apache.parquet.column.values.bloomfilter.BlockSplitBloomFilter;
import org.apache.parquet.io.api.Binary;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Random;

import static io.airlift.slice.Slices.wrappedBuffer;
import static java.lang.Double.longBitsToDouble;
import static java.lang.Float.intBitsToFloat;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSliceBloomFilter
{
    @Test
    public void testSliceHashes()
    {
        BlockSplitBloomFilter reference = new BlockSplitBloomFilter(1024);
        SliceBloomFilter bloomFilter = new SliceBloomFilter(reference);
        Random random = new Random(0);
        for (int length = 0; length <= 1024; length++) {
            byte[] bytes = new byte[length + 19];
            random.nextBytes(bytes);
            Slice base = wrappedBuffer(bytes, 3, length + 11);
            Slice value = base.slice(5, length);
            long expectedHash = reference.hash(Binary.fromConstantByteArray(value.getBytes()));
            assertThat(bloomFilter.hash(value)).isEqualTo(expectedHash);
            assertThat(bloomFilter.hash(base, 5, length)).isEqualTo(expectedHash);
        }
    }

    @Test
    public void testPrimitiveHashes()
    {
        BlockSplitBloomFilter reference = new BlockSplitBloomFilter(1024);
        SliceBloomFilter bloomFilter = new SliceBloomFilter(reference);
        Random random = new Random(0);
        for (int i = 0; i < 1000; i++) {
            int intValue = random.nextInt();
            long longValue = random.nextLong();
            float floatValue = intBitsToFloat(intValue);
            double doubleValue = longBitsToDouble(longValue);
            assertThat(bloomFilter.hash(intValue)).isEqualTo(reference.hash(intValue));
            assertThat(bloomFilter.hash(longValue)).isEqualTo(reference.hash(longValue));
            assertThat(bloomFilter.hash(floatValue)).isEqualTo(reference.hash(floatValue));
            assertThat(bloomFilter.hash(doubleValue)).isEqualTo(reference.hash(doubleValue));
        }
    }

    @Test
    public void testMerge()
            throws IOException
    {
        BlockSplitBloomFilter reference = new BlockSplitBloomFilter(1024);
        SliceBloomFilter first = new SliceBloomFilter(new BlockSplitBloomFilter(1024));
        SliceBloomFilter second = new SliceBloomFilter(new BlockSplitBloomFilter(1024));
        first.insertHash(first.hash(123));
        second.insertHash(second.hash(456));
        reference.insertHash(reference.hash(123));
        reference.insertHash(reference.hash(456));
        assertThat(first.canMergeFrom(second)).isTrue();
        first.merge(second);
        assertThat(first.findHash(first.hash(123))).isTrue();
        assertThat(first.findHash(first.hash(456))).isTrue();
        ByteArrayOutputStream expected = new ByteArrayOutputStream();
        reference.writeTo(expected);
        ByteArrayOutputStream actual = new ByteArrayOutputStream();
        first.writeTo(actual);
        assertThat(actual.toByteArray()).isEqualTo(expected.toByteArray());
        assertThat(first.getBitsetSize()).isEqualTo(reference.getBitsetSize());
        assertThat(first.getHashStrategy()).isEqualTo(reference.getHashStrategy());
        assertThat(first.getAlgorithm()).isEqualTo(reference.getAlgorithm());
        assertThat(first.getCompression()).isEqualTo(reference.getCompression());
    }
}
