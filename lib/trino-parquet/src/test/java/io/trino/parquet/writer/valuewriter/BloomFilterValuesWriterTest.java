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
package io.trino.parquet.writer.valuewriter;

import io.airlift.slice.Slice;
import org.apache.parquet.column.values.bloomfilter.AdaptiveBlockSplitBloomFilter;
import org.apache.parquet.column.values.bloomfilter.BlockSplitBloomFilter;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.io.api.Binary;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Optional;
import java.util.Random;

import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.parquet.writer.valuewriter.BloomFilterValuesWriter.createBloomFilterValuesWriter;
import static io.trino.testing.InterfaceTestUtils.assertAllMethodsOverridden;
import static org.assertj.core.api.Assertions.assertThat;

public class BloomFilterValuesWriterTest
{
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testBinaryBloomFilterCompatibility(boolean adaptive)
            throws IOException
    {
        Random random = new Random(0);
        BloomFilter expected = createBloomFilter(adaptive);
        BloomFilter actual = createBloomFilter(adaptive);
        PlainValuesWriter plainWriter = new PlainValuesWriter(1024);
        ValuesWriter writer = createBloomFilterValuesWriter(new PlainValuesWriter(1024), Optional.of(actual));
        for (int length = 0; length <= 1024; length++) {
            byte[] bytes = new byte[length + 19];
            random.nextBytes(bytes);
            Slice base = wrappedBuffer(bytes, 3, length + 11);
            Slice value = base.slice(5, length);
            expected.insertHash(expected.hash(Binary.fromConstantByteArray(value.getBytes())));
            writer.writeBytes(value);
            writer.writeBytes(base, 5, length);
            plainWriter.writeBytes(value);
            plainWriter.writeBytes(value);
            assertThat(actual.findHash(expected.hash(Binary.fromConstantByteArray(value.getBytes())))).isTrue();
        }
        ByteArrayOutputStream expectedBytes = new ByteArrayOutputStream();
        expected.writeTo(expectedBytes);
        ByteArrayOutputStream actualBytes = new ByteArrayOutputStream();
        actual.writeTo(actualBytes);
        assertThat(actualBytes.toByteArray()).isEqualTo(expectedBytes.toByteArray());
        assertThat(writer.getBytes().toByteArray()).isEqualTo(plainWriter.getBytes().toByteArray());
    }

    private static BloomFilter createBloomFilter(boolean adaptive)
    {
        if (adaptive) {
            return new AdaptiveBlockSplitBloomFilter(16 * 1024, 5, 0.01, null);
        }
        return new BlockSplitBloomFilter(16 * 1024);
    }

    @Test
    public void testAllMethodsOverridden()
    {
        assertAllMethodsOverridden(ValuesWriter.class, BloomFilterValuesWriter.class);
    }
}
