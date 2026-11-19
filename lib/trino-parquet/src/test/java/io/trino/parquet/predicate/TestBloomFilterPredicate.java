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
package io.trino.parquet.predicate;

import io.trino.parquet.BloomFilterStore;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.metadata.BlockMetadata;
import io.trino.parquet.reader.TestingParquetDataSource;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.values.bloomfilter.BlockSplitBloomFilter;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.spi.predicate.Range.range;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Float.floatToRawIntBits;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.assertj.core.api.Assertions.assertThat;

public class TestBloomFilterPredicate
{
    private static final ColumnDescriptor FIRST = column("first");
    private static final ColumnDescriptor SECOND = column("second");

    @Test
    public void testLazyHashReuse()
            throws IOException
    {
        BloomFilterPredicate predicate = predicate(Domain.multipleValues(BIGINT, List.of(11L, 22L, 33L)));
        CountingBloomFilter first = new CountingBloomFilter(11);
        assertThat(predicate.matches(store(first), List.of(FIRST))).isTrue();
        assertThat(first.hashedValues).containsExactly(11L);

        CountingBloomFilter second = new CountingBloomFilter(22);
        assertThat(predicate.matches(store(second), List.of(FIRST))).isTrue();
        assertThat(second.hashedValues).containsExactly(22L);

        CountingBloomFilter third = new CountingBloomFilter();
        assertThat(predicate.matches(store(third), List.of(FIRST))).isFalse();
        assertThat(third.hashedValues).containsExactly(33L);

        CountingBloomFilter fourth = new CountingBloomFilter(33);
        assertThat(predicate.matches(store(fourth), List.of(FIRST))).isTrue();
        assertThat(fourth.hashedValues).isEmpty();
        assertThat(predicate.matches(store(third), List.of(FIRST))).isFalse();
        assertThat(third.hashedValues).containsExactly(33L);
    }

    @Test
    public void testMissingFilterDoesNotConsumeValues()
            throws IOException
    {
        BloomFilterPredicate predicate = predicate(Domain.singleValue(BIGINT, 11L));
        assertThat(predicate.matches(new TestingBloomFilterStore(Map.of()), List.of(FIRST))).isTrue();
        CountingBloomFilter filter = new CountingBloomFilter(11);
        assertThat(predicate.matches(store(filter), List.of(FIRST))).isTrue();
        assertThat(filter.hashedValues).containsExactly(11L);
    }

    @Test
    public void testColumnSubsetsAndPredicateIsolation()
            throws IOException
    {
        BloomFilterPredicate predicate = new BloomFilterPredicate(TupleDomain.withColumnDomains(Map.of(
                FIRST, Domain.singleValue(BIGINT, 11L),
                SECOND, Domain.singleValue(BIGINT, 22L))), 256);
        CountingBloomFilter first = new CountingBloomFilter(11);
        CountingBloomFilter second = new CountingBloomFilter(22);
        TestingBloomFilterStore store = new TestingBloomFilterStore(Map.of(
                ColumnPath.get(FIRST.getPath()), first,
                ColumnPath.get(SECOND.getPath()), second));
        assertThat(predicate.matches(store, List.of(FIRST))).isTrue();
        assertThat(second.hashedValues).isEmpty();
        assertThat(predicate.matches(store, List.of(SECOND))).isTrue();
        assertThat(predicate.matches(store, List.of(FIRST, SECOND))).isTrue();
        assertThat(first.hashedValues).containsExactly(11L);
        assertThat(second.hashedValues).containsExactly(22L);

        // Another predicate on the same column must not reuse this predicate's hashes.
        assertThat(predicate(Domain.singleValue(BIGINT, 33L)).matches(store, List.of(FIRST))).isFalse();
        assertThat(first.hashedValues).containsExactly(11L, 33L);
    }

    @Test
    public void testRangeExpansion()
            throws IOException
    {
        TupleDomain<ColumnDescriptor> domain = TupleDomain.withColumnDomains(Map.of(
                FIRST, Domain.create(ValueSet.ofRanges(range(BIGINT, 11L, true, 13L, true)), false)));
        BloomFilterPredicate tooSmall = new BloomFilterPredicate(domain, 2);
        CountingBloomFilter filter = new CountingBloomFilter();
        TestingBloomFilterStore store = store(filter);
        assertThat(tooSmall.matches(store, List.of(FIRST))).isTrue();
        assertThat(store.requestedColumns).isEmpty();

        BloomFilterPredicate predicate = new BloomFilterPredicate(domain, 3);
        assertThat(predicate.matches(store, List.of(FIRST))).isFalse();
        assertThat(predicate.matches(store, List.of(FIRST))).isFalse();
        assertThat(filter.hashedValues).containsExactly(11L, 12L, 13L);
    }

    @Test
    public void testSkippedDomains()
            throws IOException
    {
        for (Domain domain : List.of(
                Domain.all(BIGINT),
                Domain.onlyNull(BIGINT),
                Domain.notNull(BIGINT),
                Domain.singleValue(DOUBLE, Double.NaN),
                Domain.singleValue(REAL, (long) floatToRawIntBits(Float.NaN)))) {
            TestingBloomFilterStore store = store(new CountingBloomFilter());
            BloomFilterPredicate predicate = predicate(domain);
            assertThat(predicate.matches(store, List.of(FIRST))).isTrue();
            assertThat(predicate.matches(store, List.of(FIRST))).isTrue();
            assertThat(store.requestedColumns).isEmpty();
        }
        TestingBloomFilterStore store = store(new CountingBloomFilter());
        assertThat(new BloomFilterPredicate(TupleDomain.none(), 256).matches(store, List.of(FIRST))).isFalse();
        assertThat(new BloomFilterPredicate(TupleDomain.all(), 256).matches(store, List.of(FIRST))).isTrue();
        assertThat(store.requestedColumns).isEmpty();
    }

    @Test
    public void testUnsupportedType()
            throws IOException
    {
        BloomFilterPredicate predicate = predicate(Domain.singleValue(createDecimalType(10, 2), 42L));
        CountingBloomFilter filter = new CountingBloomFilter();
        assertThat(predicate.matches(store(filter), List.of(FIRST))).isTrue();
        assertThat(predicate.matches(store(filter), List.of(FIRST))).isTrue();
        assertThat(filter.hashedValues).isEmpty();
    }

    @Test
    public void testSupportedTypes()
            throws IOException
    {
        for (TestValue value : List.of(
                new TestValue(TINYINT, 42L, 42),
                new TestValue(SMALLINT, 42L, 42),
                new TestValue(INTEGER, 42L, 42),
                new TestValue(DATE, 42L, 42),
                new TestValue(BIGINT, 42L, 42L),
                new TestValue(DOUBLE, 42.5, 42.5),
                new TestValue(REAL, (long) floatToRawIntBits(42.5f), 42.5f),
                new TestValue(VARCHAR, utf8Slice("value"), Binary.fromString("value")),
                new TestValue(VARBINARY, utf8Slice("value"), Binary.fromString("value")),
                new TestValue(UUID, wrappedBuffer(new byte[16]), Binary.fromConstantByteArray(new byte[16])))) {
            BloomFilterPredicate predicate = predicate(Domain.singleValue(value.type(), value.nativeValue()));
            BlockSplitBloomFilter empty = new BlockSplitBloomFilter(1024);
            assertThat(predicate.matches(store(empty), List.of(FIRST))).isFalse();
            BlockSplitBloomFilter present = new BlockSplitBloomFilter(1024);
            present.insertHash(present.hash(value.parquetValue()));
            assertThat(predicate.matches(store(present), List.of(FIRST))).isTrue();
            assertThat(predicate.matches(store(empty), List.of(FIRST))).isFalse();
        }
    }

    private static BloomFilterPredicate predicate(Domain domain)
    {
        return new BloomFilterPredicate(TupleDomain.withColumnDomains(Map.of(FIRST, domain)), 256);
    }

    private static ColumnDescriptor column(String name)
    {
        return new ColumnDescriptor(new String[] {name}, Types.required(INT64).named(name), 0, 0);
    }

    private static TestingBloomFilterStore store(BloomFilter filter)
            throws IOException
    {
        return new TestingBloomFilterStore(Map.of(ColumnPath.get(FIRST.getPath()), filter));
    }

    private record TestValue(Type type, Object nativeValue, Object parquetValue) {}

    private static final class CountingBloomFilter
            extends BlockSplitBloomFilter
    {
        private final List<Long> hashedValues = new ArrayList<>();

        public CountingBloomFilter(long... values)
        {
            super(1024);
            for (long value : values) {
                insertHash(super.hash(value));
            }
        }

        @Override
        public long hash(long value)
        {
            hashedValues.add(value);
            return super.hash(value);
        }
    }

    private static final class TestingBloomFilterStore
            extends BloomFilterStore
    {
        private final Map<ColumnPath, BloomFilter> filters;
        private final List<ColumnPath> requestedColumns = new ArrayList<>();

        public TestingBloomFilterStore(Map<ColumnPath, BloomFilter> filters)
                throws IOException
        {
            super(new TestingParquetDataSource(EMPTY_SLICE, ParquetReaderOptions.defaultOptions()), new BlockMetadata(0, 10, List.of()), Set.of(), Optional.empty());
            this.filters = filters;
        }

        @Override
        public Optional<BloomFilter> getBloomFilter(ColumnPath columnPath)
        {
            requestedColumns.add(columnPath);
            return Optional.ofNullable(filters.get(columnPath));
        }
    }
}
