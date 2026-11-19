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

import io.airlift.slice.Slice;
import io.trino.parquet.BloomFilterStore;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.SliceBloomFilter;
import io.trino.parquet.metadata.BlockMetadata;
import io.trino.parquet.reader.TestingParquetDataSource;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.values.bloomfilter.BlockSplitBloomFilter;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.schema.Types;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(MICROSECONDS)
@Fork(2)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class BenchmarkBloomFilterPredicate
{
    private static final ColumnDescriptor COLUMN = new ColumnDescriptor(new String[] {"value"}, Types.required(BINARY).named("value"), 0, 0);
    private static final ColumnPath PATH = ColumnPath.get(COLUMN.getPath());
    private static final List<ColumnDescriptor> COLUMNS = List.of(COLUMN);

    @Param({"1", "16"})
    public int rowGroups;

    @Param("64")
    public int valueCount;

    @Param({"16", "128", "1024"})
    public int length;

    @Param({"MISS", "FIRST_HIT"})
    public String outcome;

    private Domain domain;
    private TupleDomain<ColumnDescriptor> tupleDomain;
    private List<BloomFilterStore> stores;

    @Setup
    public void setup()
            throws IOException
    {
        Random random = new Random(0);
        List<Slice> values = new ArrayList<>();
        for (int i = 0; i < valueCount; i++) {
            values.add(randomSlice(random));
        }
        domain = Domain.multipleValues(VARBINARY, values);
        tupleDomain = TupleDomain.withColumnDomains(Map.of(COLUMN, domain));
        Slice first = (Slice) domain.getValues().getDiscreteSet().iterator().next();
        stores = new ArrayList<>();
        for (int group = 0; group < rowGroups; group++) {
            SliceBloomFilter filter = new SliceBloomFilter(new BlockSplitBloomFilter(16 * 1024));
            for (int i = 0; i < 1024; i++) {
                filter.insertHash(filter.hash(randomSlice(random)));
            }
            if (outcome.equals("FIRST_HIT")) {
                filter.insertHash(filter.hash(first));
            }
            stores.add(new BloomFilterStore(
                    new TestingParquetDataSource(EMPTY_SLICE, ParquetReaderOptions.defaultOptions()),
                    new BlockMetadata(group, 1024, List.of()),
                    Set.of(PATH),
                    Optional.empty())
            {
                @Override
                public Optional<BloomFilter> getBloomFilter(ColumnPath columnPath)
                {
                    return Optional.of(filter);
                }
            });
        }
        int expected = outcome.equals("FIRST_HIT") ? rowGroups : 0;
        checkState(cached() == expected && uncached() == expected, "Unexpected bloom filter matches");
    }

    private Slice randomSlice(Random random)
    {
        byte[] bytes = new byte[length];
        random.nextBytes(bytes);
        return wrappedBuffer(bytes);
    }

    @Benchmark
    public int cached()
    {
        // Include the first row group's preparation cost in every invocation.
        BloomFilterPredicate predicate = new BloomFilterPredicate(tupleDomain, 256);
        int matches = 0;
        for (BloomFilterStore store : stores) {
            if (predicate.matches(store, COLUMNS)) {
                matches++;
            }
        }
        return matches;
    }

    @Benchmark
    public int uncached()
    {
        int matches = 0;
        for (BloomFilterStore store : stores) {
            SliceBloomFilter filter = new SliceBloomFilter(store.getBloomFilter(PATH).orElseThrow());
            if (domain.getValues().getDiscreteSet().stream().anyMatch(value -> BloomFilterPredicate.checkInBloomFilter(filter, value, VARBINARY))) {
                matches++;
            }
        }
        return matches;
    }

    static void main()
            throws RunnerException
    {
        benchmark(BenchmarkBloomFilterPredicate.class).run();
    }
}
