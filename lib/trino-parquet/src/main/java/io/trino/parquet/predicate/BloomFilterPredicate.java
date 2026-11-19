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
import io.trino.parquet.SliceBloomFilter;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.FloatingPointValueSet;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.hadoop.metadata.ColumnPath;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.TypeUtils.isFloatingPointNaN;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * Reuses predicate hashes across row groups in a single row-group filtering pass.
 * Hashing is lazy to preserve early exits. This class is not thread-safe.
 */
final class BloomFilterPredicate
{
    private final TupleDomain<ColumnDescriptor> predicate;
    private final int domainCompactionThreshold;
    private final Map<ColumnDescriptor, Optional<CachedHashes>> columnHashes = new HashMap<>();

    public BloomFilterPredicate(TupleDomain<ColumnDescriptor> predicate, int domainCompactionThreshold)
    {
        this.predicate = requireNonNull(predicate, "predicate is null");
        this.domainCompactionThreshold = domainCompactionThreshold;
    }

    public boolean matches(BloomFilterStore bloomFilterStore, List<ColumnDescriptor> columns)
    {
        requireNonNull(bloomFilterStore, "bloomFilterStore is null");
        if (predicate.isNone()) {
            return false;
        }
        Map<ColumnDescriptor, Domain> domains = predicate.getDomains().orElseThrow();
        for (ColumnDescriptor column : columns) {
            Domain domain = domains.get(column);
            // Bloom filters only describe non-null values. NaN payloads may hash differently.
            if (domain == null || domain.isNullAllowed() ||
                    (domain.getValues() instanceof FloatingPointValueSet floatingPoint && floatingPoint.isNaNAllowed())) {
                continue;
            }
            Optional<CachedHashes> hashes = columnHashes.computeIfAbsent(column, _ -> extractDiscreteValues(domain.getValues())
                    .map(values -> new CachedHashes(domain.getType(), values)));
            if (hashes.isEmpty()) {
                continue;
            }
            Optional<BloomFilter> bloomFilter = bloomFilterStore.getBloomFilter(ColumnPath.get(column.getPath()));
            if (bloomFilter.isPresent() && !hashes.get().matches(new SliceBloomFilter(bloomFilter.get()))) {
                return false;
            }
        }
        return true;
    }

    public static boolean checkInBloomFilter(SliceBloomFilter bloomFilter, Object value, Type type)
    {
        OptionalLong hash = hash(bloomFilter, value, type);
        return hash.isEmpty() || bloomFilter.findHash(hash.orElseThrow());
    }

    private static OptionalLong hash(SliceBloomFilter bloomFilter, Object value, Type type)
    {
        if (isFloatingPointNaN(type, value)) {
            return OptionalLong.empty();
        }
        // TODO: Support TIMESTAMP, CHAR and DECIMAL
        if (type == TINYINT || type == SMALLINT || type == INTEGER || type == DATE) {
            return OptionalLong.of(bloomFilter.hash(toIntExact(((Number) value).longValue())));
        }
        if (type == BIGINT) {
            return OptionalLong.of(bloomFilter.hash(((Number) value).longValue()));
        }
        if (type == DOUBLE) {
            return OptionalLong.of(bloomFilter.hash(((Double) value).doubleValue()));
        }
        if (type == REAL) {
            return OptionalLong.of(bloomFilter.hash(intBitsToFloat(toIntExact(((Number) value).longValue()))));
        }
        if (type instanceof VarcharType || type instanceof VarbinaryType || type instanceof UuidType) {
            return OptionalLong.of(bloomFilter.hash((Slice) value));
        }
        return OptionalLong.empty();
    }

    private Optional<Collection<Object>> extractDiscreteValues(ValueSet values)
    {
        if (values.isDiscreteSet()) {
            return Optional.of(values.getDiscreteSet());
        }
        return values.tryExpandRanges(domainCompactionThreshold);
    }

    private static final class CachedHashes
    {
        private final Type type;
        private final LongArrayList hashes = new LongArrayList();
        private Iterator<Object> remainingValues;
        private boolean unsupportedValue;

        public CachedHashes(Type type, Collection<Object> values)
        {
            this.type = type;
            remainingValues = values.iterator();
        }

        public boolean matches(SliceBloomFilter bloomFilter)
        {
            if (unsupportedValue) {
                return true;
            }
            for (int i = 0; i < hashes.size(); i++) {
                if (bloomFilter.findHash(hashes.getLong(i))) {
                    return true;
                }
            }
            while (remainingValues != null && remainingValues.hasNext()) {
                OptionalLong hash = hash(bloomFilter, remainingValues.next(), type);
                if (hash.isEmpty()) {
                    unsupportedValue = true;
                    remainingValues = null;
                    return true;
                }
                // Keep only the hash, never the row group's bloom filter.
                hashes.add(hash.orElseThrow());
                if (bloomFilter.findHash(hash.orElseThrow())) {
                    return true;
                }
            }
            remainingValues = null;
            return false;
        }
    }
}
