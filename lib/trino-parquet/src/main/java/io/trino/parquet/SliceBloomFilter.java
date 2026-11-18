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
import io.airlift.slice.XxHash64;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.io.api.Binary;

import java.io.IOException;
import java.io.OutputStream;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Adds direct Slice hashing to Parquet bloom filters without materializing a byte array.
 */
public final class SliceBloomFilter
        implements BloomFilter
{
    private final BloomFilter delegate;

    public SliceBloomFilter(BloomFilter delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        checkArgument(delegate.getHashStrategy() == HashStrategy.XXH64, "Unsupported hash strategy: %s", delegate.getHashStrategy());
    }

    public long hash(Slice value)
    {
        return hash(value, 0, value.length());
    }

    public long hash(Slice value, int offset, int length)
    {
        // Parquet's XXH64 hash uses seed zero. Avoid the copy in Binary.getBytes().
        return XxHash64.hash(value, offset, length);
    }

    @Override
    public long hash(int value)
    {
        return delegate.hash(value);
    }

    @Override
    public long hash(long value)
    {
        return delegate.hash(value);
    }

    @Override
    public long hash(double value)
    {
        return delegate.hash(value);
    }

    @Override
    public long hash(float value)
    {
        return delegate.hash(value);
    }

    @Override
    public long hash(Binary value)
    {
        return delegate.hash(value);
    }

    @Override
    public long hash(Object value)
    {
        return delegate.hash(value);
    }

    @Override
    public void insertHash(long hash)
    {
        delegate.insertHash(hash);
    }

    @Override
    public boolean findHash(long hash)
    {
        return delegate.findHash(hash);
    }

    @Override
    public int getBitsetSize()
    {
        return delegate.getBitsetSize();
    }

    @Override
    public HashStrategy getHashStrategy()
    {
        return delegate.getHashStrategy();
    }

    @Override
    public Algorithm getAlgorithm()
    {
        return delegate.getAlgorithm();
    }

    @Override
    public Compression getCompression()
    {
        return delegate.getCompression();
    }

    @Override
    public boolean canMergeFrom(BloomFilter other)
    {
        return delegate.canMergeFrom(other);
    }

    @Override
    public void writeTo(OutputStream output)
            throws IOException
    {
        delegate.writeTo(output);
    }

    @Override
    public void merge(BloomFilter other)
            throws IOException
    {
        delegate.merge(other);
    }
}
