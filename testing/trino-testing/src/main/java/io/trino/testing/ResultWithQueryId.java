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
package io.trino.testing;

import com.google.common.collect.ImmutableList;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.trino.spi.QueryId;

import java.util.List;
import java.util.Optional;

public class ResultWithQueryId<T>
{
    private final QueryId queryId;
    private final T result;
    private final Optional<List<SpanData>> spans;

    public ResultWithQueryId(QueryId queryId, T result)
    {
        this(queryId, result, Optional.empty());
    }

    public ResultWithQueryId(QueryId queryId, T result, Optional<List<SpanData>> spans)
    {
        this.queryId = queryId;
        this.result = result;
        this.spans = spans.map(ImmutableList::copyOf);
    }

    public ResultWithQueryId<T> withSpans(List<SpanData> spans)
    {
        return new ResultWithQueryId<>(queryId, result, Optional.of(spans));
    }

    public QueryId getQueryId()
    {
        return queryId;
    }

    public List<SpanData> getSpans()
    {
        return spans.orElse(List.of());
    }

    public T getResult()
    {
        return result;
    }
}
