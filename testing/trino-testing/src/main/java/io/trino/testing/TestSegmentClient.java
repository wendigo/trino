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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.airlift.json.ObjectMapperProvider;
import io.trino.client.ClientSession;
import io.trino.client.Column;
import io.trino.client.OkHttpSegmentLoader;
import io.trino.client.QueryData;
import io.trino.client.ResultRowsDecoder;
import io.trino.client.StatementClient;
import io.trino.client.TrinoJsonCodec;
import io.trino.client.spooling.EncodedQueryData;
import io.trino.server.protocol.spooling.QueryDataJacksonModule;
import okhttp3.OkHttpClient;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.client.StatementClientFactory.newStatementClient;
import static io.trino.client.TrinoJsonCodec.jsonCodec;
import static java.time.ZoneId.systemDefault;

public class TestSegmentClient
{
    private TestSegmentClient() {}

    public static void main(String[] args)
            throws InterruptedException, IOException
    {
        TrinoJsonCodec<QueryData> clientCodec = jsonCodec(QueryData.class);
        ObjectMapper serverMapper = new ObjectMapperProvider().get().registerModule(new QueryDataJacksonModule());

        OkHttpClient okHttpClient = new OkHttpClient();
        ClientSession session = ClientSession.builder()
                .server(URI.create("http://localhost:8080"))
                .user(Optional.of("admin"))
                .source("wendigo/v1.1")
                .timeZone(systemDefault())
                .encoding(Optional.of("json"))
                .properties(Map.of("spooling_inlining_enabled", "false"))
                .build();

        List<String> serializedSegments = new ArrayList<>();
        List<Column> columns = new ArrayList<>();
        long start = System.nanoTime();
        try (StatementClient clientV1 = newStatementClient(okHttpClient, okHttpClient, session, "select comment from tpch.sf10.lineitem LIMIT 5_000_000", Optional.empty())) {
            while (clientV1.advance()) {
                if (clientV1.currentData() == null || clientV1.currentData().isNull()) {
                    continue;
                }
                columns = clientV1.currentStatusInfo().getColumns();
                if (clientV1.currentData() instanceof EncodedQueryData encodedQueryData) {
                    List<EncodedQueryData> perSegmentData = encodedQueryData.getSegments().stream().map(segment ->
                            new EncodedQueryData(encodedQueryData.getEncoding(), encodedQueryData.getMetadata(), ImmutableList.of(segment)))
                    .collect(toImmutableList());
                    for (EncodedQueryData perSegment : perSegmentData) {
                        serializedSegments.add(serverMapper.writeValueAsString(perSegment));
                    }
                }
                else {
                    throw new IllegalStateException("Current data is not EncodedQueryData");
                }
            }
        }

        System.out.println("Query finished in " + ((System.nanoTime() - start) / 1_000_000_000d) + " s");
        System.out.println("Now reading data out of band");
        AtomicInteger rowsCount = new AtomicInteger();
        try (ResultRowsDecoder decoder = new ResultRowsDecoder(new OkHttpSegmentLoader(okHttpClient))) {
            for (String segment : serializedSegments) {
                QueryData data = clientCodec.fromJson(segment);
                decoder.toRows(columns, data).forEach(_ -> rowsCount.incrementAndGet());
            }
            System.out.println("Fully read data in " + ((System.nanoTime() - start) / 1_000_000_000d) + " s");
            System.out.println("Total row count is " + rowsCount.get());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

        okHttpClient.dispatcher().executorService().shutdown();
    }
}
