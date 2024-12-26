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

import io.trino.client.ClientSession;
import io.trino.client.ResultRows;
import io.trino.client.StatementClient;
import okhttp3.OkHttpClient;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Optional;

import static io.trino.client.StatementClientFactory.newStatementClient;
import static java.time.ZoneId.systemDefault;

public class TestClient
{
    private TestClient() {}

    public static void main(String[] args)
            throws InterruptedException, IOException
    {
        OkHttpClient okHttpClient = new OkHttpClient();
        ClientSession session = ClientSession.builder()
                .server(URI.create("http://localhost:8080"))
                .user(Optional.of("admin"))
                .source("wendigo/v1.1")
                .timeZone(systemDefault())
                .encoding(Optional.of("arrow-ipc+zstd"))
                .build();

        long sum = 0;
        int iterations = 50;
        for (int i = 0; i < iterations; i++) {
            long start = System.nanoTime();
            try (StatementClient clientV1 = newStatementClient(okHttpClient, okHttpClient, session, "select comment from tpch.sf10.lineitem limit 1_000_000", Optional.empty())) {
                int rows = 0;
                while (clientV1.advance()) {
                    ResultRows resultRows = clientV1.currentRows();
                    if (!resultRows.isNull()) {
                        for (List<Object> row : resultRows) {
                            rows++;
                            if (rows == 1) {
                                System.out.println(row);
                                System.out.println(resultRows);
                            }
                        }
                    }
                }
                System.out.println("Total rows " + rows + " in " + (System.nanoTime() - start) / 1_000_000_000.0);
                sum += (System.nanoTime() - start);
            }
        }

        System.out.println("Average time " + sum / iterations / 1_000_000_000.0);

        okHttpClient.dispatcher().executorService().shutdown();
    }
}
