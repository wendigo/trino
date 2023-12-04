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
package io.trino.tests;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.trino.Session;
import io.trino.execution.QueryManager;
import io.trino.execution.SqlTaskManager;
import io.trino.server.BasicQueryInfo;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.server.testing.TestingTrinoServer.TestShutdownAction;
import io.trino.testing.DistributedQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.trino.execution.QueryState.FINISHED;
import static io.trino.memory.TestMemoryManager.createQueryRunner;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestGracefulShutdown
{
    private static final long SHUTDOWN_TIMEOUT_MILLIS = 240_000;
    private static final Session TINY_SESSION = testSessionBuilder()
            .setCatalog("tpch")
            .setSchema("tiny")
            .build();

    private ListeningExecutorService executor;

    @BeforeAll
    public void setUp()
    {
        executor = MoreExecutors.listeningDecorator(newCachedThreadPool());
    }

    @AfterAll
    public void shutdown()
    {
        executor.shutdownNow();
    }

    @Test
    @Timeout(value = SHUTDOWN_TIMEOUT_MILLIS, unit = TimeUnit.MILLISECONDS)
    public void testShutdown()
            throws Exception
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("node-scheduler.include-coordinator", "false")
                .put("shutdown.grace-period", "10s")
                .buildOrThrow();

        try (DistributedQueryRunner queryRunner = createQueryRunner(TINY_SESSION, properties)) {
            List<ListenableFuture<Void>> queryFutures = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                queryFutures.add(Futures.submit(
                        () -> {
                            queryRunner.execute("SELECT COUNT(*), clerk FROM orders GROUP BY clerk");
                        },
                        executor));
            }

            @SuppressWarnings("resource")
            TestingTrinoServer worker = queryRunner.getServers()
                    .stream()
                    .filter(server -> !server.isCoordinator())
                    .findFirst()
                    .orElseThrow();

            SqlTaskManager taskManager = worker.getTaskManager();

            // wait until tasks show up on the worker
            while (taskManager.getAllTaskInfo().isEmpty()) {
                MILLISECONDS.sleep(500);
            }

            worker.getGracefulShutdownHandler().requestShutdown();

            Futures.allAsList(queryFutures).get();

            List<BasicQueryInfo> queryInfos = queryRunner.getCoordinator().getQueryManager().getQueries();
            for (BasicQueryInfo info : queryInfos) {
                assertThat(info.getState()).isEqualTo(FINISHED);
            }

            TestShutdownAction shutdownAction = (TestShutdownAction) worker.getShutdownAction();
            shutdownAction.waitForShutdownComplete(SHUTDOWN_TIMEOUT_MILLIS);
            assertThat(shutdownAction.isNodeShutdown()).isTrue();
        }
    }

    @Test
    public void testCoordinatorShutdown()
            throws Exception
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("node-scheduler.include-coordinator", "false")
                .put("shutdown.grace-period", "10s")
                .buildOrThrow();

        int queriesCount = 5;
        try (DistributedQueryRunner queryRunner = createQueryRunner(TINY_SESSION, properties)) {
            List<ListenableFuture<Void>> queryFutures = new ArrayList<>();
            for (int i = 0; i < queriesCount; i++) {
                queryFutures.add(Futures.submit(
                        () -> {
                            queryRunner.execute("SELECT COUNT(*), clerk FROM orders GROUP BY clerk");
                        },
                        executor));
            }
            @SuppressWarnings("resource")
            TestingTrinoServer coordinator = queryRunner.getCoordinator();
            QueryManager queryManager = coordinator.getQueryManager();

            // wait until queries show up on the coordinator
            while (queryManager.getQueries().size() != queriesCount) {
                MILLISECONDS.sleep(500);
            }

            coordinator.getGracefulShutdownHandler().requestShutdown();

            // New query submission will be rejected
            assertThatThrownBy(() -> queryRunner.execute("SELECT COUNT(*), clerk FROM orders GROUP BY clerk"))
                    .hasMessageContaining("Coordinator is shutting down");

            Futures.allAsList(queryFutures).get(); // Wait for all queries to complete
            List<BasicQueryInfo> queryInfos = queryManager.getQueries();
            for (BasicQueryInfo info : queryInfos) {
                assertThat(info.getState()).isEqualTo(FINISHED);
            }

            TestShutdownAction shutdownAction = (TestShutdownAction) coordinator.getShutdownAction();
            shutdownAction.waitForShutdownComplete(SHUTDOWN_TIMEOUT_MILLIS);
            assertThat(shutdownAction.isNodeShutdown()).isTrue();
        }
    }
}
