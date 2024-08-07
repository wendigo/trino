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
package io.trino.plugin.deltalake.metastore.glue.v1;

import com.amazonaws.services.glue.AWSGlueAsync;
import com.google.inject.Inject;
import io.trino.plugin.deltalake.metastore.DeltaLakeTableOperations;
import io.trino.plugin.deltalake.metastore.DeltaLakeTableOperationsProvider;
import io.trino.plugin.hive.metastore.glue.GlueMetastoreStats;
import io.trino.spi.connector.ConnectorSession;

import static java.util.Objects.requireNonNull;

public class DeltaLakeGlueV1MetastoreTableOperationsProvider
        implements DeltaLakeTableOperationsProvider
{
    private final AWSGlueAsync glueClient;
    private final GlueMetastoreStats stats;

    @Inject
    public DeltaLakeGlueV1MetastoreTableOperationsProvider(AWSGlueAsync glueClient, GlueMetastoreStats stats)
    {
        this.glueClient = requireNonNull(glueClient, "glueClient is null");
        this.stats = requireNonNull(stats, "stats is null");
    }

    @Override
    public DeltaLakeTableOperations createTableOperations(ConnectorSession session)
    {
        return new DeltaLakeGlueV1MetastoreTableOperations(glueClient, stats);
    }
}
