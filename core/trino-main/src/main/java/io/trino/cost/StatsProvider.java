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
package io.trino.cost;

import io.trino.sql.planner.plan.PlanNode;

public interface StatsProvider
{
    PlanNodeStatsEstimate getStats(PlanNode node);

    /**
     * Offers an estimate a caller has already computed for a node this provider has not seen, so
     * that a later {@link #getStats} does not compute it again. Only ever an optimization: an
     * implementation is free to ignore it, and a caller must offer the same estimate the provider
     * would have produced itself.
     */
    default void registerStats(PlanNode node, PlanNodeStatsEstimate stats) {}
}
