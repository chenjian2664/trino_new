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
package io.trino.plugin.hive;

import io.trino.spi.connector.ConnectorDynamicFilter;
import io.trino.spi.predicate.TupleDomain;

import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

/**
 * Tracks the evolving dynamic filter state for a single Hive table scan.
 * <p>
 * {@link #update} is called by {@link HiveSplitSource#getNextBatch} on every engine batch call.
 * {@link #awaitAndGet()} blocks until the first snapshot has arrived, acting as a gate for
 * {@link BackgroundHiveSplitLoader} to avoid producing un-pruned splits before the engine's
 * dynamic-filter wait window has closed.
 */
public class DynamicFilterState
{
    private final CompletableFuture<Void> firstArrival = new CompletableFuture<>();
    private volatile ConnectorDynamicFilter latest = new ConnectorDynamicFilter(TupleDomain.all(), false);

    void update(ConnectorDynamicFilter filter)
    {
        latest = requireNonNull(filter, "filter is null");
        firstArrival.complete(null);
    }

    public ConnectorDynamicFilter get()
    {
        return latest;
    }

    public CompletableFuture<Void> await()
    {
        return firstArrival;
    }

    public boolean isReady()
    {
        return firstArrival.isDone();
    }

    public ConnectorDynamicFilter awaitAndGet()
    {
        firstArrival.join();
        return latest;
    }

    static DynamicFilterState completeState()
    {
        DynamicFilterState state = new DynamicFilterState();
        state.update(new ConnectorDynamicFilter(TupleDomain.all(), true));
        return state;
    }
}
