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
package io.trino.spi.connector;

import io.trino.spi.metrics.Metrics;
import io.trino.spi.predicate.TupleDomain;

import java.io.Closeable;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

/**
 * Source of splits to be processed.
 * <p>
 * Thread-safety: the implementations are not required to be thread-safe.
 */
public interface ConnectorSplitSource
        extends Closeable
{
    /**
     * Returns the next batch of splits.
     * <p>
     * {@code dynamicFilter} contains a snapshot of the engine's dynamic filter state captured
     * immediately before this call. The engine waits up to
     * {@link #getRequestedDynamicFilterWaitTimeoutMillis()} before the <em>first</em> call;
     * subsequent calls receive the current state without additional waiting.
     * For connectors that do not use dynamic filtering,
     * {@link ConnectorDynamicFilter#currentPredicate()} is {@link TupleDomain#all()} and
     * {@link ConnectorDynamicFilter#isComplete()} is {@code true}.
     *
     * @since 481
     */
    default CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize, ConnectorDynamicFilter dynamicFilter)
    {
        return getNextBatch(maxSize);
    }

    /**
     * @deprecated Use {@link #getNextBatch(int, ConnectorDynamicFilter)} instead.
     */
    @Deprecated(forRemoval = true, since = "481")
    default CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    void close();

    /**
     * Returns whether any more {@link ConnectorSplit} may be produced.
     * <p>
     * This method should only be called when there has been no invocation of getNextBatch,
     * or result Future of previous getNextBatch is done.
     * Calling this method at other time is not useful because the contract of such an invocation
     * will be inherently racy.
     */
    boolean isFinished();

    default Optional<List<Object>> getTableExecuteSplitsInfo()
    {
        return Optional.empty();
    }

    /**
     * Returns the maximum time in milliseconds the engine should wait for dynamic filters to be
     * collected before the first call to {@link #getNextBatch(int, ConnectorDynamicFilter)}.
     * Return {@code 0} to disable waiting (the default).
     * <p>
     * The engine reads this value once when the split source is constructed; it is treated as
     * fixed for the lifetime of the source (per table scan).
     */
    default long getRequestedDynamicFilterWaitTimeoutMillis()
    {
        return 0;
    }

    /**
     * Returns the split source's metrics, mapping a metric id to its latest value.
     * Each call must return an immutable snapshot of available metrics.
     * The metrics for each split source are collected independently and exposed via StageStats and OperatorStats.
     * This method can be called after the split source is closed, and in that case the final metrics should be returned.
     */
    default Metrics getMetrics()
    {
        return Metrics.EMPTY;
    }

    class ConnectorSplitBatch
    {
        private final List<ConnectorSplit> splits;
        private final boolean noMoreSplits;

        public ConnectorSplitBatch(List<ConnectorSplit> splits, boolean noMoreSplits)
        {
            this.splits = requireNonNull(splits, "splits is null");
            this.noMoreSplits = noMoreSplits;
        }

        public List<ConnectorSplit> getSplits()
        {
            return splits;
        }

        public boolean isNoMoreSplits()
        {
            return noMoreSplits;
        }
    }
}
