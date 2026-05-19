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
package io.trino.plugin.jdbc;

import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorDynamicFilter;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static io.trino.plugin.jdbc.JdbcDynamicFilteringSessionProperties.dynamicFilteringEnabled;
import static io.trino.plugin.jdbc.JdbcDynamicFilteringSessionProperties.getDynamicFilteringWaitTimeout;
import static java.util.Objects.requireNonNull;

/**
 * Implements waiting for collection of dynamic filters before generating splits from {@link ConnectorSplitManager}.
 * This allows JDBC based connectors to take advantage of dynamic filters during splits generation phase.
 * Implementing this as a wrapper over {@link ConnectorSplitManager} allows this class to be used by JDBC connectors
 * which don't rely on {@link JdbcSplitManager} for splits generation.
 */
public class JdbcDynamicFilteringSplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(JdbcDynamicFilteringSplitManager.class);

    private final ConnectorSplitManager delegateSplitManager;

    @Inject
    public JdbcDynamicFilteringSplitManager(
            @ForJdbcDynamicFiltering ConnectorSplitManager delegateSplitManager)
    {
        this.delegateSplitManager = requireNonNull(delegateSplitManager, "delegateSplitManager is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            Set<ColumnHandle> dynamicFilterColumns,
            Constraint constraint)
    {
        // JdbcProcedureHandle doesn't support any pushdown operation, so we rely on delegateSplitManager
        if (table instanceof JdbcProcedureHandle) {
            return delegateSplitManager.getSplits(transaction, session, table, dynamicFilterColumns, constraint);
        }

        JdbcTableHandle tableHandle = (JdbcTableHandle) table;
        // pushing DF through limit could reduce query performance
        boolean hasLimit = tableHandle.getLimit().isPresent();
        if (dynamicFilterColumns.isEmpty() || hasLimit || !dynamicFilteringEnabled(session)) {
            return delegateSplitManager.getSplits(transaction, session, table, dynamicFilterColumns, constraint);
        }

        return new DynamicFilteringSplitSource(transaction, session, tableHandle, dynamicFilterColumns, constraint);
    }

    private class DynamicFilteringSplitSource
            implements ConnectorSplitSource
    {
        private final ConnectorTransactionHandle transaction;
        private final ConnectorSession session;
        private final JdbcTableHandle table;
        private final Set<ColumnHandle> dynamicFilterColumns;
        private final Constraint constraint;
        private final long dynamicFilteringTimeoutMillis;

        @GuardedBy("this")
        private Optional<ConnectorSplitSource> delegateSplitSource = Optional.empty();

        DynamicFilteringSplitSource(
                ConnectorTransactionHandle transaction,
                ConnectorSession session,
                JdbcTableHandle table,
                Set<ColumnHandle> dynamicFilterColumns,
                Constraint constraint)
        {
            this.transaction = requireNonNull(transaction, "transaction is null");
            this.session = requireNonNull(session, "session is null");
            this.table = requireNonNull(table, "table is null");
            this.dynamicFilterColumns = ImmutableSet.copyOf(dynamicFilterColumns);
            this.constraint = requireNonNull(constraint, "constraint is null");
            this.dynamicFilteringTimeoutMillis = getDynamicFilteringWaitTimeout(session).toMillis();
        }

        @Override
        public long getRequestedDynamicFilterWaitTimeoutMillis()
        {
            return dynamicFilteringTimeoutMillis;
        }

        @Override
        public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize, ConnectorDynamicFilter dynamicFilter)
        {
            log.debug(
                    "Enumerating splits (query %s, table: %s, completed: %s)",
                    session.getQueryId(),
                    table,
                    dynamicFilter.isComplete());
            return getDelegateSplitSource(dynamicFilter).getNextBatch(maxSize, dynamicFilter);
        }

        @Override
        public void close()
        {
            getOptionalDelegateSplitSource().ifPresent(ConnectorSplitSource::close);
        }

        @Override
        public boolean isFinished()
        {
            return getOptionalDelegateSplitSource()
                    .map(ConnectorSplitSource::isFinished)
                    .orElse(false);
        }

        private synchronized ConnectorSplitSource getDelegateSplitSource(ConnectorDynamicFilter dynamicFilter)
        {
            if (delegateSplitSource.isPresent()) {
                return delegateSplitSource.get();
            }

            delegateSplitSource = Optional.of(delegateSplitManager.getSplits(
                    transaction,
                    session,
                    table.intersectedWithConstraint(dynamicFilter.currentPredicate()),
                    dynamicFilterColumns,
                    constraint));
            return delegateSplitSource.get();
        }

        private synchronized Optional<ConnectorSplitSource> getOptionalDelegateSplitSource()
        {
            return delegateSplitSource;
        }
    }
}
