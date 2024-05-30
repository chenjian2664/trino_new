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
package io.trino.plugin.mongodb;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Module;
import io.trino.operator.RetryPolicy;
import io.trino.plugin.exchange.filesystem.FileSystemExchangePlugin;
import io.trino.testing.BaseFailureRecoveryTest;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.abort;

public abstract class BaseMongoFailureRecoveryTest
        extends BaseFailureRecoveryTest
{
    public BaseMongoFailureRecoveryTest(RetryPolicy retryPolicy)
    {
        super(retryPolicy);
    }

    @Override
    protected QueryRunner createQueryRunner(
            List<TpchTable<?>> requiredTpchTables,
            Map<String, String> configProperties,
            Map<String, String> coordinatorProperties,
            Module failureInjectionModule)
            throws Exception
    {
        return MongoQueryRunner.builder(closeAfterClass(new MongoServer()))
                .setExtraProperties(configProperties)
                .setCoordinatorProperties(coordinatorProperties)
                .setAdditionalSetup(runner -> {
                    runner.installPlugin(new FileSystemExchangePlugin());
                    runner.loadExchangeManager("filesystem", ImmutableMap.of(
                            "exchange.base-directories", System.getProperty("java.io.tmpdir") + "/trino-local-file-system-exchange-manager"));
                })
                .setAdditionalModule(failureInjectionModule)
                .setInitialTables(requiredTpchTables)
                .build();
    }

    @Test
    @Override
    protected void testAnalyzeTable()
    {
        assertThatThrownBy(super::testAnalyzeTable).hasMessageMatching("This connector does not support analyze");
        abort("skipped");
    }

    @Test
    @Override
    protected void testDelete()
    {
        // This simple delete on Mongo ends up as a very simple, single-fragment, coordinator-only plan,
        // which has no ability to recover from errors. This test simply verifies that's still the case.
        Optional<String> setupQuery = Optional.of("CREATE TABLE <table> AS SELECT * FROM orders");
        String testQuery = "DELETE FROM <table> WHERE orderkey = 1";
        Optional<String> cleanupQuery = Optional.of("DROP TABLE <table>");

        assertThatQuery(testQuery)
                .withSetupQuery(setupQuery)
                .withCleanupQuery(cleanupQuery)
                .isCoordinatorOnly();
    }

    @Test
    @Override
    protected void testDeleteWithSubquery()
    {
        // TODO: solve https://github.com/trinodb/trino/issues/22256
        assertThatThrownBy(super::testDeleteWithSubquery).hasMessageContaining("This connector does not support MERGE with retries");
    }

    @Test
    @Override
    protected void testMerge()
    {
        // TODO: solve https://github.com/trinodb/trino/issues/22256
        assertThatThrownBy(super::testMerge).hasMessageContaining("This connector does not support MERGE with retries");
    }

    @Test
    @Override
    protected void testRefreshMaterializedView()
    {
        assertThatThrownBy(super::testRefreshMaterializedView)
                .hasMessageContaining("This connector does not support creating materialized views");
        abort("skipped");
    }

    @Test
    @Override
    protected void testUpdate()
    {
        // TODO: solve https://github.com/trinodb/trino/issues/22256
        assertThatThrownBy(super::testUpdate).hasMessageContaining("This connector does not support MERGE with retries");
    }

    @Test
    @Override
    protected void testUpdateWithSubquery()
    {
        // TODO: solve https://github.com/trinodb/trino/issues/22256
        assertThatThrownBy(super::testUpdateWithSubquery).hasMessageContaining("This connector does not support MERGE with retries");
    }

    @Override
    protected boolean areWriteRetriesSupported()
    {
        return true;
    }
}
