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
package io.trino.plugin.base.classloader;

import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSecurityContext;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.ptf.ConnectorTableFunction;
import io.trino.spi.type.Type;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.Set;

import static io.trino.spi.testing.InterfaceTestUtils.assertAllMethodsOverridden;

public class TestClassLoaderSafeWrappers
{
    @Test
    public void test()
            throws NoSuchMethodException
    {
        testClassLoaderSafe(ConnectorAccessControl.class, ClassLoaderSafeConnectorAccessControl.class, ImmutableSet.of(
                ClassLoaderSafeConnectorAccessControl.class.getMethod("getRowFilter", ConnectorSecurityContext.class, SchemaTableName.class),
                ClassLoaderSafeConnectorAccessControl.class.getMethod("getColumnMask", ConnectorSecurityContext.class, SchemaTableName.class, String.class, Type.class)));
        testClassLoaderSafe(ConnectorMetadata.class, ClassLoaderSafeConnectorMetadata.class);
        testClassLoaderSafe(ConnectorPageSink.class, ClassLoaderSafeConnectorPageSink.class);
        testClassLoaderSafe(ConnectorPageSinkProvider.class, ClassLoaderSafeConnectorPageSinkProvider.class);
        testClassLoaderSafe(ConnectorPageSourceProvider.class, ClassLoaderSafeConnectorPageSourceProvider.class);
        testClassLoaderSafe(ConnectorSplitManager.class, ClassLoaderSafeConnectorSplitManager.class);
        testClassLoaderSafe(ConnectorNodePartitioningProvider.class, ClassLoaderSafeNodePartitioningProvider.class);
        testClassLoaderSafe(ConnectorSplitSource.class, ClassLoaderSafeConnectorSplitSource.class);
        testClassLoaderSafe(SystemTable.class, ClassLoaderSafeSystemTable.class);
        testClassLoaderSafe(ConnectorRecordSetProvider.class, ClassLoaderSafeConnectorRecordSetProvider.class);
        testClassLoaderSafe(RecordSet.class, ClassLoaderSafeRecordSet.class);
        testClassLoaderSafe(EventListener.class, ClassLoaderSafeEventListener.class);
        testClassLoaderSafe(ConnectorTableFunction.class, ClassLoaderSafeConnectorTableFunction.class);
    }

    private static <I, C extends I> void testClassLoaderSafe(Class<I> iface, Class<C> clazz)
    {
        testClassLoaderSafe(iface, clazz, Set.of());
    }

    private static <I, C extends I> void testClassLoaderSafe(Class<I> iface, Class<C> clazz, Set<Method> exclusions)
    {
        assertAllMethodsOverridden(iface, clazz, exclusions);
    }
}
