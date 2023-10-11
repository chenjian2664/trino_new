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
package io.trino.plugin.iceberg;

import io.trino.filesystem.Location;
import org.junit.jupiter.api.Test;

import static org.apache.iceberg.FileFormat.AVRO;
import static org.junit.jupiter.api.Assumptions.abort;

public class TestIcebergMinioAvroConnectorSmokeTest
        extends BaseIcebergMinioConnectorSmokeTest
{
    public TestIcebergMinioAvroConnectorSmokeTest()
    {
        super(AVRO);
    }

    @Test
    @Override
    public void testSortedNationTable()
    {
        abort("Avro does not support file sorting");
    }

    @Test
    @Override
    public void testFileSortingWithLargerTable()
    {
        abort("Avro does not support file sorting");
    }

    @Override
    protected boolean isFileSorted(Location path, String sortColumnName)
    {
        throw new IllegalStateException("File sorting tests should be skipped for Avro");
    }
}
