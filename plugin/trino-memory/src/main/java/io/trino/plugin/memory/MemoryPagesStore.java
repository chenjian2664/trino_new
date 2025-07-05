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
package io.trino.plugin.memory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.memory.MemoryErrorCode.MEMORY_LIMIT_EXCEEDED;
import static io.trino.plugin.memory.MemoryErrorCode.MISSING_DATA;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;

@ThreadSafe
public class MemoryPagesStore
{
    private final long maxBytes;

    @GuardedBy("this")
    private long currentBytes;

    private final Map<Long, TableData> tables = new HashMap<>();

    @Inject
    public MemoryPagesStore(MemoryConfig config)
    {
        this.maxBytes = config.getMaxDataPerNode().toBytes();
    }

    public synchronized void initialize(long tableId, int rowIdIndex)
    {
        if (!tables.containsKey(tableId)) {
            tables.put(tableId, new TableData(rowIdIndex));
        }
    }

    public synchronized void add(Long tableId, Page page)
    {
        if (!contains(tableId)) {
            throw new TrinoException(MISSING_DATA, "Failed to find table on a worker.");
        }

        page.compact();

        long newSize = currentBytes + page.getRetainedSizeInBytes();
        if (maxBytes < newSize) {
            throw new TrinoException(MEMORY_LIMIT_EXCEEDED, format("Memory limit [%d] for memory connector exceeded", maxBytes));
        }
        currentBytes = newSize;

        TableData tableData = tables.get(tableId);
        tableData.add(page);
    }

    public synchronized void update(Long tableId, Page page)
    {
        if (!contains(tableId)) {
            throw new TrinoException(MISSING_DATA, "Failed to find table on a worker.");
        }

        page.compact();

        TableData tableData = tables.get(tableId);
        currentBytes -= tableData.update(page);

        if (currentBytes > maxBytes) {
            throw new TrinoException(MEMORY_LIMIT_EXCEEDED, format("Memory limit [%d] for memory connector exceeded", maxBytes));
        }
    }

    public synchronized long delete(Long tableId, Block rowIds)
    {
        if (!contains(tableId)) {
            throw new TrinoException(MISSING_DATA, "Failed to find table on a worker.");
        }

        TableData tableData = tables.get(tableId);
        long current = tableData.getRows();
        currentBytes -= tableData.delete(rowIds);
        return current - tableData.getRows();
    }

    public synchronized List<Page> getPages(
            Long tableId,
            int partNumber,
            int totalParts,
            int[] columnIndexes,
            List<Type> columnTypes,
            long expectedRows,
            OptionalLong limit,
            OptionalDouble sampleRatio)
    {
        checkArgument(columnIndexes.length == columnTypes.size(), "columnIndexes and columnTypes must have the same size");

        if (!contains(tableId)) {
            throw new TrinoException(MISSING_DATA, "Failed to find table on a worker.");
        }
        TableData tableData = tables.get(tableId);
        if (tableData.getRows() < expectedRows) {
            throw new TrinoException(MISSING_DATA,
                    format("Expected to find [%s] rows on a worker, but found [%s].", expectedRows, tableData.getRows()));
        }

        ImmutableList.Builder<Page> partitionedPages = ImmutableList.builder();

        boolean done = false;
        long totalRows = 0;
        for (int i = partNumber; i < tableData.getPages().size() && !done; i += totalParts) {
            if (sampleRatio.isPresent() && ThreadLocalRandom.current().nextDouble() >= sampleRatio.getAsDouble()) {
                continue;
            }

            Page page = tableData.getPages().get(i);
            totalRows += page.getPositionCount();
            if (limit.isPresent() && totalRows > limit.getAsLong()) {
                page = page.getRegion(0, (int) (page.getPositionCount() - (totalRows - limit.getAsLong())));
                done = true;
            }
            // Append missing columns with null values. This situation happens when a new column is added without additional insert.
            // The new added columns is behind the row_id column, so add the row_id block first
            int channelCount = page.getChannelCount();
//            if (channelCount <= columnIndexes.length) {
//                BlockBuilder builder = VARCHAR.createBlockBuilder(null, page.getPositionCount());
//                IntStream.range(0, page.getPositionCount()).forEach(_ -> builder.appendNull());
//                page = page.appendColumn(builder.build());
//            }
            for (int j = channelCount; j < columnIndexes.length; j++) {
                Type type = columnTypes.get(j);
                BlockBuilder builder = type.createBlockBuilder(null, page.getPositionCount());
                IntStream.range(0, page.getPositionCount()).forEach(_ -> builder.appendNull());
                page = page.appendColumn(builder.build());
            }
            partitionedPages.add(page.getColumns(columnIndexes));
        }

        if (partNumber == 0 && tableId == 6) {
            System.out.println("Get page size: " + tableData.getPages().size() + ", total rows: " + totalRows + ", name : " + tableData.name);
        }

        return partitionedPages.build();
    }

    public synchronized boolean contains(Long tableId)
    {
        return tables.containsKey(tableId);
    }

    public synchronized void purge(long tableId)
    {
        TableData tableData = tables.remove(tableId);
        if (tableData != null) {
            currentBytes = currentBytes - tableData.getPages().stream().mapToLong(Page::getRetainedSizeInBytes).sum();
        }
    }

    public synchronized void cleanUp(Set<Long> activeTableIds)
    {
        // We have to remember that there might be some race conditions when there are two tables created at once.
        // That can lead to a situation when MemoryPagesStore already knows about a newer second table on some worker
        // but cleanUp is triggered by insert from older first table, which MemoryTableHandle was created before
        // second table creation. Thus activeTableIds can have missing latest ids and we can only clean up tables
        // that:
        // - have smaller value then max(activeTableIds).
        // - are missing from activeTableIds set

        if (activeTableIds.isEmpty()) {
            // if activeTableIds is empty, we cannot determine latestTableId...
            return;
        }
        long latestTableId = Collections.max(activeTableIds);

        for (Iterator<Map.Entry<Long, TableData>> tableDataIterator = tables.entrySet().iterator(); tableDataIterator.hasNext(); ) {
            Map.Entry<Long, TableData> tablePagesEntry = tableDataIterator.next();
            Long tableId = tablePagesEntry.getKey();
            if (tableId < latestTableId && !activeTableIds.contains(tableId)) {
                for (Page removedPage : tablePagesEntry.getValue().getPages()) {
                    currentBytes -= removedPage.getRetainedSizeInBytes();
                }
                tableDataIterator.remove();
            }
        }
    }

    private static final class TableData
    {
        private final int rowIdIndex;
        private final List<Page> pages = new ArrayList<>();
        private long rows;

        private final String name = UUID.randomUUID().toString();

        TableData(int rowIdIndex)
        {
            this.rowIdIndex = rowIdIndex;
        }

        public void add(Page page)
        {
            pages.add(page);
            rows += page.getPositionCount();
        }

        // return how many bytes deleted
        public long update(Page page)
        {
            long bytesAdded = page.getRetainedSizeInBytes();
            long bytesDelete = delete(page.getBlock(rowIdIndex));

            add(page);
            return bytesDelete - bytesAdded;
        }

        // return how many bytes deleted
        public long delete(Block block)
        {
            Set<String> rowIds = buildRowIds(block);
            int remaining = block.getPositionCount();
            System.out.println("rowIds: " + rowIds + ", pages: " + pages.size());
            rows -= remaining;

            long bytesDeleted = 0;
            for (int i = 0; i < pages.size(); i++) {
                Page page = pages.get(i);

                long beforeBytes = page.getRetainedSizeInBytes();
                Block rowIdBlock = page.getBlock(rowIdIndex);

                int positionCount = 0;
                int[] positions = new int[page.getPositionCount()];
                for (int position = 0; position < page.getPositionCount(); position++) {
                    Slice rowId = VARCHAR.getSlice(rowIdBlock, position);
                    if (!rowIds.contains(rowId.toStringUtf8())) {
                        positions[positionCount] = position;
                        positionCount++;
                    }
                    else {
                        System.out.println("rowId to remove: " + rowId);
                        remaining--;
                    }
                }

                if (positionCount == page.getPositionCount()) {
                    continue;
                }

                page = page.getPositions(positions, 0, positionCount);
                bytesDeleted += beforeBytes - page.getRetainedSizeInBytes();

                // TODO: we actually could remove the page if it doesn't contain data anymore
                pages.set(i, page);
            }

            checkArgument(remaining == 0, "There are %s remaining rows not be deleted", remaining);
            return bytesDeleted;
        }

        private static Set<String> buildRowIds(Block block)
        {
            ImmutableSet.Builder<String> rowIdsBuilder = ImmutableSet.builder();
            for (int position = 0; position < block.getPositionCount(); position++) {
                rowIdsBuilder.add(VARCHAR.getSlice(block, position).toStringUtf8());
            }
            return rowIdsBuilder.build();
        }

        private List<Page> getPages()
        {
            return pages;
        }

        private long getRows()
        {
            return rows;
        }
    }
}
