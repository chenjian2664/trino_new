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
package io.trino.operator;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import io.trino.client.spooling.DataAttributes;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.OperationTimer.OperationTiming;
import io.trino.operator.SpoolingController.MetricSnapshot;
import io.trino.server.protocol.OutputColumn;
import io.trino.server.protocol.spooling.QueryDataEncoder;
import io.trino.server.protocol.spooling.SpooledBlock;
import io.trino.spi.Mergeable;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.spool.SpooledSegmentHandle;
import io.trino.spi.spool.SpoolingContext;
import io.trino.spi.spool.SpoolingManager;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.planner.plan.PlanNodeId;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.Duration.succinctDuration;
import static io.trino.client.spooling.DataAttribute.EXPIRES_AT;
import static io.trino.client.spooling.DataAttribute.ROWS_COUNT;
import static io.trino.client.spooling.DataAttribute.SEGMENT_SIZE;
import static io.trino.operator.OutputSpoolingOperatorFactory.OutputSpoolingOperator.State.FINISHED;
import static io.trino.operator.OutputSpoolingOperatorFactory.OutputSpoolingOperator.State.HAS_LAST_OUTPUT;
import static io.trino.operator.OutputSpoolingOperatorFactory.OutputSpoolingOperator.State.HAS_OUTPUT;
import static io.trino.operator.OutputSpoolingOperatorFactory.OutputSpoolingOperator.State.NEEDS_INPUT;
import static io.trino.operator.SpoolingController.Mode.INLINE;
import static io.trino.operator.SpoolingController.Mode.SPOOL;
import static io.trino.server.protocol.spooling.SpooledBlock.SPOOLING_METADATA_TYPE;
import static io.trino.server.protocol.spooling.SpooledBlock.createNonSpooledPage;
import static io.trino.server.protocol.spooling.SpoolingSessionProperties.getInitialSegmentSize;
import static io.trino.server.protocol.spooling.SpoolingSessionProperties.getMaxSegmentSize;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class OutputSpoolingOperatorFactory
        implements OperatorFactory
{
    private final int operatorId;
    private final PlanNodeId planNodeId;
    private final Map<Symbol, Integer> operatorLayout;
    private final SpoolingManager spoolingManager;
    private final Supplier<QueryDataEncoder> queryDataEncoderSupplier;
    private final QueryDataEncoder queryDataEncoder;
    private final AtomicInteger encoderReferencesCount = new AtomicInteger(1);

    private boolean closed;

    public OutputSpoolingOperatorFactory(int operatorId, PlanNodeId planNodeId, Map<Symbol, Integer> operatorLayout, Supplier<QueryDataEncoder> queryDataEncoderSupplier, SpoolingManager spoolingManager)
    {
        this.operatorId = operatorId;
        this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
        this.operatorLayout = ImmutableMap.copyOf(requireNonNull(operatorLayout, "layout is null"));
        this.queryDataEncoderSupplier = requireNonNull(queryDataEncoderSupplier, "queryDataEncoder is null");
        this.queryDataEncoder = queryDataEncoderSupplier.get();
        this.spoolingManager = requireNonNull(spoolingManager, "spoolingManager is null");
    }

    public static List<OutputColumn> spooledOutputLayout(OutputNode outputNode, Map<Symbol, Integer> layout)
    {
        List<String> columnNames = outputNode.getColumnNames();
        List<Symbol> outputSymbols = outputNode.getOutputSymbols();

        ImmutableList.Builder<OutputColumn> outputColumnBuilder = ImmutableList.builderWithExpectedSize(outputNode.getColumnNames().size());
        for (int i = 0; i < columnNames.size(); i++) {
            outputColumnBuilder.add(new OutputColumn(layout.get(outputSymbols.get(i)), columnNames.get(i), outputSymbols.get(i).type()));
        }
        return outputColumnBuilder.build();
    }

    @Override
    public Operator createOperator(DriverContext driverContext)
    {
        checkState(!closed, "Factory is already closed");
        OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, OutputSpoolingOperator.class.getSimpleName());
        encoderReferencesCount.incrementAndGet();
        QueryDataEncoder trackingQueryDataEncoder = new QueryDataEncoder()
        {
            private final AtomicBoolean closed = new AtomicBoolean();

            @Override
            public DataAttributes encodeTo(OutputStream output, List<Page> pages)
                    throws IOException
            {
                return queryDataEncoder.encodeTo(output, pages);
            }

            @Override
            public void close()
            {
                if (closed.getAndSet(true)) {
                    return;
                }
                closeEncoderIfNoMoreReferences();
            }

            @Override
            public String encoding()
            {
                return queryDataEncoder.encoding();
            }
        };
        return new OutputSpoolingOperator(operatorContext, trackingQueryDataEncoder, spoolingManager, operatorLayout);
    }

    @Override
    public void noMoreOperators()
    {
        closed = true;
        closeEncoderIfNoMoreReferences();
    }

    private void closeEncoderIfNoMoreReferences()
    {
        if (encoderReferencesCount.decrementAndGet() == 0) {
            queryDataEncoder.close();
        }
    }

    @Override
    public OperatorFactory duplicate()
    {
        return new OutputSpoolingOperatorFactory(operatorId, planNodeId, operatorLayout, queryDataEncoderSupplier, spoolingManager);
    }

    static class OutputSpoolingOperator
            implements Operator
    {
        private final SpoolingController controller;
        private final ZoneId clientZoneId;

        enum State
        {
            NEEDS_INPUT, // output is not ready
            HAS_OUTPUT, // output page ready
            HAS_LAST_OUTPUT, // last output page ready
            FINISHED // no more pages will be ever produced
        }

        private OutputSpoolingOperator.State state = NEEDS_INPUT;
        private final OperatorContext operatorContext;
        private final LocalMemoryContext userMemoryContext;
        private final QueryDataEncoder queryDataEncoder;
        private final SpoolingManager spoolingManager;
        private final PageBuffer buffer;
        private final Block[] emptyBlocks;
        private final OperationTiming spoolingTiming = new OperationTiming();
        private final AtomicLong encodedBytes = new AtomicLong();

        private Page outputPage;

        public OutputSpoolingOperator(OperatorContext operatorContext, QueryDataEncoder queryDataEncoder, SpoolingManager spoolingManager, Map<Symbol, Integer> layout)
        {
            this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
            this.clientZoneId = operatorContext.getSession().getTimeZoneKey().getZoneId();
            this.controller = new PipelineSpoolingController(
                    operatorContext.getDriverContext().getPipelineContext(),
                    new OperatorSpoolingController(
                        getInitialSegmentSize(operatorContext.getSession()).toBytes(),
                        getMaxSegmentSize(operatorContext.getSession()).toBytes()));
            this.userMemoryContext = operatorContext.newLocalUserMemoryContext(OutputSpoolingOperator.class.getSimpleName());
            this.queryDataEncoder = requireNonNull(queryDataEncoder, "queryDataEncoder is null");
            this.spoolingManager = requireNonNull(spoolingManager, "spoolingManager is null");
            this.emptyBlocks = emptyBlocks(layout);
            this.buffer = PageBuffer.create(userMemoryContext);

            operatorContext.setInfoSupplier(new OutputSpoolingInfoSupplier(spoolingTiming, controller, encodedBytes));
        }

        @Override
        public OperatorContext getOperatorContext()
        {
            return operatorContext;
        }

        @Override
        public boolean needsInput()
        {
            return state == NEEDS_INPUT;
        }

        @Override
        public void addInput(Page page)
        {
            checkState(needsInput(), "Operator is already finishing");
            requireNonNull(page, "page is null");

            outputPage = switch (controller.nextMode(page)) {
                case SPOOL -> {
                    buffer.add(page);
                    yield outputBuffer(false);
                }
                case BUFFER -> {
                    buffer.add(page);
                    yield null;
                }
                case INLINE -> createNonSpooledPage(page);
            };

            if (outputPage != null) {
                state = HAS_OUTPUT;
            }
        }

        @Override
        public Page getOutput()
        {
            if (state != HAS_OUTPUT && state != HAS_LAST_OUTPUT) {
                return null;
            }

            Page toReturn = outputPage;
            outputPage = null;
            state = state == HAS_LAST_OUTPUT ? FINISHED : NEEDS_INPUT;
            return toReturn;
        }

        @Override
        public void finish()
        {
            if (state == NEEDS_INPUT) {
                outputPage = outputBuffer(true);
                if (outputPage != null) {
                    state = HAS_LAST_OUTPUT;
                }
                else {
                    state = FINISHED;
                }
            }
        }

        @Override
        public boolean isFinished()
        {
            return state == FINISHED;
        }

        private Page outputBuffer(boolean finished)
        {
            if (buffer.isEmpty()) {
                return null;
            }

            synchronized (buffer) {
                return spool(buffer.removeAll(), finished);
            }
        }

        private Page spool(List<Page> pages, boolean finished)
        {
            long rows = reduce(pages, Page::getPositionCount);
            long size = reduce(pages, Page::getSizeInBytes);
            SpooledSegmentHandle segmentHandle = spoolingManager.create(new SpoolingContext(
                    queryDataEncoder.encoding(),
                    operatorContext.getDriverContext().getSession().getQueryId(),
                    rows,
                    size));
            String expiresAt = ZonedDateTime.ofInstant(segmentHandle.expirationTime(), clientZoneId).toLocalDateTime().toString();

            OperationTimer overallTimer = new OperationTimer(false);
            try (OutputStream output = spoolingManager.createOutputStream(segmentHandle)) {
                DataAttributes attributes = queryDataEncoder.encodeTo(output, pages)
                        .toBuilder()
                        .set(ROWS_COUNT, rows)
                        .set(EXPIRES_AT, expiresAt)
                        .build();

                encodedBytes.addAndGet(attributes.get(SEGMENT_SIZE, Integer.class));

                if (finished) {
                    controller.execute(SPOOL, rows, size); // final buffer
                }

                // This page is small (hundreds of bytes) so there is no point in tracking its memory usage
                return emptySingleRowPage(SpooledBlock.forLocation(spoolingManager.location(segmentHandle), attributes).serialize());
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            finally {
                overallTimer.end(spoolingTiming);
            }
        }

        private Page emptySingleRowPage(Block block)
        {
            Block[] blocks = emptyBlocks;
            blocks[blocks.length - 1] = block;
            return new Page(blocks);
        }

        static long reduce(List<Page> page, ToLongFunction<Page> reduce)
        {
            return page.stream()
                    .mapToLong(reduce)
                    .sum();
        }

        private static Block[] emptyBlocks(Map<Symbol, Integer> layout)
        {
            Block[] blocks = new Block[layout.size() + 1]; // last block contains spooled metadata
            for (Map.Entry<Symbol, Integer> entry : layout.entrySet()) {
                blocks[entry.getValue()] = entry.getKey().type().createNullBlock();
            }
            blocks[blocks.length - 1] = SPOOLING_METADATA_TYPE.createNullBlock();
            return blocks;
        }

        @Override
        public void close()
                throws Exception
        {
            userMemoryContext.close();
            queryDataEncoder.close();
        }
    }

    private static class PageBuffer
    {
        private final List<Page> buffer = new ArrayList<>();
        private final LocalMemoryContext memoryContext;

        private PageBuffer(LocalMemoryContext memoryContext)
        {
            this.memoryContext = requireNonNull(memoryContext, "memoryContext is null");
        }

        public static PageBuffer create(LocalMemoryContext memoryContext)
        {
            return new PageBuffer(memoryContext);
        }

        public void add(Page page)
        {
            buffer.add(page);
            memoryContext.setBytes(memoryContext.getBytes() + page.getRetainedSizeInBytes());
        }

        public boolean isEmpty()
        {
            return buffer.isEmpty();
        }

        public List<Page> removeAll()
        {
            List<Page> pages;
            synchronized (buffer) {
                pages = ImmutableList.copyOf(buffer);
                buffer.clear();
                memoryContext.setBytes(0);
            }
            return pages;
        }
    }

    private record OutputSpoolingInfoSupplier(
            OperationTiming spoolingTiming,
            SpoolingController controller,
            AtomicLong encodedBytes)
            implements Supplier<OutputSpoolingInfo>
    {
        private OutputSpoolingInfoSupplier
        {
            requireNonNull(spoolingTiming, "spoolingTiming is null");
            requireNonNull(controller, "controller is null");
            requireNonNull(encodedBytes, "encodedBytes is null");
        }

        @Override
        public OutputSpoolingInfo get()
        {
            MetricSnapshot inlined = controller.getMetrics(INLINE);
            MetricSnapshot spooled = controller.getMetrics(SPOOL);

            return new OutputSpoolingInfo(
                    succinctDuration(spoolingTiming.getWallNanos(), NANOSECONDS),
                    succinctDuration(spoolingTiming.getCpuNanos(), NANOSECONDS),
                    inlined.pages(),
                    inlined.positions(),
                    inlined.size(),
                    spooled.pages(),
                    spooled.positions(),
                    spooled.size(),
                    encodedBytes.get());
        }
    }

    public record OutputSpoolingInfo(
            Duration spoolingWallTime,
            Duration spoolingCpuTime,
            long inlinedPages,
            long inlinedPositions,
            long inlinedRawBytes,
            long spooledPages,
            long spooledPositions,
            long spooledRawBytes,
            long spooledEncodedBytes)
            implements Mergeable<OutputSpoolingInfo>, OperatorInfo
    {
        public OutputSpoolingInfo
        {
            requireNonNull(spoolingWallTime, "spoolingWallTime is null");
            requireNonNull(spoolingCpuTime, "spoolingCpuTime is null");
        }

        @Override
        public OutputSpoolingInfo mergeWith(OutputSpoolingInfo other)
        {
            return new OutputSpoolingInfo(
                    succinctDuration(spoolingWallTime.toMillis() + other.spoolingWallTime().toMillis(), MILLISECONDS),
                    succinctDuration(spoolingCpuTime.toMillis() + other.spoolingCpuTime().toMillis(), MILLISECONDS),
                    inlinedPages + other.inlinedPages(),
                    inlinedPositions + other.inlinedPositions,
                    inlinedRawBytes + other.inlinedRawBytes,
                    spooledPages + other.spooledPages,
                    spooledPositions + other.spooledPositions,
                    spooledRawBytes + other.spooledRawBytes,
                    spooledEncodedBytes + other.spooledEncodedBytes);
        }

        @JsonProperty
        public double getEncodedToRawBytesRatio()
        {
            return 1.0 * spooledEncodedBytes / spooledRawBytes;
        }

        @Override
        public boolean isFinal()
        {
            return true;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("spoolingWallTime", spoolingWallTime)
                    .add("spoolingCpuTime", spoolingCpuTime)
                    .add("inlinedPages", inlinedPages)
                    .add("inlinedPositions", inlinedPositions)
                    .add("inlinedRawBytes", inlinedRawBytes)
                    .add("spooledPages", spooledPages)
                    .add("spooledPositions", spooledPositions)
                    .add("spooledRawBytes", spooledRawBytes)
                    .add("spooledEncodedBytes", spooledEncodedBytes)
                    .add("encodedToRawBytesRatio", getEncodedToRawBytesRatio())
                    .toString();
        }
    }
}
