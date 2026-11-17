/*
 * This file is part of spark.
 *
 *  Copyright (c) lucko (Luck) <luck@lucko.me>
 *  Copyright (c) contributors
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package me.lucko.spark.common.sampler.jfr;

import me.lucko.spark.common.SparkPlatform;
import me.lucko.spark.common.sampler.AbstractSampler;
import me.lucko.spark.common.sampler.SamplerMode;
import me.lucko.spark.common.sampler.SamplerSettings;
import me.lucko.spark.common.sampler.SamplerType;
import me.lucko.spark.common.sampler.ThreadGrouper;
import me.lucko.spark.common.sampler.aggregator.AbstractDataAggregator;
import me.lucko.spark.common.sampler.async.ExceedingTicksFilter;
import me.lucko.spark.common.sampler.java.JavaDataAggregator;
import me.lucko.spark.common.sampler.java.JavaNodeExporter;
import me.lucko.spark.common.sampler.window.ProfilingWindowUtils;
import me.lucko.spark.common.tick.TickHook;
import me.lucko.spark.common.tick.TickReporter;
import me.lucko.spark.common.util.MethodDisambiguator;
import me.lucko.spark.common.util.SparkScheduledThreadPoolExecutor;
import me.lucko.spark.common.util.SparkThreadFactory;
import me.lucko.spark.common.util.TimeUtil;
import me.lucko.spark.common.ws.SamplerViewerSocket;
import me.lucko.spark.proto.SparkSamplerProtos.SamplerData;
import me.lucko.spark.proto.SparkSamplerProtos.SamplerMetadata;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntPredicate;
import java.util.logging.Level;

/**
 * A sampler for the profiling modes the JVM has events for: allocations and lock contention.
 *
 * <p>Used where async-profiler is not available. Execution profiling is not done here, see
 * {@link me.lucko.spark.common.sampler.java.SafepointBiasCorrector} for how the flight recorder
 * is used for that.</p>
 */
public class JfrSampler extends AbstractSampler implements JfrSampleListener {

    private final SamplerMode mode;
    private final JfrBackend backend;
    private final Aggregator dataAggregator;

    private final ScheduledExecutorService scheduler = new SparkScheduledThreadPoolExecutor(1, new SparkThreadFactory("spark-jfr-sampler-worker", false));

    private JfrSession session;
    private ScheduledFuture<?> socketStatisticsTask;

    /** The window the last watermark was in, only touched by the thread reading the events */
    private int currentWindow;

    private final AtomicBoolean warnedTruncated = new AtomicBoolean();

    public JfrSampler(SparkPlatform platform, SamplerSettings settings, SamplerMode mode, JfrBackend backend) {
        super(platform, settings);
        this.mode = mode;
        this.backend = backend;
        this.dataAggregator = new Aggregator(settings.threadGrouper(), null, -1);
    }

    public JfrSampler(SparkPlatform platform, SamplerSettings settings, SamplerMode mode, JfrBackend backend, int tickLengthThreshold) {
        super(platform, settings);
        this.mode = mode;
        this.backend = backend;
        this.dataAggregator = new Aggregator(settings.threadGrouper(), platform.getTickReporter(), tickLengthThreshold);
    }

    @Override
    public void start() {
        super.start();

        TickHook tickHook = this.platform.getTickHook();
        if (tickHook != null) {
            this.windowStatisticsCollector.startCountingTicks(tickHook);
        }

        this.currentWindow = ProfilingWindowUtils.monotonicTimeToWindow(this.startTime);
        this.windowStatisticsCollector.recordWindowStartTime(this.currentWindow);

        switch (this.mode) {
            case ALLOCATION:
                this.session = this.backend.startAllocation(this);
                break;
            case LOCK:
                this.session = this.backend.startLock(this.interval, this);
                break;
            default:
                throw new IllegalStateException("Unsupported mode: " + this.mode);
        }

        recordInitialGcStats();

        if (this.autoEndTime != -1) {
            long delay = this.autoEndTime - TimeUtil.monotonicCurrentTimeMillis();
            if (delay > 0) {
                this.scheduler.schedule(() -> {
                    try {
                        stop(false);
                        this.future.complete(this);
                    } catch (Exception e) {
                        this.future.completeExceptionally(e);
                    }
                }, delay, TimeUnit.MILLISECONDS);
            }
        }
    }

    @Override
    public void onSample(long threadId, String threadName, StackTraceElement[] stack, long nanoTime, long value) {
        if (!this.threadDumper.isThreadIncluded(threadId, threadName)) {
            return;
        }

        int window = ProfilingWindowUtils.monotonicTimeToWindow(TimeUtil.monotonicTimeMillis(nanoTime));
        this.dataAggregator.insertData(threadId, threadName, stack, nanoTime, value, window);
    }

    @Override
    public void onWatermark(long nanoTime) {
        int window = ProfilingWindowUtils.monotonicTimeToWindow(TimeUtil.monotonicTimeMillis(nanoTime));
        int previousWindow = this.currentWindow;
        if (window <= previousWindow) {
            return;
        }
        this.currentWindow = window;

        // everything in the previous window has been read
        this.scheduler.execute(() -> {
            try {
                this.windowStatisticsCollector.recordWindowStartTime(window);
                this.windowStatisticsCollector.measureNow(previousWindow);

                IntPredicate predicate = ProfilingWindowUtils.keepHistoryBefore(window);
                this.dataAggregator.pruneData(predicate);
                this.windowStatisticsCollector.pruneStatistics(predicate);

                processWindowRotate();
            } catch (Throwable e) {
                this.platform.getPlugin().log(Level.WARNING, "Exception occurred while rotating profiling window", e);
            }
        });
    }

    @Override
    public void onTruncatedStack() {
        if (this.warnedTruncated.compareAndSet(false, true)) {
            this.platform.getPlugin().log(Level.WARNING, "Some stack traces are deeper than the flight recorder is set up to record and are being left out. " +
                    "To fix this, add -XX:FlightRecorderOptions:stackdepth=2048 to the JVM arguments.");
        }
    }

    @Override
    public void onHeartbeat(long nanoTime) {
        // only for execution sampling
    }

    @Override
    public void stop(boolean cancelled) {
        super.stop(cancelled);

        if (this.session != null) {
            if (!cancelled) {
                // the events arrive late, wait for the ones still on their way
                this.session.awaitWatermark(System.nanoTime(), 3000);
                this.windowStatisticsCollector.measureNow(ProfilingWindowUtils.windowNow());
            }
            this.session.close();
            this.session = null;
        }

        if (this.socketStatisticsTask != null) {
            this.socketStatisticsTask.cancel(false);
        }

        this.scheduler.shutdown();
        this.dataAggregator.close();
    }

    @Override
    public void attachSocket(SamplerViewerSocket socket) {
        super.attachSocket(socket);

        if (this.socketStatisticsTask == null) {
            this.socketStatisticsTask = this.scheduler.scheduleAtFixedRate(this::sendStatisticsToSocket, 10, 10, TimeUnit.SECONDS);
        }
    }

    @Override
    public SamplerType getType() {
        return SamplerType.JAVA;
    }

    @Override
    public String getLibraryVersion() {
        return "jfr";
    }

    @Override
    public SamplerMode getMode() {
        return this.mode;
    }

    @Override
    public SamplerData toProto(SparkPlatform platform, ExportProps exportProps) {
        SamplerData.Builder proto = SamplerData.newBuilder();
        if (exportProps.channelInfo() != null) {
            proto.setChannelInfo(exportProps.channelInfo());
        }

        writeMetadataToProto(proto, platform, exportProps.creator(), exportProps.comment(), this.dataAggregator);

        MethodDisambiguator methodDisambiguator = new MethodDisambiguator(platform.createClassFinder());
        writeDataToProto(proto, this.dataAggregator, timeEncoder -> new JavaNodeExporter(timeEncoder, exportProps.mergeStrategy(), methodDisambiguator), exportProps.classSourceLookup().get(), platform::createClassFinder);

        return proto.build();
    }

    private static final class Aggregator extends AbstractDataAggregator {
        private final TickReporter tickReporter;
        private final ExceedingTicksFilter filter;
        private final long tickLengthThreshold;

        Aggregator(ThreadGrouper threadGrouper, TickReporter tickReporter, int tickLengthThreshold) {
            super(threadGrouper, false);
            this.tickReporter = tickReporter;
            if (tickReporter != null) {
                this.tickLengthThreshold = TimeUnit.MILLISECONDS.toMicros(tickLengthThreshold);
                this.filter = new ExceedingTicksFilter(tickLengthThreshold);
                tickReporter.addCallback(this.filter);
            } else {
                this.tickLengthThreshold = -1;
                this.filter = null;
            }
        }

        void insertData(long threadId, String threadName, StackTraceElement[] stack, long nanoTime, long value, int window) {
            if (this.filter != null && !this.filter.duringExceedingTick(nanoTime)) {
                return;
            }
            try {
                getNode(this.threadGrouper.getGroup(threadId, threadName)).log(JavaDataAggregator.STACK_TRACE_DESCRIBER, stack, value, window);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public SamplerMetadata.DataAggregator getMetadata() {
            SamplerMetadata.DataAggregator.Builder builder = SamplerMetadata.DataAggregator.newBuilder()
                    .setThreadGrouper(this.threadGrouper.asProto());
            if (this.filter != null) {
                builder.setType(SamplerMetadata.DataAggregator.Type.TICKED)
                        .setTickLengthThreshold(this.tickLengthThreshold)
                        .setNumberOfIncludedTicks(this.filter.exceedingTicksCount());
            } else {
                builder.setType(SamplerMetadata.DataAggregator.Type.SIMPLE);
            }
            return builder.build();
        }

        void close() {
            if (this.filter != null) {
                this.tickReporter.removeCallback(this.filter);
            }
        }
    }

}
