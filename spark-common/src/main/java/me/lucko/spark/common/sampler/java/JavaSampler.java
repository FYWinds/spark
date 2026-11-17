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

package me.lucko.spark.common.sampler.java;

import me.lucko.spark.common.SparkPlatform;
import me.lucko.spark.common.sampler.AbstractSampler;
import me.lucko.spark.common.sampler.SamplerMode;
import me.lucko.spark.common.sampler.SamplerSettings;
import me.lucko.spark.common.sampler.SamplerType;
import me.lucko.spark.common.sampler.jfr.JfrBackend;
import me.lucko.spark.common.sampler.jfr.JfrSession;
import me.lucko.spark.common.sampler.window.ProfilingWindowUtils;
import me.lucko.spark.common.sampler.window.WindowStatisticsCollector;
import me.lucko.spark.common.tick.TickHook;
import me.lucko.spark.common.util.MethodDisambiguator;
import me.lucko.spark.common.util.SparkScheduledThreadPoolExecutor;
import me.lucko.spark.common.util.SparkThreadFactory;
import me.lucko.spark.common.util.TimeUtil;
import me.lucko.spark.common.ws.SamplerViewerSocket;
import me.lucko.spark.proto.SparkSamplerProtos.SamplerData;
import org.jspecify.annotations.Nullable;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntPredicate;
import java.util.logging.Level;

/**
 * A sampler implementation using Java (WarmRoast).
 */
public class JavaSampler extends AbstractSampler implements Runnable {

    /** The worker pool for inserting stack nodes */
    private final ScheduledExecutorService workerPool = new SparkScheduledThreadPoolExecutor(6, new SparkThreadFactory("spark-java-sampler", false));

    /** The longest single sleep of the sampling thread, see {@link #sleepUntil(long)} */
    private static final long MAX_SLEEP_NANOS = TimeUnit.MILLISECONDS.toNanos(2);

    /** How much longer than asked a sleep has to take to count as overrunning */
    private static final long SLEEP_OVERRUN_NANOS = TimeUnit.MILLISECONDS.toNanos(4);

    /** The number of sleeps in a row that have to overrun before the thread stops sleeping */
    private static final int SLEEP_OVERRUNS = 3;

    /** How often a thread that has stopped sleeping tries a sleep again */
    private static final long SLEEP_CHECK_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(5);

    // only touched by the sampling thread
    private boolean sleepsOverrun = false;
    private int sleepOverruns = 0;
    private long sleepsLastChecked;

    /** The thread that takes the samples */
    private Thread samplingThread;
    private volatile boolean running = false;

    /** The task to send statistics to the viewer socket */
    private ScheduledFuture<?> socketStatisticsTask;

    /** The thread management interface for the current JVM */
    private final ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();

    /** Responsible for aggregating and then outputting collected sampling data */
    private final JavaDataAggregator dataAggregator;

    /** The last window that was profiled */
    private final AtomicInteger lastWindow = new AtomicInteger();

    /** The flight recorder, used to correct the stack traces of running threads if available */
    private final @Nullable JfrBackend jfr;
    private SafepointBiasCorrector corrector;
    private JfrSession jfrSession;

    public JavaSampler(SparkPlatform platform, SamplerSettings settings) {
        this(platform, settings, null);
    }

    public JavaSampler(SparkPlatform platform, SamplerSettings settings, @Nullable JfrBackend jfr) {
        super(platform, settings);
        this.dataAggregator = new SimpleJavaDataAggregator(this.workerPool, settings.threadGrouper(), settings.interval(), settings.ignoreSleeping());
        this.jfr = jfr;
    }

    public JavaSampler(SparkPlatform platform, SamplerSettings settings, TickHook tickHook, int tickLengthThreshold) {
        this(platform, settings, tickHook, tickLengthThreshold, null);
    }

    public JavaSampler(SparkPlatform platform, SamplerSettings settings, TickHook tickHook, int tickLengthThreshold, @Nullable JfrBackend jfr) {
        super(platform, settings);
        this.dataAggregator = new TickedJavaDataAggregator(this.workerPool, settings.threadGrouper(), settings.interval(), settings.ignoreSleeping(), tickHook, tickLengthThreshold);
        this.jfr = jfr;
    }

    @Override
    public void start() {
        super.start();

        TickHook tickHook = this.platform.getTickHook();
        if (tickHook != null) {
            if (this.dataAggregator instanceof TickedJavaDataAggregator) {
                WindowStatisticsCollector.ExplicitTickCounter counter = this.windowStatisticsCollector.startCountingTicksExplicit(tickHook);
                ((TickedJavaDataAggregator) this.dataAggregator).setTickCounter(counter);
            } else {
                this.windowStatisticsCollector.startCountingTicks(tickHook);
            }
        }

        if (this.jfr != null) {
            try {
                SafepointBiasCorrector corrector = new SafepointBiasCorrector(this.dataAggregator::write, this.interval);
                this.jfrSession = this.jfr.startExecution(this.interval, corrector);
                this.corrector = corrector;
                this.dataAggregator.setCorrector(corrector);
            } catch (Throwable e) {
                this.platform.getPlugin().log(Level.WARNING, "Unable to start the flight recorder, the profiler will run without it", e);
            }
        }

        this.windowStatisticsCollector.recordWindowStartTime(ProfilingWindowUtils.monotonicTimeToWindow(this.startTime));

        this.running = true;
        this.samplingThread = new Thread(this, "spark-java-sampler-ticker");
        this.samplingThread.setDaemon(true);
        this.samplingThread.start();
    }

    /**
     * Gets if the stack traces of running threads are being corrected using the flight recorder.
     */
    public boolean isUsingFlightRecorder() {
        return this.corrector != null;
    }

    @Override
    public void stop(boolean cancelled) {
        super.stop(cancelled);

        this.running = false;
        if (this.samplingThread != Thread.currentThread()) {
            this.samplingThread.interrupt();
        }

        if (this.socketStatisticsTask != null) {
            this.socketStatisticsTask.cancel(false);
        }

        if (this.jfrSession != null) {
            if (!cancelled) {
                // the flight recorder's samples arrive late, wait for the ones still on their way
                this.jfrSession.awaitWatermark(System.nanoTime(), 3000);
            }
            this.jfrSession.close();
            this.jfrSession = null;

            this.corrector.finish();
            if (this.corrector.isDisabled()) {
                this.platform.getPlugin().log(Level.WARNING, "Some stack traces were deeper than the flight recorder is set up to record, so its samples were not used. " +
                        "To fix this, add -XX:FlightRecorderOptions:stackdepth=2048 to the JVM arguments.");
            } else if (!cancelled) {
                long corrected = this.corrector.getCorrectedTime();
                long total = corrected + this.corrector.getUncorrectedTime();
                if (total != 0 && corrected * 2 < total) {
                    String reason = this.corrector.wasSamplerIrregular()
                            ? "the JVM did not sample as often as it was asked to"
                            : this.corrector.getIncompleteStacks() != 0
                            ? this.corrector.getIncompleteStacks() + " of its stack traces were incomplete"
                            : "it saw them doing so less often than the built-in sampler";
                    this.platform.getPlugin().log(Level.INFO, "The flight recorder could only be used for " + (corrected * 100 / total) + "% of the time threads were running Java code " +
                            "(" + reason + "). The rest is shown where the built-in sampler saw it.");
                } else if (total != 0) {
                    this.platform.getPlugin().log(Level.INFO, "The flight recorder was used for " + (corrected * 100 / total) + "% of the time threads were running Java code.");
                }
            }
        }

        if (!cancelled) {
            // collect statistics for the final window
            this.windowStatisticsCollector.measureNow(this.lastWindow.get());
        }

        this.workerPool.shutdown();
    }

    /**
     * Takes the samples.
     *
     * <p>Samples are not taken at a fixed rate. Game ticks start at a fixed rate, so samples taken
     * at a fixed rate keep landing on the same few points of the tick. The time to the next sample
     * is random instead, between half and one and a half times the interval. Each sample stands for
     * the time since the one before it.</p>
     */
    @Override
    public void run() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        long intervalNanos = TimeUnit.MICROSECONDS.toNanos(this.interval);

        // if the thread is held up (e.g. by the garbage collector) the time is not made up for by
        // taking samples in quick succession, they would all show what happened to be running after
        long maxSampleNanos = Math.max(intervalNanos * 2, intervalNanos + TimeUnit.MILLISECONDS.toNanos(2));

        long previous = System.nanoTime() - intervalNanos;
        long next = previous + intervalNanos;

        while (this.running) {
            try {
                if (!sleepUntil(next)) {
                    return;
                }

                long time = TimeUtil.monotonicCurrentTimeMillis();
                if (this.autoEndTime != -1 && this.autoEndTime <= time) {
                    stop(false);
                    this.future.complete(this);
                    return;
                }

                int window = ProfilingWindowUtils.monotonicTimeToWindow(time);
                ThreadInfo[] threadDumps = this.threadDumper.dumpThreads(this.threadBean);
                long now = System.nanoTime();

                long sampleMicros = TimeUnit.NANOSECONDS.toMicros(Math.min(now - previous, maxSampleNanos));
                this.workerPool.execute(new InsertDataTask(threadDumps, window, now, sampleMicros));

                previous = now;
                next = now + (intervalNanos / 2) + random.nextLong(intervalNanos);
            } catch (Throwable t) {
                if (!this.running) {
                    return; // stopped while taking a sample
                }
                stop(false);
                this.future.completeExceptionally(t);
                return;
            }
        }
    }

    /**
     * Waits until {@link System#nanoTime()} reaches the given time.
     *
     * <p>Normally by sleeping. But on Windows a sleep can end only on one of the system timer's
     * interrupts, which may be as far as 15.6ms apart: how far depends on what else is running
     * on the machine and on whether the game has a visible window. Sleeps are then far too long,
     * and worse, they end at the same instants as every other sleep in the process, such as the
     * game loop's sleep until its next tick, so every sample sees the start of a tick. When
     * sleeps are seen to overrun like that, the thread stops sleeping and yields in a loop
     * instead, which keeps one CPU core busy for as long as the profiler runs.</p>
     *
     * @return false if the sampler was stopped
     */
    private boolean sleepUntil(long nanoTime) {
        while (this.running) {
            long now = System.nanoTime();
            long remaining = nanoTime - now;
            if (remaining <= 0) {
                return true;
            }

            if (this.sleepsOverrun) {
                if (now - this.sleepsLastChecked < SLEEP_CHECK_INTERVAL_NANOS) {
                    Thread.yield();
                    continue;
                }
                // see if things have changed
                this.sleepsLastChecked = now;
                this.sleepOverruns = 0;
            }

            long sleep = Math.min(remaining, MAX_SLEEP_NANOS);
            long millis = TimeUnit.NANOSECONDS.toMillis(sleep);
            int nanos = (int) (sleep - TimeUnit.MILLISECONDS.toNanos(millis));

            try {
                Thread.sleep(millis, nanos);
            } catch (InterruptedException e) {
                continue; // stopped, or not: the loop checks
            }

            // the background profiler should cost as little as possible, so it puts up with it
            if (this.background) {
                continue;
            }

            boolean overran = System.nanoTime() - now - sleep > SLEEP_OVERRUN_NANOS;
            if (!overran) {
                this.sleepOverruns = 0;
                this.sleepsOverrun = false;
            } else if (this.sleepsOverrun || ++this.sleepOverruns >= SLEEP_OVERRUNS) {
                this.sleepsOverrun = true;
                this.sleepsLastChecked = System.nanoTime();
            }
        }
        return false;
    }

    @Override
    public void attachSocket(SamplerViewerSocket socket) {
        super.attachSocket(socket);

        if (this.socketStatisticsTask == null) {
            this.socketStatisticsTask = this.workerPool.scheduleAtFixedRate(this::sendStatisticsToSocket, 10, 10, TimeUnit.SECONDS);
        }
    }

    private final class InsertDataTask implements Runnable {
        private final ThreadInfo[] threadDumps;
        private final int window;
        private final long nanoTime;
        private final long time;

        InsertDataTask(ThreadInfo[] threadDumps, int window, long nanoTime, long time) {
            this.threadDumps = threadDumps;
            this.window = window;
            this.nanoTime = nanoTime;
            this.time = time;
        }

        @Override
        public void run() {
            for (ThreadInfo threadInfo : this.threadDumps) {
                if (threadInfo.getThreadName() == null || threadInfo.getStackTrace() == null) {
                    continue;
                }
                JavaSampler.this.dataAggregator.insertData(threadInfo, this.window, this.nanoTime, this.time);
            }

            // if we have just stepped over into a new window...
            int previousWindow = JavaSampler.this.lastWindow.getAndUpdate(previous -> Math.max(this.window, previous));
            if (previousWindow != 0 && previousWindow != this.window) {

                // record the start time for the new window
                JavaSampler.this.windowStatisticsCollector.recordWindowStartTime(this.window);

                // collect statistics for the previous window
                JavaSampler.this.windowStatisticsCollector.measureNow(previousWindow);

                // prune data older than the history size
                IntPredicate predicate = ProfilingWindowUtils.keepHistoryBefore(this.window);
                JavaSampler.this.dataAggregator.pruneData(predicate);
                JavaSampler.this.windowStatisticsCollector.pruneStatistics(predicate);

                JavaSampler.this.workerPool.execute(JavaSampler.this::processWindowRotate);
            }
        }
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

    @Override
    public SamplerType getType() {
        return SamplerType.JAVA;
    }

    @Override
    public String getLibraryVersion() {
        return this.corrector != null ? "jfr" : null;
    }

    @Override
    public SamplerMode getMode() {
        return SamplerMode.EXECUTION;
    }
}
