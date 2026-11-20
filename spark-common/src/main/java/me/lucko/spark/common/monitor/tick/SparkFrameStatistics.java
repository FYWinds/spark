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

package me.lucko.spark.common.monitor.tick;

import me.lucko.spark.api.statistic.misc.DoubleAverageInfo;
import me.lucko.spark.common.metric.Metrics;
import me.lucko.spark.common.monitor.MonitoringExecutor;
import me.lucko.spark.common.tick.TickHook;
import me.lucko.spark.common.tick.TickReporter;
import me.lucko.spark.common.tick.TickType;
import me.lucko.spark.common.util.ImmutableDoubleAverageInfo;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;

/**
 * Tick statistics for platforms whose ticks are rendered frames ({@link TickType#FRAME}).
 *
 * <p>{@link SparkTickStatistics} assumes 20 ticks per second: it sizes its averages in ticks and
 * starts them off at 20. Frame rates have no such target and vary widely, so here the averages
 * span the time they are named after, and only contain what has been measured.</p>
 */
public class SparkFrameStatistics implements TickHook.Callback, TickReporter.Callback, TickStatistics {

    private static final long SEC_IN_NANO = TimeUnit.SECONDS.toNanos(1);

    /** The number of frame rate samples kept, each about one second long */
    private static final int RATE_SAMPLES = 60 * 15;

    /** How long frame times are kept for: the longest average is over 5 minutes */
    private static final long FRAME_TIMES_RETENTION_NANOS = TimeUnit.MINUTES.toNanos(5);

    /** The most frame times kept (4 MiB), enough for 5 minutes at about 870 FPS */
    private static final int MAX_FRAME_TIMES = 1 << 18;

    private final Metrics metrics;
    private final LongSupplier nanoTimeSource;

    // the frame rate sample being counted, only accessed by the thread rendering the frames
    private boolean rateSampleStarted = false;
    private long rateSampleStart;
    private int rateSampleFrames;

    private final RateSamples rateSamples = new RateSamples();

    private volatile boolean durationSupported = false;
    private final FrameTimes frameTimes = new FrameTimes();
    private final FrameTimesWindow frameTimes10Sec = new FrameTimesWindow(this.frameTimes, TimeUnit.SECONDS.toNanos(10));
    private final FrameTimesWindow frameTimes1Min = new FrameTimesWindow(this.frameTimes, TimeUnit.MINUTES.toNanos(1));
    private final FrameTimesWindow frameTimes5Min = new FrameTimesWindow(this.frameTimes, TimeUnit.MINUTES.toNanos(5));
    private final AtomicBoolean frameTimesRecordPending = new AtomicBoolean();

    SparkFrameStatistics(Metrics metrics, LongSupplier nanoTimeSource) {
        this.metrics = metrics;
        this.nanoTimeSource = nanoTimeSource;
    }

    public SparkFrameStatistics(Metrics metrics) {
        this(metrics, System::nanoTime);
    }

    @Override
    public boolean isDurationSupported() {
        return this.durationSupported;
    }

    @Override
    public void onTick(int currentTick) {
        long now = this.nanoTimeSource.getAsLong();

        if (!this.rateSampleStarted) {
            this.rateSampleStarted = true;
            this.rateSampleStart = now;
            return;
        }

        this.rateSampleFrames++;
        long diff = now - this.rateSampleStart;
        if (diff < SEC_IN_NANO) {
            return;
        }

        this.rateSamples.add(this.rateSampleFrames, diff);
        this.rateSampleStart = now;
        this.rateSampleFrames = 0;

        if (this.metrics.shouldRecordTps()) {
            this.metrics.tps().record(tps10Sec());
        }
    }

    @Override
    public void onTick(double duration) {
        this.durationSupported = true;
        this.frameTimes.add(this.nanoTimeSource.getAsLong(), duration);

        // there are many frames between two recordings, only queue one at a time
        if (this.metrics.shouldRecordTickDuration() && this.frameTimesRecordPending.compareAndSet(false, true)) {
            // percentiles need sorting, so do that async to avoid blocking the render thread
            MonitoringExecutor.INSTANCE.execute(() -> {
                try {
                    this.metrics.tickDuration().record(this.frameTimes10Sec.toImmutable());
                } finally {
                    this.frameTimesRecordPending.set(false);
                }
            });
        }
    }

    @Override
    public int gameTargetTps() {
        // frames have no fixed target rate
        return 0;
    }

    @Override
    public double tps5Sec() {
        return this.rateSamples.average(5);
    }

    @Override
    public double tps10Sec() {
        return this.rateSamples.average(10);
    }

    @Override
    public double tps1Min() {
        return this.rateSamples.average(60);
    }

    @Override
    public double tps5Min() {
        return this.rateSamples.average(60 * 5);
    }

    @Override
    public double tps15Min() {
        return this.rateSamples.average(60 * 15);
    }

    @Override
    public DoubleAverageInfo duration10Sec() {
        if (!this.durationSupported) {
            return null;
        }
        return this.frameTimes10Sec;
    }

    @Override
    public DoubleAverageInfo duration1Min() {
        if (!this.durationSupported) {
            return null;
        }
        return this.frameTimes1Min;
    }

    @Override
    public DoubleAverageInfo duration5Min() {
        if (!this.durationSupported) {
            return null;
        }
        return this.frameTimes5Min;
    }

    /**
     * The frame rate samples, each counting the frames over (at least) one second.
     */
    private static final class RateSamples {
        private final int[] frames = new int[RATE_SAMPLES];
        private final long[] nanos = new long[RATE_SAMPLES];
        /** The index the next sample is written to */
        private int next = 0;
        private int size = 0;

        synchronized void add(int frames, long nanos) {
            this.frames[this.next] = frames;
            this.nanos[this.next] = nanos;
            this.next = (this.next + 1) % RATE_SAMPLES;
            if (this.size < RATE_SAMPLES) {
                this.size++;
            }
        }

        /**
         * Gets the frame rate over the latest samples.
         *
         * @param samples the number of samples (about the number of seconds) to average over
         * @return the frames per second, or 0 if nothing has been measured yet
         */
        synchronized double average(int samples) {
            int count = Math.min(samples, this.size);
            long frames = 0;
            long nanos = 0;
            for (int i = 1; i <= count; i++) {
                int index = (this.next - i + RATE_SAMPLES) % RATE_SAMPLES;
                frames += this.frames[index];
                nanos += this.nanos[index];
            }
            return nanos == 0 ? 0 : frames * (double) SEC_IN_NANO / nanos;
        }
    }

    /**
     * The durations of the frames that ended within the retention period, oldest first.
     */
    private static final class FrameTimes {
        /** The times (from {@link System#nanoTime()}) the frames ended at */
        private long[] ends = new long[1024];
        /** The durations of the frames, in milliseconds */
        private double[] durations = new double[1024];
        /** The index of the oldest frame, the capacity is always a power of two */
        private int first = 0;
        private int size = 0;

        synchronized void add(long end, double duration) {
            int mask = this.ends.length - 1;
            while (this.size > 0 && end - this.ends[this.first] > FRAME_TIMES_RETENTION_NANOS) {
                this.first = (this.first + 1) & mask;
                this.size--;
            }

            if (this.size == this.ends.length) {
                if (this.ends.length < MAX_FRAME_TIMES) {
                    grow();
                    mask = this.ends.length - 1;
                } else {
                    // drop the oldest frame early
                    this.first = (this.first + 1) & mask;
                    this.size--;
                }
            }

            int index = (this.first + this.size) & mask;
            this.ends[index] = end;
            this.durations[index] = duration;
            this.size++;
        }

        private void grow() {
            int mask = this.ends.length - 1;
            long[] ends = new long[this.ends.length * 2];
            double[] durations = new double[this.durations.length * 2];
            for (int i = 0; i < this.size; i++) {
                int index = (this.first + i) & mask;
                ends[i] = this.ends[index];
                durations[i] = this.durations[index];
            }
            this.ends = ends;
            this.durations = durations;
            this.first = 0;
        }

        /**
         * Copies the durations of the frames that ended less than the given time before the
         * latest frame ended.
         *
         * @param windowNanos the length of the window
         * @return the durations, oldest first
         */
        synchronized double[] window(long windowNanos) {
            if (this.size == 0) {
                return new double[0];
            }

            int mask = this.ends.length - 1;
            int last = this.first + this.size - 1;
            long newest = this.ends[last & mask];

            int count = 0;
            while (count < this.size && newest - this.ends[(last - count) & mask] < windowNanos) {
                count++;
            }

            double[] window = new double[count];
            for (int i = 0; i < count; i++) {
                window[i] = this.durations[(last - count + 1 + i) & mask];
            }
            return window;
        }
    }

    /**
     * A view of the durations of the frames in a window of time.
     */
    private static final class FrameTimesWindow implements DoubleAverageInfo {
        private final FrameTimes frameTimes;
        private final long windowNanos;

        FrameTimesWindow(FrameTimes frameTimes, long windowNanos) {
            this.frameTimes = frameTimes;
            this.windowNanos = windowNanos;
        }

        @Override
        public double mean() {
            double[] durations = this.frameTimes.window(this.windowNanos);
            if (durations.length == 0) {
                return 0;
            }
            double total = 0;
            for (double duration : durations) {
                total += duration;
            }
            return total / durations.length;
        }

        @Override
        public double max() {
            double[] durations = this.frameTimes.window(this.windowNanos);
            if (durations.length == 0) {
                return 0;
            }
            double max = durations[0];
            for (double duration : durations) {
                max = Math.max(max, duration);
            }
            return max;
        }

        @Override
        public double min() {
            double[] durations = this.frameTimes.window(this.windowNanos);
            if (durations.length == 0) {
                return 0;
            }
            double min = durations[0];
            for (double duration : durations) {
                min = Math.min(min, duration);
            }
            return min;
        }

        @Override
        public double percentile(double percentile) {
            if (percentile < 0 || percentile > 1) {
                throw new IllegalArgumentException("Invalid percentile " + percentile);
            }

            double[] durations = this.frameTimes.window(this.windowNanos);
            Arrays.sort(durations);
            return percentile(durations, percentile);
        }

        /**
         * Gets all the values at once, sorting the frame times only once.
         *
         * @return the values
         */
        ImmutableDoubleAverageInfo toImmutable() {
            double[] durations = this.frameTimes.window(this.windowNanos);
            Arrays.sort(durations);

            double total = 0;
            for (double duration : durations) {
                total += duration;
            }
            int count = durations.length;
            return new ImmutableDoubleAverageInfo(
                    count == 0 ? 0 : total / count,
                    count == 0 ? 0 : durations[count - 1],
                    count == 0 ? 0 : durations[0],
                    percentile(durations, 0.50d),
                    percentile(durations, 0.95d)
            );
        }

        private static double percentile(double[] sortedDurations, double percentile) {
            if (sortedDurations.length == 0) {
                return 0;
            }
            int rank = (int) Math.ceil(percentile * (sortedDurations.length - 1));
            return sortedDurations[rank];
        }
    }

}
