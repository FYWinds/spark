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

package me.lucko.spark.common.sampler;

import me.lucko.spark.common.SparkPlatform;
import me.lucko.spark.common.sampler.async.AsyncProfilerAccess;
import me.lucko.spark.common.sampler.async.AsyncSampler;
import me.lucko.spark.common.sampler.async.SampleCollector;
import me.lucko.spark.common.sampler.java.JavaSampler;
import me.lucko.spark.common.sampler.jfr.JfrBackend;
import me.lucko.spark.common.sampler.jfr.JfrSampler;
import me.lucko.spark.common.sampler.jfr.JfrSupport;
import me.lucko.spark.common.tick.TickHook;
import me.lucko.spark.common.util.TimeUtil;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.Level;

/**
 * Builds {@link Sampler} instances.
 */
@SuppressWarnings("UnusedReturnValue")
public class SamplerBuilder {

    private SamplerMode mode = SamplerMode.EXECUTION;
    private double samplingInterval = -1; // milliseconds (execution, lock) or bytes (allocation) depending on the mode
    private boolean ignoreSleeping = false;
    private boolean forceJavaSampler = false;
    private boolean allocLiveOnly = false;
    private long autoEndTime = -1;
    private boolean background = false;
    private ThreadDumper threadDumper = ThreadDumper.ALL;
    private Supplier<ThreadGrouper> threadGrouper = ThreadGrouper.BY_NAME;

    private int ticksOver = -1;
    private TickHook tickHook = null;

    public SamplerBuilder() {
    }

    public SamplerBuilder mode(SamplerMode mode) {
        this.mode = mode;
        return this;
    }

    public SamplerBuilder samplingInterval(double samplingInterval) {
        this.samplingInterval = samplingInterval;
        return this;
    }

    public SamplerBuilder completeAfter(long timeout, TimeUnit unit) {
        if (timeout <= 0) {
            throw new IllegalArgumentException("timeout > 0");
        }
        this.autoEndTime = TimeUtil.monotonicCurrentTimeMillis() + unit.toMillis(timeout);
        return this;
    }

    public SamplerBuilder background(boolean background) {
        this.background = background;
        return this;
    }

    public SamplerBuilder threadDumper(ThreadDumper threadDumper) {
        this.threadDumper = threadDumper;
        return this;
    }

    public SamplerBuilder threadGrouper(Supplier<ThreadGrouper> threadGrouper) {
        this.threadGrouper = threadGrouper;
        return this;
    }

    public SamplerBuilder ticksOver(int ticksOver, TickHook tickHook) {
        this.ticksOver = ticksOver;
        this.tickHook = tickHook;
        return this;
    }

    public SamplerBuilder ignoreSleeping(boolean ignoreSleeping) {
        this.ignoreSleeping = ignoreSleeping;
        return this;
    }

    public SamplerBuilder forceJavaSampler(boolean forceJavaSampler) {
        this.forceJavaSampler = forceJavaSampler;
        return this;
    }

    public SamplerBuilder allocLiveOnly(boolean allocLiveOnly) {
        this.allocLiveOnly = allocLiveOnly;
        return this;
    }

    public Sampler start(SparkPlatform platform) throws UnsupportedOperationException {
        if (this.samplingInterval <= 0) {
            throw new IllegalArgumentException("samplingInterval = " + this.samplingInterval);
        }

        AsyncProfilerAccess asyncProfiler = AsyncProfilerAccess.getInstance(platform);

        boolean onlyTicksOverMode = this.ticksOver != -1 && this.tickHook != null;
        boolean canUseAsyncProfiler = asyncProfiler.checkSupported(platform) && (!onlyTicksOverMode || platform.getTickReporter() != null);

        // where async-profiler cannot run (e.g. on Windows), the JVM's flight recorder
        // is used for allocation and lock profiling instead, if the JVM has it
        JfrBackend jfrSampler = null;

        if (this.mode == SamplerMode.ALLOCATION) {
            if (!canUseAsyncProfiler || !asyncProfiler.checkAllocationProfilingSupported(platform)) {
                jfrSampler = JfrSupport.backend();
                if (jfrSampler == null || !jfrSampler.supportsAllocation()) {
                    logFlightRecorderUnavailable(platform, jfrSampler == null ? JfrSupport.unavailableReason() : "requires Java 16 or newer");
                    throw new UnsupportedOperationException("Allocation profiling is not supported on your system. Check the console for more info.");
                }
                if (this.allocLiveOnly) {
                    throw new UnsupportedOperationException("The --alloc-live-only option needs async-profiler, which is not supported on your system.");
                }
            }
            if (this.ignoreSleeping) {
                platform.getPlugin().log(Level.WARNING, "Ignoring sleeping threads is not supported in allocation profiling mode. Sleeping threads will be included in the results.");
            }
        }

        boolean ignoreSleeping = this.ignoreSleeping;

        if (this.mode == SamplerMode.LOCK) {
            if (this.forceJavaSampler) {
                throw new UnsupportedOperationException("Lock profiling is not supported by the built-in Java sampler.");
            }
            if (!canUseAsyncProfiler || !asyncProfiler.checkLockProfilingSupported(platform)) {
                jfrSampler = JfrSupport.backend();
                if (jfrSampler == null) {
                    logFlightRecorderUnavailable(platform, JfrSupport.unavailableReason());
                    throw new UnsupportedOperationException("Lock profiling is not supported on your system. Check the console for more info.");
                }
            }
            if (ignoreSleeping) {
                // threads waiting for a lock are often parked, which is exactly what we want to measure!
                platform.getPlugin().log(Level.WARNING, "Ignoring sleeping threads is not supported in lock profiling mode. Sleeping threads will be included in the results.");
                ignoreSleeping = false;
            }
        }

        if (this.forceJavaSampler) {
            canUseAsyncProfiler = false;
        }

        // microseconds or bytes depending on the mode
        int interval = (int) (this.mode == SamplerMode.ALLOCATION ?
                this.samplingInterval :
                this.samplingInterval * 1000d // convert to microseconds
        );

        SamplerSettings settings = new SamplerSettings(interval, this.threadDumper, this.threadGrouper.get(), this.autoEndTime, this.background, ignoreSleeping);

        Sampler sampler;
        if (jfrSampler != null && onlyTicksOverMode && platform.getTickReporter() == null) {
            throw new UnsupportedOperationException("The --only-ticks-over option is not supported in this profiling mode on your system.");
        }
        if (jfrSampler != null) {
            sampler = onlyTicksOverMode
                    ? new JfrSampler(platform, settings, this.mode, jfrSampler, this.ticksOver)
                    : new JfrSampler(platform, settings, this.mode, jfrSampler);
        } else if (canUseAsyncProfiler) {
            SampleCollector<?> collector;
            switch (this.mode) {
                case ALLOCATION:
                    collector = new SampleCollector.Allocation(interval, this.allocLiveOnly);
                    break;
                case LOCK:
                    collector = new SampleCollector.Lock(interval);
                    break;
                default:
                    collector = new SampleCollector.Execution(interval);
                    break;
            }

            sampler = onlyTicksOverMode
                    ? new AsyncSampler(platform, settings, collector, this.ticksOver)
                    : new AsyncSampler(platform, settings, collector);
        } else {
            // a thread dump shows running threads at a safepoint, not where they really are.
            // if the JVM has the flight recorder, it is used to correct that. not for the
            // background profiler though, which should cost as little as possible.
            JfrBackend jfr = this.forceJavaSampler || this.background ? null : JfrSupport.backend();
            sampler = onlyTicksOverMode
                    ? new JavaSampler(platform, settings, this.tickHook, this.ticksOver, jfr)
                    : new JavaSampler(platform, settings, jfr);
        }

        sampler.start();
        return sampler;
    }

    private static void logFlightRecorderUnavailable(SparkPlatform platform, String reason) {
        platform.getPlugin().log(Level.INFO, "The JVM's flight recorder cannot be used instead of async-profiler (" + reason + ").");
    }

}
