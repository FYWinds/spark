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

import me.lucko.spark.common.sampler.jfr.JfrSampleListener;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Replaces the stack traces of threads that were running Java code when they were
 * dumped with stack traces sampled by the flight recorder.
 *
 * <p>A thread dump stops every thread at a safepoint, so a thread that is running Java
 * code is never seen where it actually was, but at the next safepoint poll. How long
 * the thread was running is still measured correctly, only the place is wrong. The
 * flight recorder samples running threads without waiting for a safepoint, but does
 * not sample at a dependable rate. So the thread dumps decide how much time there is
 * to hand out, and the flight recorder samples decide where it goes.</p>
 *
 * <p>Samples of threads that are sleeping, blocked or inside a native method are not
 * affected by safepoints and never reach this class.</p>
 *
 * <p>Samples are grouped into buckets, identified by a key that increases over time
 * (a slice of time, or a game tick). If the flight recorder has nothing for a bucket,
 * its thread dump samples are used unchanged.</p>
 *
 * <p>The flight recorder is only trusted while the JVM is seen to sample about as often as
 * it was asked to. On Windows the JVM's sampler can end up waking only on the system timer's
 * interrupts, 15.6ms apart. Anything else that waits on that timer, like a game loop sleeping
 * until its next tick, then starts at the same instants, and the samples all fall on the
 * same few points of the tick.</p>
 */
public final class SafepointBiasCorrector implements JfrSampleListener {

    /** How long flight recorder samples are kept waiting for a bucket to claim them. */
    private static final long RETAIN_NANOS = TimeUnit.SECONDS.toNanos(20);

    /** The number of heartbeats the JVM sampler's rate is judged over. */
    private static final int HEARTBEATS = 32;

    /** How long the JVM sampler can go without a heartbeat and still be trusted. */
    private static final long HEARTBEAT_TIMEOUT_NANOS = TimeUnit.MILLISECONDS.toNanos(500);

    /** The most flight recorder samples kept waiting, per thread. */
    private static final int MAX_WAITING_SAMPLES = 100_000;

    /** How long a thread can be idle for before its state is dropped. */
    private static final long IDLE_NANOS = TimeUnit.SECONDS.toNanos(60);

    /** A bucket is corrected only if it has this many times more usable flight recorder samples than rejected ones. */
    private static final int MAX_REJECTED_RATIO = 10;

    /**
     * Receives the final samples.
     */
    @FunctionalInterface
    public interface Sink {
        void write(long threadId, String threadName, StackTraceElement[] stack, long time, int window);
    }

    private final Sink sink;

    /** How far from a bucket a flight recorder sample can be and still belong to it */
    private final long graceNanos;

    /** The longest the JVM sampler usually takes between samples when it is working as asked */
    private final long maxHeartbeatGapNanos;

    // the time between the last heartbeats. only touched by the thread calling the listener methods.
    private final long[] heartbeatGaps = new long[HEARTBEATS];
    private final long[] sortedHeartbeatGaps = new long[HEARTBEATS];
    private long heartbeats = 0;
    private long lastHeartbeat;
    private volatile boolean samplerRegular = false;
    private volatile boolean samplerWasIrregular = false;

    private final Map<Long, ThreadState> threads = new ConcurrentHashMap<>();

    private volatile boolean disabled = false;
    private volatile boolean finished = false;

    private final AtomicLong correctedSamples = new AtomicLong();
    private final AtomicLong uncorrectedSamples = new AtomicLong();
    private final AtomicLong incompleteStacks = new AtomicLong();

    /**
     * @param sink where the final samples are written
     * @param interval the usual time between thread dumps, in microseconds
     */
    public SafepointBiasCorrector(Sink sink, int interval) {
        this.sink = sink;
        long intervalNanos = TimeUnit.MICROSECONDS.toNanos(interval);
        this.graceNanos = (intervalNanos * 2) + TimeUnit.MILLISECONDS.toNanos(2);
        this.maxHeartbeatGapNanos = Math.max(intervalNanos * 2, intervalNanos + TimeUnit.MILLISECONDS.toNanos(2));
    }

    /**
     * Adds a thread dump sample of a thread that was running Java code.
     *
     * @param bucketKey the bucket the sample belongs to
     * @param threadId the java thread id
     * @param threadName the thread name
     * @param stack the stack trace
     * @param nanoTime when the thread dump was taken
     * @param time the time the sample stands for, in microseconds
     * @param window the profiling window
     */
    public void addBiasedSample(long bucketKey, long threadId, String threadName, StackTraceElement[] stack, long nanoTime, long time, int window) {
        while (!this.disabled && !this.finished) {
            ThreadState state = this.threads.computeIfAbsent(threadId, id -> new ThreadState(id, threadName));
            synchronized (state) {
                if (state.removed) {
                    continue;
                }
                state.lastActivity = nanoTime;
                state.bucket(bucketKey, window).add(stack, nanoTime, time);
                return;
            }
        }

        this.uncorrectedSamples.incrementAndGet();
        this.sink.write(threadId, threadName, stack, time, window);
    }

    @Override
    public void onSample(long threadId, String threadName, StackTraceElement[] stack, long nanoTime, long value) {
        ThreadState state = this.threads.get(threadId);
        if (state == null || this.disabled) {
            return;
        }

        synchronized (state) {
            state.waiting.addLast(new Unbiased(stack, nanoTime));
            if (state.waiting.size() > MAX_WAITING_SAMPLES) {
                state.prunedBefore = state.waiting.removeFirst().nanoTime;
                state.prunedSet = true;
            }
        }
    }

    @Override
    public void onHeartbeat(long nanoTime) {
        if (this.heartbeats != 0) {
            this.heartbeatGaps[(int) ((this.heartbeats - 1) % HEARTBEATS)] = nanoTime - this.lastHeartbeat;
        }
        this.heartbeats++;
        this.lastHeartbeat = nanoTime;

        if (this.heartbeats > HEARTBEATS) {
            System.arraycopy(this.heartbeatGaps, 0, this.sortedHeartbeatGaps, 0, HEARTBEATS);
            Arrays.sort(this.sortedHeartbeatGaps);
            this.samplerRegular = this.sortedHeartbeatGaps[HEARTBEATS / 2] <= this.maxHeartbeatGapNanos;
        }
    }

    @Override
    public void onWatermark(long nanoTime) {
        if (this.heartbeats == 0 || nanoTime - this.lastHeartbeat > HEARTBEAT_TIMEOUT_NANOS) {
            this.samplerRegular = false;
        }

        for (Iterator<ThreadState> it = this.threads.values().iterator(); it.hasNext(); ) {
            ThreadState state = it.next();
            synchronized (state) {
                resolve(state, nanoTime, false);

                if (state.buckets.isEmpty() && state.waiting.isEmpty() && nanoTime - state.lastActivity > IDLE_NANOS) {
                    state.removed = true;
                    it.remove();
                }
            }
        }
    }

    @Override
    public void onTruncatedStack() {
        // the samples that are left are the shallow ones, using them would skew the result
        this.disabled = true;
    }

    /**
     * Writes out everything that is still pending.
     */
    public void finish() {
        this.finished = true;
        for (ThreadState state : this.threads.values()) {
            synchronized (state) {
                state.removed = true;
                resolve(state, 0, true);
            }
        }
        this.threads.clear();
    }

    /**
     * Gets if the flight recorder samples had to be abandoned.
     */
    public boolean isDisabled() {
        return this.disabled;
    }

    /**
     * Gets the number of thread dump samples that were replaced.
     */
    public long getCorrectedSamples() {
        return this.correctedSamples.get();
    }

    /**
     * Gets the number of thread dump samples that were used as they were.
     */
    public long getUncorrectedSamples() {
        return this.uncorrectedSamples.get();
    }

    /**
     * Gets the number of flight recorder samples that were rejected because their stack trace
     * did not reach the bottom of the stack.
     */
    public long getIncompleteStacks() {
        return this.incompleteStacks.get();
    }

    /**
     * Gets if flight recorder samples were left unused because the JVM was not sampling
     * as often as it was asked to.
     */
    public boolean wasSamplerIrregular() {
        return this.samplerWasIrregular;
    }

    // guarded by 'state'
    private void resolve(ThreadState state, long watermark, boolean all) {
        int resolved = 0;
        for (Bucket bucket : state.buckets) {
            if (!all && watermark - (bucket.end + this.graceNanos) < 0) {
                break;
            }
            resolve(state, bucket);
            resolved++;
        }
        if (resolved != 0) {
            state.buckets.subList(0, resolved).clear();
        }

        if (all) {
            state.waiting.clear();
            return;
        }

        // drop samples that no bucket can claim anymore
        long keepFrom = watermark - RETAIN_NANOS;
        while (!state.waiting.isEmpty() && state.waiting.peekFirst().nanoTime - keepFrom < 0) {
            state.waiting.removeFirst();
            state.prunedBefore = keepFrom;
            state.prunedSet = true;
        }
    }

    // guarded by 'state'
    private void resolve(ThreadState state, Bucket bucket) {
        long from = bucket.start - this.graceNanos;
        long to = bucket.end + this.graceNanos;

        // buckets are resolved in order, so anything before this one belongs to nobody
        while (!state.waiting.isEmpty() && state.waiting.peekFirst().nanoTime - from < 0) {
            state.waiting.removeFirst();
        }

        // Before Java 25 the JVM walks the stack of the running thread from another thread, and
        // sometimes gives up part way without saying so. A stack trace like that cannot be placed
        // in the call tree. The bottom frame of a thread never changes, so that is what is checked.
        StackTraceElement bottom = bottom(bucket.stacks.get(0));
        int rejected = 0;

        List<StackTraceElement[]> unbiased = new ArrayList<>();
        while (!state.waiting.isEmpty() && state.waiting.peekFirst().nanoTime - to <= 0) {
            StackTraceElement[] stack = state.waiting.removeFirst().stack;
            StackTraceElement stackBottom = bottom(stack);
            if (stackBottom.getClassName().equals(bottom.getClassName()) && stackBottom.getMethodName().equals(bottom.getMethodName())) {
                unbiased.add(stack);
            } else {
                rejected++;
            }
        }
        this.incompleteStacks.addAndGet(rejected);

        // if samples covering the start of the bucket were thrown away, the rest are not
        // representative. the same goes if too many were rejected: the walk gives up on particular
        // code, so what is left says nothing about the time spent in that code.
        boolean unrepresentative = (state.prunedSet && from - state.prunedBefore < 0) || rejected * MAX_REJECTED_RATIO > unbiased.size();

        if (!unbiased.isEmpty() && !this.samplerRegular) {
            this.samplerWasIrregular = true;
            unrepresentative = true;
        }

        if (unbiased.isEmpty() || unrepresentative || this.disabled) {
            this.uncorrectedSamples.addAndGet(bucket.stacks.size());
            for (int i = 0; i < bucket.stacks.size(); i++) {
                this.sink.write(state.threadId, state.threadName, bucket.stacks.get(i), bucket.times.get(i), bucket.window);
            }
            return;
        }

        this.correctedSamples.addAndGet(bucket.stacks.size());

        long total = bucket.totalTime;
        long each = total / unbiased.size();
        long remainder = total - (each * unbiased.size());
        for (StackTraceElement[] stack : unbiased) {
            long time = each;
            if (remainder > 0) {
                time++;
                remainder--;
            }
            if (time > 0) {
                this.sink.write(state.threadId, state.threadName, stack, time, bucket.window);
            }
        }
    }

    private static StackTraceElement bottom(StackTraceElement[] stack) {
        return stack[stack.length - 1];
    }

    private static final class ThreadState {
        private final long threadId;
        private final String threadName;

        /** Buckets that are not resolved yet, in order */
        private final List<Bucket> buckets = new ArrayList<>();

        /** Flight recorder samples waiting for their bucket to be resolved, in order */
        private final ArrayDeque<Unbiased> waiting = new ArrayDeque<>();

        private boolean prunedSet = false;
        private long prunedBefore;
        private long lastActivity;
        private boolean removed = false;

        ThreadState(long threadId, String threadName) {
            this.threadId = threadId;
            this.threadName = threadName;
        }

        Bucket bucket(long key, int window) {
            int index = this.buckets.size();
            while (index > 0) {
                Bucket bucket = this.buckets.get(index - 1);
                if (bucket.key == key) {
                    return bucket;
                }
                if (bucket.key < key) {
                    break;
                }
                index--;
            }

            Bucket bucket = new Bucket(key, window);
            this.buckets.add(index, bucket);
            return bucket;
        }
    }

    private static final class Bucket {
        private final long key;
        private final int window;
        private final List<StackTraceElement[]> stacks = new ArrayList<>();
        private final List<Long> times = new ArrayList<>();
        private long totalTime;
        private long start;
        private long end;

        Bucket(long key, int window) {
            this.key = key;
            this.window = window;
        }

        void add(StackTraceElement[] stack, long nanoTime, long time) {
            if (this.stacks.isEmpty()) {
                this.start = nanoTime;
                this.end = nanoTime;
            } else {
                if (nanoTime - this.start < 0) {
                    this.start = nanoTime;
                }
                if (nanoTime - this.end > 0) {
                    this.end = nanoTime;
                }
            }
            this.stacks.add(stack);
            this.times.add(time);
            this.totalTime += time;
        }
    }

    private static final class Unbiased {
        private final StackTraceElement[] stack;
        private final long nanoTime;

        Unbiased(StackTraceElement[] stack, long nanoTime) {
            this.stack = stack;
            this.nanoTime = nanoTime;
        }
    }

}
