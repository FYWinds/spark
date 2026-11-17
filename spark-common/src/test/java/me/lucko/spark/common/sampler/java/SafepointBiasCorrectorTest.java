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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SafepointBiasCorrectorTest {

    private static final int INTERVAL = 4000; // microseconds
    private static final long INTERVAL_NANOS = TimeUnit.MICROSECONDS.toNanos(INTERVAL);
    private static final long SECOND = TimeUnit.SECONDS.toNanos(1);

    private static final StackTraceElement ROOT = new StackTraceElement("java.lang.Thread", "run", null, 1);

    private static final StackTraceElement[] SAFEPOINT = stack("atSafepoint");
    private static final StackTraceElement[] HOT = stack("hot");
    private static final StackTraceElement[] WARM = stack("warm");

    private static StackTraceElement[] stack(String method) {
        return new StackTraceElement[]{new StackTraceElement("Test", method, null, 1), ROOT};
    }

    /** Sums the time written for each method. */
    private static final class Output implements SafepointBiasCorrector.Sink {
        final Map<String, Long> times = new HashMap<>();
        final List<Integer> windows = new ArrayList<>();

        @Override
        public void write(long threadId, String threadName, StackTraceElement[] stack, long time, int window) {
            this.times.merge(threadId + ":" + stack[0].getMethodName(), time, Long::sum);
            this.windows.add(window);
        }

        long time(long threadId, String method) {
            return this.times.getOrDefault(threadId + ":" + method, 0L);
        }
    }

    private static void addBiased(SafepointBiasCorrector corrector, long bucket, long threadId, long startNanos, int count) {
        for (int i = 0; i < count; i++) {
            corrector.addBiasedSample(bucket, threadId, "thread-" + threadId, SAFEPOINT, startNanos + (i * INTERVAL_NANOS), INTERVAL, 7);
        }
    }

    /** Reports the JVM's sampler thread as having woken up at the given rate until the given time. */
    private static void heartbeats(SafepointBiasCorrector corrector, long untilNanos, long gapNanos) {
        for (int i = 40; i >= 0; i--) {
            corrector.onHeartbeat(untilNanos - (i * gapNanos));
        }
    }

    /** A watermark, with the JVM's sampler working as asked. */
    private static void watermark(SafepointBiasCorrector corrector, long nanoTime) {
        heartbeats(corrector, nanoTime, INTERVAL_NANOS);
        corrector.onWatermark(nanoTime);
    }

    @Test
    public void testTimeIsHandedToUnbiasedSamples() {
        Output output = new Output();
        SafepointBiasCorrector corrector = new SafepointBiasCorrector(output, INTERVAL);

        // the thread was seen running 10 times = 40ms
        addBiased(corrector, 0, 1, 0, 10);

        // the flight recorder saw it 4 times, 3 of them in the same place
        corrector.onSample(1, "thread-1", HOT, INTERVAL_NANOS, 1);
        corrector.onSample(1, "thread-1", HOT, 3 * INTERVAL_NANOS, 1);
        corrector.onSample(1, "thread-1", WARM, 5 * INTERVAL_NANOS, 1);
        corrector.onSample(1, "thread-1", HOT, 7 * INTERVAL_NANOS, 1);

        // nothing is written until all the samples for the bucket are known to have arrived
        watermark(corrector, 5 * INTERVAL_NANOS);
        assertTrue(output.times.isEmpty());

        watermark(corrector, SECOND);
        assertEquals(30_000, output.time(1, "hot"));
        assertEquals(10_000, output.time(1, "warm"));
        assertEquals(0, output.time(1, "atSafepoint"));
        assertTrue(output.windows.stream().allMatch(window -> window == 7));
        assertEquals(10, corrector.getCorrectedSamples());
        assertEquals(0, corrector.getUncorrectedSamples());
    }

    @Test
    public void testTimeIsPreservedWhenItDoesNotDivide() {
        Output output = new Output();
        SafepointBiasCorrector corrector = new SafepointBiasCorrector(output, INTERVAL);

        addBiased(corrector, 0, 1, 0, 10);
        for (int i = 0; i < 3; i++) {
            corrector.onSample(1, "thread-1", HOT, i * INTERVAL_NANOS, 1);
        }

        watermark(corrector, SECOND);
        assertEquals(40_000, output.time(1, "hot"));
    }

    @Test
    public void testBiasedSamplesAreKeptWhenThereIsNothingBetter() {
        Output output = new Output();
        SafepointBiasCorrector corrector = new SafepointBiasCorrector(output, INTERVAL);

        addBiased(corrector, 0, 1, 0, 10);

        // samples of another thread, and of a thread that was never dumped
        addBiased(corrector, 0, 2, 0, 1);
        corrector.onSample(2, "thread-2", HOT, 0, 1);
        corrector.onSample(3, "thread-3", HOT, 0, 1);

        watermark(corrector, SECOND);
        assertEquals(40_000, output.time(1, "atSafepoint"));
        assertEquals(4_000, output.time(2, "hot"));
        assertEquals(0, output.time(3, "hot"));
        assertEquals(1, corrector.getCorrectedSamples());
        assertEquals(10, corrector.getUncorrectedSamples());
    }

    @Test
    public void testSamplesOutsideABucketAreNotUsed() {
        Output output = new Output();
        SafepointBiasCorrector corrector = new SafepointBiasCorrector(output, INTERVAL);

        // two buckets a second apart, e.g. two ticks that are being profiled
        addBiased(corrector, 10, 1, SECOND, 5);
        addBiased(corrector, 20, 1, 2 * SECOND, 5);

        // a sample taken between them, e.g. during a tick that is not being profiled
        corrector.onSample(1, "thread-1", WARM, SECOND + (SECOND / 2), 1);
        corrector.onSample(1, "thread-1", HOT, 2 * SECOND, 1);

        watermark(corrector, 10 * SECOND);
        assertEquals(20_000, output.time(1, "atSafepoint")); // first bucket
        assertEquals(20_000, output.time(1, "hot")); // second bucket
        assertEquals(0, output.time(1, "warm"));
    }

    @Test
    public void testSamplesThatArriveBeforeTheirBucket() {
        Output output = new Output();
        SafepointBiasCorrector corrector = new SafepointBiasCorrector(output, INTERVAL);

        // the thread has to be known before samples of it are kept
        addBiased(corrector, 1, 1, 0, 1);
        watermark(corrector, SECOND);

        // a long tick is only added once it has finished, after its samples have arrived
        corrector.onSample(1, "thread-1", HOT, 3 * SECOND, 1);
        watermark(corrector, 4 * SECOND);
        addBiased(corrector, 2, 1, 3 * SECOND, 5);
        watermark(corrector, 5 * SECOND);

        assertEquals(20_000, output.time(1, "hot"));
    }

    @Test
    public void testTruncatedStacksDisableCorrection() {
        Output output = new Output();
        SafepointBiasCorrector corrector = new SafepointBiasCorrector(output, INTERVAL);

        addBiased(corrector, 0, 1, 0, 5);
        corrector.onSample(1, "thread-1", HOT, 0, 1);
        corrector.onTruncatedStack();
        assertTrue(corrector.isDisabled());

        watermark(corrector, SECOND);
        assertEquals(20_000, output.time(1, "atSafepoint"));
        assertEquals(0, output.time(1, "hot"));

        // later samples are written straight away
        addBiased(corrector, 1, 1, SECOND, 1);
        assertEquals(24_000, output.time(1, "atSafepoint"));
    }

    @Test
    public void testFinishWritesPendingBuckets() {
        Output output = new Output();
        SafepointBiasCorrector corrector = new SafepointBiasCorrector(output, INTERVAL);

        addBiased(corrector, 0, 1, 0, 5);
        corrector.onSample(1, "thread-1", HOT, 0, 1);
        addBiased(corrector, 0, 2, 0, 5);

        heartbeats(corrector, SECOND, INTERVAL_NANOS);
        corrector.finish();
        assertEquals(20_000, output.time(1, "hot"));
        assertEquals(20_000, output.time(2, "atSafepoint"));

        // anything added afterwards is not held back
        addBiased(corrector, 1, 1, SECOND, 1);
        assertEquals(4_000, output.time(1, "atSafepoint"));
    }

    @Test
    public void testIncompleteStacksAreNotUsed() {
        Output output = new Output();
        SafepointBiasCorrector corrector = new SafepointBiasCorrector(output, INTERVAL);

        // a stack trace that stops before the bottom of the thread's stack
        StackTraceElement[] incomplete = new StackTraceElement[]{new StackTraceElement("Test", "incomplete", null, 1)};

        // one bad stack among many is left out
        addBiased(corrector, 0, 1, 0, 10);
        for (int i = 0; i < 10; i++) {
            corrector.onSample(1, "thread-1", HOT, i * INTERVAL_NANOS, 1);
        }
        corrector.onSample(1, "thread-1", incomplete, 10 * INTERVAL_NANOS, 1);

        // if there are many, what is left does not represent the bucket
        addBiased(corrector, 1, 1, SECOND, 10);
        corrector.onSample(1, "thread-1", HOT, SECOND, 1);
        corrector.onSample(1, "thread-1", incomplete, SECOND + INTERVAL_NANOS, 1);

        watermark(corrector, 10 * SECOND);
        assertEquals(40_000, output.time(1, "hot"));
        assertEquals(40_000, output.time(1, "atSafepoint"));
        assertEquals(0, output.time(1, "incomplete"));
        assertEquals(2, corrector.getIncompleteStacks());
    }

    @Test
    public void testSamplesAreNotUsedWhenTheJvmSamplesIrregularly() {
        Output output = new Output();
        SafepointBiasCorrector corrector = new SafepointBiasCorrector(output, INTERVAL);

        // nothing is known about the JVM's sampler
        addBiased(corrector, 0, 1, 0, 10);
        corrector.onSample(1, "thread-1", HOT, 0, 1);
        corrector.onWatermark(SECOND);
        assertEquals(40_000, output.time(1, "atSafepoint"));

        // it only wakes up on the system timer's interrupts
        addBiased(corrector, 1, 1, 2 * SECOND, 10);
        corrector.onSample(1, "thread-1", HOT, 2 * SECOND, 1);
        heartbeats(corrector, 3 * SECOND, TimeUnit.MICROSECONDS.toNanos(15_625));
        corrector.onWatermark(3 * SECOND);
        assertEquals(80_000, output.time(1, "atSafepoint"));
        assertTrue(corrector.wasSamplerIrregular());

        // it wakes up as often as it was asked to
        addBiased(corrector, 2, 1, 4 * SECOND, 10);
        corrector.onSample(1, "thread-1", HOT, 4 * SECOND, 1);
        watermark(corrector, 5 * SECOND);
        assertEquals(40_000, output.time(1, "hot"));

        // it stops reporting in
        addBiased(corrector, 3, 1, 6 * SECOND, 10);
        corrector.onSample(1, "thread-1", HOT, 6 * SECOND, 1);
        corrector.onWatermark(7 * SECOND);
        assertEquals(120_000, output.time(1, "atSafepoint"));
        assertEquals(40_000, output.time(1, "hot"));
    }

}
