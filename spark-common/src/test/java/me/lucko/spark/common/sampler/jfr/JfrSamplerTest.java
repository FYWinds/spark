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

import me.lucko.spark.common.sampler.Sampler;
import me.lucko.spark.common.sampler.SamplerMode;
import me.lucko.spark.common.sampler.SamplerSettings;
import me.lucko.spark.common.sampler.SamplerType;
import me.lucko.spark.common.sampler.ThreadDumper;
import me.lucko.spark.common.sampler.ThreadGrouper;
import me.lucko.spark.common.sampler.java.JavaSampler;
import me.lucko.spark.common.sampler.java.MergeStrategy;
import me.lucko.spark.common.sampler.source.ClassSourceLookup;
import me.lucko.spark.proto.SparkSamplerProtos;
import me.lucko.spark.proto.SparkSamplerProtos.SamplerMetadata;
import me.lucko.spark.test.plugin.TestCommandSender;
import me.lucko.spark.test.plugin.TestSparkPlugin;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class JfrSamplerTest {

    private static volatile long sink;
    private static volatile Object allocated;

    private static JfrBackend backend() {
        JfrBackend backend = JfrSupport.backend();
        assumeTrue(backend != null, "the flight recorder is not available: " + JfrSupport.unavailableReason());
        return backend;
    }

    private static Thread startDaemon(String name, Runnable task) {
        Thread thread = new Thread(task, name);
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // a stack deeper than the flight recorder's default limit of 64 frames
    private static long spin(int depth, AtomicBoolean running) {
        if (depth > 0) {
            return spin(depth - 1, running) + 1;
        }
        long x = 0;
        while (running.get()) {
            for (int i = 0; i < 100_000; i++) {
                x += (i * 31L) ^ (x >>> 3);
            }
            sink = x;
        }
        return x;
    }

    private static void allocate() {
        allocated = new byte[64 * 1024];
    }

    private static SamplerSettings settings(int interval, ThreadDumper threadDumper) {
        return new SamplerSettings(interval, threadDumper, ThreadGrouper.BY_POOL.get(), -1, false, false);
    }

    private static SparkSamplerProtos.SamplerData export(TestSparkPlugin plugin, Sampler sampler) {
        Sampler.ExportProps exportProps = new Sampler.ExportProps()
                .creator(TestCommandSender.INSTANCE.toData())
                .mergeStrategy(MergeStrategy.SAME_METHOD)
                .classSourceLookup(() -> ClassSourceLookup.create(plugin.platform()));

        SparkSamplerProtos.SamplerData proto = sampler.toProto(plugin.platform(), exportProps);
        assertNotNull(proto);
        return proto;
    }

    private static SparkSamplerProtos.ThreadNode findThread(SparkSamplerProtos.SamplerData proto, String name) {
        return proto.getThreadsList().stream()
                .filter(t -> t.getName().startsWith(name)) // name is suffixed with the pool size
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing thread '" + name + "' in " + proto.getThreadsList().stream().map(SparkSamplerProtos.ThreadNode::getName).collect(Collectors.toList())));
    }

    private static double totalValue(SparkSamplerProtos.ThreadNode thread) {
        return thread.getTimesList().stream().mapToDouble(Double::doubleValue).sum();
    }

    private static boolean hasMethod(SparkSamplerProtos.ThreadNode thread, String method) {
        return thread.getChildrenList().stream().anyMatch(n -> n.getClassName().equals(JfrSamplerTest.class.getName()) && n.getMethodName().equals(method));
    }

    @Test
    public void testExecutionSamples() throws Exception {
        JfrBackend backend = backend();

        AtomicBoolean running = new AtomicBoolean(true);
        Thread thread = startDaemon("Jfr Test Thread", () -> spin(200, running));

        List<StackTraceElement[]> stacks = Collections.synchronizedList(new ArrayList<>());
        AtomicLong lastSample = new AtomicLong(Long.MIN_VALUE);
        AtomicLong watermark = new AtomicLong(Long.MIN_VALUE);
        AtomicInteger outOfOrder = new AtomicInteger();
        AtomicInteger truncated = new AtomicInteger();

        // what a thread dump shows for the thread, once it has reached the loop
        Thread.sleep(500);
        StackTraceElement[] threadDump = ManagementFactory.getThreadMXBean().getThreadInfo(thread.getId(), Integer.MAX_VALUE).getStackTrace();
        assertEquals("spin", threadDump[0].getMethodName());

        long start = System.nanoTime();
        long end;
        List<Long> heartbeats = Collections.synchronizedList(new ArrayList<>());
        try (JfrSession session = backend.startExecution(4000, new JfrSampleListener() {
            @Override
            public void onSample(long threadId, String threadName, StackTraceElement[] stack, long nanoTime, long value) {
                if (threadId != thread.getId()) {
                    return;
                }
                assertEquals("Jfr Test Thread", threadName);
                if (nanoTime < lastSample.getAndSet(nanoTime) || nanoTime < watermark.get()) {
                    outOfOrder.incrementAndGet();
                }
                stacks.add(stack);
            }

            @Override
            public void onWatermark(long nanoTime) {
                watermark.set(nanoTime);
            }

            @Override
            public void onTruncatedStack() {
                truncated.incrementAndGet();
            }

            @Override
            public void onHeartbeat(long nanoTime) {
                heartbeats.add(nanoTime);
            }
        })) {
            Thread.sleep(3000);
            end = System.nanoTime();
            assertTrue(session.awaitWatermark(end, 10_000), "watermark was not reached");

            // the JVM's sampler thread reports in, however often the system lets it run
            List<Long> received;
            synchronized (heartbeats) {
                received = new ArrayList<>(heartbeats);
            }
            System.out.println("Received " + received.size() + " heartbeats");
            assertTrue(received.size() > 100, "heartbeats = " + received.size());
            for (int i = 1; i < received.size(); i++) {
                assertTrue(received.get(i) >= received.get(i - 1));
            }
        } finally {
            running.set(false);
        }

        // times are in the System.nanoTime() domain
        assertTrue(lastSample.get() > start && lastSample.get() < end + TimeUnit.SECONDS.toNanos(10), "sample time " + lastSample.get() + " not within " + start + " - " + end);
        assertEquals(0, outOfOrder.get());
        assertEquals(0, truncated.get());

        // (a few more can still arrive after the session is closed)
        List<StackTraceElement[]> received;
        synchronized (stacks) {
            received = new ArrayList<>(stacks);
        }

        // the JVM does not sample at a dependable rate, but it should manage a few times a second
        System.out.println("Received " + received.size() + " execution samples");
        assertTrue(received.size() > 20, "samples = " + received.size());

        // the stacks are what a thread dump would show, hidden frames (lambdas) included
        for (StackTraceElement[] stack : received) {
            assertEquals(threadDump.length, stack.length);
            assertEquals("spin", stack[0].getMethodName());
            assertTrue(stack[0].getLineNumber() > 0);
            for (int i = 1; i < stack.length; i++) {
                assertEquals(threadDump[i].getClassName(), stack[i].getClassName());
                assertEquals(threadDump[i].getMethodName(), stack[i].getMethodName());
                assertEquals(threadDump[i].getLineNumber(), stack[i].getLineNumber());
            }
        }
    }

    @Test
    public void testJavaSamplerWithFlightRecorder(@TempDir Path directory) throws Exception {
        JfrBackend backend = backend();

        AtomicBoolean running = new AtomicBoolean(true);
        Thread thread = startDaemon("Jfr Test Thread", () -> spin(200, running));

        try (TestSparkPlugin plugin = new TestSparkPlugin(directory)) {
            JavaSampler sampler = new JavaSampler(plugin.platform(), settings(4000, new ThreadDumper.Specific(thread)), backend);
            sampler.start();
            assertTrue(sampler.isUsingFlightRecorder());
            assertEquals("jfr", sampler.getLibraryVersion());

            Thread.sleep(3000);
            sampler.stop(false);

            SparkSamplerProtos.SamplerData proto = export(plugin, sampler);
            assertEquals(SamplerMetadata.SamplerEngine.JAVA, proto.getMetadata().getSamplerEngine());

            // the thread was running the whole time, and the time it is given is still
            // decided by the thread dumps (values are in milliseconds)
            SparkSamplerProtos.ThreadNode protoThread = findThread(proto, "Jfr Test Thread");
            double totalTime = totalValue(protoThread);
            System.out.println("Jfr Test Thread was running for " + totalTime + "ms");
            assertTrue(totalTime > 2000 && totalTime < 4000, "total = " + totalTime);
            assertTrue(hasMethod(protoThread, "spin"));
        } finally {
            running.set(false);
        }
    }

    @Test
    public void testAllocationSampler(@TempDir Path directory) throws Exception {
        JfrBackend backend = backend();
        assumeTrue(backend.supportsAllocation(), "allocation sampling is not supported by this JVM");

        AtomicBoolean running = new AtomicBoolean(true);
        Thread thread = startDaemon("Jfr Test Thread", () -> {
            while (running.get()) {
                allocate();
            }
        });

        try (TestSparkPlugin plugin = new TestSparkPlugin(directory)) {
            JfrSampler sampler = new JfrSampler(plugin.platform(), settings((int) SamplerMode.ALLOCATION.defaultInterval(), new ThreadDumper.Specific(thread)), SamplerMode.ALLOCATION, backend);
            sampler.start();
            assertEquals(SamplerType.JAVA, sampler.getType());
            assertEquals(SamplerMode.ALLOCATION, sampler.getMode());

            Thread.sleep(3000);
            sampler.stop(false);

            SparkSamplerProtos.SamplerData proto = export(plugin, sampler);
            assertEquals(SamplerMetadata.SamplerMode.ALLOCATION, proto.getMetadata().getSamplerMode());
            assertEquals(1, proto.getThreadsCount());

            // the thread allocates 64KiB arrays as fast as it can (values are in bytes)
            SparkSamplerProtos.ThreadNode protoThread = findThread(proto, "Jfr Test Thread");
            double totalBytes = totalValue(protoThread);
            System.out.println("Jfr Test Thread allocated " + totalBytes + " bytes");
            assertTrue(totalBytes > 100 * 1024 * 1024, "total = " + totalBytes);
            assertTrue(hasMethod(protoThread, "allocate"));
        } finally {
            running.set(false);
        }
    }

    @Test
    public void testLockSampler(@TempDir Path directory) throws Exception {
        JfrBackend backend = backend();

        AtomicBoolean running = new AtomicBoolean(true);
        Object monitor = new Object();
        ReentrantLock lock = new ReentrantLock();

        Supplier<Runnable> monitorContender = () -> () -> {
            while (running.get()) {
                synchronized (monitor) {
                    sleepQuietly(20);
                }
                sleepQuietly(2); // give the other thread a chance to take the monitor
            }
        };
        Supplier<Runnable> lockContender = () -> () -> {
            while (running.get()) {
                lock.lock();
                try {
                    sleepQuietly(20);
                } finally {
                    lock.unlock();
                }
                sleepQuietly(2); // give the other thread a chance to take the lock
            }
        };

        startDaemon("Monitor Test Thread 1", monitorContender.get());
        startDaemon("Monitor Test Thread 2", monitorContender.get());
        startDaemon("Lock Test Thread 1", lockContender.get());
        startDaemon("Lock Test Thread 2", lockContender.get());

        // parks without a lock, which is not contention
        startDaemon("Sleep Test Thread", () -> {
            while (running.get()) {
                sleepQuietly(20);
            }
        });

        try (TestSparkPlugin plugin = new TestSparkPlugin(directory)) {
            int interval = (int) (SamplerMode.LOCK.defaultInterval() * 1000);
            JfrSampler sampler = new JfrSampler(plugin.platform(), settings(interval, ThreadDumper.ALL), SamplerMode.LOCK, backend);
            sampler.start();

            Thread.sleep(4000);
            sampler.stop(false);

            SparkSamplerProtos.SamplerData proto = export(plugin, sampler);
            assertEquals(SamplerMetadata.SamplerMode.LOCK, proto.getMetadata().getSamplerMode());

            for (String name : new String[]{"Monitor Test Thread", "Lock Test Thread"}) {
                // two threads fight over one lock for ~4 seconds, so the pool as a whole should
                // have spent a good few seconds waiting. (values are in milliseconds)
                double totalTime = totalValue(findThread(proto, name));
                System.out.println(name + " spent " + totalTime + "ms waiting for locks");
                assertTrue(totalTime > 1000, name + " total = " + totalTime);
                assertTrue(totalTime < 12000, name + " total = " + totalTime);
            }

            assertTrue(proto.getThreadsList().stream().noneMatch(t -> t.getName().startsWith("Sleep Test Thread")));
        } finally {
            running.set(false);
        }
    }

}
