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

import me.lucko.spark.common.sampler.Sampler;
import me.lucko.spark.common.sampler.SamplerSettings;
import me.lucko.spark.common.sampler.ThreadDumper;
import me.lucko.spark.common.sampler.ThreadGrouper;
import me.lucko.spark.common.sampler.jfr.JfrBackend;
import me.lucko.spark.common.sampler.jfr.JfrSupport;
import me.lucko.spark.common.sampler.source.ClassSourceLookup;
import me.lucko.spark.proto.SparkSamplerProtos;
import me.lucko.spark.test.plugin.TestCommandSender;
import me.lucko.spark.test.plugin.TestSparkPlugin;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Measures how close the sampler gets to a workload whose real time split is known: a thread that
 * "ticks" every 50ms, spends a set time in three phases, and sleeps for the rest of the tick.
 *
 * <p>Not a test: run with SPARK_LAB=1 and read the output.</p>
 */
public class SamplerAccuracyLab {

    private static final long TICK_NANOS = TimeUnit.MILLISECONDS.toNanos(50);
    private static final String THREAD_NAME = "Lab Tick Thread";

    private static volatile long sink;

    // --- the workload ---

    /** Real time spent in each phase, in nanoseconds */
    private static final AtomicLong[] TRUTH = {new AtomicLong(), new AtomicLong(), new AtomicLong()};

    // counted loops: the JIT leaves no safepoint poll inside them, so a thread dump
    // never shows a thread in here, only on its way out
    private static long hotA(int n) {
        long x = 0;
        for (int i = 0; i < n; i++) {
            x += (i * 31L) ^ (x >>> 3);
        }
        return x;
    }

    private static long hotB(int n) {
        long x = 1;
        for (int i = 0; i < n; i++) {
            x = x * 6364136223846793005L + i;
        }
        return x;
    }

    private static long hotC(int n) {
        long x = 7;
        for (int i = 0; i < n; i++) {
            x ^= (x << 13) + i;
            x ^= (x >>> 7);
        }
        return x;
    }

    private static void phaseA(long nanos) {
        long start = System.nanoTime();
        while (System.nanoTime() - start < nanos) {
            sink += hotA(20_000);
        }
        TRUTH[0].addAndGet(System.nanoTime() - start);
    }

    private static void phaseB(long nanos) {
        long start = System.nanoTime();
        while (System.nanoTime() - start < nanos) {
            sink += hotB(20_000);
        }
        TRUTH[1].addAndGet(System.nanoTime() - start);
    }

    private static void phaseC(long nanos) {
        long start = System.nanoTime();
        while (System.nanoTime() - start < nanos) {
            sink += hotC(20_000);
        }
        TRUTH[2].addAndGet(System.nanoTime() - start);
    }

    private static void tickLoop(long workNanos, AtomicBoolean running) {
        long next = System.nanoTime();
        while (running.get()) {
            // 20% / 50% / 30%, always in the same order, like a game tick
            phaseA(workNanos / 5);
            phaseB(workNanos / 2);
            phaseC(workNanos * 3 / 10);

            next += TICK_NANOS;
            long sleep = TimeUnit.NANOSECONDS.toMillis(next - System.nanoTime());
            if (sleep > 0) {
                try {
                    Thread.sleep(sleep);
                } catch (InterruptedException e) {
                    return;
                }
            } else {
                next = System.nanoTime();
            }
        }
    }

    // --- the measurement ---

    @Test
    @EnabledIfEnvironmentVariable(named = "SPARK_LAB", matches = "1")
    public void measure(@TempDir Path directory) throws Exception {
        JfrBackend jfr = JfrSupport.backend();
        System.out.println("java " + System.getProperty("java.version") + " on " + System.getProperty("os.name")
                + ", flight recorder: " + (jfr != null ? "available" : JfrSupport.unavailableReason()));
        System.out.println(String.format(Locale.ROOT, "%-8s %-8s %9s %9s   %-17s %-17s %8s",
                "work/tick", "sampler", "busy real", "busy seen", "A/B/C real %", "A/B/C seen %", "in hot %"));

        try (TestSparkPlugin plugin = new TestSparkPlugin(directory)) {
            for (int workMillis : new int[]{2, 20}) {
                for (boolean useJfr : new boolean[]{false, true}) {
                    if (useJfr && jfr == null) {
                        continue;
                    }
                    run(plugin, workMillis, useJfr ? jfr : null);
                }
            }
        }
    }

    private static void run(TestSparkPlugin plugin, int workMillis, JfrBackend jfr) throws Exception {
        AtomicBoolean running = new AtomicBoolean(true);
        Thread thread = new Thread(() -> tickLoop(TimeUnit.MILLISECONDS.toNanos(workMillis), running), THREAD_NAME);
        thread.setDaemon(true);
        thread.start();
        Thread.sleep(1000); // let the JIT finish

        SamplerSettings settings = new SamplerSettings(4000, new ThreadDumper.Specific(thread), ThreadGrouper.BY_NAME.get(), -1, false, false);
        JavaSampler sampler = new JavaSampler(plugin.platform(), settings, jfr);

        for (AtomicLong truth : TRUTH) {
            truth.set(0);
        }
        sampler.start();
        Thread.sleep(8000);
        long[] truth = {TRUTH[0].get(), TRUTH[1].get(), TRUTH[2].get()};
        sampler.stop(false);
        running.set(false);
        thread.join();

        SparkSamplerProtos.SamplerData proto = sampler.toProto(plugin.platform(), new Sampler.ExportProps()
                .creator(TestCommandSender.INSTANCE.toData())
                .mergeStrategy(MergeStrategy.SAME_METHOD)
                .classSourceLookup(() -> ClassSourceLookup.create(plugin.platform())));

        SparkSamplerProtos.ThreadNode node = proto.getThreadsList().stream()
                .filter(t -> t.getName().equals(THREAD_NAME))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no data for the thread"));

        // inclusive time of each method, and the time of the frames at the top of the stack
        Map<String, Double> inclusive = new HashMap<>();
        Map<String, Double> self = new HashMap<>();
        List<SparkSamplerProtos.StackTraceNode> nodes = node.getChildrenList();
        for (SparkSamplerProtos.StackTraceNode n : nodes) {
            double time = sum(n.getTimesList());
            double children = 0;
            for (int ref : n.getChildrenRefsList()) {
                children += sum(nodes.get(ref).getTimesList());
            }
            inclusive.merge(n.getMethodName(), time, Double::sum);
            self.merge(n.getMethodName(), time - children, Double::sum);
        }

        double total = sum(node.getTimesList());
        double sleeping = 0;
        for (Map.Entry<String, Double> entry : self.entrySet()) {
            if (entry.getKey().startsWith("sleep")) {
                sleeping += entry.getValue();
            }
        }
        double busy = total - sleeping;
        double hot = self.getOrDefault("hotA", 0d) + self.getOrDefault("hotB", 0d) + self.getOrDefault("hotC", 0d);

        double truthBusy = (truth[0] + truth[1] + truth[2]) / 1e6;
        System.out.println(String.format(Locale.ROOT, "%-9s %-8s %7.0fms %7.0fms   %-17s %-17s %7.1f%%",
                workMillis + "ms", jfr != null ? "java+jfr" : "java",
                truthBusy, busy,
                split(truth[0] / 1e6, truth[1] / 1e6, truth[2] / 1e6),
                split(inclusive.getOrDefault("phaseA", 0d), inclusive.getOrDefault("phaseB", 0d), inclusive.getOrDefault("phaseC", 0d)),
                busy == 0 ? 0 : 100 * hot / busy));
    }

    private static double sum(List<Double> values) {
        double sum = 0;
        for (double value : values) {
            sum += value;
        }
        return sum;
    }

    private static String split(double a, double b, double c) {
        double total = a + b + c;
        if (total == 0) {
            return "-";
        }
        return String.format(Locale.ROOT, "%.0f / %.0f / %.0f", 100 * a / total, 100 * b / total, 100 * c / total);
    }

}
