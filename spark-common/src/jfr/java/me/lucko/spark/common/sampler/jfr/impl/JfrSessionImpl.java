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

package me.lucko.spark.common.sampler.jfr.impl;

import jdk.jfr.consumer.RecordedClass;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordedFrame;
import jdk.jfr.consumer.RecordedMethod;
import jdk.jfr.consumer.RecordedStackTrace;
import jdk.jfr.consumer.RecordedThread;
import jdk.jfr.consumer.RecordingStream;
import me.lucko.spark.common.sampler.jfr.JfrSampleListener;
import me.lucko.spark.common.sampler.jfr.JfrSession;
import me.lucko.spark.common.util.log.SparkStaticLogger;

import java.io.IOException;
import java.nio.channels.Selector;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.AbstractOwnableSynchronizer;
import java.util.logging.Level;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A flight recorder stream that hands its events to a {@link JfrSampleListener}.
 */
final class JfrSessionImpl implements JfrSession {

    static final String EXECUTION_SAMPLE = "jdk.ExecutionSample";
    static final String NATIVE_SAMPLE = "jdk.NativeMethodSample";
    static final String ALLOCATION_SAMPLE = "jdk.ObjectAllocationSample";
    static final String MONITOR_ENTER = "jdk.JavaMonitorEnter";
    static final String THREAD_PARK = "jdk.ThreadPark";

    /** The most allocation samples the JVM emits, see the 'throttle' setting of the event. */
    private static final String ALLOCATION_THROTTLE = System.getProperty("spark.jfrAllocationThrottle", "1000/s");

    /** How often a marker is written. */
    private static final long MARKER_INTERVAL_MILLIS = 100;

    private static final int MAX_CACHED_FRAMES = 50_000;

    private static final StackTraceElement[] NO_FRAMES = new StackTraceElement[0];

    private static final Pattern HIDDEN_CLASS_NAME = Pattern.compile("^(.*)[+.](0x[0-9a-fA-F]+)(?:\\.\\d+)?$");

    enum Mode {
        EXECUTION, ALLOCATION, LOCK
    }

    static JfrSessionImpl start(Mode mode, int intervalMicros, JfrSampleListener listener) {
        JfrSessionImpl session = new JfrSessionImpl(listener);
        try {
            session.start(mode, intervalMicros);
        } catch (RuntimeException e) {
            session.close();
            throw e;
        }
        return session;
    }

    private final JfrSampleListener listener;
    private final RecordingStream stream;

    private Thread markerThread;
    private Thread timerThread;
    private Selector heartbeatSelector;
    private volatile boolean closed = false;

    // the difference between System.nanoTime() and the flight recorder's clock,
    // measured by the last few markers. only touched by the stream thread.
    private final long[] offsets = new long[16];
    private int offsetCount = 0;
    private long offset;

    private final Object watermarkMutex = new Object[0];
    private boolean hasWatermark = false;
    private long watermark;

    // only touched by the stream thread
    private final Map<RecordedMethod, StackTraceElement> frames = new IdentityHashMap<>();
    private final Map<String, Boolean> lockClasses = new HashMap<>();
    private final Set<Long> allocatingThreads = new HashSet<>();

    private JfrSessionImpl(JfrSampleListener listener) {
        this.listener = listener;
        this.stream = new RecordingStream();
    }

    private void start(Mode mode, int intervalMicros) {
        this.stream.setOrdered(true);
        this.stream.setReuse(true);
        this.stream.setMaxAge(Duration.ofMinutes(2));
        this.stream.onError(e -> SparkStaticLogger.log(Level.WARNING, "Exception occurred while reading flight recorder events", e));

        this.stream.enable(SparkMarkerEvent.class);
        this.stream.onEvent(SparkMarkerEvent.NAME, this::onMarker);

        switch (mode) {
            case EXECUTION:
                Duration period = Duration.ofNanos(TimeUnit.MICROSECONDS.toNanos(intervalMicros));
                this.stream.enable(EXECUTION_SAMPLE).withPeriod(period);
                this.stream.onEvent(EXECUTION_SAMPLE, this::onExecutionSample);

                // Threads inside native methods are sampled by the same JVM thread, one of them
                // each time it wakes up. With a thread that never leaves a native method, that
                // shows how often the JVM really samples.
                this.stream.enable(NATIVE_SAMPLE).withPeriod(period).withoutStackTrace();
                this.stream.onEvent(NATIVE_SAMPLE, this::onNativeSample);
                startHeartbeatThread();
                break;
            case ALLOCATION:
                this.stream.enable(ALLOCATION_SAMPLE).with("throttle", ALLOCATION_THROTTLE);
                this.stream.onEvent(ALLOCATION_SAMPLE, this::onAllocationSample);
                break;
            case LOCK:
                Duration threshold = Duration.ofNanos(TimeUnit.MICROSECONDS.toNanos(intervalMicros));
                this.stream.enable(MONITOR_ENTER).withThreshold(threshold);
                this.stream.enable(THREAD_PARK).withThreshold(threshold);
                this.stream.onEvent(MONITOR_ENTER, this::onMonitorEnter);
                this.stream.onEvent(THREAD_PARK, this::onThreadPark);
                break;
            default:
                throw new AssertionError(mode);
        }

        this.stream.startAsync();

        this.markerThread = new Thread(this::writeMarkers, "spark-jfr-marker");
        this.markerThread.setDaemon(true);
        this.markerThread.start();

        if (mode == Mode.EXECUTION && System.getProperty("os.name", "").toLowerCase(Locale.ROOT).startsWith("windows")) {
            // The JVM's sampler thread sleeps between samples. Windows may only wake it up on a
            // timer interrupt, by default every 15.6ms, however short the sleep is. On some JVMs
            // a thread in a long sleep makes the JVM ask for 1ms interrupts while the sleep lasts.
            this.timerThread = new Thread(JfrSessionImpl::sleepForever, "spark-jfr-timer-resolution");
            this.timerThread.setDaemon(true);
            this.timerThread.start();
        }
    }

    private void startHeartbeatThread() {
        try {
            // waiting on a selector is a native method on every platform
            Selector selector = Selector.open();
            this.heartbeatSelector = selector;

            Thread thread = new Thread(() -> {
                try {
                    while (!this.closed) {
                        selector.select();
                    }
                } catch (Exception e) {
                    // closed
                }
            }, "spark-jfr-heartbeat");
            thread.setDaemon(true);
            thread.start();
        } catch (IOException e) {
            // without it the listener gets no heartbeats, and has to assume the worst
        }
    }

    private void onNativeSample(RecordedEvent event) {
        if (this.offsetCount != 0) {
            this.listener.onHeartbeat(epochNanos(event.getStartTime()) + this.offset);
        }
    }

    private void writeMarkers() {
        while (!this.closed) {
            SparkMarkerEvent event = new SparkMarkerEvent();
            event.nanoTime = System.nanoTime();
            event.commit();

            try {
                Thread.sleep(MARKER_INTERVAL_MILLIS);
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    private static void sleepForever() {
        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            // closed
        }
    }

    private void onMarker(RecordedEvent event) {
        long nanoTime = event.getLong("nanoTime");

        // The marker's timestamp is taken slightly after its nanoTime value was read, so each
        // measurement of the offset is a little too small. The largest recent one is the closest.
        this.offsets[this.offsetCount++ % this.offsets.length] = nanoTime - epochNanos(event.getStartTime());
        long offset = Long.MIN_VALUE;
        for (int i = 0; i < Math.min(this.offsetCount, this.offsets.length); i++) {
            offset = Math.max(offset, this.offsets[i]);
        }
        this.offset = offset;

        synchronized (this.watermarkMutex) {
            this.watermark = nanoTime;
            this.hasWatermark = true;
            this.watermarkMutex.notifyAll();
        }

        this.listener.onWatermark(nanoTime);
    }

    private void onExecutionSample(RecordedEvent event) {
        deliver(event, event.getThread("sampledThread"), event.getStartTime(), 1);
    }

    private void onAllocationSample(RecordedEvent event) {
        // The weight of a sample is what its thread has allocated since the thread's last sample.
        // For the first one that goes back to before the session started, possibly to when the
        // thread started.
        RecordedThread thread = event.getThread();
        if (thread == null || this.allocatingThreads.add(thread.getJavaThreadId())) {
            return;
        }
        deliver(event, thread, event.getStartTime(), event.getLong("weight"));
    }

    private void onMonitorEnter(RecordedEvent event) {
        deliver(event, event.getThread(), event.getEndTime(), event.getDuration().toNanos());
    }

    private void onThreadPark(RecordedEvent event) {
        // threads park for all sorts of reasons, only waiting for a lock is contention
        RecordedClass parkedClass = event.getClass("parkedClass");
        if (parkedClass == null || !isLockClass(parkedClass.getName())) {
            return;
        }
        deliver(event, event.getThread(), event.getEndTime(), event.getDuration().toNanos());
    }

    private boolean isLockClass(String className) {
        Boolean result = this.lockClasses.get(className);
        if (result == null) {
            try {
                result = AbstractOwnableSynchronizer.class.isAssignableFrom(Class.forName(className, false, ClassLoader.getSystemClassLoader()));
            } catch (Throwable e) {
                result = false;
            }
            this.lockClasses.put(className, result);
        }
        return result;
    }

    private void deliver(RecordedEvent event, RecordedThread thread, Instant time, long value) {
        if (this.offsetCount == 0 || thread == null || thread.getJavaThreadId() <= 0) {
            return;
        }

        RecordedStackTrace stackTrace = event.getStackTrace();
        if (stackTrace == null) {
            return;
        }
        if (stackTrace.isTruncated()) {
            this.listener.onTruncatedStack();
            return;
        }

        StackTraceElement[] stack = convert(stackTrace.getFrames());
        if (stack.length == 0) {
            return;
        }

        this.listener.onSample(thread.getJavaThreadId(), thread.getJavaName(), stack, epochNanos(time) + this.offset, value);
    }

    /**
     * Converts a stack trace to what a thread dump of the same stack would have returned.
     *
     * @return the stack trace, or an empty array if it cannot be converted
     */
    private StackTraceElement[] convert(List<RecordedFrame> recordedFrames) {
        if (this.frames.size() > MAX_CACHED_FRAMES) {
            this.frames.clear();
        }

        StackTraceElement[] stack = new StackTraceElement[recordedFrames.size()];
        int i = 0;

        for (RecordedFrame frame : recordedFrames) {
            RecordedMethod method = frame.getMethod();
            RecordedClass type = method == null ? null : method.getType();
            if (type == null) {
                return NO_FRAMES;
            }

            int line;
            if ("Native".equals(frame.getType())) {
                line = -2; // see StackTraceElement#isNativeMethod
            } else {
                line = frame.getLineNumber() > 0 ? frame.getLineNumber() : -1;
            }

            StackTraceElement element = this.frames.get(method);
            if (element == null || element.getLineNumber() != line) {
                element = new StackTraceElement(className(type.getName()), method.getName(), null, line);
                this.frames.put(method, element);
            }
            stack[i++] = element;
        }

        return stack;
    }

    /**
     * Gets the name {@link Class#getName()} returns for a class.
     *
     * <p>The flight recorder writes the names of hidden classes (which lambdas are) differently,
     * and not the same way in every Java version: {@code Foo$$Lambda+0x0000021fa3158a00.1280851663}
     * or {@code Foo$$Lambda.0x000000009b168210} for {@code Foo$$Lambda/0x000000009b168210}.</p>
     */
    static String className(String name) {
        if (name.indexOf("0x") == -1) {
            return name;
        }
        Matcher matcher = HIDDEN_CLASS_NAME.matcher(name);
        return matcher.matches() ? matcher.group(1) + '/' + matcher.group(2) : name;
    }

    private static long epochNanos(Instant instant) {
        return TimeUnit.SECONDS.toNanos(instant.getEpochSecond()) + instant.getNano();
    }

    @Override
    public boolean awaitWatermark(long nanoTime, long timeoutMillis) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        synchronized (this.watermarkMutex) {
            while (!this.hasWatermark || this.watermark - nanoTime < 0) {
                long remaining = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
                if (remaining <= 0 || this.closed) {
                    return false;
                }
                try {
                    this.watermarkMutex.wait(remaining);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
            return true;
        }
    }

    @Override
    public void close() {
        if (this.closed) {
            return;
        }
        this.closed = true;

        if (this.markerThread != null) {
            this.markerThread.interrupt();
        }
        if (this.timerThread != null) {
            this.timerThread.interrupt();
        }
        if (this.heartbeatSelector != null) {
            try {
                this.heartbeatSelector.close();
            } catch (IOException e) {
                // ignore
            }
        }
        this.stream.close();

        synchronized (this.watermarkMutex) {
            this.watermarkMutex.notifyAll();
        }
    }

}
