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

/**
 * Receives the samples read from a {@link JfrSession}.
 *
 * <p>All times are in the {@link System#nanoTime()} domain. Samples are delivered in
 * chronological order, a second or two after they were taken.</p>
 */
public interface JfrSampleListener {

    /**
     * Called for each sample.
     *
     * @param threadId the java thread id of the sampled thread
     * @param threadName the name of the sampled thread
     * @param stack the stack trace, top frame first
     * @param nanoTime when the sample was taken
     * @param value the value of the sample (bytes for allocations, nanoseconds for locks, unused otherwise)
     */
    void onSample(long threadId, String threadName, StackTraceElement[] stack, long nanoTime, long value);

    /**
     * Called when every sample taken before the given time has been delivered.
     *
     * @param nanoTime the watermark
     */
    void onWatermark(long nanoTime);

    /**
     * Called when a sample had to be dropped because its stack was deeper than the
     * depth the flight recorder was configured with.
     */
    void onTruncatedStack();

    /**
     * Called each time the JVM's own sampler thread has woken up to take samples, whether or not
     * it found a thread to sample. Only called for execution sampling.
     *
     * <p>The JVM is asked to sample at an interval, but how well it keeps to it is up to the
     * operating system's timers, so this is the only way to know how often it really samples.</p>
     *
     * @param nanoTime when it woke up
     */
    void onHeartbeat(long nanoTime);

    /**
     * Called when a thread was sampled inside a native call that is not a native method, with
     * a frame the call was made from. Such a thread looks like it is running Java code in that
     * frame, to a thread dump. Only called for execution sampling.
     *
     * @param frame the top frame of the thread, or the top one that is not hidden
     */
    default void onNativeCaller(StackTraceElement frame) {

    }

}
