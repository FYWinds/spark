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
 * Access to the JDK Flight Recorder, for JVMs that have it.
 *
 * <p>The implementation lives in a separate source set because it needs APIs which
 * do not exist on Java 8.</p>
 */
public interface JfrBackend {

    /**
     * Starts sampling threads that are running Java code. Unlike a thread dump, these
     * samples are not taken at safepoints.
     *
     * @param intervalMicros the sampling interval
     * @param listener the listener
     * @return the session
     */
    JfrSession startExecution(int intervalMicros, JfrSampleListener listener);

    /**
     * Gets if {@link #startAllocation(JfrSampleListener)} is supported.
     */
    boolean supportsAllocation();

    /**
     * Starts sampling object allocations. The value of each sample is the number of
     * bytes it represents.
     *
     * @param listener the listener
     * @return the session
     */
    JfrSession startAllocation(JfrSampleListener listener);

    /**
     * Starts recording lock contention. The value of each sample is the time spent
     * waiting for the lock, in nanoseconds.
     *
     * @param thresholdMicros waits shorter than this are not recorded
     * @param listener the listener
     * @return the session
     */
    JfrSession startLock(int thresholdMicros, JfrSampleListener listener);

}
