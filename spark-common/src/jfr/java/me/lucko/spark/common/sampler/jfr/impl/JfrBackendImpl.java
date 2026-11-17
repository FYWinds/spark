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

import jdk.jfr.EventType;
import jdk.jfr.FlightRecorder;
import me.lucko.spark.common.sampler.jfr.JfrBackend;
import me.lucko.spark.common.sampler.jfr.JfrSampleListener;
import me.lucko.spark.common.sampler.jfr.JfrSession;

import java.lang.management.ManagementFactory;
import javax.management.ObjectName;

/**
 * Implements {@link JfrBackend} using the event streaming API.
 *
 * <p>Loaded reflectively, and only on JVMs that have the API.</p>
 */
public final class JfrBackendImpl implements JfrBackend {

    /**
     * The stack depth the flight recorder is configured with. Deeper stacks are cut off at
     * the root end, which makes them useless for building a call tree. The default is 64.
     */
    private static final int STACK_DEPTH = 2048;

    private final boolean supportsAllocation;

    public JfrBackendImpl() {
        if (!FlightRecorder.isAvailable()) {
            throw new UnsupportedOperationException("the flight recorder is not available on this JVM");
        }

        configureStackDepth();

        boolean supportsAllocation = false;
        for (EventType type : FlightRecorder.getFlightRecorder().getEventTypes()) {
            if (type.getName().equals(JfrSessionImpl.ALLOCATION_SAMPLE)) {
                supportsAllocation = true;
                break;
            }
        }
        this.supportsAllocation = supportsAllocation;
    }

    private static void configureStackDepth() {
        // the depth can only be changed before the flight recorder is first used
        if (FlightRecorder.isInitialized()) {
            return;
        }

        try {
            ManagementFactory.getPlatformMBeanServer().invoke(
                    new ObjectName("com.sun.management:type=DiagnosticCommand"),
                    "jfrConfigure",
                    new Object[]{new String[]{"stackdepth=" + STACK_DEPTH}},
                    new String[]{String[].class.getName()}
            );
        } catch (Exception e) {
            // ignore - truncated stacks are detected when they are read
        }
    }

    @Override
    public JfrSession startExecution(int intervalMicros, JfrSampleListener listener) {
        return JfrSessionImpl.start(JfrSessionImpl.Mode.EXECUTION, intervalMicros, listener);
    }

    @Override
    public boolean supportsAllocation() {
        return this.supportsAllocation;
    }

    @Override
    public JfrSession startAllocation(JfrSampleListener listener) {
        if (!this.supportsAllocation) {
            throw new UnsupportedOperationException("allocation sampling requires Java 16 or newer");
        }
        return JfrSessionImpl.start(JfrSessionImpl.Mode.ALLOCATION, 0, listener);
    }

    @Override
    public JfrSession startLock(int thresholdMicros, JfrSampleListener listener) {
        return JfrSessionImpl.start(JfrSessionImpl.Mode.LOCK, thresholdMicros, listener);
    }

}
