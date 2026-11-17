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

import org.jspecify.annotations.Nullable;

/**
 * Loads the {@link JfrBackend}, if the JVM supports it.
 */
public final class JfrSupport {
    private JfrSupport() {}

    private static final String IMPLEMENTATION = "me.lucko.spark.common.sampler.jfr.impl.JfrBackendImpl";

    private static boolean loaded;
    private static JfrBackend backend;
    private static String unavailableReason;

    /**
     * Gets the backend.
     *
     * @return the backend, or null if the flight recorder cannot be used on this JVM
     */
    public static synchronized @Nullable JfrBackend backend() {
        if (!loaded) {
            loaded = true;
            try {
                // needs the event streaming API added in Java 14
                Class.forName("jdk.jfr.consumer.RecordingStream");
                backend = (JfrBackend) Class.forName(IMPLEMENTATION).getDeclaredConstructor().newInstance();
            } catch (ClassNotFoundException e) {
                unavailableReason = "requires Java 14 or newer";
            } catch (Throwable e) {
                Throwable cause = e.getCause() != null ? e.getCause() : e;
                unavailableReason = String.valueOf(cause.getMessage() != null ? cause.getMessage() : cause);
            }
        }
        return backend;
    }

    /**
     * Gets why {@link #backend()} returned null.
     */
    public static synchronized String unavailableReason() {
        return unavailableReason;
    }

}
