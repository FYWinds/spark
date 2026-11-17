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
 * A running flight recorder stream.
 */
public interface JfrSession extends AutoCloseable {

    /**
     * Blocks until every sample taken before the given time has been delivered to the listener.
     *
     * @param nanoTime the time to wait for
     * @param timeoutMillis the maximum time to wait
     * @return true if the watermark was reached
     */
    boolean awaitWatermark(long nanoTime, long timeoutMillis);

    @Override
    void close();

}
