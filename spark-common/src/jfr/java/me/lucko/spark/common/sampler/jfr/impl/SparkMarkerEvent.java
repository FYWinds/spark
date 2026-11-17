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

import jdk.jfr.Category;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;

/**
 * An event spark writes into the recording at regular intervals.
 *
 * <p>It carries the value of {@link System#nanoTime()}, which lets the reader convert the
 * flight recorder's timestamps, and because events are read in order, seeing a marker
 * means every earlier event has been read.</p>
 */
@Name(SparkMarkerEvent.NAME)
@Label("Marker")
@Category("spark")
@StackTrace(false)
public final class SparkMarkerEvent extends Event {
    public static final String NAME = "spark.Marker";

    @Label("Nano Time")
    public long nanoTime;

}
