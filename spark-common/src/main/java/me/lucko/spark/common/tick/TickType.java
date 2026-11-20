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

package me.lucko.spark.common.tick;

/**
 * What the "ticks" counted by a platform's {@link TickHook} and {@link TickReporter} are.
 */
public enum TickType {

    /**
     * Game ticks, which the game aims to run at a fixed rate.
     */
    GAME_TICK("TPS", "Tick durations", "Tick", "ticks"),

    /**
     * Rendered frames. They have no fixed target rate, and a frame lasts from its start
     * to the start of the next frame.
     */
    FRAME("FPS", "Frame times", "Frame", "frames");

    private final String rateName;
    private final String durationsName;
    private final String displayName;
    private final String pluralName;

    TickType(String rateName, String durationsName, String displayName, String pluralName) {
        this.rateName = rateName;
        this.durationsName = durationsName;
        this.displayName = displayName;
        this.pluralName = pluralName;
    }

    /**
     * Gets the name of the rate, e.g. "TPS".
     *
     * @return the rate name
     */
    public String rateName() {
        return this.rateName;
    }

    /**
     * Gets the name of the durations, e.g. "Tick durations".
     *
     * @return the durations name
     */
    public String durationsName() {
        return this.durationsName;
    }

    /**
     * Gets the name of a single tick, e.g. "Tick".
     *
     * @return the display name
     */
    public String displayName() {
        return this.displayName;
    }

    /**
     * Gets the name of several ticks in lower case, e.g. "ticks".
     *
     * @return the plural name
     */
    public String pluralName() {
        return this.pluralName;
    }
}
