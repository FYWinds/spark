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

package me.lucko.spark.common.monitor.tick;

import me.lucko.spark.api.statistic.misc.DoubleAverageInfo;
import me.lucko.spark.common.metric.Metrics;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SparkFrameStatisticsTest {

    private long now = 0;

    private final SparkFrameStatistics statistics = new SparkFrameStatistics(new Metrics(), () -> this.now);

    private void frameStarts(long stepMillis, long untilMillis) {
        while (this.now < TimeUnit.MILLISECONDS.toNanos(untilMillis)) {
            this.now += TimeUnit.MILLISECONDS.toNanos(stepMillis);
            this.statistics.onTick(0);
        }
    }

    private void frameEnds(double duration, long atMillis) {
        this.now = TimeUnit.MILLISECONDS.toNanos(atMillis);
        this.statistics.onTick(duration);
    }

    @Test
    public void testNoTarget() {
        assertEquals(0, this.statistics.gameTargetTps());
        assertEquals(0, this.statistics.gameMaxIdealDuration());
    }

    @Test
    public void testFrameRate() {
        this.statistics.onTick(0);
        assertEquals(0, this.statistics.tps5Sec());

        // 100 FPS for 3 seconds, then 50 FPS for 10 seconds
        frameStarts(10, 3000);
        assertEquals(100, this.statistics.tps5Sec(), 1e-9);
        frameStarts(20, 13000);

        assertEquals(50, this.statistics.tps5Sec(), 1e-9);
        assertEquals(50, this.statistics.tps10Sec(), 1e-9);
        assertEquals((300 + 500) / 13d, this.statistics.tps1Min(), 1e-9);
        assertEquals((300 + 500) / 13d, this.statistics.tps15Min(), 1e-9);
    }

    @Test
    public void testFrameRateWithLongFrame() {
        this.statistics.onTick(0);
        frameStarts(10, 1000);
        // a single frame lasting 4 seconds
        frameStarts(4000, 5000);

        assertEquals(101 / 5d, this.statistics.tps5Sec(), 1e-9);
    }

    @Test
    public void testFrameTimeWindows() {
        assertFalse(this.statistics.isDurationSupported());
        assertNull(this.statistics.duration10Sec());

        // a frame ending every second, lasting 1ms, 2ms, ... 20ms
        for (int i = 0; i < 20; i++) {
            frameEnds(i + 1, i * 1000L);
        }

        assertTrue(this.statistics.isDurationSupported());

        DoubleAverageInfo last10Sec = this.statistics.duration10Sec();
        assertEquals(11, last10Sec.min());
        assertEquals(20, last10Sec.max());
        assertEquals(15.5, last10Sec.mean(), 1e-9);
        assertEquals(16, last10Sec.median());
        assertEquals(20, last10Sec.percentile95th());

        DoubleAverageInfo last1Min = this.statistics.duration1Min();
        assertEquals(1, last1Min.min());
        assertEquals(20, last1Min.max());
    }

    @Test
    public void testFrameTimesExpire() {
        frameEnds(100, 0);
        frameEnds(5, TimeUnit.MINUTES.toMillis(6));

        assertEquals(5, this.statistics.duration5Min().max());
        assertEquals(5, this.statistics.duration5Min().min());
    }

    @Test
    public void testManyFrameTimes() {
        // more frames than the initial capacity, over less than 10 seconds
        for (int i = 0; i < 5000; i++) {
            frameEnds(i % 2 == 0 ? 1 : 3, i);
        }

        DoubleAverageInfo last10Sec = this.statistics.duration10Sec();
        assertEquals(2, last10Sec.mean(), 1e-9);
        assertEquals(1, last10Sec.min());
        assertEquals(3, last10Sec.max());
    }

}
