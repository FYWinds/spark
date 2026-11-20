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

package me.lucko.spark.forge;

import cpw.mods.fml.common.FMLCommonHandler;
import cpw.mods.fml.common.eventhandler.SubscribeEvent;
import cpw.mods.fml.common.gameevent.TickEvent;
import me.lucko.spark.common.tick.AbstractTickReporter;
import me.lucko.spark.common.tick.TickReporter;

/**
 * Reports the duration of each frame the client renders.
 *
 * <p>A frame starts with its render tick, and lasts until the next one starts. That includes
 * the game ticks run before rendering, the buffer swap and the frame rate limiter. The duration
 * of a frame is reported when the next frame starts.</p>
 */
public class Forge1710FrameReporter extends AbstractTickReporter implements TickReporter {

    /** The time the latest frame started at, or 0 if no frame has started yet */
    private long frameStart = 0;

    @SubscribeEvent
    public void onRenderTick(TickEvent.RenderTickEvent e) {
        if (e.phase != TickEvent.Phase.START) {
            return;
        }

        long now = System.nanoTime();
        long last = this.frameStart;
        this.frameStart = now;

        if (last != 0) {
            onTick((now - last) / 1000000d);
        }
    }

    @Override
    public void start() {
        FMLCommonHandler.instance().bus().register(this);
    }

    @Override
    public void close() {
        FMLCommonHandler.instance().bus().unregister(this);
    }

}
