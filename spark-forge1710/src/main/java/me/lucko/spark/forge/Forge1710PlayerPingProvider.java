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

import me.lucko.spark.common.monitor.ping.PlayerPingProvider;

import net.minecraft.entity.player.EntityPlayerMP;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.management.ServerConfigurationManager;

import java.util.HashMap;
import java.util.Map;

public class Forge1710PlayerPingProvider implements PlayerPingProvider {
    private final MinecraftServer server;

    public Forge1710PlayerPingProvider(MinecraftServer server) {
        this.server = server;
    }

    @Override
    public Map<String, Integer> poll() {
        Map<String, Integer> pings = new HashMap<>();
        ServerConfigurationManager playerList = this.server.getConfigurationManager();
        if (playerList == null) {
            return pings;
        }

        // polled off the server thread: snapshot the list instead of iterating it
        for (Object player : playerList.playerEntityList.toArray()) {
            if (player instanceof EntityPlayerMP) {
                EntityPlayerMP playerMP = (EntityPlayerMP) player;
                pings.put(playerMP.getGameProfile().getName(), playerMP.ping);
            }
        }
        return pings;
    }
}
