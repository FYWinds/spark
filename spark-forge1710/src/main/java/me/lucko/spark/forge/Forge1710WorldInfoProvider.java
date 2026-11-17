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

import cpw.mods.fml.relauncher.Side;
import cpw.mods.fml.relauncher.SideOnly;
import me.lucko.spark.common.platform.world.AbstractChunkInfo;
import me.lucko.spark.common.platform.world.CountMap;
import me.lucko.spark.common.platform.world.WorldInfoProvider;

import net.minecraft.client.Minecraft;
import net.minecraft.client.multiplayer.WorldClient;
import net.minecraft.entity.Entity;
import net.minecraft.entity.EntityList;
import net.minecraft.server.MinecraftServer;
import net.minecraft.world.GameRules;
import net.minecraft.util.MathHelper;
import net.minecraft.world.World;
import net.minecraft.world.WorldServer;
import net.minecraft.world.chunk.Chunk;
import net.minecraft.world.chunk.IChunkProvider;
import net.minecraft.world.gen.ChunkProviderServer;

import java.util.*;
import java.util.stream.Collectors;

/**
 * World information for 1.7.10.
 *
 * <p>The polls read unsynchronized world state, so they rely on the default
 * {@link #mustCallSync()} and are run on the server/client thread by spark.</p>
 */
public abstract class Forge1710WorldInfoProvider implements WorldInfoProvider {
    @Override
    public Collection<DataPackInfo> pollDataPacks() {
        return Collections.emptyList();
    }

    /**
     * Gets a readable name for the world which is unique across dimensions,
     * e.g. {@code Overworld (DIM0)}.
     *
     * @param world the world
     * @return the name
     */
    protected static String worldName(World world) {
        int dimension = world.provider.dimensionId;
        String name;
        try {
            name = world.provider.getDimensionName();
        } catch (RuntimeException e) {
            name = null;
        }
        return name == null || name.isEmpty()
                ? "DIM" + dimension
                : name + " (DIM" + dimension + ")";
    }

    protected static int countTileEntities(List<Chunk> chunks) {
        int tileEntities = 0;
        for (Chunk chunk : chunks) {
            if (chunk != null && chunk.chunkTileEntityMap != null) {
                tileEntities += chunk.chunkTileEntityMap.size();
            }
        }
        return tileEntities;
    }

    protected static void addChunks(ChunksResult<ForgeChunkInfo> data, World world, List<Chunk> chunks) {
        List<ForgeChunkInfo> list = new ArrayList<>();
        for (Chunk chunk : chunks) {
            if (chunk != null && hasEntities(chunk)) {
                list.add(new ForgeChunkInfo(chunk));
            }
        }
        data.put(worldName(world), list);
    }

    private static boolean hasEntities(Chunk chunk) {
        for (List<?> entityList : chunk.entityLists) {
            if (!entityList.isEmpty()) {
                return true;
            }
        }
        return false;
    }

    protected void encodeGameRules(GameRulesResult result, GameRules rules, String worldName) {
        for (String rule : rules.getRules()) {
            if (rule == null) {
                continue;
            }
            String value = rules.getGameRuleStringValue(rule);
            if (value != null) {
                result.put(rule, worldName, value);
            }
        }

    }

    protected void setDefaultGameRules(GameRulesResult result) {
        GameRules vanillaRules = new GameRules();

        result.getRules().entrySet().stream().filter(entry -> entry.getValue().getDefaultValue() == null).map(Map.Entry::getKey).collect(Collectors.toList()).forEach(rule -> {
            String def = vanillaRules.getGameRuleStringValue(rule);
            result.putDefault(rule, def != null ? def : "");
        });
    }

    public static final class Server extends Forge1710WorldInfoProvider {
        private final MinecraftServer server;

        public Server(MinecraftServer server) {
            this.server = server;
        }

        @SuppressWarnings("unchecked")
        private static List<Chunk> loadedChunks(WorldServer world) {
            ChunkProviderServer provider = world.theChunkProviderServer;
            return provider != null ? (List<Chunk>) provider.loadedChunks : Collections.emptyList();
        }

        @Override
        public ChunksResult<ForgeChunkInfo> pollChunks() {
            WorldServer[] worlds = this.server.worldServers;
            if (worlds == null) {
                return null;
            }

            ChunksResult<ForgeChunkInfo> data = new ChunksResult<>();
            for (WorldServer world : worlds) {
                if (world != null) {
                    addChunks(data, world, loadedChunks(world));
                }
            }
            return data;
        }

        @Override
        public CountsResult pollCounts() {
            WorldServer[] worlds = this.server.worldServers;
            if (worlds == null) {
                return null;
            }

            int players = this.server.getCurrentPlayerCount();
            int entities = 0;
            int tileEntities = 0;
            int chunks = 0;

            for (WorldServer world : worlds) {
                if (world == null) {
                    continue;
                }

                List<Chunk> loadedChunks = loadedChunks(world);
                entities += world.loadedEntityList.size();
                tileEntities += countTileEntities(loadedChunks);
                chunks += loadedChunks.size();
            }

            return new CountsResult(players, entities, tileEntities, chunks);
        }

        @Override
        public GameRulesResult pollGameRules() {
            WorldServer[] worlds = this.server.worldServers;
            if (worlds == null) {
                return null;
            }

            GameRulesResult data = new GameRulesResult();
            for (WorldServer world : worlds) {
                if (world != null) {
                    encodeGameRules(data, world.getGameRules(), worldName(world));
                }
            }

            setDefaultGameRules(data);

            return data;
        }
    }

    @SideOnly(Side.CLIENT)
    public static final class Client extends Forge1710WorldInfoProvider {
        private final Minecraft client;

        public Client(Minecraft client) {
            this.client = client;
        }

        /**
         * While there is no world there is nothing to ask the client thread for, and it can be
         * busy loading one for longer than spark waits for an answer.
         */
        @Override
        public boolean mustCallSync() {
            return this.client.theWorld != null;
        }

        /**
         * Gets the world, if this is the client thread.
         */
        private WorldClient world() {
            return this.client.func_152345_ab() ? this.client.theWorld : null;
        }

        /**
         * Gets the chunks the client has loaded.
         *
         * <p>The client's chunk provider keeps a list of them, but mods that optimise it leave the
         * list empty (it is not used by the game). Asking for every chunk in view distance works
         * whatever the provider does: it returns an empty chunk for the ones it does not have.</p>
         */
        private List<Chunk> loadedChunks(WorldClient world) {
            Entity viewer = this.client.thePlayer;
            IChunkProvider provider = world.getChunkProvider();
            if (viewer == null || provider == null) {
                return Collections.emptyList();
            }

            int centerX = MathHelper.floor_double(viewer.posX) >> 4;
            int centerZ = MathHelper.floor_double(viewer.posZ) >> 4;
            int radius = Math.min(Math.max(this.client.gameSettings.renderDistanceChunks, 2), 64) + 2;

            List<Chunk> chunks = new ArrayList<>();
            for (int x = centerX - radius; x <= centerX + radius; x++) {
                for (int z = centerZ - radius; z <= centerZ + radius; z++) {
                    Chunk chunk = provider.provideChunk(x, z);
                    if (chunk != null && !chunk.isEmpty()) {
                        chunks.add(chunk);
                    }
                }
            }
            return chunks;
        }

        @Override
        public ChunksResult<ForgeChunkInfo> pollChunks() {
            WorldClient world = world();
            if (world == null) {
                return null;
            }

            ChunksResult<ForgeChunkInfo> data = new ChunksResult<>();
            addChunks(data, world, loadedChunks(world));
            return data;
        }

        @Override
        public CountsResult pollCounts() {
            WorldClient world = world();
            if (world == null) {
                return null;
            }

            List<Chunk> loadedChunks = loadedChunks(world);
            return new CountsResult(-1, world.loadedEntityList.size(), countTileEntities(loadedChunks), loadedChunks.size());
        }

        @Override
        public GameRulesResult pollGameRules() {
            WorldClient world = world();
            if (world == null) {
                return null;
            }

            GameRulesResult data = new GameRulesResult();

            encodeGameRules(data, world.getGameRules(), worldName(world));

            setDefaultGameRules(data);

            return data;
        }
    }

    static final class ForgeChunkInfo extends AbstractChunkInfo<Class<? extends Entity>> {
        private final CountMap<Class<? extends Entity>> entityCounts;

        ForgeChunkInfo(Chunk chunk) {
            super(chunk.xPosition, chunk.zPosition);

            this.entityCounts = new CountMap.Simple<>(new HashMap<>());
            for(List<Entity> entityList : chunk.entityLists) {
                entityList.forEach(entity -> {
                    this.entityCounts.increment(entity.getClass());
                });
            }
        }

        @Override
        public CountMap<Class<? extends Entity>> getEntityCounts() {
            return this.entityCounts;
        }

        @Override
        public String entityTypeName(Class<? extends Entity> type) {
            return (String)EntityList.classToStringMapping.get(type);
        }
    }


}
