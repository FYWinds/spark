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

package me.lucko.spark.forge.plugin;

import com.google.common.collect.Queues;
import cpw.mods.fml.common.FMLCommonHandler;
import cpw.mods.fml.common.event.FMLServerStartingEvent;
import cpw.mods.fml.common.eventhandler.SubscribeEvent;
import cpw.mods.fml.common.gameevent.TickEvent;
import me.lucko.spark.common.monitor.ping.PlayerPingProvider;
import me.lucko.spark.common.platform.PlatformInfo;
import me.lucko.spark.common.platform.serverconfig.ServerConfigProvider;
import me.lucko.spark.common.platform.world.WorldInfoProvider;
import me.lucko.spark.common.sampler.ThreadDumper;
import me.lucko.spark.common.tick.TickHook;
import me.lucko.spark.common.tick.TickReporter;
import me.lucko.spark.forge.*;
import net.minecraft.command.ICommandSender;
import net.minecraft.entity.player.EntityPlayer;
import net.minecraft.entity.player.EntityPlayerMP;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.management.ServerConfigurationManager;

import java.util.Arrays;
import java.util.Queue;
import java.util.logging.Level;
import java.util.stream.Stream;

public class Forge1710ServerSparkPlugin extends Forge1710SparkPlugin {
    private final Queue<Runnable> scheduledServerTasks = Queues.newArrayDeque();

    private final Thread serverThread;
    private final ThreadDumper.GameThread gameThreadDumper;

    public static Forge1710ServerSparkPlugin register(Forge1710SparkMod mod, FMLServerStartingEvent event) {
        Forge1710ServerSparkPlugin plugin = new Forge1710ServerSparkPlugin(mod, event.getServer());
        plugin.enable();

        FMLCommonHandler.instance().bus().register(plugin);

        // register commands & permissions
        event.registerServerCommand(plugin.new VanillaCommand());

        return plugin;
    }

    @Override
    public void disable() {
        super.disable();
        FMLCommonHandler.instance().bus().unregister(this);
    }

    @SubscribeEvent
    public void onServerTickStart(TickEvent.ServerTickEvent event) {
        if (event.phase == TickEvent.Phase.START) {
            runScheduledTasks();
        }
    }

    // Only fired on a client, i.e. when this is the integrated server.
    @SubscribeEvent
    public void onClientTickStart(TickEvent.ClientTickEvent event) {
        // The integrated server does not tick while the game is paused, so nothing would run
        // the tasks until the player comes back. Its world does not change while paused either,
        // so they are run from the client thread instead.
        if (event.phase == TickEvent.Phase.START && Forge1710ClientSparkPlugin.isGamePaused()) {
            runScheduledTasks();
        }
    }

    private void runScheduledTasks() {
        Runnable task;
        while ((task = pollScheduledTask()) != null) {
            try {
                task.run();
            } catch (RuntimeException | LinkageError e) {
                log(Level.WARNING, "Exception while executing a task on the server thread", e);
            }
        }
    }

    private Runnable pollScheduledTask() {
        synchronized (this.scheduledServerTasks) {
            return this.scheduledServerTasks.poll();
        }
    }

    private final MinecraftServer server;

    public Forge1710ServerSparkPlugin(Forge1710SparkMod mod, MinecraftServer server) {
        super(mod);
        this.server = server;
        // constructed from FMLServerStartingEvent, which is fired on the server thread
        this.serverThread = Thread.currentThread();
        this.gameThreadDumper = new ThreadDumper.GameThread();
        this.gameThreadDumper.setThread(this.serverThread);
    }

    @Override
    public ThreadDumper getDefaultThreadDumper() {
        return this.gameThreadDumper.get();
    }

    @Override
    public boolean hasPermission(ICommandSender sender, String permission) {
        if (sender instanceof EntityPlayerMP) {
            EntityPlayerMP player = (EntityPlayerMP)sender;
            if(isOp(player))
                return true;
            else {
                String serverOwner = MinecraftServer.getServer().getServerOwner();
                if(player.getGameProfile().getName() != null && serverOwner != null)
                    return serverOwner.equals(player.getGameProfile().getName());
                else
                    return false;
            }
        } else {
            return true;
        }
    }

    @Override
    public Stream<Forge1710CommandSender> getCommandSenders() {
        // may be called off the server thread: snapshot the player list instead of streaming it
        ServerConfigurationManager playerList = this.server.getConfigurationManager();
        Object[] players = playerList != null ? playerList.playerEntityList.toArray() : new Object[0];

        return Stream.concat(
                Arrays.stream(players).filter(player -> player instanceof EntityPlayer).map(player -> (ICommandSender) player),
                Stream.of((ICommandSender) this.server)
        ).map(sender -> new Forge1710CommandSender(sender, this));
    }

    @Override
    public TickHook createTickHook() {
        return new Forge1710TickHook(TickEvent.Type.SERVER);
    }

    @Override
    public TickReporter createTickReporter() {
        return new Forge1710TickReporter(TickEvent.Type.SERVER);
    }

    @Override
    public WorldInfoProvider createWorldInfoProvider() {
        return new Forge1710WorldInfoProvider.Server(this.server);
    }

    @Override
    public PlayerPingProvider createPlayerPingProvider() {
        return new Forge1710PlayerPingProvider(this.server);
    }

    @Override
    public ServerConfigProvider createServerConfigProvider() {
        return new Forge1710ServerConfigProvider();
    }

    @Override
    public void executeSync(Runnable task) {
        if (Thread.currentThread() == this.serverThread) {
            task.run();
            return;
        }

        synchronized (this.scheduledServerTasks) {
            this.scheduledServerTasks.add(task);
        }
    }

    @Override
    public PlatformInfo getPlatformInfo() {
        return new Forge1710PlatformInfo(PlatformInfo.Type.SERVER);
    }

    @Override
    public String getCommandName() {
        return "spark";
    }
}
