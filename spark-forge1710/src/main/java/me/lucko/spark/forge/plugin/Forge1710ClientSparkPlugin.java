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

import cpw.mods.fml.common.gameevent.TickEvent;
import me.lucko.spark.common.platform.PlatformInfo;
import me.lucko.spark.common.platform.world.WorldInfoProvider;
import me.lucko.spark.common.sampler.ThreadDumper;
import me.lucko.spark.common.tick.TickHook;
import me.lucko.spark.common.tick.TickReporter;
import me.lucko.spark.forge.*;
import net.minecraft.client.Minecraft;
import net.minecraft.client.entity.EntityClientPlayerMP;
import net.minecraft.command.ICommandSender;
import net.minecraftforge.client.ClientCommandHandler;
import net.minecraftforge.common.MinecraftForge;

import java.util.logging.Level;
import java.util.stream.Stream;

public class Forge1710ClientSparkPlugin extends Forge1710SparkPlugin {

    public static void register(Forge1710SparkMod mod) {
        Forge1710ClientSparkPlugin plugin = new Forge1710ClientSparkPlugin(mod, Minecraft.getMinecraft());
        plugin.enable();

        // register listeners
        MinecraftForge.EVENT_BUS.register(plugin);

        // register commands
        ClientCommandHandler.instance.registerCommand(plugin.new VanillaCommand());
    }

    private final Minecraft minecraft;
    private final ThreadDumper gameThreadDumper;

    public Forge1710ClientSparkPlugin(Forge1710SparkMod mod, Minecraft minecraft) {
        super(mod);
        this.minecraft = minecraft;
        this.gameThreadDumper = new ThreadDumper.Specific(minecraft.field_152352_aC);
    }

    @Override
    public ThreadDumper getDefaultThreadDumper() {
        return this.gameThreadDumper;
    }

    @Override
    public boolean hasPermission(ICommandSender sender, String permission) {
        return true;
    }

    @Override
    public Stream<Forge1710CommandSender> getCommandSenders() {
        EntityClientPlayerMP player = this.minecraft.thePlayer;
        if (player == null) {
            return Stream.empty();
        }
        return Stream.of(new Forge1710CommandSender(player, this));
    }

    @Override
    public TickHook createTickHook() {
        return new Forge1710TickHook(TickEvent.Type.CLIENT);
    }

    @Override
    public TickReporter createTickReporter() {
        return new Forge1710TickReporter(TickEvent.Type.CLIENT);
    }

    @Override
    public WorldInfoProvider createWorldInfoProvider() {
        return new Forge1710WorldInfoProvider.Client(this.minecraft);
    }

    @Override
    public void executeSync(Runnable task) {
        // queue without waiting (like upstream's executeIfPossible): blocking here would stall
        // the calling thread for as long as the client thread is busy
        this.minecraft.func_152344_a(() -> {
            try {
                task.run();
            } catch (RuntimeException | LinkageError e) {
                log(Level.WARNING, "Exception while executing a task on the client thread", e);
            }
        });
    }

    @Override
    public PlatformInfo getPlatformInfo() {
        return new Forge1710PlatformInfo(PlatformInfo.Type.CLIENT);
    }

    @Override
    public String getCommandName() {
        return "sparkc";
    }


    /**
     * Gets if the game is paused, which stops the integrated server from ticking.
     */
    static boolean isGamePaused() {
        return Minecraft.getMinecraft().isGamePaused();
    }
}
