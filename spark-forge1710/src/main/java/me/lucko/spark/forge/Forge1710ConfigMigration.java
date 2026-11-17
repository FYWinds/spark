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

import com.google.common.collect.ImmutableSet;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Moves the files older versions of spark created directly in the {@code config/} directory
 * into spark's own {@code config/spark/} directory.
 *
 * <p>Only files that are unambiguously spark's are moved, existing files are never
 * overwritten and nothing is deleted.</p>
 */
public final class Forge1710ConfigMigration {
    private Forge1710ConfigMigration() {}

    /** Largest config.json / activity.json that is inspected */
    private static final long MAX_JSON_SIZE = 1024 * 1024;

    /** heap-yyyy-MM-dd_HH.mm.ss.(hprof|phd), see SparkPlatform#resolveSaveFile */
    private static final Pattern HEAP_DUMP = Pattern.compile("heap-\\d{4}-\\d{2}-\\d{2}_\\d{2}\\.\\d{2}\\.\\d{2}\\.(hprof|phd)");

    /** Every key spark has ever written to or read from config.json */
    private static final Set<String> CONFIG_KEYS = ImmutableSet.of(
            "_header",
            "backgroundProfiler",
            "backgroundProfilerEngine",
            "backgroundProfilerInterval",
            "backgroundProfilerThreadGrouper",
            "backgroundProfilerThreadDumper",
            "_marker_background_profiler_failed",
            "trustedKeys",
            "viewerUrl",
            "bytebinUrl",
            "bytesocksHost",
            "disableResponseBroadcast"
    );

    /** The keys of an entry in activity.json */
    private static final Set<String> ACTIVITY_KEYS = ImmutableSet.of("user", "time", "type", "data");

    /** Temporary directories, see TemporaryFiles */
    private static final Set<String> TEMP_DIRECTORIES = ImmutableSet.of("tmp", "tmp-client");
    private static final String TEMP_README = "about.txt";
    private static final String TEMP_README_MARKER = "spark uses this folder to store some temporary data";

    /**
     * Moves spark's files from the legacy directory into the new one.
     *
     * @param legacyDirectory the directory older versions used ({@code config/})
     * @param directory the new directory ({@code config/spark/})
     * @param logger the logger
     */
    public static void migrate(Path legacyDirectory, Path directory, Logger logger) {
        List<Path> files = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(legacyDirectory)) {
            for (Path path : stream) {
                if (isSparkFile(path)) {
                    files.add(path);
                }
            }
        } catch (IOException | DirectoryIteratorException | SecurityException e) {
            logger.warn("Unable to check " + legacyDirectory + " for files of older spark versions", e);
            return;
        }

        if (files.isEmpty()) {
            return;
        }

        try {
            Files.createDirectories(directory);
        } catch (IOException | SecurityException e) {
            logger.warn("Unable to create " + directory + ", files of older spark versions are left in " + legacyDirectory, e);
            return;
        }

        for (Path source : files) {
            Path target = directory.resolve(source.getFileName().toString());
            if (Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
                logger.warn("Not migrating " + source + " to " + target + " as the target already exists");
                continue;
            }

            try {
                // fails instead of overwriting if the target appeared in the meantime
                Files.move(source, target);
                logger.info("Migrated " + source + " to " + target);
            } catch (IOException | SecurityException e) {
                logger.warn("Unable to migrate " + source + " to " + target, e);
            }
        }
    }

    private static boolean isSparkFile(Path path) {
        try {
            String name = path.getFileName().toString();

            if (Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)) {
                return TEMP_DIRECTORIES.contains(name) && isSparkTempDirectory(path);
            }
            if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
                return false;
            }

            if (name.endsWith(".sparkprofile") || name.endsWith(".sparkheap") || HEAP_DUMP.matcher(name).matches()) {
                return true;
            }
            if (name.equals("config.json")) {
                return isSparkConfig(readJson(path));
            }
            if (name.equals("activity.json")) {
                return isSparkActivityLog(readJson(path));
            }
            return false;
        } catch (IOException | RuntimeException e) {
            return false;
        }
    }

    /**
     * A spark config is a non-empty object containing only keys spark uses.
     */
    private static boolean isSparkConfig(JsonElement json) {
        if (json == null || !json.isJsonObject()) {
            return false;
        }

        Set<Map.Entry<String, JsonElement>> entries = json.getAsJsonObject().entrySet();
        if (entries.isEmpty()) {
            return false;
        }
        for (Map.Entry<String, JsonElement> entry : entries) {
            if (!CONFIG_KEYS.contains(entry.getKey())) {
                return false;
            }
        }
        return true;
    }

    /**
     * A spark activity log is a non-empty array of {user, time, type[, data]} objects.
     */
    private static boolean isSparkActivityLog(JsonElement json) {
        if (json == null || !json.isJsonArray() || json.getAsJsonArray().size() == 0) {
            return false;
        }

        for (JsonElement element : json.getAsJsonArray()) {
            if (!element.isJsonObject()) {
                return false;
            }

            JsonObject activity = element.getAsJsonObject();
            if (!activity.has("user") || !activity.has("time") || !activity.has("type")) {
                return false;
            }
            for (Map.Entry<String, JsonElement> entry : activity.entrySet()) {
                if (!ACTIVITY_KEYS.contains(entry.getKey())) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * A spark temporary directory contains spark's readme and nothing but temporary files.
     */
    private static boolean isSparkTempDirectory(Path directory) throws IOException {
        Path readme = directory.resolve(TEMP_README);
        if (!Files.isRegularFile(readme, LinkOption.NOFOLLOW_LINKS) || Files.size(readme) > MAX_JSON_SIZE) {
            return false;
        }
        if (!new String(Files.readAllBytes(readme), StandardCharsets.UTF_8).contains(TEMP_README_MARKER)) {
            return false;
        }

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory)) {
            for (Path path : stream) {
                String name = path.getFileName().toString();
                if (name.equals(TEMP_README)) {
                    continue;
                }
                if (!name.endsWith(".tmp") || !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
                    return false;
                }
            }
        } catch (DirectoryIteratorException e) {
            throw e.getCause();
        }
        return true;
    }

    private static JsonElement readJson(Path path) throws IOException {
        if (Files.size(path) > MAX_JSON_SIZE) {
            return null;
        }
        try (BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            return new JsonParser().parse(reader);
        }
    }
}
