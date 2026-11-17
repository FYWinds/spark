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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import cpw.mods.fml.common.Loader;
import me.lucko.spark.common.platform.serverconfig.ConfigParser;
import me.lucko.spark.common.platform.serverconfig.ExcludedConfigFilter;
import me.lucko.spark.common.platform.serverconfig.PropertiesConfigParser;
import me.lucko.spark.common.platform.serverconfig.ServerConfigProvider;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Provides {@code server.properties} and Forge's own configuration files.
 */
public class Forge1710ServerConfigProvider extends ServerConfigProvider {

    /** Forge's own configuration files, relative to the config directory */
    private static final String[] FORGE_CONFIG_FILES = {"forge.cfg", "forgeChunkLoading.cfg"};

    /** A collection of paths to be excluded from the files */
    private static final Collection<String> HIDDEN_PATHS = ImmutableSet.<String>builder()
            .addAll(BASE_HIDDEN_PATHS)
            .add("server-port")
            .add("query<dot>port")
            .add("rcon<dot>port")
            .add("resource-pack*")
            .addAll(getSystemPropertyList("spark.serverconfigs.hiddenpaths"))
            .build();

    public Forge1710ServerConfigProvider() {
        super(files(), HIDDEN_PATHS);
    }

    private static Map<String, ConfigParser> files() {
        ImmutableMap.Builder<String, ConfigParser> files = ImmutableMap.builder();
        files.put("server.properties", PropertiesConfigParser.INSTANCE);

        // the keys are shown in the viewer, so only include the files if their path relative to
        // the working directory is known (avoids exposing absolute paths)
        String configDirectory = relativeConfigDirectory();
        if (configDirectory != null) {
            for (String file : FORGE_CONFIG_FILES) {
                files.put(configDirectory + "/" + file, ForgeConfigParser.INSTANCE);
            }
        }
        return files.build();
    }

    private static String relativeConfigDirectory() {
        File configDir = Loader.instance().getConfigDir();
        if (configDir == null) {
            return null;
        }

        try {
            Path workingDirectory = new File(".").getCanonicalFile().toPath();
            Path configPath = configDir.getCanonicalFile().toPath();
            if (!configPath.startsWith(workingDirectory) || configPath.equals(workingDirectory)) {
                return null;
            }
            return workingDirectory.relativize(configPath).toString().replace(File.separatorChar, '/');
        } catch (IOException | IllegalArgumentException e) {
            return null;
        }
    }

    /**
     * A {@link ConfigParser} for the configuration format written by Forge's
     * {@code net.minecraftforge.common.config.Configuration}.
     *
     * <p>Forge's own loader cannot be reused: it creates missing files and renames files it
     * fails to parse. This parser is read-only and lenient: lines it does not understand are
     * skipped.</p>
     *
     * <pre>
     * category {
     *     B:someBoolean=true
     *     I:"quoted name"=5
     *     S:someList &lt;
     *         a
     *         b
     *      &gt;
     *     nested {
     *         D:someDouble=0.5
     *     }
     * }
     * </pre>
     */
    enum ForgeConfigParser implements ConfigParser {
        INSTANCE;

        private static final Gson GSON = new Gson();

        @Override
        public JsonElement load(String file, ExcludedConfigFilter filter) throws IOException {
            Map<String, Object> values = this.parse(Paths.get(file));
            if (values == null) {
                return null;
            }

            return filter.apply(GSON.toJsonTree(values));
        }

        @Override
        public Map<String, Object> parse(BufferedReader reader) throws IOException {
            Map<String, Object> root = new LinkedHashMap<>();
            Deque<Map<String, Object>> categories = new ArrayDeque<>();
            categories.push(root);

            List<Object> list = null;
            char listType = 0;

            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (!line.isEmpty() && line.charAt(0) == '\uFEFF') {
                    line = line.substring(1).trim();
                }

                if (list != null) {
                    if (line.equals(">")) {
                        list = null;
                    } else {
                        list.add(convert(listType, line));
                    }
                    continue;
                }

                if (line.isEmpty() || line.charAt(0) == '#' || line.charAt(0) == '~') {
                    continue;
                }

                if (line.equals("}")) {
                    if (categories.size() > 1) {
                        categories.pop();
                    }
                    continue;
                }

                // "T:name=value": the value may end with '{' or '<', so check for properties first
                char type = typeOf(line);
                String declaration = type == 0 ? line : line.substring(2);
                int separator = separatorIndex(declaration);

                if (separator > 0) {
                    if (categories.size() > 1) {
                        String name = unquote(declaration.substring(0, separator).trim());
                        categories.peek().put(name, convert(type, declaration.substring(separator + 1)));
                    }
                } else if (line.endsWith("{")) {
                    String name = unquote(line.substring(0, line.length() - 1).trim());
                    Map<String, Object> parent = categories.peek();
                    Object existing = parent.get(name);

                    Map<String, Object> category;
                    if (existing instanceof Map) {
                        //noinspection unchecked
                        category = (Map<String, Object>) existing;
                    } else {
                        category = new LinkedHashMap<>();
                        parent.put(name, category);
                    }
                    categories.push(category);
                } else if (line.endsWith("<") && categories.size() > 1) {
                    listType = type;
                    list = new ArrayList<>();
                    categories.peek().put(unquote(declaration.substring(0, declaration.length() - 1).trim()), list);
                }
            }

            return root;
        }

        /**
         * Gets the type prefix ({@code B}, {@code I}, {@code D}, {@code S}) of a property declaration.
         */
        private static char typeOf(String declaration) {
            return declaration.length() > 2 && declaration.charAt(1) == ':' ? declaration.charAt(0) : 0;
        }

        /**
         * Finds the '=' separating the property name from its value, skipping a quoted name.
         */
        private static int separatorIndex(String declaration) {
            int start = 0;
            if (declaration.startsWith("\"")) {
                start = declaration.indexOf('"', 1);
                if (start == -1) {
                    return -1;
                }
            }
            return declaration.indexOf('=', start);
        }

        private static String unquote(String name) {
            if (name.length() >= 2 && name.startsWith("\"") && name.endsWith("\"")) {
                return name.substring(1, name.length() - 1);
            }
            return name;
        }

        private static Object convert(char type, String value) {
            String trimmed = value.trim();
            try {
                switch (type) {
                    case 'B':
                        if (trimmed.equalsIgnoreCase("true") || trimmed.equalsIgnoreCase("false")) {
                            return Boolean.parseBoolean(trimmed);
                        }
                        break;
                    case 'I':
                        return Long.parseLong(trimmed);
                    case 'D':
                        double d = Double.parseDouble(trimmed);
                        if (!Double.isNaN(d) && !Double.isInfinite(d)) {
                            return d;
                        }
                        break;
                    default:
                        break;
                }
            } catch (NumberFormatException e) {
                // keep the raw value
            }
            return value;
        }
    }

}
