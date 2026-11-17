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
import cpw.mods.fml.common.InjectedModContainer;
import cpw.mods.fml.common.Loader;
import cpw.mods.fml.common.ModContainer;
import me.lucko.spark.common.sampler.source.ClassSourceLookup;
import me.lucko.spark.common.util.classfinder.ClassFinder;

import java.io.File;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.security.CodeSource;
import java.security.ProtectionDomain;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Identifies the mod a class (or a Mixin-merged method) comes from.
 *
 * <p>On 1.7.10 every mod class is defined by the LaunchClassLoader, which records the
 * resource URL of the class file ({@code jar:file:/.../mods/foo.jar!/foo/Bar.class}) as the
 * class' code source. The jar/directory part of that URL is mapped to the mod(s) FML loaded
 * from it ({@link ModContainer#getSource()}). Jars inside the mods directory without a mod
 * container (coremods, tweakers, libraries) are identified by their file name. Classes of
 * Minecraft, Forge and FML and of libraries outside the mods directory are not attributed.</p>
 *
 * <p>If SpongePowered Mixin is installed, methods merged into a class by a mixin carry a
 * runtime-visible {@code @MixinMerged} annotation naming the mixin class; such methods are
 * attributed to the mod whose jar contains that mixin class. Mixin is only accessed through
 * reflection, so this works (as a no-op) when it is absent.</p>
 */
public class Forge1710ClassSourceLookup implements ClassSourceLookup {

    /** Mod ids of the containers FML injects for Minecraft, MCP, FML and Forge themselves. */
    public static final Set<String> BUILTIN_MOD_IDS = ImmutableSet.of("Minecraft", "mcp", "FML", "Forge");

    private static final String MIXIN_MERGED_ANNOTATION = "org.spongepowered.asm.mixin.transformer.meta.MixinMerged";
    private static final String MIXIN_MERGED_RESOURCE = MIXIN_MERGED_ANNOTATION.replace('.', '/') + ".class";

    /** Mods by the canonical path of the jar/directory they were loaded from */
    private final Map<Path, JarMods> modsBySource;
    /** Canonical path of the mods directory, or null if unknown */
    private final Path modsDirectory;
    /** Class loader used to locate the class files of mixins */
    private final ClassLoader resourceLoader;
    /** Supplies the class finder used to resolve the classes of method calls, created on first use */
    private final Supplier<ClassFinder> classFinderSupplier;
    private final boolean mixinPresent;

    private ClassFinder classFinder;
    private final Map<String, Optional<Path>> canonicalPaths = new ConcurrentHashMap<>();
    private final Map<Class<?>, MergedMethods> mergedMethodsByClass = new ConcurrentHashMap<>();
    private final Map<String, Optional<String>> mixinSources = new ConcurrentHashMap<>();
    private final Map<Class<?>, Method> mixinAttributes = new ConcurrentHashMap<>();

    public Forge1710ClassSourceLookup(Supplier<ClassFinder> classFinderSupplier) {
        this.classFinderSupplier = classFinderSupplier;
        this.resourceLoader = Forge1710ClassSourceLookup.class.getClassLoader();
        this.mixinPresent = this.resourceLoader != null && this.resourceLoader.getResource(MIXIN_MERGED_RESOURCE) != null;
        this.modsDirectory = findModsDirectory();
        this.modsBySource = indexModSources(Loader.instance().getModList());
    }

    // ---------------------------------------------------------------------------------------
    // classes
    // ---------------------------------------------------------------------------------------

    @Override
    public String identify(Class<?> clazz) {
        try {
            ProtectionDomain protectionDomain = clazz.getProtectionDomain();
            CodeSource codeSource = protectionDomain == null ? null : protectionDomain.getCodeSource();
            URL location = codeSource == null ? null : codeSource.getLocation();
            if (location == null) {
                return null;
            }
            return identifyLocation(location, clazz.getName());
        } catch (Exception | LinkageError e) {
            return null;
        }
    }

    /**
     * Identifies the mod owning the class file at the given location.
     *
     * @param location the code source or resource URL of the class
     * @param className the binary name of the class
     * @return the source, or null
     */
    private String identifyLocation(URL location, String className) {
        Path root = classpathRoot(location, className);
        if (root == null) {
            return null;
        }

        JarMods mods = this.modsBySource.get(root);
        if (mods != null) {
            return mods.select(className);
        }

        if (this.modsDirectory != null && root.startsWith(this.modsDirectory) && !root.equals(this.modsDirectory)) {
            return nameFromFile(root);
        }
        return null;
    }

    /**
     * Resolves the canonical path of the jar or directory a class was loaded from.
     *
     * <p>The LaunchClassLoader records the URL of the class file itself rather than of the
     * classpath entry, so both {@code jar:<file url>!/<entry>} and plain {@code file:}
     * URLs pointing at either a jar or a class file inside a directory are handled.</p>
     */
    private Path classpathRoot(URL location, String className) {
        String protocol = location.getProtocol();
        String root;

        if ("jar".equals(protocol)) {
            String spec = location.getPath();
            int separator = spec.indexOf("!/");
            root = separator == -1 ? spec : spec.substring(0, separator);
        } else if ("file".equals(protocol)) {
            root = location.toExternalForm();
            if (root.endsWith(".class")) {
                // strip "<package path>/<class name>.class"
                int end = root.length();
                for (int i = packageDepth(className); i >= 0 && end > 0; i--) {
                    end = root.lastIndexOf('/', end - 1);
                }
                if (end <= 0) {
                    return null;
                }
                root = root.substring(0, end);
            }
        } else {
            return null;
        }

        Optional<Path> cached = this.canonicalPaths.get(root);
        if (cached == null) {
            cached = Optional.ofNullable(resolveFileUrl(root));
            this.canonicalPaths.put(root, cached);
        }
        return cached.orElse(null);
    }

    private static Path resolveFileUrl(String fileUrl) {
        URL url;
        try {
            url = new URL(fileUrl);
        } catch (MalformedURLException e) {
            return null;
        }
        if (!"file".equals(url.getProtocol())) {
            return null;
        }

        File file;
        try {
            file = new File(url.toURI());
        } catch (URISyntaxException | IllegalArgumentException e) {
            // not a valid URI (e.g. unencoded spaces): use the raw path, minus the slash before a drive letter
            String path = url.getPath();
            if (path.length() > 2 && path.charAt(0) == '/' && path.charAt(2) == ':') {
                path = path.substring(1);
            }
            file = new File(path);
        }
        return canonicalPath(file);
    }

    private static int packageDepth(String className) {
        int depth = 0;
        for (int i = 0; i < className.length(); i++) {
            if (className.charAt(i) == '.') {
                depth++;
            }
        }
        return depth;
    }

    private static String nameFromFile(Path path) {
        Path fileName = path.getFileName();
        if (fileName == null) {
            return null;
        }

        String name = fileName.toString();
        String lowerCase = name.toLowerCase(Locale.ROOT);
        if (lowerCase.endsWith(".jar") || lowerCase.endsWith(".zip")) {
            name = name.substring(0, name.length() - 4);
        }
        return name.isEmpty() ? null : name;
    }

    // ---------------------------------------------------------------------------------------
    // mixin-merged methods
    // ---------------------------------------------------------------------------------------

    @Override
    public boolean supportsIdentifyingMethodCalls() {
        return this.mixinPresent;
    }

    @Override
    public String identify(MethodCall methodCall) {
        MergedMethods merged = mergedMethods(methodCall.getClassName(), methodCall.getMethodName());
        if (merged == null) {
            return null;
        }
        return identifyMixin(merged.byNameAndDescriptor.get(methodCall.getMethodName() + methodCall.getMethodDescriptor()));
    }

    @Override
    public String identify(MethodCallByLine methodCall) {
        // line numbers cannot be resolved through reflection, so only attribute the frame if
        // every method with that name in the class was merged from the same mixin
        MergedMethods merged = mergedMethods(methodCall.getClassName(), methodCall.getMethodName());
        if (merged == null) {
            return null;
        }
        return identifyMixin(merged.byName.get(methodCall.getMethodName()));
    }

    private MergedMethods mergedMethods(String className, String methodName) {
        if (!this.mixinPresent || "native".equals(className) || "<init>".equals(methodName) || "<clinit>".equals(methodName)) {
            return null;
        }

        try {
            if (this.classFinder == null) {
                this.classFinder = this.classFinderSupplier.get();
            }

            Class<?> clazz = this.classFinder.findClass(className);
            if (clazz == null || clazz.getClassLoader() == null) {
                return null;
            }

            MergedMethods merged = this.mergedMethodsByClass.get(clazz);
            if (merged == null) {
                merged = scanMergedMethods(clazz);
                this.mergedMethodsByClass.put(clazz, merged);
            }
            return merged;
        } catch (Exception | LinkageError e) {
            return null;
        }
    }

    private MergedMethods scanMergedMethods(Class<?> clazz) throws ReflectiveOperationException {
        Map<String, String> byNameAndDescriptor = new HashMap<>();
        Map<String, String> byName = new HashMap<>();
        Set<String> ambiguousNames = new HashSet<>();

        for (Method method : clazz.getDeclaredMethods()) {
            String name = method.getName();
            String mixin = mixinOf(method);

            if (mixin == null) {
                ambiguousNames.add(name);
                continue;
            }

            byNameAndDescriptor.put(name + methodDescriptor(method), mixin);
            String previous = byName.put(name, mixin);
            if (previous != null && !previous.equals(mixin)) {
                ambiguousNames.add(name);
            }
        }

        byName.keySet().removeAll(ambiguousNames);
        return byNameAndDescriptor.isEmpty() ? MergedMethods.NONE : new MergedMethods(byNameAndDescriptor, byName);
    }

    /**
     * Gets the name of the mixin which merged the given method into its class, if any.
     *
     * <p>The annotation is matched by name so that no Mixin class needs to be linked.</p>
     */
    private String mixinOf(Method method) throws ReflectiveOperationException {
        for (Annotation annotation : method.getDeclaredAnnotations()) {
            Class<? extends Annotation> type = annotation.annotationType();
            if (!type.getName().equals(MIXIN_MERGED_ANNOTATION)) {
                continue;
            }

            Method attribute = this.mixinAttributes.get(type);
            if (attribute == null) {
                attribute = type.getMethod("mixin");
                this.mixinAttributes.put(type, attribute);
            }

            Object mixin = attribute.invoke(annotation);
            return mixin instanceof String ? (String) mixin : null;
        }
        return null;
    }

    /**
     * Identifies the mod owning the given mixin class, by locating its class file.
     */
    private String identifyMixin(String mixinClassName) {
        if (mixinClassName == null || mixinClassName.isEmpty()) {
            return null;
        }

        Optional<String> cached = this.mixinSources.get(mixinClassName);
        if (cached == null) {
            String source = null;
            try {
                URL resource = this.resourceLoader.getResource(mixinClassName.replace('.', '/') + ".class");
                if (resource != null) {
                    source = identifyLocation(resource, mixinClassName);
                }
            } catch (Exception | LinkageError e) {
                // ignore
            }
            cached = Optional.ofNullable(source);
            this.mixinSources.put(mixinClassName, cached);
        }
        return cached.orElse(null);
    }

    private static String methodDescriptor(Method method) {
        StringBuilder descriptor = new StringBuilder("(");
        for (Class<?> parameterType : method.getParameterTypes()) {
            appendDescriptor(descriptor, parameterType);
        }
        descriptor.append(')');
        appendDescriptor(descriptor, method.getReturnType());
        return descriptor.toString();
    }

    private static void appendDescriptor(StringBuilder descriptor, Class<?> type) {
        while (type.isArray()) {
            descriptor.append('[');
            type = type.getComponentType();
        }

        if (!type.isPrimitive()) {
            descriptor.append('L').append(type.getName().replace('.', '/')).append(';');
        } else if (type == void.class) {
            descriptor.append('V');
        } else if (type == boolean.class) {
            descriptor.append('Z');
        } else if (type == byte.class) {
            descriptor.append('B');
        } else if (type == char.class) {
            descriptor.append('C');
        } else if (type == short.class) {
            descriptor.append('S');
        } else if (type == int.class) {
            descriptor.append('I');
        } else if (type == long.class) {
            descriptor.append('J');
        } else if (type == float.class) {
            descriptor.append('F');
        } else {
            descriptor.append('D');
        }
    }

    private static final class MergedMethods {
        static final MergedMethods NONE = new MergedMethods(Collections.emptyMap(), Collections.emptyMap());

        /** name + descriptor -> mixin class name */
        final Map<String, String> byNameAndDescriptor;
        /** name -> mixin class name, only for names whose methods were all merged from one mixin */
        final Map<String, String> byName;

        MergedMethods(Map<String, String> byNameAndDescriptor, Map<String, String> byName) {
            this.byNameAndDescriptor = byNameAndDescriptor;
            this.byName = byName;
        }
    }

    // ---------------------------------------------------------------------------------------
    // mod index
    // ---------------------------------------------------------------------------------------

    private static Path findModsDirectory() {
        File configDir = Loader.instance().getConfigDir();
        File gameDir = configDir == null ? null : configDir.getParentFile();
        return gameDir == null ? null : canonicalPath(new File(gameDir, "mods"));
    }

    private static Map<Path, JarMods> indexModSources(List<ModContainer> mods) {
        Map<Path, JarMods> index = new HashMap<>();
        Set<Path> builtinSources = new HashSet<>();

        for (ModContainer mod : mods) {
            try {
                indexModSource(mod, index, builtinSources);
            } catch (RuntimeException | LinkageError e) {
                // skip containers that cannot describe themselves
            }
        }

        // never attribute the Forge/FML jar to a mod, even if another container claims it
        index.keySet().removeAll(builtinSources);
        return index;
    }

    private static void indexModSource(ModContainer mod, Map<Path, JarMods> index, Set<Path> builtinSources) {
        String modId = mod.getModId();
        File source = mod.getSource();
        if (modId == null || source == null) {
            return;
        }

        // InjectedModContainer substitutes "minecraft.jar" for containers without a source
        if (mod instanceof InjectedModContainer && ((InjectedModContainer) mod).wrappedContainer.getSource() == null) {
            return;
        }

        Path path = source.exists() ? canonicalPath(source) : null;
        if (path == null) {
            return;
        }

        if (BUILTIN_MOD_IDS.contains(modId)) {
            builtinSources.add(path);
            return;
        }

        JarMods jarMods = index.get(path);
        if (jarMods == null) {
            jarMods = new JarMods();
            index.put(path, jarMods);
        }
        jarMods.add(modId, anchorPackage(mod));
    }

    /**
     * Gets the package of the class representing the mod, used to tell apart the mods of a
     * jar containing more than one mod.
     */
    private static String anchorPackage(ModContainer mod) {
        Class<?> anchor = null;

        Object instance = mod.getMod();
        if (instance != null) {
            anchor = instance.getClass();
        } else if (mod instanceof InjectedModContainer) {
            // coremod containers: the container class is part of the coremod itself
            anchor = ((InjectedModContainer) mod).wrappedContainer.getClass();
        }

        if (anchor == null || anchor.getName().startsWith("cpw.mods.fml.") || anchor.getName().startsWith("net.minecraftforge.")) {
            return null;
        }
        return packageOf(anchor.getName());
    }

    private static String packageOf(String className) {
        int lastDot = className.lastIndexOf('.');
        return lastDot == -1 ? "" : className.substring(0, lastDot);
    }

    private static Path canonicalPath(File file) {
        try {
            return file.getCanonicalFile().toPath();
        } catch (IOException | InvalidPathException | SecurityException e) {
            try {
                return file.getAbsoluteFile().toPath().normalize();
            } catch (InvalidPathException ex) {
                return null;
            }
        }
    }

    /**
     * The mod(s) loaded from a single jar or directory.
     */
    private static final class JarMods {
        private final List<String> ids = new ArrayList<>(1);
        private final List<String> anchors = new ArrayList<>(1);

        void add(String id, String anchorPackage) {
            // keep mods with a known package first, they are the better fallback
            if (anchorPackage != null) {
                int index = 0;
                while (index < this.anchors.size() && this.anchors.get(index) != null) {
                    index++;
                }
                this.ids.add(index, id);
                this.anchors.add(index, anchorPackage);
            } else {
                this.ids.add(id);
                this.anchors.add(null);
            }
        }

        /**
         * Selects the mod a class belongs to: the mod whose package shares the most leading
         * segments with the package of the class, or the first mod of the jar.
         */
        String select(String className) {
            if (this.ids.size() == 1) {
                return this.ids.get(0);
            }

            String classPackage = packageOf(className);
            String best = this.ids.get(0);
            int bestScore = 0;

            for (int i = 0; i < this.ids.size(); i++) {
                String anchor = this.anchors.get(i);
                if (anchor == null) {
                    continue;
                }

                int score = commonSegments(classPackage, anchor);
                if (score > bestScore) {
                    best = this.ids.get(i);
                    bestScore = score;
                }
            }
            return best;
        }

        private static int commonSegments(String a, String b) {
            int segments = 0;
            int length = Math.min(a.length(), b.length());
            int i = 0;
            for (; i < length; i++) {
                char c = a.charAt(i);
                if (c != b.charAt(i)) {
                    return segments;
                }
                if (c == '.') {
                    segments++;
                }
            }

            // the shorter string ended on a segment boundary
            boolean aEnds = i == a.length() || a.charAt(i) == '.';
            boolean bEnds = i == b.length() || b.charAt(i) == '.';
            return aEnds && bEnds && i > 0 ? segments + 1 : segments;
        }
    }
}
