package se.afshin.yavari.clientapp.config;

import java.util.Arrays;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Environment accessor. Blank values count as absent. Parse failures go to {@link Problems}. */
public final class Env {
    private final Function<String, String> source;

    public Env(Function<String, String> source) { this.source = source; }

    public Optional<String> get(String name) {
        String v = source.apply(name);
        return (v == null || v.isBlank()) ? Optional.empty() : Optional.of(v.trim());
    }

    public String get(String name, String def) { return get(name).orElse(def); }

    /** Backward-compatible variant that discards its problems; prefer the 3-arg overload. */
    public boolean getBoolean(String name, boolean def) {
        return getBoolean(name, def, new Problems());
    }

    public boolean getBoolean(String name, boolean def, Problems problems) {
        Optional<String> v = get(name);
        if (v.isEmpty()) return def;
        String s = v.get();
        if (s.equalsIgnoreCase("true") || s.equalsIgnoreCase("yes")) return true;
        if (s.equalsIgnoreCase("false") || s.equalsIgnoreCase("no")) return false;
        problems.add(name + " must be true or false, got '" + s + "'");
        return def;
    }

    public long getLong(String name, long def, Problems problems) {
        Optional<String> v = get(name);
        if (v.isEmpty()) return def;
        try {
            return Long.parseLong(v.get());
        } catch (NumberFormatException e) {
            problems.add(name + " must be a number, got '" + v.get() + "'");
            return def;
        }
    }

    public <E extends Enum<E>> E getEnum(String name, Class<E> type, E def, Problems problems) {
        Optional<String> v = get(name);
        if (v.isEmpty()) return def;
        for (E c : type.getEnumConstants()) {
            if (c.name().equalsIgnoreCase(v.get())) return c;
        }
        String allowed = Arrays.stream(type.getEnumConstants()).map(Enum::name).collect(Collectors.joining(", "));
        problems.add(name + " must be one of " + allowed + ", got '" + v.get() + "'");
        return def;
    }

    /** Returns the value, or records "<name> is required" and returns null. */
    public String require(String name, Problems problems) {
        Optional<String> v = get(name);
        if (v.isEmpty()) {
            problems.add(name + " is required");
            return null;
        }
        return v.get();
    }
}
