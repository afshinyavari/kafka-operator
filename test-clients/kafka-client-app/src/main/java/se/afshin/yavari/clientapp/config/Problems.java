package se.afshin.yavari.clientapp.config;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Collects configuration problems so startup can report all of them at once. */
public final class Problems {
    private final List<String> items = new ArrayList<>();

    public void add(String problem) { items.add(problem); }
    public boolean isEmpty() { return items.isEmpty(); }
    public List<String> list() { return Collections.unmodifiableList(items); }
    public String message() { return String.join("\n", items); }
}
