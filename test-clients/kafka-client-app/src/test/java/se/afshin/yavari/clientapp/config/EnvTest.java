package se.afshin.yavari.clientapp.config;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class EnvTest {

    private static Env env(Map<String, String> m) { return new Env(m::get); }

    @Test
    void blankIsAbsent() {
        Env e = env(Map.of("A", "  "));
        assertTrue(e.get("A").isEmpty());
        assertEquals("d", e.get("A", "d"));
    }

    @Test
    void booleanParsesCaseInsensitive() {
        Env e = env(Map.of("A", "TRUE", "B", "no"));
        assertTrue(e.getBoolean("A", false));
        assertFalse(e.getBoolean("B", true));
        assertTrue(e.getBoolean("MISSING", true));
    }

    @Test
    void validatingBooleanParsesCaseInsensitiveAndRecordsProblemOnGarbage() {
        Problems p = new Problems();
        Env e = env(Map.of("A", "TRUE", "B", "no", "C", "maybe"));
        assertTrue(e.getBoolean("A", false, p));
        assertFalse(e.getBoolean("B", true, p));
        assertTrue(e.getBoolean("MISSING", true, p));
        assertEquals(0, p.list().size());
        assertTrue(e.getBoolean("C", true, p));
        assertEquals(1, p.list().size());
        assertEquals("C must be true or false, got 'maybe'", p.list().get(0));
    }

    @Test
    void longRecordsProblemOnGarbage() {
        Problems p = new Problems();
        Env e = env(Map.of("A", "12x"));
        assertEquals(5L, e.getLong("A", 5L, p));
        assertEquals(1, p.list().size());
        assertTrue(p.list().get(0).contains("A"));
    }

    @Test
    void enumIsCaseInsensitiveAndRecordsProblem() {
        enum Color { RED, BLUE }
        Problems p = new Problems();
        Env e = env(Map.of("A", "blue", "B", "green"));
        assertEquals(Color.BLUE, e.getEnum("A", Color.class, Color.RED, p));
        assertEquals(Color.RED, e.getEnum("B", Color.class, Color.RED, p));
        assertEquals(Color.RED, e.getEnum("MISSING", Color.class, Color.RED, p));
        assertEquals(1, p.list().size());
        assertTrue(p.list().get(0).contains("B") && p.list().get(0).contains("RED, BLUE"));
    }

    @Test
    void requireRecordsProblemAndReturnsNull() {
        Problems p = new Problems();
        Env e = env(Map.of());
        assertNull(e.require("A", p));
        assertEquals("A is required", p.list().get(0));
        assertEquals("A is required", p.message());
    }
}
