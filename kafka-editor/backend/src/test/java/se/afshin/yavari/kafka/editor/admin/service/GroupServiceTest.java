package se.afshin.yavari.kafka.editor.admin.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

/** Pure unit tests for consumer-lag computation. */
class GroupServiceTest {

    @Test
    void lagIsEndMinusCommitted() {
        assertEquals(5L, GroupService.lagOf(10L, 15L));
    }

    @Test
    void lagIsNullWhenThereIsNoCommit() {
        assertNull(GroupService.lagOf(null, 15L));
    }

    @Test
    void lagClampsANegativeValueToZero() {
        assertEquals(0L, GroupService.lagOf(20L, 15L));
    }

    @Test
    void lagIsZeroWhenCaughtUp() {
        assertEquals(0L, GroupService.lagOf(15L, 15L));
    }
}
