package se.afshin.yavari.kafka.editor.admin.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import se.afshin.yavari.kafka.editor.admin.dto.AclEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.junit.jupiter.api.Test;

/** Pure unit tests for ACL entry <-> Kafka binding mapping. */
class AclServiceTest {

    @Test
    void entryRoundTripsThroughABinding() {
        AclEntry entry = new AclEntry("TOPIC", "orders", "LITERAL",
                "User:alice", "*", "READ", "ALLOW");
        AclBinding binding = AclService.toBinding(entry);
        assertEquals(entry, AclService.toEntry(binding));
    }

    @Test
    void hostDefaultsToWildcard() {
        AclEntry entry = new AclEntry("TOPIC", "orders", "LITERAL",
                "User:bob", null, "WRITE", "ALLOW");
        assertEquals("*", AclService.toBinding(entry).entry().host());
    }

    @Test
    void patternTypeDefaultsToLiteral() {
        AclEntry entry = new AclEntry("GROUP", "g", "", "User:x", "*",
                "READ", "ALLOW");
        assertEquals("LITERAL",
                AclService.toBinding(entry).pattern().patternType().name());
    }

    @Test
    void invalidResourceTypeIsRejected() {
        AclEntry entry = new AclEntry("BOGUS", "x", "LITERAL", "User:x", "*",
                "READ", "ALLOW");
        assertThrows(IllegalArgumentException.class,
                () -> AclService.toBinding(entry));
    }
}
