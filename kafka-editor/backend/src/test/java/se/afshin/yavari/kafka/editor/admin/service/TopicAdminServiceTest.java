package se.afshin.yavari.kafka.editor.admin.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import se.afshin.yavari.kafka.editor.admin.AdminApiException;
import org.junit.jupiter.api.Test;

/** Pure unit tests for topic-name validation — no broker needed. */
class TopicAdminServiceTest {

    @Test
    void acceptsValidNames() {
        assertDoesNotThrow(
                () -> TopicAdminService.validateTopicName("orders.v1_2-test"));
        assertDoesNotThrow(() -> TopicAdminService.validateTopicName("a"));
    }

    @Test
    void rejectsBlankNames() {
        assertThrows(AdminApiException.class,
                () -> TopicAdminService.validateTopicName(""));
        assertThrows(AdminApiException.class,
                () -> TopicAdminService.validateTopicName(null));
    }

    @Test
    void rejectsIllegalCharacters() {
        assertThrows(AdminApiException.class,
                () -> TopicAdminService.validateTopicName("bad topic"));
        assertThrows(AdminApiException.class,
                () -> TopicAdminService.validateTopicName("bad/topic"));
    }

    @Test
    void rejectsOverlongNames() {
        assertThrows(AdminApiException.class,
                () -> TopicAdminService.validateTopicName("a".repeat(250)));
    }

    @Test
    void rejectsDotNames() {
        assertThrows(AdminApiException.class,
                () -> TopicAdminService.validateTopicName("."));
        assertThrows(AdminApiException.class,
                () -> TopicAdminService.validateTopicName(".."));
    }
}
