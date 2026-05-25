package se.afshin.yavari.kafka.editor.admin.dto;

import java.util.List;

/**
 * One page of browsed messages. {@code nextOffset} is where a follow-up page
 * should resume; {@code truncated} is true when more records remain unread.
 */
public record MessagePage(
        String topic,
        int partition,
        long startOffset,
        long endOffset,
        long nextOffset,
        boolean truncated,
        List<RenderedRecord> rows) {
}
