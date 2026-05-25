package se.afshin.yavari.kafka.editor.model;

/**
 * Windowing for an aggregation. `type` is none / tumbling / hopping / sliding /
 * session; `sizeMs` is the window size (or session gap).
 */
public record WindowConfig(
        String type,
        long sizeMs,
        Long advanceMs,
        Long graceMs) {
}
