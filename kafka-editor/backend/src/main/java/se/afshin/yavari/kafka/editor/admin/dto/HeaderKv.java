package se.afshin.yavari.kafka.editor.admin.dto;

/** One record header, value rendered as UTF-8 text. */
public record HeaderKv(String key, String value) {
}
