package se.afshin.yavari.kafka.editor.admin.dto;

/** One topic configuration entry. */
public record ConfigKv(
        String name,
        String value,
        boolean isDefault,
        boolean sensitive,
        boolean readOnly) {
}
