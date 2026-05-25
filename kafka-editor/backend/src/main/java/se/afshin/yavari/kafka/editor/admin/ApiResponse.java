package se.afshin.yavari.kafka.editor.admin;

/**
 * Uniform envelope for admin endpoints. {@code capability} lets the UI tell a
 * genuine error apart from a feature the broker simply does not offer (ACLs,
 * Kafka Connect): {@code supported}, {@code unsupported}, {@code not_configured}.
 * Hard failures (unreachable, timeout) instead throw and become an HTTP error
 * with an {@link ApiError} body.
 */
public record ApiResponse<T>(T data, String capability, ApiError error) {

    public static <T> ApiResponse<T> ok(T data) {
        return new ApiResponse<>(data, "supported", null);
    }

    public static <T> ApiResponse<T> unsupported(String message) {
        return new ApiResponse<>(null, "unsupported",
                ApiError.of(message, "UNSUPPORTED"));
    }

    public static <T> ApiResponse<T> notConfigured(String message) {
        return new ApiResponse<>(null, "not_configured",
                ApiError.of(message, "NOT_CONFIGURED"));
    }
}
