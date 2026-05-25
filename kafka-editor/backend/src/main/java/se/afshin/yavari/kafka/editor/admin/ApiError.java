package se.afshin.yavari.kafka.editor.admin;

/**
 * A structured error body for admin endpoints. {@code kind} is a stable code
 * the front-end can branch on (AUTH, NOT_FOUND, TIMEOUT, CONFLICT, BAD_REQUEST,
 * UNSUPPORTED, NOT_CONFIGURED, INTERNAL).
 */
public record ApiError(String error, String detail, String kind) {

    public static ApiError of(String error, String kind) {
        return new ApiError(error, null, kind);
    }
}
