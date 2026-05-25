package se.afshin.yavari.kafka.editor.admin;

/**
 * A failure in an admin operation, carrying the HTTP status and an
 * {@link ApiError} {@code kind} to report to the client. Mapped to a JSON
 * response by {@link AdminExceptionMapper} — scoped to admin endpoints only,
 * so existing run endpoints are unaffected.
 */
public class AdminApiException extends RuntimeException {

    private final int status;
    private final String kind;

    public AdminApiException(int status, String kind, String message) {
        super(message);
        this.status = status;
        this.kind = kind;
    }

    public int status() {
        return status;
    }

    public String kind() {
        return kind;
    }
}
