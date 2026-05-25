package se.afshin.yavari.kafka.operator.connect;

/** Thrown by {@link ConnectRestClient} on a non-2xx response or transport error. The
 *  reconciler pattern-matches on {@link #httpStatus()} to decide reschedule cadence. */
public class ConnectRestException extends RuntimeException {

    private final int httpStatus;

    public ConnectRestException(int httpStatus, String message) {
        super(message);
        this.httpStatus = httpStatus;
    }

    public ConnectRestException(int httpStatus, String message, Throwable cause) {
        super(message, cause);
        this.httpStatus = httpStatus;
    }

    /** HTTP status code, or 0 for transport-level errors (connect refused, timeout). */
    public int httpStatus() { return httpStatus; }
}
