package se.afshin.yavari.kafka.smt;

/**
 * Thrown from any schema-registry call. The {@link Kind} decides how the SMT reacts:
 * <ul>
 *   <li>{@link Kind#NOT_FOUND} — the source registry has no schema for the id in the
 *       record. Usually a payload that merely starts with {@code 0x00}. Routed through
 *       {@code behavior.on.error} and remembered by the negative cache.
 *   <li>{@link Kind#TRANSIENT} — network failure, timeout, 408/429/5xx. Retrying later
 *       may succeed. Always re-thrown as a Connect {@code RetriableException}; never
 *       cached and never passed through, since a passthrough would ship the record with
 *       an id that means something else on the target.
 *   <li>{@link Kind#PERMANENT} — everything else (4xx other than 404 on the id lookup,
 *       incompatible schema, auth rejected after a refresh, reference cycle, malformed
 *       response). Re-thrown as {@code ConnectException}; retrying won't help.
 * </ul>
 */
public class RegistryException extends Exception {

    public enum Kind { NOT_FOUND, TRANSIENT, PERMANENT }

    private final Kind kind;

    public RegistryException(String message) { this(Kind.PERMANENT, message, null); }

    public RegistryException(String message, Throwable cause) { this(Kind.PERMANENT, message, cause); }

    public RegistryException(Kind kind, String message) { this(kind, message, null); }

    public RegistryException(Kind kind, String message, Throwable cause) {
        super(message, cause);
        this.kind = kind == null ? Kind.PERMANENT : kind;
    }

    public Kind kind() { return kind; }

    public boolean isNotFound() { return kind == Kind.NOT_FOUND; }

    public boolean isTransient() { return kind == Kind.TRANSIENT; }

    public static RegistryException notFound(String message) {
        return new RegistryException(Kind.NOT_FOUND, message);
    }

    public static RegistryException transientError(String message, Throwable cause) {
        return new RegistryException(Kind.TRANSIENT, message, cause);
    }

    /** Classifies an HTTP failure status: 408, 429 and 5xx are transient, the rest permanent. */
    public static Kind kindForStatus(int status) {
        return status == 408 || status == 429 || status >= 500 ? Kind.TRANSIENT : Kind.PERMANENT;
    }

    public static RegistryException forStatus(int status, String message) {
        return new RegistryException(kindForStatus(status), message);
    }
}
