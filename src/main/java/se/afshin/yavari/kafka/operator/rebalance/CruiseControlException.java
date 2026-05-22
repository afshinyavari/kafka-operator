package se.afshin.yavari.kafka.operator.rebalance;

/**
 * Thrown by {@link HttpCruiseControlClient} on transport failures (connection refused,
 * timeout, malformed response). The state machine catches it and moves the
 * {@code KafkaRebalance} to {@code NOT_READY}.
 */
public class CruiseControlException extends RuntimeException {

    public CruiseControlException(String message) {
        super(message);
    }

    public CruiseControlException(String message, Throwable cause) {
        super(message, cause);
    }
}
