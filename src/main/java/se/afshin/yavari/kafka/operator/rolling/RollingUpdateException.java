package se.afshin.yavari.kafka.operator.rolling;

public class RollingUpdateException extends RuntimeException {
    public RollingUpdateException(String message) {
        super(message);
    }
}
