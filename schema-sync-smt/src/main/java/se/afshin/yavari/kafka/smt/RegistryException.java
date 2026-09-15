package se.afshin.yavari.kafka.smt;

/** Thrown from any schema-registry call. The SMT translates these via
 *  {@code behavior.on.error}. */
public class RegistryException extends Exception {
    public RegistryException(String message) { super(message); }
    public RegistryException(String message, Throwable cause) { super(message, cause); }
}
