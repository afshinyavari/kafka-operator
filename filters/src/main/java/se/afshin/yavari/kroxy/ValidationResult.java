package se.afshin.yavari.kroxy;

public class ValidationResult {
    private final boolean valid;
    private final String errorMessage;

    private ValidationResult(boolean valid, String errorMessage) {
        this.valid = valid;
        this.errorMessage = errorMessage;
    }

    public static ValidationResult valid() {
        return new ValidationResult(true, null);
    }

    public static ValidationResult invalid(String errorMessage) {
        return new ValidationResult(false, errorMessage);
    }

    public boolean isValid() { return valid; }
    public String getErrorMessage() { return errorMessage; }
}
