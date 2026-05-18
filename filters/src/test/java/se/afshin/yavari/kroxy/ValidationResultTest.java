package se.afshin.yavari.kroxy;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ValidationResultTest {

    @Test
    void validResultHasNoMessage() {
        ValidationResult r = ValidationResult.valid();
        assertThat(r.isValid()).isTrue();
        assertThat(r.getErrorMessage()).isNull();
    }

    @Test
    void invalidResultCarriesMessage() {
        ValidationResult r = ValidationResult.invalid("bad xml");
        assertThat(r.isValid()).isFalse();
        assertThat(r.getErrorMessage()).isEqualTo("bad xml");
    }
}
