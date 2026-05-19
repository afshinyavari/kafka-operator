package se.afshin.yavari.kafka.operator.podset;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.PodSpec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class PodSpecHasherTest {

    private PodSpecHasher hasher;

    @BeforeEach
    void setup() {
        hasher = new PodSpecHasher();
    }

    @Test
    void hash_is16HexChars() {
        String h = hasher.hash(new PodSpec());
        assertThat(h).hasSize(16).matches("[0-9a-f]+");
    }

    @Test
    void hash_sameSpec_sameResult() {
        PodSpec a = specWithImage("kafka:4.0.0");
        PodSpec b = specWithImage("kafka:4.0.0");
        assertThat(hasher.hash(a)).isEqualTo(hasher.hash(b));
    }

    @Test
    void hash_differentSpec_differentResult() {
        PodSpec a = specWithImage("kafka:4.0.0");
        PodSpec b = specWithImage("kafka:3.9.0");
        assertThat(hasher.hash(a)).isNotEqualTo(hasher.hash(b));
    }

    @Test
    void hash_emptySpec_doesNotThrow() {
        assertThatCode(() -> hasher.hash(new PodSpec())).doesNotThrowAnyException();
    }

    // --- helpers ---

    private PodSpec specWithImage(String image) {
        Container c = new Container();
        c.setName("kafka");
        c.setImage(image);
        PodSpec spec = new PodSpec();
        spec.setContainers(List.of(c));
        return spec;
    }
}
