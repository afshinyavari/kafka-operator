package se.afshin.yavari.kafka.operator.externalaccess;

import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.Ingress;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HttpIngressBuilderTest {

    private static final String NAME = "kafka-ui";
    private static final String NS = "kafka";
    private static final Map<String, String> LABELS = Map.of("app", NAME);

    private final HttpIngressBuilder builder = new HttpIngressBuilder();

    @Test
    void buildsRuleHostAndBackend() {
        HttpIngressConfig cfg = new HttpIngressConfig();
        cfg.setIngressClassName("nginx");

        Ingress ing = builder.build(NAME, NS, LABELS, ownerRef(),
                "kafka-ui.example.com", NAME, 8080, cfg);

        assertThat(ing.getMetadata().getName()).isEqualTo(NAME);
        assertThat(ing.getSpec().getIngressClassName()).isEqualTo("nginx");
        assertThat(ing.getSpec().getRules()).hasSize(1);
        var rule = ing.getSpec().getRules().get(0);
        assertThat(rule.getHost()).isEqualTo("kafka-ui.example.com");
        var path = rule.getHttp().getPaths().get(0);
        assertThat(path.getPath()).isEqualTo("/");
        assertThat(path.getBackend().getService().getName()).isEqualTo(NAME);
        assertThat(path.getBackend().getService().getPort().getNumber()).isEqualTo(8080);
        // No TLS block when tlsSecretRef is unset.
        assertThat(ing.getSpec().getTls()).isNullOrEmpty();
    }

    @Test
    void withTlsSecret_addsTlsBlock() {
        HttpIngressConfig cfg = new HttpIngressConfig();
        cfg.setTlsSecretRef("kafka-ui-tls");

        Ingress ing = builder.build(NAME, NS, LABELS, ownerRef(),
                "kafka-ui.example.com", NAME, 8080, cfg);

        assertThat(ing.getSpec().getTls()).hasSize(1);
        var tls = ing.getSpec().getTls().get(0);
        assertThat(tls.getSecretName()).isEqualTo("kafka-ui-tls");
        assertThat(tls.getHosts()).containsExactly("kafka-ui.example.com");
    }

    @Test
    void noConfig_stillBuildsWithDefaults() {
        Ingress ing = builder.build(NAME, NS, LABELS, ownerRef(),
                "kafka-ui.example.com", NAME, 8080, null);
        assertThat(ing.getSpec().getIngressClassName()).isNull();
    }

    @Test
    void customAnnotations_areApplied() {
        HttpIngressConfig cfg = new HttpIngressConfig();
        cfg.setAnnotations(Map.of("nginx.ingress.kubernetes.io/rewrite-target", "/"));

        Ingress ing = builder.build(NAME, NS, LABELS, ownerRef(),
                "kafka-ui.example.com", NAME, 8080, cfg);
        assertThat(ing.getMetadata().getAnnotations())
                .containsEntry("nginx.ingress.kubernetes.io/rewrite-target", "/");
    }

    @Test
    void blankHost_throws() {
        assertThatThrownBy(() -> builder.build(NAME, NS, LABELS, ownerRef(), "", NAME, 8080, null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("advertised host");
    }

    private io.fabric8.kubernetes.api.model.OwnerReference ownerRef() {
        return new OwnerReferenceBuilder()
                .withApiVersion("kafka.yavari.afshin.se/v1alpha1")
                .withKind("KafkaUI").withName(NAME).withUid("u").withController(true).build();
    }
}
