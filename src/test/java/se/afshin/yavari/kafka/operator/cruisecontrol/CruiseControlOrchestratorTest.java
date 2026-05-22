package se.afshin.yavari.kafka.operator.cruisecontrol;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.CruiseControlApiSecurity;
import se.afshin.yavari.kafka.operator.crd.KafkaCluster;
import se.afshin.yavari.kafka.operator.crd.KafkaClusterCruiseControlSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaClusterSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaProxyMtlsConfig;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class CruiseControlOrchestratorTest {

    // ── resolveCcCertSecret ──────────────────────────────────────────────────

    @Test
    void resolveCertSecretFallsBackToOperatorAdminCert() {
        KafkaCluster cr = cluster(new KafkaClusterCruiseControlSpec(), new KafkaProxyMtlsConfig());
        assertThat(CruiseControlOrchestrator.resolveCcCertSecret(cr))
                .isEqualTo(KafkaProxyMtlsConfig.DEFAULT_ADMIN_CLIENT_CERT_SECRET);
    }

    @Test
    void resolveCertSecretUsesExplicitRefWhenSet() {
        KafkaClusterCruiseControlSpec cc = new KafkaClusterCruiseControlSpec();
        cc.setBrokerClientCertSecretRef("dedicated-cc-tls");
        assertThat(CruiseControlOrchestrator.resolveCcCertSecret(cluster(cc, new KafkaProxyMtlsConfig())))
                .isEqualTo("dedicated-cc-tls");
    }

    @Test
    void resolveCertSecretNullWhenNoMtls() {
        assertThat(CruiseControlOrchestrator.resolveCcCertSecret(
                cluster(new KafkaClusterCruiseControlSpec(), null))).isNull();
    }

    // ── referencesSecret ─────────────────────────────────────────────────────

    @Test
    void referencesSecretMatchesClientCert() {
        KafkaCluster cr = cluster(new KafkaClusterCruiseControlSpec(), new KafkaProxyMtlsConfig());
        assertThat(CruiseControlOrchestrator.referencesSecret(cr,
                KafkaProxyMtlsConfig.DEFAULT_ADMIN_CLIENT_CERT_SECRET)).isTrue();
        assertThat(CruiseControlOrchestrator.referencesSecret(cr, "unrelated")).isFalse();
    }

    @Test
    void referencesSecretMatchesBasicAuthSecret() {
        KafkaClusterCruiseControlSpec cc = new KafkaClusterCruiseControlSpec();
        CruiseControlApiSecurity api = new CruiseControlApiSecurity();
        api.setEnabled(true);
        api.setBasicAuthSecretRef("cc-auth");
        cc.setApiSecurity(api);
        assertThat(CruiseControlOrchestrator.referencesSecret(cluster(cc, null), "cc-auth")).isTrue();
    }

    @Test
    void referencesSecretFalseWhenCruiseControlDisabled() {
        assertThat(CruiseControlOrchestrator.referencesSecret(cluster(null, null), "any")).isFalse();
    }

    // ── rollWillHappen ───────────────────────────────────────────────────────

    @Test
    void rollWhenNoExistingDeployment() {
        assertThat(CruiseControlOrchestrator.rollWillHappen(null, "img:1", "hash1")).isTrue();
    }

    @Test
    void rollWhenImageChanges() {
        Deployment existing = deployment("img:1", "hash1");
        assertThat(CruiseControlOrchestrator.rollWillHappen(existing, "img:2", "hash1")).isTrue();
    }

    @Test
    void rollWhenConfigHashChanges() {
        Deployment existing = deployment("img:1", "hash1");
        assertThat(CruiseControlOrchestrator.rollWillHappen(existing, "img:1", "hash2")).isTrue();
    }

    @Test
    void noRollWhenImageAndHashUnchanged() {
        Deployment existing = deployment("img:1", "hash1");
        assertThat(CruiseControlOrchestrator.rollWillHappen(existing, "img:1", "hash1")).isFalse();
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private static KafkaCluster cluster(KafkaClusterCruiseControlSpec cc, KafkaProxyMtlsConfig mtls) {
        KafkaCluster cr = new KafkaCluster();
        cr.setMetadata(new ObjectMetaBuilder().withName("my-kafka").build());
        KafkaClusterSpec spec = new KafkaClusterSpec();
        spec.setCruiseControl(cc);
        spec.setProxyMtls(mtls);
        cr.setSpec(spec);
        return cr;
    }

    private static Deployment deployment(String image, String configHash) {
        return new DeploymentBuilder()
                .withNewSpec()
                    .withNewTemplate()
                        .withNewMetadata()
                            .withAnnotations(Map.of(
                                    CruiseControlDeploymentBuilder.CONFIG_HASH_ANNOTATION, configHash))
                        .endMetadata()
                        .withNewSpec()
                            .addNewContainer().withName("cruise-control").withImage(image).endContainer()
                        .endSpec()
                    .endTemplate()
                .endSpec()
                .build();
    }
}
