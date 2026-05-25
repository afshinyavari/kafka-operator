package se.afshin.yavari.kafka.operator.ui;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.KafkaUI;
import se.afshin.yavari.kafka.operator.crd.KafkaUIEnvVar;
import se.afshin.yavari.kafka.operator.crd.KafkaUIOidcConfig;
import se.afshin.yavari.kafka.operator.crd.KafkaUISecretKeyRef;
import se.afshin.yavari.kafka.operator.crd.KafkaUISpec;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class UIDeploymentBuilderTest {

    private final UIDeploymentBuilder builder = new UIDeploymentBuilder();

    @Test
    void build_defaults_producesMatchingDeployment() {
        Deployment dep = builder.build(ui(), ownerRef());

        assertThat(dep.getMetadata().getName()).isEqualTo("kafka-ui");
        assertThat(dep.getMetadata().getNamespace()).isEqualTo("kafka");
        assertThat(dep.getSpec().getReplicas()).isEqualTo(1);
        assertThat(dep.getMetadata().getOwnerReferences()).hasSize(1);
        assertThat(dep.getMetadata().getOwnerReferences().get(0).getController()).isTrue();

        Container c = dep.getSpec().getTemplate().getSpec().getContainers().get(0);
        assertThat(c.getImage()).isEqualTo("kafka-editor:dev");
        assertThat(c.getImagePullPolicy()).isEqualTo("IfNotPresent");
        assertThat(c.getPorts().get(0).getContainerPort()).isEqualTo(8080);
        assertThat(c.getReadinessProbe().getHttpGet().getPath()).isEqualTo("/q/health/ready");
        assertThat(c.getLivenessProbe().getHttpGet().getPath()).isEqualTo("/q/health/live");

        Map<String, EnvVar> env = envByName(c.getEnv());
        assertThat(env).containsKeys(
                "QUARKUS_OIDC_AUTH_SERVER_URL",
                "QUARKUS_OIDC_CLIENT_ID",
                "QUARKUS_OIDC_CREDENTIALS_SECRET",
                "KAFKA_EDITOR_BOOTSTRAP_SERVERS",
                "KAFKA_EDITOR_SECURITY_PROTOCOL",
                "KAFKA_EDITOR_TLS_DIR",
                "KAFKA_EDITOR_NAMESPACE",
                "QUARKUS_HTTP_PORT");
        // KAFKA_EDITOR_NAMESPACE is sourced via the downward API.
        assertThat(env.get("KAFKA_EDITOR_NAMESPACE").getValueFrom().getFieldRef().getFieldPath())
                .isEqualTo("metadata.namespace");
        assertThat(env.get("QUARKUS_OIDC_AUTH_SERVER_URL").getValue()).isEqualTo("http://issuer");
        assertThat(env.get("KAFKA_EDITOR_TLS_DIR").getValue()).isEqualTo("/etc/kafka-tls");
        assertThat(env.get("KAFKA_EDITOR_SECURITY_PROTOCOL").getValue()).isEqualTo("SASL_SSL");
        // Default discovery: kafka-proxy.kafka.svc.cluster.local:9094
        assertThat(env.get("KAFKA_EDITOR_BOOTSTRAP_SERVERS").getValue())
                .isEqualTo("kafka-proxy.kafka.svc.cluster.local:9094");
        assertThat(env.get("QUARKUS_OIDC_CREDENTIALS_SECRET").getValueFrom().getSecretKeyRef().getName())
                .isEqualTo("kafka-ui-oidc");
        assertThat(env.get("QUARKUS_OIDC_CREDENTIALS_SECRET").getValueFrom().getSecretKeyRef().getKey())
                .isEqualTo("client-secret");

        // The legacy KAFKA_UI_* and OIDC_* names are gone — backend reads
        // QUARKUS_OIDC_* (auto-mapped by MicroProfile Config) and KAFKA_EDITOR_*.
        assertThat(env).doesNotContainKeys(
                "OIDC_ISSUER_URL", "OIDC_CLIENT_ID", "OIDC_CLIENT_SECRET",
                "KAFKA_UI_KAFKA_SECURITY_PROTOCOL", "KAFKA_UI_KAFKA_SSL_TLS_DIR",
                "UI_PORT", "KAFKA_UI_CLUSTER_NAMESPACE",
                "KAFKA_UI_PROXY_SERVICE_NAME", "KAFKA_UI_PROXY_PORT",
                "KAFKA_UI_APICURIO_SERVICE_NAME", "KAFKA_UI_APICURIO_PORT",
                "KAFKA_UI_DNS_SUFFIX");

        // TLS secret mounted (used by AdminClientFactory's PEM keystore + truststore).
        assertThat(c.getVolumeMounts().get(0).getName()).isEqualTo("kafka-client-tls");
        assertThat(c.getVolumeMounts().get(0).getMountPath()).isEqualTo("/etc/kafka-tls");
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes().get(0)
                .getSecret().getSecretName()).isEqualTo("kafka-proxy-test-client-tls");

        // SA wired.
        assertThat(dep.getSpec().getTemplate().getSpec().getServiceAccountName()).isEqualTo("kafka-ui");
    }

    @Test
    void build_extraEnv_overridesDefault() {
        KafkaUI ui = ui();
        KafkaUIEnvVar override = new KafkaUIEnvVar();
        override.setName("KAFKA_EDITOR_BOOTSTRAP_SERVERS");
        override.setValue("override-broker:9092");
        ui.getSpec().setEnv(List.of(override));

        Deployment dep = builder.build(ui, ownerRef());
        Container c = dep.getSpec().getTemplate().getSpec().getContainers().get(0);
        Map<String, EnvVar> env = envByName(c.getEnv());
        assertThat(env.get("KAFKA_EDITOR_BOOTSTRAP_SERVERS").getValue()).isEqualTo("override-broker:9092");
        // appears exactly once (override removed the default).
        long count = c.getEnv().stream()
                .filter(e -> "KAFKA_EDITOR_BOOTSTRAP_SERVERS".equals(e.getName()))
                .count();
        assertThat(count).isEqualTo(1L);
    }

    @Test
    void build_dnsSuffix_appearsInBootstrap() {
        KafkaUI ui = ui();
        // clusterset.local makes the proxy reachable cross-cluster via
        // Submariner Lighthouse — but the SAME local proxy is what kafka-editor
        // wants to call, so the operator still produces an in-cluster name.
        ui.getSpec().getDiscovery().setDnsSuffix("clusterset.local");
        Deployment dep = builder.build(ui, ownerRef());
        Map<String, EnvVar> env = envByName(
                dep.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv());
        assertThat(env.get("KAFKA_EDITOR_BOOTSTRAP_SERVERS").getValue())
                .isEqualTo("kafka-proxy.kafka.clusterset.local:9094");
    }

    // ---- helpers ----

    private KafkaUI ui() {
        KafkaUI ui = new KafkaUI();
        ObjectMeta meta = new ObjectMeta();
        meta.setName("kafka-ui");
        meta.setNamespace("kafka");
        meta.setUid("uid-123");
        ui.setMetadata(meta);

        KafkaUISpec spec = new KafkaUISpec();
        KafkaUIOidcConfig oidc = new KafkaUIOidcConfig();
        oidc.setIssuerUrl("http://issuer");
        oidc.setClientId("kafka-ui-web");
        KafkaUISecretKeyRef ref = new KafkaUISecretKeyRef();
        ref.setName("kafka-ui-oidc");
        ref.setKey("client-secret");
        oidc.setClientSecretRef(ref);
        spec.setOidc(oidc);
        ui.setSpec(spec);
        return ui;
    }

    private io.fabric8.kubernetes.api.model.OwnerReference ownerRef() {
        return new OwnerReferenceBuilder()
                .withApiVersion("kafka.yavari.afshin.se/v1alpha1")
                .withKind("KafkaUI")
                .withName("kafka-ui")
                .withUid("uid-123")
                .withController(true)
                .build();
    }

    private Map<String, EnvVar> envByName(List<EnvVar> env) {
        return env.stream().collect(java.util.stream.Collectors.toMap(EnvVar::getName, e -> e));
    }
}
