package se.afshin.yavari.kafka.operator.apicurio;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.ApicurioRegistry;
import se.afshin.yavari.kafka.operator.crd.ApicurioRegistrySpec;
import se.afshin.yavari.kafka.operator.crd.ApicurioRegistryStorageConfig;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ApicurioDeploymentBuilderTest {

    private ApicurioDeploymentBuilder builder;

    @BeforeEach
    void setup() {
        builder = new ApicurioDeploymentBuilder();
    }

    @Test
    void memBackend_setsNoKafkaEnvVarsAndNoVolumes() {
        ApicurioRegistry r = registry("mem", null);

        Deployment dep = builder.build(r, "ns", null, null, null, "h");
        List<EnvVar> env = registryEnv(dep);
        Map<String, String> envMap = envAsMap(env);

        assertThat(envMap).containsOnlyKeys("JAVA_TOOL_OPTIONS");
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes()).isEmpty();
        assertThat(registryVolumeMounts(dep)).isEmpty();
    }

    @Test
    void postgresqlBackend_setsJdbcEnvVarsAndNoKafkaVars() {
        ApicurioRegistry r = registry("postgresql", null);
        ApicurioRegistryStorageConfig storage = r.getSpec().getStorage();
        storage.setJdbcUrl("jdbc:postgresql://db:5432/registry");
        storage.setJdbcSecretRef("pg-creds");

        Deployment dep = builder.build(r, "ns", null, null, null, "h");
        Map<String, String> envMap = envAsMap(registryEnv(dep));

        assertThat(envMap).containsKey("QUARKUS_DATASOURCE_JDBC_URL");
        assertThat(envMap.get("QUARKUS_DATASOURCE_JDBC_URL")).isEqualTo("jdbc:postgresql://db:5432/registry");
        assertThat(envMap).doesNotContainKeys(
                "KAFKA_BOOTSTRAP_SERVERS",
                "KAFKA_TOPIC",
                "KAFKA_SECURITY_PROTOCOL");
    }

    @Test
    void kafkasqlBackend_plaintext_setsBootstrapAndTopic_noTls() {
        ApicurioRegistry r = registry("kafkasql", null);

        var cfg = new ApicurioDeploymentBuilder.KafkasqlConfig(
                "brokers-a-headless.ns.svc.cluster.local:9092",
                "kafkasql-journal",
                null,
                "kafka-ubi:4.0.0");
        Deployment dep = builder.build(r, "ns", null, null, cfg, "h");
        Map<String, String> envMap = envAsMap(registryEnv(dep));

        assertThat(envMap.get("KAFKA_BOOTSTRAP_SERVERS"))
                .isEqualTo("brokers-a-headless.ns.svc.cluster.local:9092");
        assertThat(envMap.get("KAFKA_TOPIC")).isEqualTo("kafkasql-journal");
        assertThat(envMap).doesNotContainKey("KAFKA_SECURITY_PROTOCOL");
        assertThat(dep.getSpec().getTemplate().getSpec().getVolumes()).isEmpty();
        assertThat(dep.getSpec().getTemplate().getSpec().getInitContainers()).isEmpty();
        assertThat(registryVolumeMounts(dep)).isEmpty();
    }

    @Test
    void kafkasqlBackend_mtls_addsInitContainerAndPkcs12Mount() {
        ApicurioRegistry r = registry("kafkasql", null);

        var cfg = new ApicurioDeploymentBuilder.KafkasqlConfig(
                "brokers-a-headless.ns.svc.cluster.local:9092",
                "kafkasql-journal",
                "schema-registry-client-tls",
                "kafka-ubi:4.0.0");
        Deployment dep = builder.build(r, "ns", null, null, cfg, "h");
        Map<String, String> envMap = envAsMap(registryEnv(dep));

        // Env vars now point at the PKCS12 emptyDir, not the cert-manager PEM mount
        assertThat(envMap.get("KAFKA_SECURITY_PROTOCOL")).isEqualTo("SSL");
        assertThat(envMap.get("KAFKA_SSL_KEYSTORE_TYPE")).isEqualTo("PKCS12");
        assertThat(envMap.get("KAFKA_SSL_KEYSTORE_LOCATION"))
                .isEqualTo("/tmp/pkcs12/keystore.p12");
        assertThat(envMap.get("KAFKA_SSL_KEYSTORE_PASSWORD")).isEqualTo("changeit");
        assertThat(envMap.get("KAFKA_SSL_KEY_PASSWORD")).isEqualTo("changeit");
        assertThat(envMap.get("KAFKA_SSL_TRUSTSTORE_TYPE")).isEqualTo("PKCS12");
        assertThat(envMap.get("KAFKA_SSL_TRUSTSTORE_LOCATION"))
                .isEqualTo("/tmp/pkcs12/truststore.p12");
        assertThat(envMap.get("KAFKA_SSL_TRUSTSTORE_PASSWORD")).isEqualTo("changeit");

        // Two volumes: PEM Secret + PKCS12 emptyDir
        List<Volume> volumes = dep.getSpec().getTemplate().getSpec().getVolumes();
        assertThat(volumes).extracting(Volume::getName)
                .contains("kafkasql-client-tls", "kafkasql-pkcs12");
        Volume tls = volumes.stream().filter(v -> "kafkasql-client-tls".equals(v.getName())).findFirst().orElseThrow();
        assertThat(tls.getSecret().getSecretName()).isEqualTo("schema-registry-client-tls");

        // Init container converts PEM → PKCS12 using openssl + keytool from the kafka image
        List<Container> initContainers = dep.getSpec().getTemplate().getSpec().getInitContainers();
        assertThat(initContainers).hasSize(1);
        Container init = initContainers.get(0);
        assertThat(init.getName()).isEqualTo("pem-to-pkcs12");
        assertThat(init.getImage()).isEqualTo("kafka-ubi:4.0.0");
        assertThat(init.getCommand()).containsExactly("/bin/bash", "-c",
                init.getCommand().get(2)); // self-reference, just sanity-check the command shape
        assertThat(init.getCommand().get(2))
                .contains("openssl pkcs12 -export")
                .contains("keytool -importcert");
        assertThat(init.getVolumeMounts()).extracting(VolumeMount::getName)
                .contains("kafkasql-client-tls", "kafkasql-pkcs12");

        // Registry container mounts only PKCS12 (read-only)
        List<VolumeMount> mounts = registryVolumeMounts(dep);
        assertThat(mounts).extracting(VolumeMount::getName).contains("kafkasql-pkcs12");
        VolumeMount m = mounts.stream().filter(v -> "kafkasql-pkcs12".equals(v.getName())).findFirst().orElseThrow();
        assertThat(m.getMountPath()).isEqualTo("/tmp/pkcs12");
        assertThat(m.getReadOnly()).isTrue();
    }

    // helpers

    private static ApicurioRegistry registry(String storageType, String image) {
        ApicurioRegistry r = new ApicurioRegistry();
        ObjectMeta meta = new ObjectMeta();
        meta.setName("schema-registry");
        meta.setNamespace("ns");
        r.setMetadata(meta);
        ApicurioRegistrySpec spec = new ApicurioRegistrySpec();
        if (image != null) spec.setImage(image);
        ApicurioRegistryStorageConfig storage = new ApicurioRegistryStorageConfig();
        storage.setType(storageType);
        spec.setStorage(storage);
        r.setSpec(spec);
        return r;
    }

    private static List<EnvVar> registryEnv(Deployment dep) {
        return dep.getSpec().getTemplate().getSpec().getContainers().stream()
                .filter(c -> "registry".equals(c.getName())).findFirst().orElseThrow()
                .getEnv();
    }

    private static List<VolumeMount> registryVolumeMounts(Deployment dep) {
        return dep.getSpec().getTemplate().getSpec().getContainers().stream()
                .filter(c -> "registry".equals(c.getName())).findFirst().orElseThrow()
                .getVolumeMounts();
    }

    private static Map<String, String> envAsMap(List<EnvVar> env) {
        java.util.HashMap<String, String> m = new java.util.HashMap<>();
        for (EnvVar e : env) {
            m.put(e.getName(), e.getValue());
        }
        return m;
    }
}
