package se.afshin.yavari.kafka.operator.proxy;

import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import jakarta.enterprise.context.ApplicationScoped;
import se.afshin.yavari.kafka.operator.crd.KafkaProxy;
import se.afshin.yavari.kafka.operator.crd.KafkaProxySpec;
import se.afshin.yavari.kafka.operator.crd.KafkaProxyTlsConfig;
import se.afshin.yavari.kafka.operator.infra.SecurityContextDefaults;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class ProxyDeploymentBuilder {

    /** PodTemplate annotation that mirrors the SHA-256 of the generated Kroxylicious YAML.
     *  Changing this value flips the PodTemplate hash, which makes Kubernetes auto-roll the
     *  Deployment when the config changes (otherwise ConfigMap edits wouldn't restart pods).
     *  KafkaProxyReconciler also reads it back to decide whether a roll is being triggered
     *  and therefore whether the cross-cluster roll gate should fire. */
    public static final String CONFIG_HASH_ANNOTATION = "kafka.yavari.afshin.se/config-hash";

    /** Port the Kroxylicious management endpoint serves Prometheus metrics on (default 9190).
     *  Only exposed as a container port when {@code spec.metricsEnabled} is set. */
    public static final int METRICS_PORT = 9190;

    public Deployment build(KafkaProxy proxy, String namespace, String configHash) {
        String name = proxy.getMetadata().getName();
        KafkaProxySpec spec = proxy.getSpec();
        Map<String, String> labels = labels(name);

        var volumes = new ArrayList<io.fabric8.kubernetes.api.model.Volume>();
        var mounts = new ArrayList<io.fabric8.kubernetes.api.model.VolumeMount>();

        // Config volume
        volumes.add(new VolumeBuilder()
                .withName("config")
                .withNewConfigMap().withName(name + "-config").endConfigMap()
                .build());
        mounts.add(new VolumeMountBuilder()
                .withName("config")
                .withMountPath("/etc/kroxy")
                .build());

        // RBAC rules volume — mounted at a separate path to avoid collision with /etc/kroxy dir mount
        if (spec.getRbacRef() != null) {
            volumes.add(new VolumeBuilder()
                    .withName("rbac-rules")
                    .withNewConfigMap().withName(spec.getRbacRef() + "-kafka-rules").endConfigMap()
                    .build());
            mounts.add(new VolumeMountBuilder()
                    .withName("rbac-rules")
                    .withMountPath("/etc/kroxy-rbac")
                    .build());
        }

        // Mount the pre-provisioned proxy TLS secrets. Operator does not create these;
        // cert-manager / mcs-setup provisions them with the cert-manager convention
        // (tls.crt + tls.key + ca.crt). Field overrides default to "{name}-client-tls"
        // and "{name}-server-tls"; KafkaProxyReconciler verifies existence before this
        // builder runs and reschedules if missing.
        KafkaProxyTlsConfig tls = spec.getTls();
        String clientCertSecret = (tls != null && tls.getClientCertSecretRef() != null)
                ? tls.getClientCertSecretRef() : name + "-client-tls";
        String serverCertSecret = (tls != null && tls.getServerCertSecretRef() != null)
                ? tls.getServerCertSecretRef() : name + "-server-tls";
        volumes.add(new VolumeBuilder()
                .withName("proxy-tls")
                .withNewSecret().withSecretName(clientCertSecret).endSecret()
                .build());
        mounts.add(new VolumeMountBuilder()
                .withName("proxy-tls")
                .withMountPath("/etc/proxy/kafka-tls")
                .withReadOnly(true)
                .build());
        volumes.add(new VolumeBuilder()
                .withName("server-tls")
                .withNewSecret().withSecretName(serverCertSecret).endSecret()
                .build());
        mounts.add(new VolumeMountBuilder()
                .withName("server-tls")
                .withMountPath("/etc/proxy/server-tls")
                .withReadOnly(true)
                .build());

        // TCP probes on the kafka client port — Kroxylicious only opens the TCP listener once
        // the upstream broker connection is wired and the filter chain is initialised, so a
        // bare TCP accept is a reasonable readiness signal without forcing the (opt-in)
        // management HTTP endpoint to be enabled in every config.
        int clientPort = spec.getClientPort();
        ContainerBuilder container = new ContainerBuilder()
                .withName("kroxylicious")
                .withImage(spec.getImage())
                .withArgs("--config", "/etc/kroxy/config.yaml")
                .withVolumeMounts(mounts)
                .withResources(new ResourceRequirementsBuilder()
                        .withRequests(java.util.Map.of(
                                "cpu", Quantity.parse("100m"),
                                "memory", Quantity.parse("256Mi")))
                        .withLimits(java.util.Map.of(
                                "cpu", Quantity.parse("1"),
                                "memory", Quantity.parse("1Gi")))
                        .build())
                .withNewReadinessProbe()
                    .withNewTcpSocket()
                        .withPort(new IntOrString(clientPort))
                    .endTcpSocket()
                    .withInitialDelaySeconds(5)
                    .withPeriodSeconds(5)
                    .withFailureThreshold(3)
                .endReadinessProbe()
                .withNewLivenessProbe()
                    .withNewTcpSocket()
                        .withPort(new IntOrString(clientPort))
                    .endTcpSocket()
                    .withInitialDelaySeconds(15)
                    .withPeriodSeconds(20)
                    .withFailureThreshold(3)
                .endLivenessProbe()
                .withSecurityContext(SecurityContextDefaults.containerDefaults());
        if (spec.isMetricsEnabled()) {
            container.addNewPort()
                    .withName("metrics")
                    .withContainerPort(METRICS_PORT)
                    .endPort();
        }

        return new DeploymentBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(namespace)
                    .withLabels(labels)
                .endMetadata()
                .withNewSpec()
                    .withReplicas(spec.getReplicas())
                    .withNewSelector()
                        .withMatchLabels(labels)
                    .endSelector()
                    .withNewTemplate()
                        .withNewMetadata()
                            .withLabels(labels)
                            .withAnnotations(Map.of(CONFIG_HASH_ANNOTATION, configHash))
                        .endMetadata()
                        .withNewSpec()
                            .withContainers(container.build())
                            .withVolumes(volumes)
                            .withSecurityContext(SecurityContextDefaults.podDefaults())
                        .endSpec()
                    .endTemplate()
                .endSpec()
                .build();
    }

    static Map<String, String> labels(String name) {
        return Map.of(
                "app", "kroxylicious",
                "app.instance", name,
                "app.managed-by", "kafka-operator"
        );
    }
}
