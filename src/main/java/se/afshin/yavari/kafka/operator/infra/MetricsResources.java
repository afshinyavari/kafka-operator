package se.afshin.yavari.kafka.operator.infra;

import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.GenericKubernetesResourceBuilder;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import jakarta.enterprise.context.ApplicationScoped;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * Builds the Prometheus metrics plumbing — a dedicated {@code <baseName>-metrics} ClusterIP
 * Service and a matching {@code monitoring.coreos.com/v1} ServiceMonitor — shared by every
 * workload the operator exposes metrics for (Kafka node pools, the Kroxylicious proxy,
 * Cruise Control, MirrorMaker2).
 *
 * <p>The builder only constructs the objects; callers apply them — the Service via the
 * Kubernetes client, the ServiceMonitor via {@link OptionalResourceApplier} (which silently
 * no-ops when the Prometheus Operator CRD is absent).
 */
@ApplicationScoped
public class MetricsResources {

    /** Conventional suffix for the dedicated metrics Service / ServiceMonitor. */
    public static final String METRICS_SUFFIX = "-metrics";

    /** Default Prometheus scrape interval — matches the broker ServiceMonitor. */
    public static final String DEFAULT_INTERVAL = "30s";

    /**
     * Builds a {@code <baseName>-metrics} ClusterIP Service exposing a single metrics port.
     *
     * @param labels         labels for the Service metadata — must equal the ServiceMonitor
     *                       {@code matchLabels} so the monitor selects this Service
     * @param selectorLabels pod selector — labels carried by the workload's pods
     */
    public Service metricsService(String baseName, String namespace,
                                  Map<String, String> labels,
                                  Map<String, String> selectorLabels,
                                  String portName, int port,
                                  List<OwnerReference> ownerRefs) {
        return new ServiceBuilder()
                .withNewMetadata()
                    .withName(baseName + METRICS_SUFFIX)
                    .withNamespace(namespace)
                    .withLabels(labels)
                    .withOwnerReferences(ownerRefs)
                .endMetadata()
                .withNewSpec()
                    .withType("ClusterIP")
                    .withSelector(selectorLabels)
                    .addNewPort()
                        .withName(portName)
                        .withPort(port)
                        .withTargetPort(new IntOrString(port))
                    .endPort()
                .endSpec()
                .build();
    }

    /**
     * Builds a {@code <baseName>-metrics} ServiceMonitor scraping {@code portName} at the
     * {@link #DEFAULT_INTERVAL default interval}.
     */
    public GenericKubernetesResource serviceMonitor(String baseName, String namespace,
                                                    Map<String, String> labels,
                                                    Map<String, String> matchLabels,
                                                    String portName,
                                                    List<OwnerReference> ownerRefs) {
        return serviceMonitor(baseName, namespace, labels, matchLabels, portName,
                DEFAULT_INTERVAL, ownerRefs);
    }

    /**
     * Builds a {@code <baseName>-metrics} ServiceMonitor selecting the metrics Service by
     * {@code matchLabels} and scraping {@code portName} at {@code interval}.
     */
    public GenericKubernetesResource serviceMonitor(String baseName, String namespace,
                                                    Map<String, String> labels,
                                                    Map<String, String> matchLabels,
                                                    String portName, String interval,
                                                    List<OwnerReference> ownerRefs) {
        Map<String, Object> spec = Map.of(
                "selector", Map.of("matchLabels", matchLabels),
                "endpoints", List.of(Map.of("port", portName, "interval", interval)));
        return new GenericKubernetesResourceBuilder()
                .withApiVersion(OptionalResourceApplier.SERVICEMONITOR_API)
                .withKind(OptionalResourceApplier.SERVICEMONITOR_KIND)
                .withNewMetadata()
                    .withName(baseName + METRICS_SUFFIX)
                    .withNamespace(namespace)
                    .withLabels(labels)
                    .withOwnerReferences(ownerRefs)
                .endMetadata()
                .addToAdditionalProperties("spec", spec)
                .build();
    }

    /**
     * Loads a bundled JMX-exporter config from {@code /metrics/<resourceName>} on the
     * classpath. Used for Cruise Control and MirrorMaker2, whose JMX MBean set is fixed and
     * therefore does not need a user-supplied config.
     */
    public static String jmxConfig(String resourceName) {
        String path = "/metrics/" + resourceName;
        try (InputStream in = MetricsResources.class.getResourceAsStream(path)) {
            if (in == null) {
                throw new IllegalStateException("Bundled JMX config not found on classpath: " + path);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read bundled JMX config " + path, e);
        }
    }
}
