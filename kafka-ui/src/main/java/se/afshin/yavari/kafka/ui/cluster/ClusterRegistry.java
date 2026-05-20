package se.afshin.yavari.kafka.ui.cluster;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import se.afshin.yavari.kafka.ui.crd.KafkaClusterCr;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * Lists the {@code KafkaCluster} CRs in the operator namespace and derives the
 * proxy + apicurio Service URLs for each. The list is queried on demand (no
 * informer) — KafkaCluster CRs change rarely and a stale entry would not cause
 * corruption, only a transient connect failure.
 */
@ApplicationScoped
public class ClusterRegistry {

    private static final Logger LOG = Logger.getLogger(ClusterRegistry.class);

    @Inject
    KubernetesClient client;

    @ConfigProperty(name = "kafka-ui.cluster.dns-suffix")
    String dnsSuffix;

    @ConfigProperty(name = "kafka-ui.proxy.port")
    int proxyPort;

    @ConfigProperty(name = "kafka-ui.apicurio.port")
    int apicurioPort;

    @ConfigProperty(name = "kafka-ui.cluster.namespace", defaultValue = "kafka")
    String operatorNamespace;

    /** Service name fronting the KafkaProxy. Defaults match the convention
     *  {@code metadata.name} of the {@code KafkaProxy} CR (one per namespace
     *  in the Phase-1 MVP). */
    @ConfigProperty(name = "kafka-ui.proxy.service-name", defaultValue = "kafka-proxy")
    String proxyServiceName;

    /** Service name fronting the Apicurio rbac-proxy. Defaults match the
     *  operator convention {@code {ApicurioRegistry.metadata.name}-rbac-proxy}. */
    @ConfigProperty(name = "kafka-ui.apicurio.service-name", defaultValue = "apicurio-rbac-proxy")
    String apicurioServiceName;

    void onStart(@Observes StartupEvent ev) {
        try {
            List<ClusterCoordinates> all = list();
            LOG.infof("ClusterRegistry initialised — %d KafkaCluster CR(s) visible: %s",
                    all.size(), all.stream().map(ClusterCoordinates::id).toList());
        } catch (Exception e) {
            LOG.warnf(e, "ClusterRegistry startup probe failed (continuing); first call to list() will retry");
        }
    }

    public List<ClusterCoordinates> list() {
        return client.resources(KafkaClusterCr.class).inNamespace(operatorNamespace).list()
                .getItems().stream()
                .map(cr -> toCoordinates(cr.getMetadata().getName(), cr.getMetadata().getNamespace()))
                .sorted(Comparator.comparing(ClusterCoordinates::id))
                .toList();
    }

    public Optional<ClusterCoordinates> byId(String id) {
        return list().stream().filter(c -> c.id().equals(id)).findFirst();
    }

    private ClusterCoordinates toCoordinates(String crName, String namespace) {
        // Service names follow the operator's existing convention (no per-cluster
        // suffix in Phase 1 — one KafkaProxy / one ApicurioRegistry per namespace).
        // Submariner Lighthouse local-prefers a reachable replica when resolving
        // the {service}.{ns}{dns-suffix} hostnames.
        String proxyHost = proxyServiceName + "." + namespace + dnsSuffix;
        String apicurioHost = apicurioServiceName + "." + namespace + dnsSuffix;
        return new ClusterCoordinates(
                crName,
                namespace,
                proxyHost + ":" + proxyPort,
                "http://" + apicurioHost + ":" + apicurioPort);
    }
}
