package se.afshin.yavari.kafka.operator.backup;

import io.fabric8.kubernetes.client.KubernetesClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import se.afshin.yavari.kafka.operator.apicurio.ApicurioOrchestrator;
import se.afshin.yavari.kafka.operator.apicurio.ApicurioProxyContainerBuilder;
import se.afshin.yavari.kafka.operator.crd.KafkaCluster;
import se.afshin.yavari.kafka.operator.crd.KafkaClusterApicurioSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaClusterRef;
import se.afshin.yavari.kafka.operator.crd.KafkaProxyMtlsConfig;
import se.afshin.yavari.kafka.operator.topic.BrokerBootstrapResolver;

/**
 * Resolves a {@link KafkaClusterRef} to a {@link ResolvedBackupEndpoint}: the broker
 * headless service address (direct, internal listener — see {@link BrokerBootstrapResolver}),
 * the mTLS PEM Secret when the cluster has {@code proxyMtls}, and the Apicurio URL when the
 * cluster has an Apicurio sub-spec.
 *
 * <p>v1 requires the referenced KafkaCluster to be in the same namespace as the backup CR
 * (matching the MM2 resolver's limitation).
 */
@ApplicationScoped
public class BackupEndpointResolver {

    @Inject KubernetesClient client;
    @Inject BrokerBootstrapResolver bootstrapResolver;

    public ResolvedBackupEndpoint resolve(KafkaClusterRef ref, String crNamespace) {
        if (ref == null || ref.getName() == null || ref.getName().isBlank()) {
            throw new IllegalArgumentException("clusterRef.name is required");
        }
        String ns = (ref.getNamespace() != null && !ref.getNamespace().isBlank())
                ? ref.getNamespace() : crNamespace;
        if (!ns.equals(crNamespace)) {
            throw new IllegalStateException("Cross-namespace clusterRef not supported in v1 "
                    + "(referenced " + ns + "/" + ref.getName() + " from namespace " + crNamespace + ")");
        }
        KafkaCluster cluster = client.resources(KafkaCluster.class)
                .inNamespace(ns).withName(ref.getName()).get();
        if (cluster == null) {
            throw new IllegalStateException(
                    "Referenced KafkaCluster " + ns + "/" + ref.getName() + " not found");
        }

        String bootstrap = bootstrapResolver.resolve(ref.getName(), ns);

        String tlsSecret = null;
        KafkaProxyMtlsConfig proxyMtls = cluster.getSpec().getProxyMtls();
        if (proxyMtls != null) {
            tlsSecret = proxyMtls.resolveAdminClientCertSecret();
        }

        String apicurioUrl = null;
        KafkaClusterApicurioSpec apicurio = cluster.getSpec().getApicurio();
        if (apicurio != null) {
            if (apicurio.getRbacRef() != null && !apicurio.getRbacRef().isBlank()) {
                apicurioUrl = "http://" + ApicurioOrchestrator.APICURIO_NAME + "-rbac-proxy."
                        + ns + ".svc.cluster.local:" + ApicurioProxyContainerBuilder.PROXY_PORT;
            } else {
                apicurioUrl = "http://" + ApicurioOrchestrator.APICURIO_NAME + "."
                        + ns + ".svc.cluster.local:8080";
            }
        }

        return new ResolvedBackupEndpoint(bootstrap, tlsSecret, apicurioUrl);
    }
}
