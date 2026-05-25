package se.afshin.yavari.kafka.operator.connect;

import io.fabric8.kubernetes.client.KubernetesClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import se.afshin.yavari.kafka.operator.crd.KafkaConnect;
import se.afshin.yavari.kafka.operator.crd.KafkaConnectStatus;
import se.afshin.yavari.kafka.operator.crd.KafkaConnectorSpec;

/**
 * Looks up the parent {@link KafkaConnect} CR named by a
 * {@link KafkaConnectorSpec#getConnectClusterRef()} and resolves it to a
 * {@link ConnectEndpoint}.
 *
 * <p>Strategy: read {@code parent.status.url} (the authoritative contract); fall back to
 * the convention {@code http://<name>-connect.<ns>.svc.cluster.local:8083} when the
 * status field is null (parent has not reconciled yet).
 *
 * <p>v1 enforces same-namespace lookup — cross-namespace is deferred.
 */
@ApplicationScoped
public class ConnectEndpointResolver {

    @Inject KubernetesClient client;

    public ConnectEndpoint resolve(String parentName, String namespace) {
        if (parentName == null || parentName.isBlank()) {
            return new ConnectEndpoint(null, null, null, false,
                    "spec.connectClusterRef.name must not be blank");
        }
        KafkaConnect parent = client.resources(KafkaConnect.class)
                .inNamespace(namespace).withName(parentName).get();
        if (parent == null) {
            return new ConnectEndpoint(null, null, null, false,
                    "Referenced KafkaConnect '" + parentName + "' not found in namespace " + namespace);
        }
        KafkaConnectStatus st = parent.getStatus();
        boolean ready = st != null && st.getPhase() == KafkaConnectStatus.Phase.READY;
        String url = st != null && st.getUrl() != null && !st.getUrl().isBlank()
                ? st.getUrl()
                : "http://" + parentName + "-connect." + namespace + ".svc.cluster.local:"
                        + parent.getSpec().getRestPort();
        String tlsSecret = st != null ? st.getTlsSecretRef() : null;
        String authSecret = st != null ? st.getAuthSecretRef() : null;
        String message = ready ? null : "Parent KafkaConnect '" + parentName + "' is not READY yet";
        return new ConnectEndpoint(url, tlsSecret, authSecret, ready, message);
    }
}
