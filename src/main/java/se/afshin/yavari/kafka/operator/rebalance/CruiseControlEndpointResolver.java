package se.afshin.yavari.kafka.operator.rebalance;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.KubernetesClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import se.afshin.yavari.kafka.operator.crd.CruiseControlApiSecurity;
import se.afshin.yavari.kafka.operator.crd.KafkaCluster;
import se.afshin.yavari.kafka.operator.cruisecontrol.CruiseControlOrchestrator;
import se.afshin.yavari.kafka.operator.rebalance.CruiseControlClient.CruiseControlEndpoint;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Resolves the in-cluster Cruise Control REST endpoint for a {@link KafkaCluster}. v1 is
 * same-namespace only: the {@code KafkaRebalance}, the {@code KafkaCluster}, and Cruise
 * Control all live in one namespace.
 */
@ApplicationScoped
public class CruiseControlEndpointResolver {

    @Inject KubernetesClient client;

    /**
     * @throws IllegalStateException if the cluster has no {@code spec.cruiseControl}, or the
     *         basic-auth Secret is referenced but missing
     */
    public CruiseControlEndpoint resolve(KafkaCluster cluster, String namespace) {
        if (cluster.getSpec() == null || cluster.getSpec().getCruiseControl() == null) {
            throw new IllegalStateException("KafkaCluster '" + cluster.getMetadata().getName()
                    + "' has no spec.cruiseControl — deploy Cruise Control first");
        }
        String baseUrl = "http://" + CruiseControlOrchestrator.CC_NAME + "." + namespace
                + ".svc.cluster.local:" + CruiseControlOrchestrator.REST_PORT;

        String username = null;
        String password = null;
        CruiseControlApiSecurity api = cluster.getSpec().getCruiseControl().getApiSecurity();
        if (api != null && api.isEnabled() && api.getBasicAuthSecretRef() != null) {
            Secret secret = client.secrets().inNamespace(namespace)
                    .withName(api.getBasicAuthSecretRef()).get();
            if (secret == null) {
                throw new IllegalStateException("Cruise Control basic-auth Secret '"
                        + api.getBasicAuthSecretRef() + "' not found in namespace " + namespace);
            }
            username = decode(secret, "username");
            password = decode(secret, "password");
        }
        return new CruiseControlEndpoint(baseUrl, username, password);
    }

    private static String decode(Secret secret, String key) {
        if (secret.getData() == null || !secret.getData().containsKey(key)) {
            return null;
        }
        return new String(Base64.getDecoder().decode(secret.getData().get(key)),
                StandardCharsets.UTF_8);
    }
}
