package se.afshin.yavari.kafka.operator.endpoint;

import io.fabric8.kubernetes.client.KubernetesClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import se.afshin.yavari.kafka.operator.apicurio.ApicurioOrchestrator;
import se.afshin.yavari.kafka.operator.apicurio.ApicurioProxyContainerBuilder;
import se.afshin.yavari.kafka.operator.crd.KafkaCluster;
import se.afshin.yavari.kafka.operator.crd.KafkaClusterApicurioSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaEndpoint;
import se.afshin.yavari.kafka.operator.crd.KafkaEndpointExternal;
import se.afshin.yavari.kafka.operator.crd.KafkaEndpointSchemaRegistryRef;
import se.afshin.yavari.kafka.operator.crd.KafkaProxyMtlsConfig;
import se.afshin.yavari.kafka.operator.crd.SchemaRegistryType;
import se.afshin.yavari.kafka.operator.proxy.KafkaProxyOrchestrator;

/**
 * Translates a {@link KafkaEndpoint} (managed ref or external) into a
 * {@link ResolvedKafkaEndpoint}. Shared by MirrorMaker2 and KafkaConnect reconcilers.
 *
 * <p>For a managed ref, the bootstrap target is the cluster's <strong>proxy</strong>
 * Service (not the broker headless service) — clients must speak through the existing
 * Kroxylicious proxy so RBAC, schema validation, and audit filters apply. The proxy
 * service is named {@code kafka-proxy} ({@link KafkaProxyOrchestrator#PROXY_NAME}) and
 * listens on {@code spec.proxy.clientPort} (default 9094).
 *
 * <p>TLS material for managed refs comes from the cluster's
 * {@code proxyMtls.adminClientCertSecretRef} (defaults to {@code kafka-operator-client-tls}).
 * The cert's CN is in broker {@code super.users} via the proxy principal, so MM2/Connect
 * have sufficient privilege. A future hardening pass should issue per-CR certs; v1 reuses
 * the operator's admin cert.
 */
@ApplicationScoped
public class KafkaEndpointResolver {

    @Inject KubernetesClient client;

    /** {@code crNamespace} is the namespace of the CR consuming this endpoint
     *  (MirrorMaker2 or KafkaConnect). */
    public ResolvedKafkaEndpoint resolve(KafkaEndpoint endpoint, String crNamespace) {
        if (endpoint == null) {
            throw new IllegalArgumentException("endpoint is null");
        }
        ResolvedKafkaEndpoint base;
        if (endpoint.hasManaged()) {
            base = resolveManaged(endpoint, crNamespace);
        } else if (endpoint.hasExternal()) {
            base = resolveExternal(endpoint.getExternal());
        } else {
            throw new IllegalStateException("KafkaEndpoint has neither kafkaClusterRef nor external set");
        }
        String authSecret = endpoint.getSchemaRegistryAuthSecretRef();
        if (authSecret != null && !authSecret.isBlank() && base.schemaRegistryUrl() != null) {
            return new ResolvedKafkaEndpoint(base.bootstrap(), base.tlsSecretRef(), base.sasl(),
                    base.schemaRegistryUrl(), authSecret, base.schemaRegistryConfluent());
        }
        return base;
    }

    private ResolvedKafkaEndpoint resolveManaged(KafkaEndpoint endpoint, String crNamespace) {
        var ref = endpoint.getKafkaClusterRef();
        String ns = ref.getNamespace() != null && !ref.getNamespace().isBlank()
                ? ref.getNamespace() : crNamespace;
        KafkaCluster cluster = client.resources(KafkaCluster.class)
                .inNamespace(ns).withName(ref.getName()).get();
        if (cluster == null) {
            throw new IllegalStateException("Referenced KafkaCluster " + ns + "/" + ref.getName() + " not found");
        }
        if (!ns.equals(crNamespace)) {
            throw new IllegalStateException(
                    "Cross-namespace KafkaClusterRef not yet supported (referenced "
                            + ns + "/" + ref.getName() + " from CR namespace " + crNamespace + ")");
        }
        int clientPort = cluster.getSpec().getProxy() != null
                ? cluster.getSpec().getProxy().getClientPort()
                : 9094;
        String bootstrap = KafkaProxyOrchestrator.PROXY_NAME + "." + ns + ".svc.cluster.local:" + clientPort;

        String tlsSecret = null;
        KafkaProxyMtlsConfig proxyMtls = cluster.getSpec().getProxyMtls();
        if (proxyMtls != null) {
            tlsSecret = proxyMtls.resolveAdminClientCertSecret();
        }

        String schemaRegistryUrl = null;
        KafkaClusterApicurioSpec apicurio = cluster.getSpec().getApicurio();
        if (apicurio != null && apicurio.getRbacRef() != null) {
            schemaRegistryUrl = "http://" + ApicurioOrchestrator.APICURIO_NAME + "-rbac-proxy."
                    + ns + ".svc.cluster.local:" + ApicurioProxyContainerBuilder.PROXY_PORT;
        }

        return new ResolvedKafkaEndpoint(bootstrap, tlsSecret, null,
                schemaRegistryUrl, null, false);
    }

    private ResolvedKafkaEndpoint resolveExternal(KafkaEndpointExternal ext) {
        ResolvedKafkaEndpoint.Sasl sasl = null;
        if (ext.getSasl() != null) {
            sasl = new ResolvedKafkaEndpoint.Sasl(
                    ext.getSasl().getMechanism(), ext.getSasl().getSecretRef());
        }
        String schemaUrl = null;
        String schemaAuth = null;
        boolean confluent = false;
        KafkaEndpointSchemaRegistryRef sr = ext.getSchemaRegistry();
        if (sr != null) {
            if (sr.getType() == SchemaRegistryType.CONFLUENT) {
                throw new IllegalStateException("schemaRegistry.type=CONFLUENT is not supported in v1");
            }
            schemaUrl = sr.getUrl();
            schemaAuth = sr.getAuthSecretRef();
            confluent = sr.getType() == SchemaRegistryType.CONFLUENT;
        }
        return new ResolvedKafkaEndpoint(ext.getBootstrap(), ext.getTlsSecretRef(), sasl,
                schemaUrl, schemaAuth, confluent);
    }
}
