package se.afshin.yavari.kafka.operator.mm2;

import io.fabric8.kubernetes.client.KubernetesClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import se.afshin.yavari.kafka.operator.apicurio.ApicurioOrchestrator;
import se.afshin.yavari.kafka.operator.apicurio.ApicurioProxyContainerBuilder;
import se.afshin.yavari.kafka.operator.crd.KafkaCluster;
import se.afshin.yavari.kafka.operator.crd.KafkaClusterApicurioSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaProxyMtlsConfig;
import se.afshin.yavari.kafka.operator.crd.Mm2Endpoint;
import se.afshin.yavari.kafka.operator.crd.Mm2ExternalEndpoint;
import se.afshin.yavari.kafka.operator.crd.Mm2SchemaRegistryRef;
import se.afshin.yavari.kafka.operator.crd.SchemaRegistryType;
import se.afshin.yavari.kafka.operator.proxy.KafkaProxyOrchestrator;

/**
 * Translates an {@link Mm2Endpoint} (managed ref or external) into a {@link ResolvedEndpoint}.
 *
 * <p>For a managed ref, the bootstrap target is the cluster's <strong>proxy</strong>
 * Service (not the broker headless service) — MM2 must speak through the existing
 * Kroxylicious proxy so RBAC, schema validation, and audit filters apply. The proxy
 * service is named {@code kafka-proxy} ({@link KafkaProxyOrchestrator#PROXY_NAME}) and
 * listens on {@code spec.proxy.clientPort} (default 9094).
 *
 * <p>TLS material for managed refs comes from the cluster's
 * {@code proxyMtls.adminClientCertSecretRef} (defaults to {@code kafka-operator-client-tls}).
 * The cert's CN is in broker {@code super.users} via the proxy principal, so MM2 has
 * sufficient privilege. A future hardening pass should issue a per-MM2-CR cert; for v1
 * we reuse the operator's admin cert.
 */
@ApplicationScoped
public class Mm2EndpointResolver {

    @Inject KubernetesClient client;

    /** {@code mm2Namespace} is the namespace of the MirrorMaker2 CR. */
    public ResolvedEndpoint resolve(Mm2Endpoint endpoint, String mm2Namespace) {
        if (endpoint == null) {
            throw new IllegalArgumentException("endpoint is null");
        }
        ResolvedEndpoint base;
        if (endpoint.hasManaged()) {
            base = resolveManaged(endpoint, mm2Namespace);
        } else if (endpoint.hasExternal()) {
            base = resolveExternal(endpoint.getExternal());
        } else {
            throw new IllegalStateException("Mm2Endpoint has neither kafkaClusterRef nor external set");
        }
        // An endpoint-level OAuth Secret overrides the auth derived above. This is how a
        // managed target authenticates to its RBAC-proxied Apicurio: resolveManaged() can't
        // mint credentials, so the user supplies a client-credentials Secret on the endpoint.
        String authSecret = endpoint.getSchemaRegistryAuthSecretRef();
        if (authSecret != null && !authSecret.isBlank() && base.schemaRegistryUrl() != null) {
            return new ResolvedEndpoint(base.bootstrap(), base.tlsSecretRef(), base.sasl(),
                    base.schemaRegistryUrl(), authSecret, base.schemaRegistryConfluent());
        }
        return base;
    }

    private ResolvedEndpoint resolveManaged(Mm2Endpoint endpoint, String mm2Namespace) {
        var ref = endpoint.getKafkaClusterRef();
        String ns = ref.getNamespace() != null && !ref.getNamespace().isBlank()
                ? ref.getNamespace() : mm2Namespace;
        KafkaCluster cluster = client.resources(KafkaCluster.class)
                .inNamespace(ns).withName(ref.getName()).get();
        if (cluster == null) {
            throw new IllegalStateException("Referenced KafkaCluster " + ns + "/" + ref.getName() + " not found");
        }
        // Bootstrap: proxy Service name + clientPort. Same-namespace path is the common
        // case; cross-namespace would require a copied Secret (deferred).
        if (!ns.equals(mm2Namespace)) {
            throw new IllegalStateException(
                    "Cross-namespace KafkaClusterRef not yet supported (referenced "
                            + ns + "/" + ref.getName() + " from MM2 namespace " + mm2Namespace + ")");
        }
        int clientPort = cluster.getSpec().getProxy() != null
                ? cluster.getSpec().getProxy().getClientPort()
                : 9094;
        String bootstrap = KafkaProxyOrchestrator.PROXY_NAME + "." + ns + ".svc.cluster.local:" + clientPort;

        // TLS material — when proxyMtls is set, this end is mTLS.
        String tlsSecret = null;
        KafkaProxyMtlsConfig proxyMtls = cluster.getSpec().getProxyMtls();
        if (proxyMtls != null) {
            tlsSecret = proxyMtls.resolveAdminClientCertSecret();
        }

        // Schema registry — derive from the cluster's apicurio sub-spec if present.
        String schemaRegistryUrl = null;
        KafkaClusterApicurioSpec apicurio = cluster.getSpec().getApicurio();
        if (apicurio != null && apicurio.getRbacRef() != null) {
            schemaRegistryUrl = "http://" + ApicurioOrchestrator.APICURIO_NAME + "-rbac-proxy."
                    + ns + ".svc.cluster.local:" + ApicurioProxyContainerBuilder.PROXY_PORT;
        }

        return new ResolvedEndpoint(bootstrap, tlsSecret, null,
                schemaRegistryUrl, null, false);
    }

    private ResolvedEndpoint resolveExternal(Mm2ExternalEndpoint ext) {
        ResolvedEndpoint.Mm2Sasl sasl = null;
        if (ext.getSasl() != null) {
            sasl = new ResolvedEndpoint.Mm2Sasl(
                    ext.getSasl().getMechanism(), ext.getSasl().getSecretRef());
        }
        String schemaUrl = null;
        String schemaAuth = null;
        boolean confluent = false;
        Mm2SchemaRegistryRef sr = ext.getSchemaRegistry();
        if (sr != null) {
            if (sr.getType() == SchemaRegistryType.CONFLUENT) {
                throw new IllegalStateException("schemaRegistry.type=CONFLUENT is not supported in v1");
            }
            schemaUrl = sr.getUrl();
            schemaAuth = sr.getAuthSecretRef();
            confluent = sr.getType() == SchemaRegistryType.CONFLUENT;
        }
        return new ResolvedEndpoint(ext.getBootstrap(), ext.getTlsSecretRef(), sasl,
                schemaUrl, schemaAuth, confluent);
    }
}
