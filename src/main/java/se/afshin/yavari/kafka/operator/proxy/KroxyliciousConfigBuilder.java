package se.afshin.yavari.kafka.operator.proxy;

import jakarta.enterprise.context.ApplicationScoped;
import se.afshin.yavari.kafka.operator.crd.BrokerNodeIdRange;
import se.afshin.yavari.kafka.operator.crd.KafkaProxy;
import se.afshin.yavari.kafka.operator.crd.KafkaProxyCustomFilter;
import se.afshin.yavari.kafka.operator.crd.KafkaProxyFiltersConfig;
import se.afshin.yavari.kafka.operator.crd.KafkaProxyOidcConfig;
import se.afshin.yavari.kafka.operator.crd.KafkaProxySpec;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class KroxyliciousConfigBuilder {

    public String build(KafkaProxy proxy, int brokerCount, int brokerNodeIdBase,
                        String namespace, boolean mcsEnabled) {
        return build(proxy, brokerCount, brokerNodeIdBase, namespace, mcsEnabled,
                ExternalAccessResolution.internal());
    }

    public String build(KafkaProxy proxy, int brokerCount, int brokerNodeIdBase,
                        String namespace, boolean mcsEnabled, ExternalAccessResolution external) {
        KafkaProxySpec spec = proxy.getSpec();
        String dnsSuffix = mcsEnabled ? "clusterset.local" : "cluster.local";
        String poolHeadless = spec.getPoolRef() + "-headless." + namespace + ".svc." + dnsSuffix;

        StringBuilder cfg = new StringBuilder();
        List<String> activeFilters = new ArrayList<>();

        // Virtual cluster
        cfg.append("virtualClusters:\n");
        cfg.append("  - name: ").append(spec.getClusterRef()).append("\n");
        cfg.append("    targetCluster:\n");
        cfg.append("      bootstrapServers: ").append(poolHeadless).append(":9092\n");

        // Target (proxy→broker) mTLS — always present. The client cert secret is mounted at
        // /etc/proxy/kafka-tls by ProxyDeploymentBuilder; the secret name (default or override)
        // is resolved there, the paths inside the pod are fixed.
        appendTargetTls(cfg);

        cfg.append("    gateways:\n");
        cfg.append("      - name: gateway\n");
        String serviceName = proxy.getMetadata().getName();
        // Gateway API and Ingress with SSL passthrough use a single external port and dispatch
        // to the right broker by TLS SNI hostname. That requires Kroxylicious' sniHostIdentifiesNode
        // mode (single listen port + per-broker hostnames via $(nodeId)). LoadBalancer and the
        // default ClusterIP keep portIdentifiesNode (one port per broker).
        boolean sniMode = external.type() == se.afshin.yavari.kafka.operator.crd.ExternalAccessType.GATEWAY
                || external.type() == se.afshin.yavari.kafka.operator.crd.ExternalAccessType.INGRESS;
        String advertisedHost = (external.advertisedHost() != null)
                ? external.advertisedHost()
                : serviceName + "." + namespace + ".svc." + dnsSuffix;
        if (sniMode) {
            // Kroxylicious 0.21 SniHostIdentifiesNodeIdentificationStrategy accepts only
            // bootstrapAddress + advertisedBrokerAddressPattern. Node IDs are derived from the
            // SNI hostname matching the pattern (e.g. broker-7.<host> → nodeId=7).
            cfg.append("        sniHostIdentifiesNode:\n");
            cfg.append("          bootstrapAddress: bootstrap.").append(advertisedHost).append(":").append(spec.getClientPort()).append("\n");
            cfg.append("          advertisedBrokerAddressPattern: broker-$(nodeId).").append(advertisedHost).append("\n");
        } else {
            cfg.append("        portIdentifiesNode:\n");
            cfg.append("          bootstrapAddress: 0.0.0.0:").append(spec.getClientPort()).append("\n");
            cfg.append("          advertisedBrokerAddressPattern: ").append(advertisedHost).append("\n");
            cfg.append("          nodeIdRanges:\n");
            List<BrokerNodeIdRange> ranges = spec.getBrokerNodeIdRanges();
            if (ranges != null && !ranges.isEmpty()) {
                for (BrokerNodeIdRange r : ranges) {
                    cfg.append("            - name: ").append(r.getName()).append("\n");
                    cfg.append("              start: ").append(r.getStart()).append("\n");
                    cfg.append("              end: ").append(r.getEnd()).append("\n");
                }
            } else {
                cfg.append("            - name: brokers\n");
                cfg.append("              start: ").append(brokerNodeIdBase).append("\n");
                cfg.append("              end: ").append(brokerNodeIdBase + brokerCount - 1).append("\n");
            }
        }

        appendGatewayTls(cfg);

        // mTLS CN subject builder (saslSubjectBuilder removed — not in 0.21.0 VirtualCluster schema)
        cfg.append("    subjectBuilder:\n");
        cfg.append("      type: CnSubjectBuilderService\n");

        cfg.append("    logNetwork: false\n");
        cfg.append("    logFrames: false\n");
        cfg.append("\n");

        // Filter definitions
        cfg.append("filterDefinitions:\n");

        if (spec.getOidc() != null) {
            // Request path order: oauth-bearer-validation → jwt-groups → sasl-handshake-synthesizer
            // Response path order (reversed): sasl-handshake-synthesizer → jwt-groups (noop) → oauth-bearer-validation
            // oauth-bearer-validation runs first on the request path so forged JWTs are rejected
            // before jwt-groups stores their (potentially fabricated) group claims.
            appendOauthBearerFilter(cfg, spec.getOidc());
            activeFilters.add("oauth-bearer-validation");

            appendJwtGroupFilter(cfg, spec.getOidc());
            activeFilters.add("jwt-groups");

            appendSaslHandshakeSynthesizerFilter(cfg);
            activeFilters.add("sasl-handshake-synthesizer");
        }

        if (spec.getRbacRef() != null) {
            appendAuthorizationFilter(cfg);
            activeFilters.add("authorization");
        }

        KafkaProxyFiltersConfig filters = spec.getFilters();
        if (filters != null && filters.getXmlValidation() != null && filters.getXmlValidation().isEnabled()) {
            appendXmlValidationFilter(cfg, filters, poolHeadless);
            activeFilters.add("xml-validation");
        }

        for (KafkaProxyCustomFilter custom : spec.getCustomFilters()) {
            appendCustomFilter(cfg, custom);
            activeFilters.add(custom.getName());
        }

        if (!activeFilters.isEmpty()) {
            cfg.append("\ndefaultFilters:\n");
            activeFilters.forEach(f -> cfg.append("  - ").append(f).append("\n"));
        }

        return cfg.toString();
    }

    // ProxyDeploymentBuilder mounts the client cert secret at /etc/proxy/kafka-tls and the
    // server cert secret at /etc/proxy/server-tls. Both secrets bundle ca.crt (cert-manager
    // convention). Kroxylicious 0.21.0 Tls schema: trust + key only — when trust is set on a
    // gateway, Kroxylicious requests client certs (mTLS); there is no separate clientAuth field.

    private void appendTargetTls(StringBuilder cfg) {
        cfg.append("      tls:\n");
        cfg.append("        key:\n");
        cfg.append("          privateKeyFile: /etc/proxy/kafka-tls/tls.key\n");
        cfg.append("          certificateFile: /etc/proxy/kafka-tls/tls.crt\n");
        cfg.append("        trust:\n");
        cfg.append("          storeFile: /etc/proxy/kafka-tls/ca.crt\n");
        cfg.append("          storeType: PEM\n");
    }

    private void appendGatewayTls(StringBuilder cfg) {
        cfg.append("        tls:\n");
        cfg.append("          key:\n");
        cfg.append("            privateKeyFile: /etc/proxy/server-tls/tls.key\n");
        cfg.append("            certificateFile: /etc/proxy/server-tls/tls.crt\n");
        cfg.append("          trust:\n");
        cfg.append("            storeFile: /etc/proxy/server-tls/ca.crt\n");
        cfg.append("            storeType: PEM\n");
    }

    private void appendSaslHandshakeSynthesizerFilter(StringBuilder cfg) {
        cfg.append("  - name: sasl-handshake-synthesizer\n");
        cfg.append("    type: SaslHandshakeSynthesizerFilterFactory\n");
    }

    private void appendJwtGroupFilter(StringBuilder cfg, KafkaProxyOidcConfig oidc) {
        cfg.append("  - name: jwt-groups\n");
        cfg.append("    type: JwtGroupFilterFactory\n");
        cfg.append("    config:\n");
        cfg.append("      groupsClaim: ").append(oidc.getGroupsClaim()).append("\n");
    }

    private void appendOauthBearerFilter(StringBuilder cfg, KafkaProxyOidcConfig oidc) {
        cfg.append("  - name: oauth-bearer-validation\n");
        cfg.append("    type: OauthBearerValidation\n");
        cfg.append("    config:\n");
        cfg.append("      jwksEndpointUrl: ").append(oidc.getJwksEndpointUrl()).append("\n");
        if (oidc.getExpectedIssuer() != null) {
            cfg.append("      expectedIssuer: ").append(oidc.getExpectedIssuer()).append("\n");
        }
        if (oidc.getExpectedAudience() != null) {
            cfg.append("      expectedAudience: ").append(oidc.getExpectedAudience()).append("\n");
        }
    }

    private void appendAuthorizationFilter(StringBuilder cfg) {
        cfg.append("  - name: authorization\n");
        cfg.append("    type: Authorization\n");
        cfg.append("    config:\n");
        cfg.append("      authorizer: GroupAwareAuthorizerService\n");
        cfg.append("      authorizerConfig:\n");
        cfg.append("        rulesFile: /etc/kroxy-rbac/rbac-rules.yaml\n");
    }

    private void appendXmlValidationFilter(StringBuilder cfg, KafkaProxyFiltersConfig filters,
                                            String poolHeadless) {
        cfg.append("  - name: xml-validation\n");
        cfg.append("    type: XmlValidationFilterFactory\n");
        cfg.append("    config:\n");
        cfg.append("      bootstrapServers: ").append(poolHeadless).append(":9092\n");
        cfg.append("      schemaTopic: ").append(filters.getXmlValidation().getSchemaTopic()).append("\n");
        // The broker INTERNAL listener is SSL (proxyMtls is required for the proxy to deploy),
        // so the filter's internal Kafka client must mTLS using the proxy's already-mounted certs.
        cfg.append("      securityProtocol: SSL\n");
        cfg.append("      sslKeystoreCertPath: /etc/proxy/kafka-tls/tls.crt\n");
        cfg.append("      sslKeystoreKeyPath: /etc/proxy/kafka-tls/tls.key\n");
        cfg.append("      sslTruststoreCertPath: /etc/proxy/kafka-tls/ca.crt\n");
    }

    private void appendCustomFilter(StringBuilder cfg, KafkaProxyCustomFilter custom) {
        cfg.append("  - name: ").append(custom.getName()).append("\n");
        cfg.append("    type: ").append(custom.getType()).append("\n");
        if (custom.getConfig() != null && !custom.getConfig().isEmpty()) {
            cfg.append("    config:\n");
            appendMap(cfg, custom.getConfig(), "      ");
        }
    }

    @SuppressWarnings("unchecked")
    private void appendMap(StringBuilder cfg, Map<String, Object> map, String indent) {
        map.forEach((key, value) -> {
            if (value instanceof Map<?, ?> nested) {
                cfg.append(indent).append(key).append(":\n");
                appendMap(cfg, (Map<String, Object>) nested, indent + "  ");
            } else if (value instanceof List<?> list) {
                cfg.append(indent).append(key).append(":\n");
                list.forEach(item -> cfg.append(indent).append("  - ").append(item).append("\n"));
            } else {
                cfg.append(indent).append(key).append(": ").append(value).append("\n");
            }
        });
    }
}
