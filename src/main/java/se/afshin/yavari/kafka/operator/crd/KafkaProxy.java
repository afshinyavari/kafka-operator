package se.afshin.yavari.kafka.operator.crd;

import io.fabric8.kubernetes.api.model.ObjectMeta;

/**
 * Internal Java model used by the proxy builders. As of Wave 4b of the audit, the proxy
 * is a sub-spec of {@link KafkaCluster} ({@code spec.proxy}), not a separate CRD. This
 * class is no longer a fabric8 {@code CustomResource} — it's a plain POJO that
 * {@link se.afshin.yavari.kafka.operator.proxy.KafkaProxyOrchestrator} synthesises from
 * the parent cluster's spec and hands to the existing builders (KroxyliciousConfigBuilder,
 * ProxyDeploymentBuilder, ...) so the builder code didn't have to change shape.
 */
public class KafkaProxy {

    private ObjectMeta metadata;
    private KafkaProxySpec spec;
    private KafkaProxyStatus status;

    public ObjectMeta getMetadata() { return metadata; }
    public void setMetadata(ObjectMeta metadata) { this.metadata = metadata; }

    public KafkaProxySpec getSpec() { return spec; }
    public void setSpec(KafkaProxySpec spec) { this.spec = spec; }

    public KafkaProxyStatus getStatus() { return status; }
    public void setStatus(KafkaProxyStatus status) { this.status = status; }

    /** Stable identifier used by builders that expect {@code CustomResource.getKind()} on
     *  the synthetic object (e.g. for owner references on rendered resources). */
    public String getKind() { return "KafkaCluster"; }
    public String getApiVersion() { return "kafka.yavari.afshin.se/v1alpha1"; }
}
