package se.afshin.yavari.kafka.operator.crd;

import io.fabric8.kubernetes.api.model.ObjectMeta;

/**
 * Internal Java model used by the Apicurio builders. As of Wave 4c of the audit, Apicurio
 * is an optional sub-spec of {@link KafkaCluster} ({@code spec.apicurio}), not a separate
 * CRD. This class is no longer a fabric8 {@code CustomResource} — it's a plain POJO that
 * {@link se.afshin.yavari.kafka.operator.apicurio.ApicurioOrchestrator} synthesises from
 * the parent cluster's spec and hands to the existing builders so the builder code didn't
 * have to change shape.
 */
public class ApicurioRegistry {

    private ObjectMeta metadata;
    private ApicurioRegistrySpec spec;
    private ApicurioRegistryStatus status;

    public ObjectMeta getMetadata() { return metadata; }
    public void setMetadata(ObjectMeta metadata) { this.metadata = metadata; }

    public ApicurioRegistrySpec getSpec() { return spec; }
    public void setSpec(ApicurioRegistrySpec spec) { this.spec = spec; }

    public ApicurioRegistryStatus getStatus() { return status; }
    public void setStatus(ApicurioRegistryStatus status) { this.status = status; }

    public String getKind() { return "KafkaCluster"; }
    public String getApiVersion() { return "kafka.yavari.afshin.se/v1alpha1"; }
}
