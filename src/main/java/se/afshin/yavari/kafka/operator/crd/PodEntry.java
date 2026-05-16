package se.afshin.yavari.kafka.operator.crd;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.PodSpec;

public class PodEntry {

    private ObjectMeta metadata;
    private PodSpec spec;

    public ObjectMeta getMetadata() { return metadata; }
    public void setMetadata(ObjectMeta metadata) { this.metadata = metadata; }

    public PodSpec getSpec() { return spec; }
    public void setSpec(PodSpec spec) { this.spec = spec; }
}
