package se.afshin.yavari.kafka.operator.crd;

import io.fabric8.generator.annotation.Required;
import io.fabric8.generator.annotation.ValidationRule;

public class ClusterEntry {

    /** Logical cluster identifier — must match the KAFKA_CLUSTER_ID env var on the operator running on this cluster. */
    @Required
    @ValidationRule(value = "self.size() > 0", message = "id must not be blank")
    private String id;

    /** External address for the KRaft controller on this cluster, used in controller.quorum.voters. Format: "host:9093" */
    @Required
    @ValidationRule(value = "self.size() > 0", message = "controllerAdvertisedAddress must not be blank")
    private String controllerAdvertisedAddress;

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getControllerAdvertisedAddress() { return controllerAdvertisedAddress; }
    public void setControllerAdvertisedAddress(String controllerAdvertisedAddress) {
        this.controllerAdvertisedAddress = controllerAdvertisedAddress;
    }
}
