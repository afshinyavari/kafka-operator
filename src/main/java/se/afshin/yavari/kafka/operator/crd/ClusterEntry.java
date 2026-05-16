package se.afshin.yavari.kafka.operator.crd;

public class ClusterEntry {

    /** Logical cluster identifier — must match the KAFKA_CLUSTER_ID env var on the operator running on this cluster. */
    private String id;

    /** External address for the KRaft controller on this cluster, used in controller.quorum.voters. Format: "host:9093" */
    private String controllerAdvertisedAddress;

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getControllerAdvertisedAddress() { return controllerAdvertisedAddress; }
    public void setControllerAdvertisedAddress(String controllerAdvertisedAddress) {
        this.controllerAdvertisedAddress = controllerAdvertisedAddress;
    }
}
