package se.afshin.yavari.kafka.operator.crd;

/**
 * Broker capacity inputs for the Cruise Control {@code capacity.json} file. Cruise Control
 * needs per-broker DISK / CPU / NW_IN / NW_OUT capacity to reason about rebalances.
 *
 * <p>All fields are optional: the {@code CruiseControlCapacityBuilder} derives disk and CPU
 * from the broker {@link KafkaNodePool} (storage size, resource limits) when not set, and
 * falls back to documented defaults for network capacity (no reliable Kubernetes source).
 */
public class CruiseControlCapacityConfig {

    /** Per-broker disk capacity in MB (string). Null → derived from the broker pool storage. */
    private String disk;

    /** Per-broker CPU capacity in cores. Null → derived from the broker pool CPU limit. */
    private Double cpu;

    /** Per-broker inbound network capacity in KB/s (string). Null → default. */
    private String inboundNetwork;

    /** Per-broker outbound network capacity in KB/s (string). Null → default. */
    private String outboundNetwork;

    public String getDisk() { return disk; }
    public void setDisk(String disk) { this.disk = disk; }

    public Double getCpu() { return cpu; }
    public void setCpu(Double cpu) { this.cpu = cpu; }

    public String getInboundNetwork() { return inboundNetwork; }
    public void setInboundNetwork(String inboundNetwork) { this.inboundNetwork = inboundNetwork; }

    public String getOutboundNetwork() { return outboundNetwork; }
    public void setOutboundNetwork(String outboundNetwork) { this.outboundNetwork = outboundNetwork; }
}
