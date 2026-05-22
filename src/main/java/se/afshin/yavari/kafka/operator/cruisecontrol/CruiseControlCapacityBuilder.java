package se.afshin.yavari.kafka.operator.cruisecontrol;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.enterprise.context.ApplicationScoped;
import se.afshin.yavari.kafka.operator.crd.CruiseControlCapacityConfig;

/**
 * Renders the Cruise Control {@code capacity.json}. Cruise Control needs per-broker
 * DISK / CPU / NW_IN / NW_OUT capacity to reason about rebalances.
 *
 * <p>v1 emits a single wildcard entry ({@code brokerId: "-1"}) that Cruise Control applies
 * as the default for every broker — covering all brokers across all MCS clusters without
 * enumerating node IDs. Values come from {@code spec.cruiseControl.capacity}; unset fields
 * fall back to documented defaults. Per-broker overrides are a follow-up.
 */
@ApplicationScoped
public class CruiseControlCapacityBuilder {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    static final String DEFAULT_DISK = "100000";     // MB  (~100 GB)
    static final String DEFAULT_CPU = "100";         // Cruise Control CPU capacity units
    static final String DEFAULT_NW_IN = "100000";    // KB/s (~100 MB/s)
    static final String DEFAULT_NW_OUT = "100000";   // KB/s

    public String build(CruiseControlCapacityConfig capacity) {
        String disk = DEFAULT_DISK;
        String cpu = DEFAULT_CPU;
        String nwIn = DEFAULT_NW_IN;
        String nwOut = DEFAULT_NW_OUT;

        if (capacity != null) {
            if (notBlank(capacity.getDisk())) disk = capacity.getDisk();
            if (capacity.getCpu() != null) cpu = trimNumber(capacity.getCpu());
            if (notBlank(capacity.getInboundNetwork())) nwIn = capacity.getInboundNetwork();
            if (notBlank(capacity.getOutboundNetwork())) nwOut = capacity.getOutboundNetwork();
        }

        ObjectNode root = MAPPER.createObjectNode();
        ArrayNode brokers = root.putArray("brokerCapacities");
        ObjectNode entry = brokers.addObject();
        entry.put("brokerId", "-1");
        ObjectNode cap = entry.putObject("capacity");
        cap.put("DISK", disk);
        cap.put("CPU", cpu);
        cap.put("NW_IN", nwIn);
        cap.put("NW_OUT", nwOut);
        entry.put("doc", "Default capacity for all brokers (operator-generated).");

        try {
            return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(root);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to render Cruise Control capacity.json", e);
        }
    }

    private static boolean notBlank(String s) { return s != null && !s.isBlank(); }

    /** Renders a double without a trailing {@code .0} (e.g. 4.0 → "4", 4.5 → "4.5"). */
    static String trimNumber(double d) {
        if (d == Math.rint(d) && !Double.isInfinite(d)) {
            return Long.toString((long) d);
        }
        return Double.toString(d);
    }
}
