package se.afshin.yavari.kafka.operator.crd;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The kind of rebalance a {@link KafkaRebalance} requests, mapping to a Cruise Control
 * REST endpoint:
 * <ul>
 *   <li>{@code full} → {@code /kafkacruisecontrol/rebalance}
 *   <li>{@code add-brokers} → {@code /kafkacruisecontrol/add_broker}
 *   <li>{@code remove-brokers} → {@code /kafkacruisecontrol/remove_broker}
 * </ul>
 */
public enum KafkaRebalanceMode {

    @JsonProperty("full")
    FULL,

    @JsonProperty("add-brokers")
    ADD_BROKERS,

    @JsonProperty("remove-brokers")
    REMOVE_BROKERS
}
