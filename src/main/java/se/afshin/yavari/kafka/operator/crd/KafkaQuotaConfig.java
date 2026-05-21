package se.afshin.yavari.kafka.operator.crd;

/**
 * Per-user Kafka client quota. Applied via {@code AdminClient.alterClientQuotas} on
 * the user principal — under SASL/OAUTHBEARER that's the JWT {@code sub} claim, which
 * is also what Kroxylicious presents to the broker.
 *
 * <p>All fields are optional; null = no quota set / quota removed on the broker.
 * Bytes-per-second values are {@code Long}; ratios are {@code Double} (Kafka accepts
 * fractional values, e.g. {@code 0.5} for "50% of one I/O thread").
 *
 * <p>See {@code org.apache.kafka.common.quota.ClientQuotaEntity} for the underlying API.
 */
public class KafkaQuotaConfig {

    /** Bytes/sec the user may produce. */
    private Long producerByteRate;

    /** Bytes/sec the user may consume. */
    private Long consumerByteRate;

    /** Fraction of broker network/IO threads the user may use. {@code 0.5} == 50% of one thread. */
    private Double requestPercentage;

    /** Mutations/sec on controller (topic creates, ACL writes, ...). */
    private Double controllerMutationRate;

    public Long getProducerByteRate() { return producerByteRate; }
    public void setProducerByteRate(Long producerByteRate) { this.producerByteRate = producerByteRate; }

    public Long getConsumerByteRate() { return consumerByteRate; }
    public void setConsumerByteRate(Long consumerByteRate) { this.consumerByteRate = consumerByteRate; }

    public Double getRequestPercentage() { return requestPercentage; }
    public void setRequestPercentage(Double requestPercentage) { this.requestPercentage = requestPercentage; }

    public Double getControllerMutationRate() { return controllerMutationRate; }
    public void setControllerMutationRate(Double controllerMutationRate) { this.controllerMutationRate = controllerMutationRate; }
}
