package se.afshin.yavari.kafka.operator.crd;

public class KafkaRbacUser {
    private String name;
    private KafkaRbacKafkaAccess kafka;
    /** Optional Kafka client quotas — applied to the user principal on the broker. */
    private KafkaQuotaConfig quotas;

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public KafkaRbacKafkaAccess getKafka() { return kafka; }
    public void setKafka(KafkaRbacKafkaAccess kafka) { this.kafka = kafka; }

    public KafkaQuotaConfig getQuotas() { return quotas; }
    public void setQuotas(KafkaQuotaConfig quotas) { this.quotas = quotas; }
}
