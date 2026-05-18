package se.afshin.yavari.kafka.operator.crd;

public class KafkaRbacUser {
    private String name;
    private KafkaRbacKafkaAccess kafka;

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public KafkaRbacKafkaAccess getKafka() { return kafka; }
    public void setKafka(KafkaRbacKafkaAccess kafka) { this.kafka = kafka; }
}
