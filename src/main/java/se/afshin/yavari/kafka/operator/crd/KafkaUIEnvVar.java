package se.afshin.yavari.kafka.operator.crd;

public class KafkaUIEnvVar {
    private String name;
    private String value;
    private KafkaUISecretKeyRef valueFrom;

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getValue() { return value; }
    public void setValue(String value) { this.value = value; }

    public KafkaUISecretKeyRef getValueFrom() { return valueFrom; }
    public void setValueFrom(KafkaUISecretKeyRef valueFrom) { this.valueFrom = valueFrom; }
}
