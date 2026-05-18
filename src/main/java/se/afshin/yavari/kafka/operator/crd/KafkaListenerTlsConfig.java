package se.afshin.yavari.kafka.operator.crd;

public class KafkaListenerTlsConfig {

    private boolean mutualTls;

    public boolean isMutualTls() { return mutualTls; }
    public void setMutualTls(boolean mutualTls) { this.mutualTls = mutualTls; }
}
