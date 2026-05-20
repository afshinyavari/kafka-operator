package se.afshin.yavari.kafka.operator.crd;

public class KafkaUIProbesConfig {
    private KafkaUIProbeConfig readiness = new KafkaUIProbeConfig("/q/health/ready", 5, 5);
    private KafkaUIProbeConfig liveness = new KafkaUIProbeConfig("/q/health/live", 15, 10);

    public KafkaUIProbeConfig getReadiness() { return readiness; }
    public void setReadiness(KafkaUIProbeConfig readiness) { this.readiness = readiness; }

    public KafkaUIProbeConfig getLiveness() { return liveness; }
    public void setLiveness(KafkaUIProbeConfig liveness) { this.liveness = liveness; }
}
