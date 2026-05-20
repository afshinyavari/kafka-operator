package se.afshin.yavari.kafka.operator.crd;

public class KafkaUIProbeConfig {
    private String path;
    private int initialDelaySeconds;
    private int periodSeconds;

    public KafkaUIProbeConfig() {}

    public KafkaUIProbeConfig(String path, int initialDelaySeconds, int periodSeconds) {
        this.path = path;
        this.initialDelaySeconds = initialDelaySeconds;
        this.periodSeconds = periodSeconds;
    }

    public String getPath() { return path; }
    public void setPath(String path) { this.path = path; }

    public int getInitialDelaySeconds() { return initialDelaySeconds; }
    public void setInitialDelaySeconds(int initialDelaySeconds) { this.initialDelaySeconds = initialDelaySeconds; }

    public int getPeriodSeconds() { return periodSeconds; }
    public void setPeriodSeconds(int periodSeconds) { this.periodSeconds = periodSeconds; }
}
