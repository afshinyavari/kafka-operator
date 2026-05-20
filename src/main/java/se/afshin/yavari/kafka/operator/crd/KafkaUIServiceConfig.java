package se.afshin.yavari.kafka.operator.crd;

public class KafkaUIServiceConfig {
    private String type = "NodePort";
    private int port = 8080;
    private Integer nodePort = 30808;

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }

    public int getPort() { return port; }
    public void setPort(int port) { this.port = port; }

    public Integer getNodePort() { return nodePort; }
    public void setNodePort(Integer nodePort) { this.nodePort = nodePort; }
}
