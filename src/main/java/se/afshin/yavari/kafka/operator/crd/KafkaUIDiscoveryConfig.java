package se.afshin.yavari.kafka.operator.crd;

public class KafkaUIDiscoveryConfig {
    private String clusterNamespace = "kafka";
    private String proxyServiceName = "kafka-proxy";
    private int proxyPort = 9094;
    private String apicurioServiceName = "apicurio-rbac-proxy";
    private int apicurioPort = 8082;
    private String dnsSuffix = "";

    public String getClusterNamespace() { return clusterNamespace; }
    public void setClusterNamespace(String clusterNamespace) { this.clusterNamespace = clusterNamespace; }

    public String getProxyServiceName() { return proxyServiceName; }
    public void setProxyServiceName(String proxyServiceName) { this.proxyServiceName = proxyServiceName; }

    public int getProxyPort() { return proxyPort; }
    public void setProxyPort(int proxyPort) { this.proxyPort = proxyPort; }

    public String getApicurioServiceName() { return apicurioServiceName; }
    public void setApicurioServiceName(String apicurioServiceName) { this.apicurioServiceName = apicurioServiceName; }

    public int getApicurioPort() { return apicurioPort; }
    public void setApicurioPort(int apicurioPort) { this.apicurioPort = apicurioPort; }

    public String getDnsSuffix() { return dnsSuffix; }
    public void setDnsSuffix(String dnsSuffix) { this.dnsSuffix = dnsSuffix; }
}
