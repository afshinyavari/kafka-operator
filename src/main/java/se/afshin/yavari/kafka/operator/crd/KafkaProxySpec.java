package se.afshin.yavari.kafka.operator.crd;

import java.util.List;

public class KafkaProxySpec {
    private String clusterRef;
    private String poolRef;
    private int replicas = 1;
    private String image;
    private int clientPort = 9094;
    private String rbacRef;
    private String apicurioRef;
    private KafkaProxyTlsConfig tls;
    private KafkaProxyOidcConfig oidc;
    private McsConfig mcs;
    private KafkaProxyFiltersConfig filters = new KafkaProxyFiltersConfig();
    private List<KafkaProxyCustomFilter> customFilters = List.of();
    private List<BrokerNodeIdRange> brokerNodeIdRanges = List.of();
    private List<String> targetClusters = List.of();
    private KafkaProxyExternalAccessConfig externalAccess;

    public String getClusterRef() { return clusterRef; }
    public void setClusterRef(String clusterRef) { this.clusterRef = clusterRef; }

    public String getPoolRef() { return poolRef; }
    public void setPoolRef(String poolRef) { this.poolRef = poolRef; }

    public int getReplicas() { return replicas; }
    public void setReplicas(int replicas) { this.replicas = replicas; }

    public String getImage() { return image; }
    public void setImage(String image) { this.image = image; }

    public int getClientPort() { return clientPort; }
    public void setClientPort(int clientPort) { this.clientPort = clientPort; }

    public String getRbacRef() { return rbacRef; }
    public void setRbacRef(String rbacRef) { this.rbacRef = rbacRef; }

    public String getApicurioRef() { return apicurioRef; }
    public void setApicurioRef(String apicurioRef) { this.apicurioRef = apicurioRef; }

    public KafkaProxyTlsConfig getTls() { return tls; }
    public void setTls(KafkaProxyTlsConfig tls) { this.tls = tls; }

    public KafkaProxyOidcConfig getOidc() { return oidc; }
    public void setOidc(KafkaProxyOidcConfig oidc) { this.oidc = oidc; }

    public McsConfig getMcs() { return mcs; }
    public void setMcs(McsConfig mcs) { this.mcs = mcs; }

    public KafkaProxyFiltersConfig getFilters() { return filters; }
    public void setFilters(KafkaProxyFiltersConfig filters) { this.filters = filters; }

    public List<KafkaProxyCustomFilter> getCustomFilters() { return customFilters; }
    public void setCustomFilters(List<KafkaProxyCustomFilter> customFilters) { this.customFilters = customFilters; }

    public List<BrokerNodeIdRange> getBrokerNodeIdRanges() { return brokerNodeIdRanges; }
    public void setBrokerNodeIdRanges(List<BrokerNodeIdRange> brokerNodeIdRanges) { this.brokerNodeIdRanges = brokerNodeIdRanges; }

    public List<String> getTargetClusters() { return targetClusters; }
    public void setTargetClusters(List<String> targetClusters) { this.targetClusters = targetClusters; }

    public KafkaProxyExternalAccessConfig getExternalAccess() { return externalAccess; }
    public void setExternalAccess(KafkaProxyExternalAccessConfig externalAccess) { this.externalAccess = externalAccess; }
}
