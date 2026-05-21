package se.afshin.yavari.kafka.operator.crd;

import java.util.List;

/**
 * Kroxylicious proxy sub-spec on {@link KafkaClusterSpec}. The proxy is mandatory in the
 * post-Wave-4 schema: every KafkaCluster has exactly one proxy, owned by the cluster.
 *
 * <p>Fields dropped vs. the old standalone {@code KafkaProxySpec}: {@code clusterRef} (the
 * parent IS the cluster), {@code poolRef}/{@code brokerNodeIdRanges} (derived from
 * {@code spec.clusters[]} + the operator's deterministic node-id scheme),
 * {@code targetClusters}/{@code mcs} (derived from {@code spec.clusters[]}).
 */
public class KafkaClusterProxySpec {

    private int replicas = 1;
    private String image;
    private int clientPort = 9094;
    private String rbacRef;
    private String apicurioRef;
    private KafkaProxyTlsConfig tls;
    private KafkaProxyOidcConfig oidc;
    private KafkaProxyFiltersConfig filters = new KafkaProxyFiltersConfig();
    private List<KafkaProxyCustomFilter> customFilters = List.of();
    private KafkaProxyExternalAccessConfig externalAccess;

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

    public KafkaProxyFiltersConfig getFilters() { return filters; }
    public void setFilters(KafkaProxyFiltersConfig filters) { this.filters = filters; }

    public List<KafkaProxyCustomFilter> getCustomFilters() { return customFilters; }
    public void setCustomFilters(List<KafkaProxyCustomFilter> customFilters) { this.customFilters = customFilters; }

    public KafkaProxyExternalAccessConfig getExternalAccess() { return externalAccess; }
    public void setExternalAccess(KafkaProxyExternalAccessConfig externalAccess) { this.externalAccess = externalAccess; }
}
