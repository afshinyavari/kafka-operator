package se.afshin.yavari.kafka.operator.crd;

import se.afshin.yavari.kafka.operator.externalaccess.HttpExternalAccessConfig;

/**
 * Optional Apicurio Registry sub-spec on {@link KafkaClusterSpec}. Null = no registry.
 *
 * <p>Fields dropped vs. the old standalone {@code ApicurioRegistrySpec}: {@code mcs} +
 * {@code targetClusters} (derived from {@code spec.clusters[]}), {@code exportService}
 * (always-on when MCS is enabled, off otherwise).
 *
 * <p>The {@code storage.clusterRef} on the storage block is also dropped — the parent
 * KafkaCluster IS the cluster.
 */
public class KafkaClusterApicurioSpec {

    private String image = "quay.io/apicurio/apicurio-registry-kafkasql:latest-snapshot";
    private String rbacProxyImage;
    private String rbacRef;
    private int replicas = 1;
    private ApicurioRegistryOidcConfig oidc;
    private ApicurioRegistryStorageConfig storage = new ApicurioRegistryStorageConfig();
    private HttpExternalAccessConfig externalAccess;

    public String getImage() { return image; }
    public void setImage(String image) { this.image = image; }

    public String getRbacProxyImage() { return rbacProxyImage; }
    public void setRbacProxyImage(String rbacProxyImage) { this.rbacProxyImage = rbacProxyImage; }

    public String getRbacRef() { return rbacRef; }
    public void setRbacRef(String rbacRef) { this.rbacRef = rbacRef; }

    public int getReplicas() { return replicas; }
    public void setReplicas(int replicas) { this.replicas = replicas; }

    public ApicurioRegistryOidcConfig getOidc() { return oidc; }
    public void setOidc(ApicurioRegistryOidcConfig oidc) { this.oidc = oidc; }

    public ApicurioRegistryStorageConfig getStorage() { return storage; }
    public void setStorage(ApicurioRegistryStorageConfig storage) { this.storage = storage; }

    public HttpExternalAccessConfig getExternalAccess() { return externalAccess; }
    public void setExternalAccess(HttpExternalAccessConfig externalAccess) { this.externalAccess = externalAccess; }
}
