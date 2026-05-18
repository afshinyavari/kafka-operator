package se.afshin.yavari.kafka.operator.crd;

public class ApicurioRegistrySpec {
    private String image = "quay.io/apicurio/apicurio-registry-mem:latest-snapshot";
    private String rbacProxyImage;
    private String rbacRef;
    private int replicas = 1;
    private ApicurioRegistryOidcConfig oidc;
    private ApicurioRegistryStorageConfig storage = new ApicurioRegistryStorageConfig();
    private boolean exportService = false;

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

    public boolean isExportService() { return exportService; }
    public void setExportService(boolean exportService) { this.exportService = exportService; }
}
