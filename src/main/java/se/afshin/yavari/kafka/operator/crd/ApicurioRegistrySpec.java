package se.afshin.yavari.kafka.operator.crd;

import se.afshin.yavari.kafka.operator.externalaccess.HttpExternalAccessConfig;

public class ApicurioRegistrySpec {
    private String image = "quay.io/apicurio/apicurio-registry-kafkasql:latest-snapshot";
    private String rbacProxyImage;
    private String rbacRef;
    private int replicas = 1;
    private ApicurioRegistryOidcConfig oidc;
    private ApicurioRegistryStorageConfig storage = new ApicurioRegistryStorageConfig();
    private boolean exportService = false;
    /** Optional. When set, the rbac-proxy Service is exposed externally. Null = internal-only. */
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

    public boolean isExportService() { return exportService; }
    public void setExportService(boolean exportService) { this.exportService = exportService; }

    public HttpExternalAccessConfig getExternalAccess() { return externalAccess; }
    public void setExternalAccess(HttpExternalAccessConfig externalAccess) { this.externalAccess = externalAccess; }
}
