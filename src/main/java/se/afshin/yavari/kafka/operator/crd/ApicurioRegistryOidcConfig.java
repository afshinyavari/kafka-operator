package se.afshin.yavari.kafka.operator.crd;

public class ApicurioRegistryOidcConfig {
    private String issuerUrl;
    private String groupsClaim = "realm_access.roles";

    public String getIssuerUrl() { return issuerUrl; }
    public void setIssuerUrl(String issuerUrl) { this.issuerUrl = issuerUrl; }

    public String getGroupsClaim() { return groupsClaim; }
    public void setGroupsClaim(String groupsClaim) { this.groupsClaim = groupsClaim; }
}
