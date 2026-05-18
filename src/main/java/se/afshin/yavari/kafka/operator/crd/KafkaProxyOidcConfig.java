package se.afshin.yavari.kafka.operator.crd;

public class KafkaProxyOidcConfig {
    private String jwksEndpointUrl;
    private String groupsClaim = "realm_access.roles";

    public String getJwksEndpointUrl() { return jwksEndpointUrl; }
    public void setJwksEndpointUrl(String jwksEndpointUrl) { this.jwksEndpointUrl = jwksEndpointUrl; }

    public String getGroupsClaim() { return groupsClaim; }
    public void setGroupsClaim(String groupsClaim) { this.groupsClaim = groupsClaim; }
}
