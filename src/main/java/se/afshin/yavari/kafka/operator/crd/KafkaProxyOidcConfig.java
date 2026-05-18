package se.afshin.yavari.kafka.operator.crd;

public class KafkaProxyOidcConfig {
    private String jwksEndpointUrl;
    private String groupsClaim = "realm_access.roles";
    private String expectedIssuer;
    private String expectedAudience;

    public String getJwksEndpointUrl() { return jwksEndpointUrl; }
    public void setJwksEndpointUrl(String jwksEndpointUrl) { this.jwksEndpointUrl = jwksEndpointUrl; }

    public String getGroupsClaim() { return groupsClaim; }
    public void setGroupsClaim(String groupsClaim) { this.groupsClaim = groupsClaim; }

    public String getExpectedIssuer() { return expectedIssuer; }
    public void setExpectedIssuer(String expectedIssuer) { this.expectedIssuer = expectedIssuer; }

    public String getExpectedAudience() { return expectedAudience; }
    public void setExpectedAudience(String expectedAudience) { this.expectedAudience = expectedAudience; }
}
