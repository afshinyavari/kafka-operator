package se.afshin.yavari.kroxy.auth.oauth;

import com.fasterxml.jackson.annotation.JsonProperty;

public class JwtGroupFilterConfig {

    @JsonProperty(required = true)
    private String groupsClaim;

    public String getGroupsClaim() { return groupsClaim; }
    public void setGroupsClaim(String groupsClaim) { this.groupsClaim = groupsClaim; }
}
