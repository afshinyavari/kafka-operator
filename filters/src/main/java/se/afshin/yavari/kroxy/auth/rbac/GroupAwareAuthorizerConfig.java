package se.afshin.yavari.kroxy.auth.rbac;

import com.fasterxml.jackson.annotation.JsonProperty;

public class GroupAwareAuthorizerConfig {

    @JsonProperty(required = true)
    private String rulesFile;

    public String getRulesFile() { return rulesFile; }
    public void setRulesFile(String rulesFile) { this.rulesFile = rulesFile; }
}
