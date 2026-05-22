package se.afshin.yavari.kafka.operator.crd;

/**
 * Optional basic-auth on the Cruise Control REST API. Disabled by default — the REST API
 * is reachable only on an in-cluster ClusterIP Service, so v1 leaves it open unless the
 * operator explicitly opts in.
 */
public class CruiseControlApiSecurity {

    /** When true, Cruise Control's webserver requires basic auth. */
    private boolean enabled = false;

    /** Secret holding the Cruise Control {@code auth-credentials.properties} file
     *  (htpasswd-style {@code user: password, ROLE} lines) under key
     *  {@code auth-credentials.properties}. Required when {@code enabled} is true. */
    private String basicAuthSecretRef;

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }

    public String getBasicAuthSecretRef() { return basicAuthSecretRef; }
    public void setBasicAuthSecretRef(String basicAuthSecretRef) {
        this.basicAuthSecretRef = basicAuthSecretRef;
    }
}
