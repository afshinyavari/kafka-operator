package se.afshin.yavari.kafka.operator.externalaccess;

/**
 * Gateway API parent reference for an HTTP {@code HTTPRoute}. {@code tlsSecretRef} is informational
 * only — the actual TLS termination happens on the parent {@code Gateway} listener; this field
 * is here so the CR can record which Secret backs that listener for documentation/discovery.
 */
public class HttpGatewayConfig {
    private String parentGatewayName;
    private String parentGatewayNamespace;
    private String sectionName;
    private String tlsSecretRef;

    public String getParentGatewayName() { return parentGatewayName; }
    public void setParentGatewayName(String parentGatewayName) { this.parentGatewayName = parentGatewayName; }

    public String getParentGatewayNamespace() { return parentGatewayNamespace; }
    public void setParentGatewayNamespace(String parentGatewayNamespace) { this.parentGatewayNamespace = parentGatewayNamespace; }

    public String getSectionName() { return sectionName; }
    public void setSectionName(String sectionName) { this.sectionName = sectionName; }

    public String getTlsSecretRef() { return tlsSecretRef; }
    public void setTlsSecretRef(String tlsSecretRef) { this.tlsSecretRef = tlsSecretRef; }
}
