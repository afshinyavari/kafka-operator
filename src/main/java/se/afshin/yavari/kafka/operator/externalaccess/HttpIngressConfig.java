package se.afshin.yavari.kafka.operator.externalaccess;

import java.util.Map;

/**
 * Ingress sub-config for HTTP services. {@code tlsSecretRef}, when set, causes the Ingress to
 * carry a {@code spec.tls[]} entry that terminates TLS at the controller.
 */
public class HttpIngressConfig {
    private String ingressClassName;
    private String tlsSecretRef;
    private Map<String, String> annotations = Map.of();

    public String getIngressClassName() { return ingressClassName; }
    public void setIngressClassName(String ingressClassName) { this.ingressClassName = ingressClassName; }

    public String getTlsSecretRef() { return tlsSecretRef; }
    public void setTlsSecretRef(String tlsSecretRef) { this.tlsSecretRef = tlsSecretRef; }

    public Map<String, String> getAnnotations() { return annotations; }
    public void setAnnotations(Map<String, String> annotations) {
        this.annotations = annotations != null ? annotations : Map.of();
    }
}
