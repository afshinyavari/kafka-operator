package se.afshin.yavari.kafka.operator.crd;

import io.fabric8.generator.annotation.Required;

/** Pulls connector configuration from a Secret. Keys from the Secret are merged into
 *  the rendered Connect config <em>in the operator</em> (resolved before PUT to Connect
 *  REST), so the user can keep credentials out of the CR YAML.
 *
 *  <p>All keys sourced from the Secret are treated as sensitive (operator does not
 *  re-PUT on every reconcile when Connect masks them — drift is detected via the
 *  {@code observedConfigHash} cache; see ConnectorDriftDetector). */
public class KafkaConnectorConfigFromSource {

    @Required
    private String secretRef;

    /** Optional prefix applied to each key from the Secret (e.g. "database." so the
     *  Secret key {@code password} maps to connector config key {@code database.password}). */
    private String prefix;

    public String getSecretRef() { return secretRef; }
    public void setSecretRef(String secretRef) { this.secretRef = secretRef; }

    public String getPrefix() { return prefix; }
    public void setPrefix(String prefix) { this.prefix = prefix; }
}
