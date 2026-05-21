package se.afshin.yavari.kafka.operator.crd;

import io.fabric8.generator.annotation.Required;
import io.fabric8.generator.annotation.ValidationRule;

/** External (non-operator-managed) Kafka endpoint description. Used when one end of an
 *  MM2 flow is outside this operator's control plane — e.g. Confluent Cloud, MSK, an
 *  on-prem cluster, or another operator instance. */
public class Mm2ExternalEndpoint {

    /** Kafka bootstrap servers (host:port[,host:port,...]). */
    @Required
    @ValidationRule(value = "self.size() > 0", message = "bootstrap must not be blank")
    private String bootstrap;

    /** Name of a Secret in the same namespace holding TLS material. Expected keys:
     *  tls.crt, tls.key, ca.crt (PEM). Optional — omit for PLAINTEXT or when SASL alone
     *  is sufficient. */
    private String tlsSecretRef;

    /** Optional SASL credentials. */
    private Mm2SaslConfig sasl;

    /** Optional schema registry on this endpoint. When unset, schema-sync is disabled
     *  even if spec.schemaSync.enabled is true (caller logs a warning). */
    private Mm2SchemaRegistryRef schemaRegistry;

    public String getBootstrap() { return bootstrap; }
    public void setBootstrap(String bootstrap) { this.bootstrap = bootstrap; }

    public String getTlsSecretRef() { return tlsSecretRef; }
    public void setTlsSecretRef(String tlsSecretRef) { this.tlsSecretRef = tlsSecretRef; }

    public Mm2SaslConfig getSasl() { return sasl; }
    public void setSasl(Mm2SaslConfig sasl) { this.sasl = sasl; }

    public Mm2SchemaRegistryRef getSchemaRegistry() { return schemaRegistry; }
    public void setSchemaRegistry(Mm2SchemaRegistryRef schemaRegistry) { this.schemaRegistry = schemaRegistry; }
}
