package se.afshin.yavari.kafka.operator.crd;

import io.fabric8.generator.annotation.Required;
import io.fabric8.generator.annotation.ValidationRule;

/** External (non-operator-managed) Kafka endpoint description. Used when an endpoint
 *  is outside this operator's control plane — e.g. Confluent Cloud, MSK, an on-prem
 *  cluster, or another operator instance. */
public class KafkaEndpointExternal {

    /** Kafka bootstrap servers (host:port[,host:port,...]). */
    @Required
    @ValidationRule(value = "self.size() > 0", message = "bootstrap must not be blank")
    private String bootstrap;

    /** Name of a Secret in the same namespace holding TLS material. Expected keys:
     *  tls.crt, tls.key, ca.crt (PEM). Optional — omit for PLAINTEXT or when SASL alone
     *  is sufficient. */
    private String tlsSecretRef;

    /** Optional SASL credentials. */
    private KafkaEndpointSasl sasl;

    /** Optional schema registry on this endpoint. */
    private KafkaEndpointSchemaRegistryRef schemaRegistry;

    public String getBootstrap() { return bootstrap; }
    public void setBootstrap(String bootstrap) { this.bootstrap = bootstrap; }

    public String getTlsSecretRef() { return tlsSecretRef; }
    public void setTlsSecretRef(String tlsSecretRef) { this.tlsSecretRef = tlsSecretRef; }

    public KafkaEndpointSasl getSasl() { return sasl; }
    public void setSasl(KafkaEndpointSasl sasl) { this.sasl = sasl; }

    public KafkaEndpointSchemaRegistryRef getSchemaRegistry() { return schemaRegistry; }
    public void setSchemaRegistry(KafkaEndpointSchemaRegistryRef schemaRegistry) { this.schemaRegistry = schemaRegistry; }
}
