package se.afshin.yavari.kafka.operator.crd;

import io.fabric8.generator.annotation.Required;
import io.fabric8.generator.annotation.ValidationRule;

/** SASL credentials for an external endpoint. The referenced Secret must contain
 *  keys "username" and "password" (or "token" for OAUTHBEARER). */
public class KafkaEndpointSasl {

    /** SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER). */
    @Required
    @ValidationRule(value = "self in ['PLAIN', 'SCRAM-SHA-256', 'SCRAM-SHA-512', 'OAUTHBEARER']",
            message = "mechanism must be one of PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER")
    private String mechanism;

    /** Name of the Secret holding "username" and "password" keys (or an OAuth token for OAUTHBEARER). */
    @Required
    @ValidationRule(value = "self.size() > 0", message = "secretRef must not be blank")
    private String secretRef;

    public String getMechanism() { return mechanism; }
    public void setMechanism(String mechanism) { this.mechanism = mechanism; }

    public String getSecretRef() { return secretRef; }
    public void setSecretRef(String secretRef) { this.secretRef = secretRef; }
}
