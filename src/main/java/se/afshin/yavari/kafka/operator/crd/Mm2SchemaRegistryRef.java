package se.afshin.yavari.kafka.operator.crd;

import io.fabric8.generator.annotation.Required;
import io.fabric8.generator.annotation.ValidationRule;

/** Reference to a schema registry on an external endpoint. For managed endpoints,
 *  the schema registry is derived from the referenced KafkaCluster's spec.apicurio. */
public class Mm2SchemaRegistryRef {

    @Required
    @ValidationRule(value = "self.size() > 0", message = "url must not be blank")
    private String url;

    /** Wire format. v1 supports APICURIO only; CONFLUENT is rejected at reconcile time. */
    private SchemaRegistryType type = SchemaRegistryType.APICURIO;

    /** Optional Secret holding HTTP auth for the registry. Keys "username" + "password"
     *  for basic auth, or "token" for bearer. */
    private String authSecretRef;

    public String getUrl() { return url; }
    public void setUrl(String url) { this.url = url; }

    public SchemaRegistryType getType() { return type; }
    public void setType(SchemaRegistryType type) { this.type = type; }

    public String getAuthSecretRef() { return authSecretRef; }
    public void setAuthSecretRef(String authSecretRef) { this.authSecretRef = authSecretRef; }
}
