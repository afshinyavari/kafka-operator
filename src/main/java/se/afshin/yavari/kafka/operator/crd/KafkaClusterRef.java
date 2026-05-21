package se.afshin.yavari.kafka.operator.crd;

import io.fabric8.generator.annotation.Required;
import io.fabric8.generator.annotation.ValidationRule;

/** Reference to a KafkaCluster CR. Namespace is optional — defaults to the referencing CR's namespace. */
public class KafkaClusterRef {

    @Required
    @ValidationRule(value = "self.size() > 0", message = "name must not be blank")
    private String name;

    private String namespace;

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getNamespace() { return namespace; }
    public void setNamespace(String namespace) { this.namespace = namespace; }
}
