package se.afshin.yavari.kafka.operator.crd;

import io.fabric8.generator.annotation.Required;
import io.fabric8.generator.annotation.ValidationRule;

/** Reference to a {@link KafkaConnect} CR. Object-shaped from day one so a future
 *  {@code namespace} field can be added without a spec break. v1 enforces same-namespace
 *  at reconcile time. */
public class KafkaConnectClusterRef {

    @Required
    @ValidationRule(value = "self.size() > 0", message = "name must not be blank")
    private String name;

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
}
