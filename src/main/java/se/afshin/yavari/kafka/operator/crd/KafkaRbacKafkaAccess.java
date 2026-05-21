package se.afshin.yavari.kafka.operator.crd;

import io.fabric8.generator.annotation.ValidationRule;

import java.util.List;

public class KafkaRbacKafkaAccess {

    @ValidationRule(value = "self.all(t, t != '*')",
            message = "Wildcard '*' is not allowed in topics. Enumerate topic names explicitly "
                    + "to avoid accidentally granting cluster-wide access.")
    private List<String> topics = List.of();

    private List<String> operations = List.of();

    public List<String> getTopics() { return topics; }
    public void setTopics(List<String> topics) { this.topics = topics; }

    public List<String> getOperations() { return operations; }
    public void setOperations(List<String> operations) { this.operations = operations; }
}
