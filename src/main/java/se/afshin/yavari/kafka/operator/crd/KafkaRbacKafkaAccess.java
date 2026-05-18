package se.afshin.yavari.kafka.operator.crd;

import java.util.List;

public class KafkaRbacKafkaAccess {
    private List<String> topics = List.of();
    private List<String> operations = List.of();

    public List<String> getTopics() { return topics; }
    public void setTopics(List<String> topics) { this.topics = topics; }

    public List<String> getOperations() { return operations; }
    public void setOperations(List<String> operations) { this.operations = operations; }
}
