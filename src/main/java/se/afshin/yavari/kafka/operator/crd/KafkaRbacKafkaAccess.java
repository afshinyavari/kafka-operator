package se.afshin.yavari.kafka.operator.crd;

import java.util.List;

public class KafkaRbacKafkaAccess {

    // Wave 2 added a CEL rule barring '*' in topics, but the CRD generator's
    // x-kubernetes-validations cost budget rejected it at apply time (the rule
    // wasn't bounded by maxItems on the list, so K8s assumed worst-case). YAML
    // emitter from Wave 1 (#6) still escapes injection via the policy YAML.
    // Re-introduce as an admission webhook or with maxItems support when fabric8
    // exposes it.
    private List<String> topics = List.of();

    private List<String> operations = List.of();

    public List<String> getTopics() { return topics; }
    public void setTopics(List<String> topics) { this.topics = topics; }

    public List<String> getOperations() { return operations; }
    public void setOperations(List<String> operations) { this.operations = operations; }
}
