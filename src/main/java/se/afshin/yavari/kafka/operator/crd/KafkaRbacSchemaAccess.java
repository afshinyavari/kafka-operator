package se.afshin.yavari.kafka.operator.crd;

import java.util.List;

public class KafkaRbacSchemaAccess {
    private List<String> artifacts = List.of();
    private List<String> actions = List.of();

    public List<String> getArtifacts() { return artifacts; }
    public void setArtifacts(List<String> artifacts) { this.artifacts = artifacts; }

    public List<String> getActions() { return actions; }
    public void setActions(List<String> actions) { this.actions = actions; }
}
