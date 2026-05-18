package se.afshin.yavari.kafka.operator.crd;

import java.util.List;

public class KafkaRbacSpec {
    private List<KafkaRbacGroup> groups = List.of();
    private List<KafkaRbacUser> users = List.of();

    public List<KafkaRbacGroup> getGroups() { return groups; }
    public void setGroups(List<KafkaRbacGroup> groups) { this.groups = groups; }

    public List<KafkaRbacUser> getUsers() { return users; }
    public void setUsers(List<KafkaRbacUser> users) { this.users = users; }
}
