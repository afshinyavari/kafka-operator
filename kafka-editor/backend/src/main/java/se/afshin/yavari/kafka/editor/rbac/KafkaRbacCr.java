package se.afshin.yavari.kafka.editor.rbac;

import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Kind;
import io.fabric8.kubernetes.model.annotation.Plural;
import io.fabric8.kubernetes.model.annotation.Singular;
import io.fabric8.kubernetes.model.annotation.Version;

/**
 * Local mirror of the operator's KafkaRbac CRD. Spec fields that the editor reads
 * are modelled; everything else is ignored via Jackson's unknown-property handling.
 */
@Group("kafka.yavari.afshin.se")
@Version("v1alpha1")
@Kind("KafkaRbac")
@Plural("kafkarbacs")
@Singular("kafkarbac")
public class KafkaRbacCr extends CustomResource<KafkaRbacCr.Spec, Void> implements Namespaced {

    public static class Spec {
        private java.util.List<Group> groups = java.util.List.of();
        private java.util.List<User> users = java.util.List.of();

        public java.util.List<Group> getGroups() { return groups; }
        public void setGroups(java.util.List<Group> groups) { this.groups = groups == null ? java.util.List.of() : groups; }

        public java.util.List<User> getUsers() { return users; }
        public void setUsers(java.util.List<User> users) { this.users = users == null ? java.util.List.of() : users; }
    }

    public static class Group {
        private String name;
        private KafkaAccess kafka;
        private SchemaAccess schemaRegistry;

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }

        public KafkaAccess getKafka() { return kafka; }
        public void setKafka(KafkaAccess kafka) { this.kafka = kafka; }

        public SchemaAccess getSchemaRegistry() { return schemaRegistry; }
        public void setSchemaRegistry(SchemaAccess schemaRegistry) { this.schemaRegistry = schemaRegistry; }
    }

    public static class User {
        private String name;
        private KafkaAccess kafka;

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }

        public KafkaAccess getKafka() { return kafka; }
        public void setKafka(KafkaAccess kafka) { this.kafka = kafka; }
    }

    public static class KafkaAccess {
        private java.util.List<String> topics = java.util.List.of();
        private java.util.List<String> operations = java.util.List.of();

        public java.util.List<String> getTopics() { return topics; }
        public void setTopics(java.util.List<String> topics) { this.topics = topics == null ? java.util.List.of() : topics; }

        public java.util.List<String> getOperations() { return operations; }
        public void setOperations(java.util.List<String> operations) { this.operations = operations == null ? java.util.List.of() : operations; }
    }

    public static class SchemaAccess {
        private java.util.List<String> artifacts = java.util.List.of();
        private java.util.List<String> actions = java.util.List.of();

        public java.util.List<String> getArtifacts() { return artifacts; }
        public void setArtifacts(java.util.List<String> artifacts) { this.artifacts = artifacts == null ? java.util.List.of() : artifacts; }

        public java.util.List<String> getActions() { return actions; }
        public void setActions(java.util.List<String> actions) { this.actions = actions == null ? java.util.List.of() : actions; }
    }
}
