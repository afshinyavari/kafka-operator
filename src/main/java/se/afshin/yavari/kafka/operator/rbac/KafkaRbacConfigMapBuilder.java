package se.afshin.yavari.kafka.operator.rbac;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import jakarta.enterprise.context.ApplicationScoped;
import se.afshin.yavari.kafka.operator.crd.KafkaRbac;
import se.afshin.yavari.kafka.operator.crd.KafkaRbacGroup;
import se.afshin.yavari.kafka.operator.crd.KafkaRbacUser;

import java.util.List;
import java.util.Map;

@ApplicationScoped
public class KafkaRbacConfigMapBuilder {

    public ConfigMap buildKafkaRules(KafkaRbac rbac, String namespace) {
        StringBuilder yaml = new StringBuilder();
        yaml.append("groups:\n");
        for (KafkaRbacGroup group : rbac.getSpec().getGroups()) {
            if (group.getKafka() == null) continue;
            yaml.append("  - name: ").append(group.getName()).append("\n");
            yaml.append("    topics:\n");
            group.getKafka().getTopics().forEach(t -> yaml.append("      - ").append(t).append("\n"));
            yaml.append("    operations:\n");
            group.getKafka().getOperations().forEach(o -> yaml.append("      - ").append(o).append("\n"));
        }
        yaml.append("users:\n");
        for (KafkaRbacUser user : rbac.getSpec().getUsers()) {
            if (user.getKafka() == null) continue;
            yaml.append("  - name: ").append(user.getName()).append("\n");
            yaml.append("    topics:\n");
            user.getKafka().getTopics().forEach(t -> yaml.append("      - ").append(t).append("\n"));
            yaml.append("    operations:\n");
            user.getKafka().getOperations().forEach(o -> yaml.append("      - ").append(o).append("\n"));
        }

        return new ConfigMapBuilder()
                .withNewMetadata()
                    .withName(rbac.getMetadata().getName() + "-kafka-rules")
                    .withNamespace(namespace)
                    .withOwnerReferences(ownerRef(rbac))
                .endMetadata()
                .withData(Map.of("rbac-rules.yaml", yaml.toString()))
                .build();
    }

    public ConfigMap buildApicurioPolicy(KafkaRbac rbac, String namespace) {
        StringBuilder yaml = new StringBuilder("rules:\n");
        for (KafkaRbacGroup group : rbac.getSpec().getGroups()) {
            if (group.getSchemaRegistry() == null) continue;
            yaml.append("  - roles:\n");
            yaml.append("      - ").append(group.getName()).append("\n");
            yaml.append("    resources:\n");
            for (String artifact : group.getSchemaRegistry().getArtifacts()) {
                // Quote the artifact value to handle YAML special chars like '*'
                String quotedArtifact = artifact.contains("*") ? "'" + artifact + "'" : artifact;
                yaml.append("      - artifact: ").append(quotedArtifact).append("\n");
                yaml.append("        actions:\n");
                group.getSchemaRegistry().getActions().forEach(a -> yaml.append("          - ").append(a).append("\n"));
            }
        }

        return new ConfigMapBuilder()
                .withNewMetadata()
                    .withName(rbac.getMetadata().getName() + "-apicurio-policy")
                    .withNamespace(namespace)
                    .withOwnerReferences(ownerRef(rbac))
                .endMetadata()
                .withData(Map.of("policy.yaml", yaml.toString()))
                .build();
    }

    private List<OwnerReference> ownerRef(KafkaRbac rbac) {
        return List.of(new OwnerReferenceBuilder()
                .withApiVersion(rbac.getApiVersion())
                .withKind(rbac.getKind())
                .withName(rbac.getMetadata().getName())
                .withUid(rbac.getMetadata().getUid())
                .withController(true)
                .withBlockOwnerDeletion(true)
                .build());
    }
}
