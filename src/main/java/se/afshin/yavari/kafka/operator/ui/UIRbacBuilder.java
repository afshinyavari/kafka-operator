package se.afshin.yavari.kafka.operator.ui;

import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.ServiceAccountBuilder;
import io.fabric8.kubernetes.api.model.rbac.PolicyRuleBuilder;
import io.fabric8.kubernetes.api.model.rbac.Role;
import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.fabric8.kubernetes.api.model.rbac.RoleBindingBuilder;
import io.fabric8.kubernetes.api.model.rbac.RoleBuilder;
import jakarta.enterprise.context.ApplicationScoped;
import se.afshin.yavari.kafka.operator.crd.KafkaUI;

import java.util.List;

@ApplicationScoped
public class UIRbacBuilder {

    public ServiceAccount serviceAccount(KafkaUI ui, OwnerReference ownerRef) {
        return new ServiceAccountBuilder()
                .withNewMetadata()
                    .withName(ui.getMetadata().getName())
                    .withNamespace(ui.getMetadata().getNamespace())
                    .withLabels(UILabels.labels(ui.getMetadata().getName()))
                    .withOwnerReferences(ownerRef)
                .endMetadata()
                .build();
    }

    public Role role(KafkaUI ui, OwnerReference ownerRef) {
        return new RoleBuilder()
                .withNewMetadata()
                    .withName(ui.getMetadata().getName())
                    .withNamespace(ui.getMetadata().getNamespace())
                    .withLabels(UILabels.labels(ui.getMetadata().getName()))
                    .withOwnerReferences(ownerRef)
                .endMetadata()
                .withRules(new PolicyRuleBuilder()
                        .withApiGroups("kafka.yavari.afshin.se")
                        .withResources("kafkaclusters", "kafkarbacs")
                        .withVerbs("get", "list", "watch")
                        .build())
                .build();
    }

    public RoleBinding roleBinding(KafkaUI ui, OwnerReference ownerRef) {
        String name = ui.getMetadata().getName();
        String namespace = ui.getMetadata().getNamespace();
        return new RoleBindingBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(namespace)
                    .withLabels(UILabels.labels(name))
                    .withOwnerReferences(ownerRef)
                .endMetadata()
                .withSubjects(List.of(new io.fabric8.kubernetes.api.model.rbac.SubjectBuilder()
                        .withKind("ServiceAccount")
                        .withName(name)
                        .withNamespace(namespace)
                        .build()))
                .withNewRoleRef()
                    .withApiGroup("rbac.authorization.k8s.io")
                    .withKind("Role")
                    .withName(name)
                .endRoleRef()
                .build();
    }
}
