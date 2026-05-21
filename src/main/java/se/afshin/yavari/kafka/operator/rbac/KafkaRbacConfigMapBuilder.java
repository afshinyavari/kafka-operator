package se.afshin.yavari.kafka.operator.rbac;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import jakarta.enterprise.context.ApplicationScoped;
import se.afshin.yavari.kafka.operator.crd.KafkaRbac;
import se.afshin.yavari.kafka.operator.crd.KafkaRbacGroup;
import se.afshin.yavari.kafka.operator.crd.KafkaRbacUser;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class KafkaRbacConfigMapBuilder {

    /** Block-style YAML, no document marker, no extra quoting — matches the previous hand-rolled
     *  format closely enough that consumers (Kroxylicious RBAC filter, Apicurio policy engine)
     *  see the same shape, but with proper escaping of YAML-special chars in CR fields. */
    private static final ObjectMapper YAML = new ObjectMapper(
            new YAMLFactory()
                    .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
                    .enable(YAMLGenerator.Feature.MINIMIZE_QUOTES));

    public ConfigMap buildKafkaRules(KafkaRbac rbac, String namespace) {
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("groups", kafkaEntries(rbac.getSpec().getGroups(),
                KafkaRbacGroup::getName, KafkaRbacGroup::getKafka));
        root.put("users", kafkaEntries(rbac.getSpec().getUsers(),
                KafkaRbacUser::getName, KafkaRbacUser::getKafka));

        return new ConfigMapBuilder()
                .withNewMetadata()
                    .withName(rbac.getMetadata().getName() + "-kafka-rules")
                    .withNamespace(namespace)
                    .withOwnerReferences(ownerRef(rbac))
                .endMetadata()
                .withData(Map.of("rbac-rules.yaml", dump(root)))
                .build();
    }

    public ConfigMap buildApicurioPolicy(KafkaRbac rbac, String namespace) {
        List<Map<String, Object>> rules = new ArrayList<>();
        for (KafkaRbacGroup group : rbac.getSpec().getGroups()) {
            if (group.getSchemaRegistry() == null) continue;
            List<Map<String, Object>> resources = new ArrayList<>();
            for (String artifact : group.getSchemaRegistry().getArtifacts()) {
                Map<String, Object> r = new LinkedHashMap<>();
                r.put("artifact", artifact);
                r.put("actions", group.getSchemaRegistry().getActions());
                resources.add(r);
            }
            Map<String, Object> rule = new LinkedHashMap<>();
            rule.put("roles", List.of(group.getName()));
            rule.put("resources", resources);
            rules.add(rule);
        }
        Map<String, Object> root = Map.of("rules", rules);

        return new ConfigMapBuilder()
                .withNewMetadata()
                    .withName(rbac.getMetadata().getName() + "-apicurio-policy")
                    .withNamespace(namespace)
                    .withOwnerReferences(ownerRef(rbac))
                .endMetadata()
                .withData(Map.of("policy.yaml", dump(root)))
                .build();
    }

    private interface KafkaAccessFor<T> {
        se.afshin.yavari.kafka.operator.crd.KafkaRbacKafkaAccess apply(T t);
    }
    private interface NameOf<T> {
        String apply(T t);
    }

    private static <T> List<Map<String, Object>> kafkaEntries(List<T> source,
                                                              NameOf<T> nameOf,
                                                              KafkaAccessFor<T> kafkaOf) {
        List<Map<String, Object>> out = new ArrayList<>();
        if (source == null) return out;
        for (T item : source) {
            var kafka = kafkaOf.apply(item);
            if (kafka == null) continue;
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("name", nameOf.apply(item));
            entry.put("topics", kafka.getTopics());
            entry.put("operations", kafka.getOperations());
            out.add(entry);
        }
        return out;
    }

    private static String dump(Map<String, Object> root) {
        try {
            return YAML.writeValueAsString(root);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to render RBAC YAML", e);
        }
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
