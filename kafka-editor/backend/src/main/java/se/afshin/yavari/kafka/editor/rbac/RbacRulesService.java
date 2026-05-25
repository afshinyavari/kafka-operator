package se.afshin.yavari.kafka.editor.rbac;

import io.fabric8.kubernetes.client.KubernetesClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Reads the {@code KafkaRbac} CR for a given cluster and resolves what the
 * caller's groups can see. Read-only — final enforcement is the proxy's job.
 *
 * <p>Any Kafka topic-level grant ({@code READ}, {@code WRITE}, {@code DESCRIBE},
 * {@code ALL}, or {@code *}) makes the topic visible in the editor's listing —
 * the proxy still enforces fine-grained access for actual reads and writes.
 */
@ApplicationScoped
public class RbacRulesService {

    private static final Logger LOG = Logger.getLogger(RbacRulesService.class);
    private static final String WILDCARD = "*";

    @Inject
    KubernetesClient client;

    /**
     * Resolve the union of permissions across all {@code KafkaRbac} CRs in the
     * given namespace, filtered to the caller's groups.
     */
    public UserRbac forUser(String namespace, Set<String> userGroups) {
        List<KafkaRbacCr> rbacs = client.resources(KafkaRbacCr.class).inNamespace(namespace)
                .list().getItems();

        Set<String> topics = new HashSet<>();
        Set<String> schemas = new HashSet<>();
        boolean allTopics = false;
        boolean allSchemas = false;

        for (KafkaRbacCr rbac : rbacs) {
            KafkaRbacCr.Spec spec = rbac.getSpec();
            if (spec == null) continue;
            for (KafkaRbacCr.Group group : spec.getGroups()) {
                if (group.getName() == null || !userGroups.contains(group.getName())) continue;

                KafkaRbacCr.KafkaAccess kafka = group.getKafka();
                if (kafka != null && grantsRead(kafka.getOperations())) {
                    for (String t : kafka.getTopics()) {
                        if (WILDCARD.equals(t)) allTopics = true;
                        topics.add(t);
                    }
                }
                KafkaRbacCr.SchemaAccess schema = group.getSchemaRegistry();
                if (schema != null && grantsRead(schema.getActions())) {
                    for (String a : schema.getArtifacts()) {
                        if (WILDCARD.equals(a)) allSchemas = true;
                        schemas.add(a);
                    }
                }
            }
        }

        LOG.debugf("UserRbac for groups=%s in ns=%s → topics=%s schemas=%s",
                userGroups, namespace, topics, schemas);
        return new UserRbac(
                Set.copyOf(userGroups),
                Set.copyOf(topics),
                Set.copyOf(schemas),
                allTopics,
                allSchemas);
    }

    /**
     * Returns every distinct schema artifact name appearing in any group's
     * {@code schemaRegistry.artifacts} list across all {@code KafkaRbac} CRs,
     * excluding the wildcard. Used by the schemas page to enumerate when the
     * caller has wildcard schema access — the apicurio-rbac-proxy has no
     * list endpoint, so the UI iterates known names instead.
     */
    public Set<String> allKnownSchemaArtifacts(String namespace) {
        Set<String> out = new TreeSet<>();
        for (KafkaRbacCr rbac : client.resources(KafkaRbacCr.class).inNamespace(namespace)
                .list().getItems()) {
            if (rbac.getSpec() == null) continue;
            for (KafkaRbacCr.Group g : rbac.getSpec().getGroups()) {
                if (g.getSchemaRegistry() == null) continue;
                for (String a : g.getSchemaRegistry().getArtifacts()) {
                    if (a != null && !WILDCARD.equals(a)) out.add(a);
                }
            }
        }
        return out;
    }

    /** Any topic-level grant makes the topic visible in the editor's listing. */
    private static boolean grantsRead(List<String> ops) {
        for (String op : ops) {
            if (op == null) continue;
            switch (op.toUpperCase()) {
                case "READ", "WRITE", "DESCRIBE", "ALL", "*" -> {
                    return true;
                }
                default -> {}
            }
        }
        return false;
    }
}
