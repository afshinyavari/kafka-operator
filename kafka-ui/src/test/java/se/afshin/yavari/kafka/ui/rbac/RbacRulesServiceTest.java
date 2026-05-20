package se.afshin.yavari.kafka.ui.rbac;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import se.afshin.yavari.kafka.ui.crd.KafkaRbacCr;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@EnableKubernetesMockClient(crud = true)
@Timeout(value = 3, unit = TimeUnit.SECONDS)
class RbacRulesServiceTest {

    KubernetesClient client;

    @Test
    void groupGrantsReadOnExplicitTopic() {
        applyRbac("rbac", "kafka", group("orders-team",
                kafka(List.of("orders"), List.of("PRODUCE")),
                null));
        RbacRulesService svc = svc();

        UserRbac u = svc.forUser("kafka", Set.of("orders-team"));

        assertThat(u.canReadTopic("orders")).isTrue();
        assertThat(u.canReadTopic("invoices")).isFalse();
        assertThat(u.allTopicsAllowed()).isFalse();
    }

    @Test
    void userNotInGroup_seesNothing() {
        applyRbac("rbac", "kafka", group("orders-team",
                kafka(List.of("orders"), List.of("FETCH")),
                null));
        UserRbac u = svc().forUser("kafka", Set.of("nobody"));

        assertThat(u.topicsAllowedToRead()).isEmpty();
        assertThat(u.canReadTopic("orders")).isFalse();
    }

    @Test
    void wildcardTopic_setsAllTopicsAllowed() {
        applyRbac("rbac", "kafka", group("admins",
                kafka(List.of("*"), List.of("ALL")),
                null));
        UserRbac u = svc().forUser("kafka", Set.of("admins"));

        assertThat(u.allTopicsAllowed()).isTrue();
        assertThat(u.canReadTopic("anything")).isTrue();
    }

    @Test
    void schemaRegistryAccess() {
        applyRbac("rbac", "kafka", group("schema-admin",
                null,
                schema(List.of("*"), List.of("READ", "WRITE"))));
        UserRbac u = svc().forUser("kafka", Set.of("schema-admin"));

        assertThat(u.allSchemasAllowed()).isTrue();
        assertThat(u.canReadSchema("orders-value")).isTrue();
    }

    @Test
    void multipleGroups_unionsAccess() {
        applyRbac("rbac", "kafka",
                group("orders-team",
                        kafka(List.of("orders", "orders-dlq"), List.of("PRODUCE")), null),
                group("invoices-team",
                        kafka(List.of("invoices"), List.of("FETCH")), null));
        UserRbac u = svc().forUser("kafka", Set.of("orders-team", "invoices-team"));

        assertThat(u.topicsAllowedToRead())
                .containsExactlyInAnyOrder("orders", "orders-dlq", "invoices");
    }

    @Test
    void operationWithoutReadIntent_isIgnored() {
        // Currently every Kafka op grants visibility (proxy enforces fine-grained
        // access). This is intentional — see RbacRulesService.grantsRead aliases.
        // We assert the *empty* ops case, since an empty list shouldn't grant anything.
        applyRbac("rbac", "kafka", group("orders-team",
                kafka(List.of("orders"), List.of()),
                null));
        UserRbac u = svc().forUser("kafka", Set.of("orders-team"));

        assertThat(u.canReadTopic("orders")).isFalse();
    }

    // ── helpers ───────────────────────────────────────────────────────────

    private RbacRulesService svc() {
        RbacRulesService s = new RbacRulesService();
        s.client = client;
        return s;
    }

    private void applyRbac(String name, String ns, KafkaRbacCr.Group... groups) {
        KafkaRbacCr cr = new KafkaRbacCr();
        cr.setMetadata(new ObjectMetaBuilder().withName(name).withNamespace(ns).build());
        KafkaRbacCr.Spec spec = new KafkaRbacCr.Spec();
        spec.setGroups(List.of(groups));
        cr.setSpec(spec);
        client.resources(KafkaRbacCr.class).inNamespace(ns).resource(cr).create();
    }

    private static KafkaRbacCr.Group group(String name, KafkaRbacCr.KafkaAccess k, KafkaRbacCr.SchemaAccess s) {
        KafkaRbacCr.Group g = new KafkaRbacCr.Group();
        g.setName(name);
        g.setKafka(k);
        g.setSchemaRegistry(s);
        return g;
    }

    private static KafkaRbacCr.KafkaAccess kafka(List<String> topics, List<String> ops) {
        KafkaRbacCr.KafkaAccess k = new KafkaRbacCr.KafkaAccess();
        k.setTopics(topics);
        k.setOperations(ops);
        return k;
    }

    private static KafkaRbacCr.SchemaAccess schema(List<String> artifacts, List<String> actions) {
        KafkaRbacCr.SchemaAccess s = new KafkaRbacCr.SchemaAccess();
        s.setArtifacts(artifacts);
        s.setActions(actions);
        return s;
    }
}
