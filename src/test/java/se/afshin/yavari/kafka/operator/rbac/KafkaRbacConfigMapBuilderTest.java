package se.afshin.yavari.kafka.operator.rbac;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.KafkaRbac;
import se.afshin.yavari.kafka.operator.crd.KafkaRbacGroup;
import se.afshin.yavari.kafka.operator.crd.KafkaRbacKafkaAccess;
import se.afshin.yavari.kafka.operator.crd.KafkaRbacSchemaAccess;
import se.afshin.yavari.kafka.operator.crd.KafkaRbacSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaRbacUser;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaRbacConfigMapBuilderTest {

    private static final String NS = "kafka";
    private static final String RBAC_NAME = "my-rbac";

    private KafkaRbacConfigMapBuilder builder;

    @BeforeEach
    void setup() {
        builder = new KafkaRbacConfigMapBuilder();
    }

    @Test
    void buildKafkaRules_groupWithKafka_included() {
        KafkaRbac rbac = rbac(List.of(groupWithKafka("admins", List.of("*"), List.of("READ", "WRITE"))), List.of());

        ConfigMap cm = builder.buildKafkaRules(rbac, NS);
        String yaml = cm.getData().get("rbac-rules.yaml");

        assertThat(yaml).contains("- name: admins");
        // YAML emitter quotes wildcards as '*' — semantically the same string scalar.
        assertThat(yaml).contains("- '*'");
        assertThat(yaml).contains("- READ");
        assertThat(yaml).contains("- WRITE");
    }

    @Test
    void buildKafkaRules_groupWithoutKafka_skipped() {
        KafkaRbacGroup g = new KafkaRbacGroup();
        g.setName("readers");
        // kafka is null
        KafkaRbac rbac = rbac(List.of(g), List.of());

        ConfigMap cm = builder.buildKafkaRules(rbac, NS);
        String yaml = cm.getData().get("rbac-rules.yaml");

        assertThat(yaml).doesNotContain("readers");
    }

    @Test
    void buildKafkaRules_userWithKafka_included() {
        KafkaRbac rbac = rbac(List.of(), List.of(userWithKafka("alice", List.of("my-topic"), List.of("READ"))));

        ConfigMap cm = builder.buildKafkaRules(rbac, NS);
        String yaml = cm.getData().get("rbac-rules.yaml");

        assertThat(yaml).contains("- name: alice");
        assertThat(yaml).contains("- my-topic");
        assertThat(yaml).contains("- READ");
    }

    @Test
    void buildKafkaRules_cmNameAndKey() {
        KafkaRbac rbac = rbac(List.of(), List.of());

        ConfigMap cm = builder.buildKafkaRules(rbac, NS);

        assertThat(cm.getMetadata().getName()).isEqualTo(RBAC_NAME + "-kafka-rules");
        assertThat(cm.getMetadata().getNamespace()).isEqualTo(NS);
        assertThat(cm.getData()).containsKey("rbac-rules.yaml");
        assertThat(cm.getMetadata().getOwnerReferences()).hasSize(1);
    }

    @Test
    void buildKafkaRules_groupWithFetch_injectsConsumerOffsetsTopic() {
        // A consumer-group consume hangs (no DENY in proxy log) when the rendered rule
        // doesn't grant FETCH on __consumer_offsets — the Kroxylicious AuthorizationFilter
        // silently filters the broker's offset response. The builder must inject the
        // internal topic whenever FETCH is in the operations list.
        KafkaRbac rbac = rbac(List.of(groupWithKafka("orders-team", List.of("orders"), List.of("PRODUCE", "FETCH"))), List.of());

        String yaml = builder.buildKafkaRules(rbac, NS).getData().get("rbac-rules.yaml");

        assertThat(yaml).contains("- orders");
        assertThat(yaml).contains("- __consumer_offsets");
    }

    @Test
    void buildKafkaRules_groupWithProduceOnly_doesNotInjectConsumerOffsets() {
        // PRODUCE-only roles have no consumer-group state, so no injection.
        KafkaRbac rbac = rbac(List.of(groupWithKafka("orders-prod", List.of("orders"), List.of("PRODUCE"))), List.of());

        String yaml = builder.buildKafkaRules(rbac, NS).getData().get("rbac-rules.yaml");

        assertThat(yaml).doesNotContain("__consumer_offsets");
    }

    @Test
    void buildKafkaRules_userWithFetch_injectsConsumerOffsetsTopic() {
        // Same logic for user (mTLS CN) rules.
        KafkaRbac rbac = rbac(List.of(), List.of(userWithKafka("alice", List.of("orders"), List.of("FETCH"))));

        String yaml = builder.buildKafkaRules(rbac, NS).getData().get("rbac-rules.yaml");

        assertThat(yaml).contains("- __consumer_offsets");
    }

    @Test
    void buildKafkaRules_wildcardTopic_doesNotDuplicateConsumerOffsets() {
        // '*' already covers everything; injecting __consumer_offsets alongside would
        // produce a duplicate entry the YAML reader-side could trip over.
        KafkaRbac rbac = rbac(List.of(groupWithKafka("any", List.of("*"), List.of("FETCH"))), List.of());

        String yaml = builder.buildKafkaRules(rbac, NS).getData().get("rbac-rules.yaml");

        assertThat(yaml).doesNotContain("__consumer_offsets");
    }

    @Test
    void buildApicurioPolicy_groupWithSchemaRegistry_included() {
        KafkaRbacGroup g = groupWithSchemaRegistry("devs", List.of("my-schema"), List.of("READ"));
        KafkaRbac rbac = rbac(List.of(g), List.of());

        ConfigMap cm = builder.buildApicurioPolicy(rbac, NS);
        String yaml = cm.getData().get("policy.yaml");

        assertThat(yaml).contains("- devs");
        assertThat(yaml).contains("artifact: my-schema");
        assertThat(yaml).contains("- READ");
    }

    @Test
    void buildApicurioPolicy_wildcardArtifact_quoted() {
        KafkaRbacGroup g = groupWithSchemaRegistry("all", List.of("*"), List.of("READ"));
        KafkaRbac rbac = rbac(List.of(g), List.of());

        ConfigMap cm = builder.buildApicurioPolicy(rbac, NS);
        String yaml = cm.getData().get("policy.yaml");

        assertThat(yaml).contains("artifact: '*'");
    }

    @Test
    void buildApicurioPolicy_nonWildcard_notQuoted() {
        KafkaRbacGroup g = groupWithSchemaRegistry("team", List.of("orders-schema"), List.of("READ"));
        KafkaRbac rbac = rbac(List.of(g), List.of());

        ConfigMap cm = builder.buildApicurioPolicy(rbac, NS);
        String yaml = cm.getData().get("policy.yaml");

        assertThat(yaml).contains("artifact: orders-schema");
        assertThat(yaml).doesNotContain("artifact: 'orders-schema'");
    }

    @Test
    void buildApicurioPolicy_cmNameAndKey() {
        KafkaRbac rbac = rbac(List.of(), List.of());

        ConfigMap cm = builder.buildApicurioPolicy(rbac, NS);

        assertThat(cm.getMetadata().getName()).isEqualTo(RBAC_NAME + "-apicurio-policy");
        assertThat(cm.getMetadata().getNamespace()).isEqualTo(NS);
        assertThat(cm.getData()).containsKey("policy.yaml");
        assertThat(cm.getMetadata().getOwnerReferences()).hasSize(1);
    }

    @Test
    void buildKafkaRules_groupNameWithYamlSpecialChars_emittedSafely() throws Exception {
        // Hostile CR field that, with the old string-concat builder, would have injected an
        // extra rule. The YAML emitter must round-trip through SnakeYAML / Jackson with the
        // group name preserved as a single scalar.
        String hostile = "evil\n  - name: admin\n    operations:\n      - All\n      - Delete";
        KafkaRbac rbac = rbac(List.of(groupWithKafka(hostile, List.of("t"), List.of("READ"))), List.of());

        ConfigMap cm = builder.buildKafkaRules(rbac, NS);
        String yaml = cm.getData().get("rbac-rules.yaml");

        // The output must round-trip cleanly back to the same logical structure.
        var parsed = (java.util.Map<String, Object>) new ObjectMapper(new YAMLFactory())
                .readValue(yaml, java.util.Map.class);
        var groups = (java.util.List<java.util.Map<String, Object>>) parsed.get("groups");
        assertThat(groups).hasSize(1);
        assertThat(groups.get(0).get("name")).isEqualTo(hostile);
        assertThat(groups.get(0).get("operations")).isEqualTo(java.util.List.of("READ"));
    }

    @Test
    void buildApicurioPolicy_artifactNameWithYamlSpecialChars_emittedSafely() throws Exception {
        String hostile = "wild: 'card\nactions:\n  - All";
        KafkaRbacGroup g = groupWithSchemaRegistry("team", List.of(hostile), List.of("READ"));
        KafkaRbac rbac = rbac(List.of(g), List.of());

        ConfigMap cm = builder.buildApicurioPolicy(rbac, NS);
        String yaml = cm.getData().get("policy.yaml");

        var parsed = (java.util.Map<String, Object>) new ObjectMapper(new YAMLFactory())
                .readValue(yaml, java.util.Map.class);
        var rules = (java.util.List<java.util.Map<String, Object>>) parsed.get("rules");
        var resources = (java.util.List<java.util.Map<String, Object>>) rules.get(0).get("resources");
        assertThat(resources.get(0).get("artifact")).isEqualTo(hostile);
        assertThat(resources.get(0).get("actions")).isEqualTo(java.util.List.of("READ"));
    }

    // --- helpers ---

    private KafkaRbac rbac(List<KafkaRbacGroup> groups, List<KafkaRbacUser> users) {
        KafkaRbac rbac = new KafkaRbac();
        ObjectMeta meta = new ObjectMeta();
        meta.setName(RBAC_NAME);
        meta.setNamespace(NS);
        meta.setUid("test-uid-1234");
        rbac.setMetadata(meta);
        KafkaRbacSpec spec = new KafkaRbacSpec();
        spec.setGroups(groups);
        spec.setUsers(users);
        rbac.setSpec(spec);
        return rbac;
    }

    private KafkaRbacGroup groupWithKafka(String name, List<String> topics, List<String> ops) {
        KafkaRbacGroup g = new KafkaRbacGroup();
        g.setName(name);
        KafkaRbacKafkaAccess kafka = new KafkaRbacKafkaAccess();
        kafka.setTopics(topics);
        kafka.setOperations(ops);
        g.setKafka(kafka);
        return g;
    }

    private KafkaRbacGroup groupWithSchemaRegistry(String name, List<String> artifacts, List<String> actions) {
        KafkaRbacGroup g = new KafkaRbacGroup();
        g.setName(name);
        KafkaRbacSchemaAccess schema = new KafkaRbacSchemaAccess();
        schema.setArtifacts(artifacts);
        schema.setActions(actions);
        g.setSchemaRegistry(schema);
        return g;
    }

    private KafkaRbacUser userWithKafka(String name, List<String> topics, List<String> ops) {
        KafkaRbacUser u = new KafkaRbacUser();
        u.setName(name);
        KafkaRbacKafkaAccess kafka = new KafkaRbacKafkaAccess();
        kafka.setTopics(topics);
        kafka.setOperations(ops);
        u.setKafka(kafka);
        return u;
    }
}
