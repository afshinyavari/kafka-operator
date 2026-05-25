package se.afshin.yavari.kafka.operator.mm2;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.MirrorMaker2;
import se.afshin.yavari.kafka.operator.crd.MirrorMaker2Spec;
import se.afshin.yavari.kafka.operator.crd.Mm2FlowConfig;
import se.afshin.yavari.kafka.operator.crd.Mm2SchemaSyncConfig;
import se.afshin.yavari.kafka.operator.endpoint.ResolvedKafkaEndpoint;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class Mm2ConfigBuilderTest {

    private final Mm2ConfigBuilder builder = new Mm2ConfigBuilder();

    private MirrorMaker2 cr(MirrorMaker2Spec spec) {
        MirrorMaker2 cr = new MirrorMaker2();
        cr.setMetadata(new ObjectMetaBuilder().withName("my-mm2").withNamespace("kafka").build());
        cr.setSpec(spec);
        return cr;
    }

    @Test
    void plainManagedToManagedNoSchemaSync() {
        MirrorMaker2Spec spec = new MirrorMaker2Spec();
        spec.setFlow(new Mm2FlowConfig());
        spec.getFlow().setReplicationFactor(3);
        ResolvedKafkaEndpoint src = new ResolvedKafkaEndpoint(
                "kafka-proxy.kafka.svc.cluster.local:9094",
                "kafka-operator-client-tls", null, null, null, false);
        ResolvedKafkaEndpoint tgt = new ResolvedKafkaEndpoint(
                "kafka-proxy.dr.svc.cluster.local:9094",
                "kafka-operator-client-tls", null, null, null, false);

        String props = builder.build(cr(spec), src, tgt);

        assertThat(props).contains("clusters=source,target");
        assertThat(props).contains("source.bootstrap.servers=kafka-proxy.kafka.svc.cluster.local:9094");
        assertThat(props).contains("target.bootstrap.servers=kafka-proxy.dr.svc.cluster.local:9094");
        assertThat(props).contains("source.security.protocol=SSL");
        assertThat(props).contains("target.security.protocol=SSL");
        assertThat(props).contains("source.ssl.keystore.location=/etc/mm2/pkcs12/source/keystore.p12");
        assertThat(props).contains("target.ssl.truststore.location=/etc/mm2/pkcs12/target/truststore.p12");
        // Hostname verification disabled — the proxy advertises addresses outside its cert SANs.
        assertThat(props).contains("source.ssl.endpoint.identification.algorithm=");
        assertThat(props).contains("target.ssl.endpoint.identification.algorithm=");
        assertThat(props).contains("source->target.enabled=true");
        assertThat(props).contains("source->target.replication.factor=3");
        // Heartbeats are emitted GLOBALLY (not prefixed) so MM2 does not spin up a
        // herder for the reverse target->source pair. Default emitHeartbeats=true.
        assertThat(props).contains("emit.heartbeats.enabled=true");
        assertThat(props).doesNotContain("source->target.emit.heartbeats.enabled");
        assertThat(props).contains("offset.storage.topic=mm2-offsets.my-mm2");
        assertThat(props).contains("config.storage.topic=mm2-configs.my-mm2");
        assertThat(props).contains("status.storage.topic=mm2-status.my-mm2");
        // No SMT stanza
        assertThat(props).doesNotContain("transforms.schemaSync.type");
    }

    @Test
    void externalSourceSaslSslEmitsSaslConfig() {
        MirrorMaker2Spec spec = new MirrorMaker2Spec();
        spec.setFlow(new Mm2FlowConfig());
        ResolvedKafkaEndpoint src = new ResolvedKafkaEndpoint(
                "broker.external.example.com:9093",
                "external-tls",
                new ResolvedKafkaEndpoint.Sasl("SCRAM-SHA-512", "ext-creds"),
                null, null, false);
        ResolvedKafkaEndpoint tgt = new ResolvedKafkaEndpoint(
                "kafka-proxy.kafka.svc.cluster.local:9094",
                "kafka-operator-client-tls", null, null, null, false);

        String props = builder.build(cr(spec), src, tgt);

        assertThat(props).contains("source.security.protocol=SASL_SSL");
        assertThat(props).contains("source.sasl.mechanism=SCRAM-SHA-512");
        assertThat(props).contains("source.sasl.jaas.config=");
    }

    @Test
    void schemaSyncEnabledEmitsSmtStanza() {
        MirrorMaker2Spec spec = new MirrorMaker2Spec();
        spec.setFlow(new Mm2FlowConfig());
        Mm2SchemaSyncConfig sync = new Mm2SchemaSyncConfig();
        sync.setEnabled(true);
        sync.setApplyToTopics(List.of("events\\..*"));
        spec.setSchemaSync(sync);

        ResolvedKafkaEndpoint src = new ResolvedKafkaEndpoint(
                "kafka-proxy.src.svc.cluster.local:9094",
                "tls-src", null,
                "http://apicurio-rbac-proxy.src.svc.cluster.local:8080",
                null, false);
        ResolvedKafkaEndpoint tgt = new ResolvedKafkaEndpoint(
                "kafka-proxy.dst.svc.cluster.local:9094",
                "tls-dst", null,
                "http://apicurio-rbac-proxy.dst.svc.cluster.local:8080",
                null, false);

        String props = builder.build(cr(spec), src, tgt);

        assertThat(props).contains("source->target.transforms=schemaSync");
        assertThat(props).contains(
                "source->target.transforms.schemaSync.type=se.afshin.yavari.kafka.smt.ApicurioSchemaTransferSmt");
        assertThat(props).contains(
                "source->target.transforms.schemaSync.source.url=http://apicurio-rbac-proxy.src.svc.cluster.local:8080");
        assertThat(props).contains(
                "source->target.transforms.schemaSync.target.url=http://apicurio-rbac-proxy.dst.svc.cluster.local:8080");
        assertThat(props).contains("source->target.transforms.schemaSync.apply.to=VALUE");
        assertThat(props).contains("source->target.transforms.schemaSync.behavior.on.error=WARN");
        assertThat(props).contains("source->target.transforms.schemaSync.apply.to.topics=events\\..*");
    }

    @Test
    void schemaSyncWithRegistryAuthEmitsOauthDir() {
        MirrorMaker2Spec spec = new MirrorMaker2Spec();
        spec.setFlow(new Mm2FlowConfig());
        Mm2SchemaSyncConfig sync = new Mm2SchemaSyncConfig();
        sync.setEnabled(true);
        spec.setSchemaSync(sync);

        // External source registry needs no auth; managed target sits behind the RBAC proxy.
        ResolvedKafkaEndpoint src = new ResolvedKafkaEndpoint(
                "broker:9092", null, null, "http://reg.src:8080", null, false);
        ResolvedKafkaEndpoint tgt = new ResolvedKafkaEndpoint(
                "kafka-proxy.kafka.svc.cluster.local:9094", "tls", null,
                "http://apicurio-rbac-proxy.kafka.svc.cluster.local:8082",
                "mm2-schema-registry-oauth", false);

        String props = builder.build(cr(spec), src, tgt);

        assertThat(props).contains(
                "source->target.transforms.schemaSync.target.auth.oauth.dir=/etc/mm2/registry-auth/target");
        // Source carries no auth Secret → no source oauth dir emitted.
        assertThat(props).doesNotContain("schemaSync.source.auth.oauth.dir");
    }

    @Test
    void schemaSyncDisabledDoesNotEmitSmtEvenWhenRegistriesPresent() {
        MirrorMaker2Spec spec = new MirrorMaker2Spec();
        spec.setFlow(new Mm2FlowConfig());
        ResolvedKafkaEndpoint src = new ResolvedKafkaEndpoint(
                "broker:9092", null, null, "http://reg.src", null, false);
        ResolvedKafkaEndpoint tgt = new ResolvedKafkaEndpoint(
                "broker:9092", null, null, "http://reg.dst", null, false);

        String props = builder.build(cr(spec), src, tgt);
        assertThat(props).doesNotContain("transforms.schemaSync.type");
    }

    @Test
    void flowNameOverrideRenamesInternalTopics() {
        MirrorMaker2Spec spec = new MirrorMaker2Spec();
        Mm2FlowConfig flow = new Mm2FlowConfig();
        flow.setFlowName("custom-flow");
        spec.setFlow(flow);
        ResolvedKafkaEndpoint ep = new ResolvedKafkaEndpoint("b:9092", null, null, null, null, false);

        String props = builder.build(cr(spec), ep, ep);

        assertThat(props).contains("offset.storage.topic=mm2-offsets.custom-flow");
        assertThat(props).contains("config.storage.topic=mm2-configs.custom-flow");
        assertThat(props).contains("status.storage.topic=mm2-status.custom-flow");
    }

    @Test
    void emitHeartbeatsFalseDisablesGloballyToSuppressReverseHerder() {
        MirrorMaker2Spec spec = new MirrorMaker2Spec();
        Mm2FlowConfig flow = new Mm2FlowConfig();
        flow.setEmitHeartbeats(false);
        spec.setFlow(flow);
        ResolvedKafkaEndpoint ep = new ResolvedKafkaEndpoint("b:9092", null, null, null, null, false);

        String props = builder.build(cr(spec), ep, ep);

        // Global false → MM2's clusterPairs() yields only source->target (no reverse herder).
        assertThat(props).contains("emit.heartbeats.enabled=false");
        assertThat(props).doesNotContain("source->target.emit.heartbeats.enabled");
    }

    @Test
    void additionalPropertiesPassthrough() {
        MirrorMaker2Spec spec = new MirrorMaker2Spec();
        Mm2FlowConfig flow = new Mm2FlowConfig();
        flow.setAdditionalProperties(java.util.Map.of(
                "source->target.refresh.topics.interval.seconds", "30",
                "custom.key", "custom-value"));
        spec.setFlow(flow);
        ResolvedKafkaEndpoint ep = new ResolvedKafkaEndpoint("b:9092", null, null, null, null, false);

        String props = builder.build(cr(spec), ep, ep);

        assertThat(props).contains("source->target.refresh.topics.interval.seconds=30");
        assertThat(props).contains("custom.key=custom-value");
    }
}
