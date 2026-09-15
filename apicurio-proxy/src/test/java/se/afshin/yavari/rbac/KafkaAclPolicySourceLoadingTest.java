package se.afshin.yavari.rbac;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static se.afshin.yavari.rbac.PolicyEngine.Action.WRITE;

class KafkaAclPolicySourceLoadingTest {

    private static final AclBinding ORDERS_WRITE = new AclBinding(
            new ResourcePattern(ResourceType.TOPIC, "orders", PatternType.LITERAL),
            new AccessControlEntry("User:CN=svc", "*", AclOperation.WRITE, AclPermissionType.ALLOW));

    @Test
    void notLoadedUntilFirstSuccessfulRefresh() {
        KafkaAclPolicySource src = new KafkaAclPolicySource(() -> List.of(ORDERS_WRITE), List.of("-value", "-key"));
        assertThat(src.isEnabled()).isTrue();
        assertThat(src.isLoaded()).isFalse();
        assertThat(src.isAllowed("CN=svc", "orders-value", WRITE)).isFalse();
        src.refresh();
        assertThat(src.isLoaded()).isTrue();
        assertThat(src.isAllowed("CN=svc", "orders-value", WRITE)).isTrue();
    }

    @Test
    void failedRefreshKeepsLastSnapshot() {
        AtomicInteger calls = new AtomicInteger();
        KafkaAclPolicySource src = new KafkaAclPolicySource(() -> {
            if (calls.incrementAndGet() > 1) throw new RuntimeException("kafka down");
            return List.of(ORDERS_WRITE);
        }, List.of("-value", "-key"));
        src.refresh();
        src.refresh(); // throws inside, must be swallowed
        assertThat(src.isLoaded()).isTrue();
        assertThat(src.isAllowed("CN=svc", "orders-value", WRITE)).isTrue();
    }

    @Test
    void disabledSourceDeniesAndReportsDisabled() {
        KafkaAclPolicySource src = new KafkaAclPolicySource(null, List.of("-value"));
        assertThat(src.isEnabled()).isFalse();
        src.refresh(); // no-op
        assertThat(src.isLoaded()).isFalse();
        assertThat(src.isAllowed("CN=svc", "orders-value", WRITE)).isFalse();
    }

    @Test
    void replaceSnapshotIsUsedImmediately() {
        KafkaAclPolicySource src = new KafkaAclPolicySource(List::of, List.of("-value", "-key"));
        src.replaceSnapshot(List.of(ORDERS_WRITE));
        assertThat(src.isAllowed("CN=svc", "orders-key", WRITE)).isTrue();
    }

    @Test
    void adminPropsFromEnvWithPkcs12() {
        Map<String, String> env = Map.of(
                "PROXY_KAFKA_BOOTSTRAP", "kafka:9093",
                "PROXY_KAFKA_SSL_KEYSTORE", "/k/user.p12", "PROXY_KAFKA_SSL_KEYSTORE_PASSWORD", "pw",
                "PROXY_KAFKA_SSL_TRUSTSTORE", "/k/ca.p12", "PROXY_KAFKA_SSL_TRUSTSTORE_PASSWORD", "cpw");
        Properties p = KafkaAclPolicySource.adminProps(env::get);
        assertThat(p).containsEntry(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "kafka:9093")
                .containsEntry(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL")
                .containsEntry(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "/k/user.p12")
                .containsEntry(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "pw")
                .containsEntry(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/k/ca.p12");
    }

    @Test
    void adminPropsPlaintextWhenRequested() {
        Map<String, String> env = Map.of("PROXY_KAFKA_BOOTSTRAP", "kafka:9092",
                "PROXY_KAFKA_SECURITY_PROTOCOL", "PLAINTEXT");
        Properties p = KafkaAclPolicySource.adminProps(env::get);
        assertThat(p).containsEntry(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT")
                .doesNotContainKey(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
    }
}
