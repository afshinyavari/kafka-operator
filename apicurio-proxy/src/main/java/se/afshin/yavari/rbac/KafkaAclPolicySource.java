package se.afshin.yavari.rbac;

import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Authorizes mTLS principals from Kafka topic ACLs. Artifact {@code orders-value} maps to
 * topic {@code orders}; the principal's topic permissions decide the artifact actions:
 * READ/DESCRIBE → READ, WRITE → READ+WRITE, DELETE → DELETE, ALL → everything. Evaluation
 * follows Kafka's authorizer: a matching DENY for an operation (or ALL) wins over any
 * ALLOW; LITERAL patterns match exactly (or via the {@code *} wildcard name), PREFIXED
 * patterns match by prefix; principal {@code User:*} matches everyone. Host is ignored.
 *
 * <p>The bean keeps a snapshot of all ACL bindings from {@code Admin.describeAcls}, refreshed
 * every {@code proxy.kafka.acl.refresh-seconds}. It is enabled by {@code PROXY_KAFKA_BOOTSTRAP};
 * without it, certificate identities are denied. A failed refresh keeps the last snapshot.
 */
@ApplicationScoped
public class KafkaAclPolicySource {

    static final String WILDCARD = "*";

    @ConfigProperty(name = "proxy.kafka.acl.refresh-seconds", defaultValue = "30")
    long refreshSeconds;

    @ConfigProperty(name = "proxy.artifact.suffixes", defaultValue = "-value,-key")
    String suffixCsv;

    private Supplier<Collection<AclBinding>> loader;
    private List<String> suffixes;
    private Admin admin;
    private final AtomicReference<List<AclBinding>> snapshot = new AtomicReference<>(null);
    private volatile boolean running = true;
    private Thread refresher;

    /** CDI constructor: wiring happens in {@link #onStart}. */
    public KafkaAclPolicySource() {}

    /** Test seam: explicit loader ({@code null} = disabled) and suffix list. */
    KafkaAclPolicySource(Supplier<Collection<AclBinding>> loader, List<String> suffixes) {
        this.loader = loader;
        this.suffixes = suffixes;
    }

    void onStart(@Observes StartupEvent ev) {
        this.suffixes = Arrays.stream(suffixCsv.split(",")).map(String::trim).filter(x -> !x.isEmpty()).toList();
        String bootstrap = System.getenv("PROXY_KAFKA_BOOTSTRAP");
        if (bootstrap == null || bootstrap.isBlank()) {
            System.out.println("[KafkaAclPolicySource] PROXY_KAFKA_BOOTSTRAP unset — mTLS identities will be denied");
            return;
        }
        this.admin = Admin.create(adminProps(System::getenv));
        this.loader = () -> {
            try {
                return admin.describeAcls(AclBindingFilter.ANY).values().get();
            } catch (Exception e) {
                throw new IllegalStateException("describeAcls failed: " + e.getMessage(), e);
            }
        };
        refresh();
        refresher = new Thread(() -> {
            while (running) {
                try {
                    Thread.sleep(refreshSeconds * 1000L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                refresh();
            }
        }, "kafka-acl-refresh");
        refresher.setDaemon(true);
        refresher.start();
    }

    @PreDestroy
    void onStop() {
        running = false;
        if (refresher != null) refresher.interrupt();
        if (admin != null) admin.close();
    }

    /** Loads a new snapshot; a failure keeps the previous one and logs. */
    void refresh() {
        if (loader == null) return;
        try {
            List<AclBinding> loaded = new ArrayList<>(loader.get());
            snapshot.set(loaded);
            System.out.println("[KafkaAclPolicySource] Loaded " + loaded.size() + " ACL bindings");
        } catch (Exception e) {
            List<AclBinding> kept = snapshot.get();
            System.err.println("[KafkaAclPolicySource] ACL refresh failed, keeping "
                    + (kept == null ? "nothing" : kept.size() + " bindings") + ": " + e.getMessage());
        }
    }

    void replaceSnapshot(Collection<AclBinding> acls) {
        snapshot.set(new ArrayList<>(acls));
    }

    public boolean isEnabled() { return loader != null; }

    public boolean isLoaded() { return snapshot.get() != null; }

    public boolean isAllowed(String principal, String artifact, PolicyEngine.Action action) {
        List<AclBinding> acls = snapshot.get();
        if (acls == null) return false;
        return evaluate(acls, principal, topicForArtifact(artifact, suffixes), action);
    }

    /** Admin client config from {@code PROXY_KAFKA_*}: bootstrap, security protocol
     *  (SSL default | PLAINTEXT) and PKCS12/JKS/PEM stores via {@link KafkaSslProps}. */
    static Properties adminProps(Function<String, String> env) {
        Properties p = new Properties();
        p.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, env.apply("PROXY_KAFKA_BOOTSTRAP"));
        p.put(CommonClientConfigs.CLIENT_ID_CONFIG, "apicurio-rbac-proxy-acl");
        p.put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, 15_000);
        p.put(CommonClientConfigs.DEFAULT_API_TIMEOUT_MS_CONFIG, 20_000);
        String protocol = Optional.ofNullable(env.apply("PROXY_KAFKA_SECURITY_PROTOCOL"))
                .filter(x -> !x.isBlank()).orElse("SSL");
        p.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, protocol);
        if ("SSL".equalsIgnoreCase(protocol)) {
            KafkaSslProps.apply(p, KafkaSslProps.fromEnv(env, "PROXY_KAFKA_SSL"),
                    KafkaSslProps.trustFromEnv(env, "PROXY_KAFKA_SSL"));
        }
        return p;
    }

    /** Maps an artifact id to the topic it belongs to by stripping the first matching suffix. */
    public static String topicForArtifact(String artifact, List<String> suffixes) {
        if (artifact == null || WILDCARD.equals(artifact)) return WILDCARD;
        for (String suffix : suffixes) {
            if (artifact.length() > suffix.length() && artifact.endsWith(suffix)) {
                return artifact.substring(0, artifact.length() - suffix.length());
            }
        }
        return artifact;
    }

    /** Topic operations any one of which grants the artifact action. {@code ALL} is not
     *  listed: an ACL entry with operation ALL matches every operation during evaluation,
     *  so a specific DENY (e.g. on WRITE) still wins over an ALLOW ALL, as in Kafka. */
    public static Set<AclOperation> grantingOperations(PolicyEngine.Action action) {
        return switch (action) {
            case READ -> EnumSet.of(AclOperation.READ, AclOperation.DESCRIBE, AclOperation.WRITE);
            case WRITE -> EnumSet.of(AclOperation.WRITE);
            case DELETE -> EnumSet.of(AclOperation.DELETE);
        };
    }

    /** Kafka-style evaluation: for each granting operation, allowed if some ALLOW (for that
     *  operation or ALL) matches and no DENY (for that operation or ALL) matches. */
    public static boolean evaluate(Collection<AclBinding> acls, String principal, String topic,
                                   PolicyEngine.Action action) {
        String kafkaPrincipal = "User:" + principal;
        for (AclOperation op : grantingOperations(action)) {
            boolean allowed = false;
            boolean denied = false;
            for (AclBinding b : acls) {
                if (!matches(b, kafkaPrincipal, topic)) continue;
                AccessControlEntry e = b.entry();
                boolean opMatches = e.operation() == op || e.operation() == AclOperation.ALL;
                if (!opMatches) continue;
                if (e.permissionType() == AclPermissionType.DENY) denied = true;
                else if (e.permissionType() == AclPermissionType.ALLOW) allowed = true;
            }
            if (allowed && !denied) return true;
        }
        return false;
    }

    private static boolean matches(AclBinding b, String kafkaPrincipal, String topic) {
        ResourcePattern r = b.pattern();
        if (r.resourceType() != ResourceType.TOPIC) return false;
        String p = b.entry().principal();
        if (!p.equals(kafkaPrincipal) && !p.equals("User:" + WILDCARD)) return false;
        if (r.patternType() == PatternType.LITERAL) {
            return WILDCARD.equals(r.name()) || r.name().equals(topic);
        }
        if (r.patternType() == PatternType.PREFIXED) {
            return !WILDCARD.equals(topic) && topic.startsWith(r.name());
        }
        return false;
    }
}
