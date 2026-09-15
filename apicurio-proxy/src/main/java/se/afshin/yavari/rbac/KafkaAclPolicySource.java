package se.afshin.yavari.rbac;

import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;

import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * Authorizes mTLS principals from Kafka topic ACLs. Artifact {@code orders-value} maps to
 * topic {@code orders}; the principal's topic permissions decide the artifact actions:
 * READ/DESCRIBE → READ, WRITE → READ+WRITE, DELETE → DELETE, ALL → everything. Evaluation
 * follows Kafka's authorizer: a matching DENY for an operation (or ALL) wins over any
 * ALLOW; LITERAL patterns match exactly (or via the {@code *} wildcard name), PREFIXED
 * patterns match by prefix; principal {@code User:*} matches everyone. Host is ignored.
 */
public class KafkaAclPolicySource {

    static final String WILDCARD = "*";

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
