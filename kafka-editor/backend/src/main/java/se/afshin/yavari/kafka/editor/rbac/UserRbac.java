package se.afshin.yavari.kafka.editor.rbac;

import java.util.Set;

/**
 * Aggregated view of "what this user can do" derived from the {@code KafkaRbac}
 * CRs and the groups in the user's JWT.
 *
 * <p>Final enforcement happens at the KafkaProxy / apicurio-rbac-proxy; this
 * struct is only used to gate UI affordances (hide topics they can't see,
 * disable Produce buttons, etc.).
 *
 * <p>{@link #allTopicsAllowed} / {@link #allSchemasAllowed} are {@code true}
 * when any of the user's groups grants a wildcard ({@code "*"}) — in that case
 * the explicit allow-list sets are still populated with the wildcard literal so
 * callers can short-circuit by checking the flags.
 */
public record UserRbac(
        Set<String> groups,
        Set<String> topicsAllowedToRead,
        Set<String> schemasAllowedToRead,
        boolean allTopicsAllowed,
        boolean allSchemasAllowed) {

    public boolean canReadTopic(String topic) {
        return allTopicsAllowed || topicsAllowedToRead.contains(topic);
    }

    public boolean canReadSchema(String artifact) {
        return allSchemasAllowed || schemasAllowedToRead.contains(artifact);
    }

    public static UserRbac empty(Set<String> groups) {
        return new UserRbac(groups, Set.of(), Set.of(), false, false);
    }
}
