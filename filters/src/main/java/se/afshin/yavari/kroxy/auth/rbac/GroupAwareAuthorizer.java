package se.afshin.yavari.kroxy.auth.rbac;

import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.authorizer.service.AuthorizeResult;
import io.kroxylicious.authorizer.service.Authorizer;
import io.kroxylicious.authorizer.service.ResourceType;
import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.authentication.User;
import se.afshin.yavari.kroxy.auth.Group;
import se.afshin.yavari.kroxy.auth.JwtGroupStore;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;

class GroupAwareAuthorizer implements Authorizer {

    private final AtomicReference<RbacRules> rules;

    GroupAwareAuthorizer(AtomicReference<RbacRules> rules) {
        this.rules = rules;
    }

    @Override
    public CompletionStage<AuthorizeResult> authorize(Subject subject, List<Action> actions) {
        RbacRules current = rules.get();
        List<Action> allowed = new ArrayList<>();
        List<Action> denied = new ArrayList<>();

        for (Action action : actions) {
            if (isAllowed(subject, action, current)) {
                allowed.add(action);
            } else {
                denied.add(action);
            }
        }

        return CompletableFuture.completedFuture(new AuthorizeResult(subject, allowed, denied));
    }

    @Override
    public Optional<Set<Class<? extends ResourceType<?>>>> supportedResourceTypes() {
        return Optional.empty();
    }

    private static boolean isAllowed(Subject subject, Action action, RbacRules rules) {
        String topic = action.resourceName();
        String operation = operationName(action);

        // Check user rules (mTLS CN)
        Optional<User> user = subject.uniquePrincipalOfType(User.class);
        if (user.isPresent()) {
            String userName = user.get().name();
            for (RbacUser rule : rules.users) {
                if (rule.name.equals(userName) && matchesTopic(rule.topics, topic) && matchesOp(rule.operations, operation)) {
                    return true;
                }
            }
        }

        // Check group rules (SASL JWT groups via Subject principals or JwtGroupStore fallback)
        Set<Group> groups = subject.allPrincipalsOfType(Group.class);
        if (groups.isEmpty() && user.isPresent()) {
            // OauthBearerValidationFilter sets Subject{User(sub)} without Group principals.
            // JwtGroupFilter stored groups in JwtGroupStore keyed by the same sub UUID.
            Set<String> jwtGroups = JwtGroupStore.get(user.get().name());
            for (String groupName : jwtGroups) {
                for (RbacGroup rule : rules.groups) {
                    if (rule.name.equals(groupName) && matchesTopic(rule.topics, topic) && matchesOp(rule.operations, operation)) {
                        return true;
                    }
                }
            }
        } else {
            for (Group group : groups) {
                for (RbacGroup rule : rules.groups) {
                    if (rule.name.equals(group.name()) && matchesTopic(rule.topics, topic) && matchesOp(rule.operations, operation)) {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    private static boolean matchesTopic(List<String> allowed, String topic) {
        return allowed.contains("*") || allowed.contains(topic);
    }

    private static boolean matchesOp(List<String> allowed, String operation) {
        if (allowed.contains("*") || allowed.contains(operation)) return true;
        // Semantic aliases: PRODUCE → {WRITE, DESCRIBE}; FETCH → {READ, DESCRIBE}
        if (allowed.contains("PRODUCE") && (operation.equals("WRITE") || operation.equals("DESCRIBE"))) return true;
        if (allowed.contains("FETCH") && (operation.equals("READ") || operation.equals("DESCRIBE"))) return true;
        return false;
    }

    private static String operationName(Action action) {
        ResourceType<?> op = action.operation();
        return op instanceof Enum<?> e ? e.name() : op.toString();
    }
}
