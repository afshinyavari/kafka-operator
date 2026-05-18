package se.afshin.yavari.kroxy.auth.oauth;

import io.kroxylicious.proxy.authentication.SaslSubjectBuilder;
import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.authentication.User;
import se.afshin.yavari.kroxy.auth.Group;
import se.afshin.yavari.kroxy.auth.JwtGroupStore;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

class JwtGroupSaslSubjectBuilder implements SaslSubjectBuilder {

    @Override
    public CompletionStage<Subject> buildSaslSubject(Context context) {
        String authzId = context.clientSaslContext().authorizationId();
        User user = new User(authzId);

        Set<String> groups = JwtGroupStore.getAndRemove(authzId);
        Subject subject;
        if (groups == null || groups.isEmpty()) {
            subject = new Subject(user);
        } else {
            List<io.kroxylicious.proxy.authentication.Principal> principals = new ArrayList<>();
            principals.add(user);
            groups.forEach(g -> principals.add(new Group(g)));
            subject = new Subject(principals.toArray(new io.kroxylicious.proxy.authentication.Principal[0]));
        }

        return CompletableFuture.completedFuture(subject);
    }
}
