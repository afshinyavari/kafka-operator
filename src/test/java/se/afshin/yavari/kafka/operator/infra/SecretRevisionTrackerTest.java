package se.afshin.yavari.kafka.operator.infra;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "rawtypes"})
class SecretRevisionTrackerTest {

    private static final String NS = "kafka";

    private KubernetesClient client;
    private Map<String, Secret> secrets;
    private SecretRevisionTracker tracker;

    @BeforeEach
    void setUp() throws Exception {
        client = mock(KubernetesClient.class);
        secrets = new HashMap<>();

        MixedOperation secretsOp = mock(MixedOperation.class);
        NonNamespaceOperation nsOp = mock(NonNamespaceOperation.class);
        when(client.secrets()).thenReturn(secretsOp);
        when(secretsOp.inNamespace(NS)).thenReturn(nsOp);
        when(nsOp.withName(anyString())).thenAnswer(inv -> {
            String name = inv.getArgument(0);
            Resource resource = mock(Resource.class);
            when(resource.get()).thenReturn(secrets.get(name));
            return resource;
        });

        tracker = new SecretRevisionTracker();
        var field = SecretRevisionTracker.class.getDeclaredField("client");
        field.setAccessible(true);
        field.set(tracker, client);
    }

    @Test
    void revisionsOf_returnsDeterministicNameRevPairs() {
        secrets.put("alpha", secret("alpha", "10"));
        secrets.put("beta",  secret("beta",  "20"));

        // Note: input order is intentionally not sorted; output must be.
        String revs = tracker.revisionsOf(List.of("beta", "alpha"), NS);

        assertThat(revs).isEqualTo("alpha=10,beta=20");
    }

    @Test
    void revisionsOf_missingSecret_emitsSentinel() {
        secrets.put("alpha", secret("alpha", "10"));
        // beta is missing

        String revs = tracker.revisionsOf(List.of("alpha", "beta"), NS);

        assertThat(revs).isEqualTo("alpha=10,beta=MISSING");
    }

    @Test
    void revisionsOf_changingResourceVersion_changesOutput() {
        secrets.put("alpha", secret("alpha", "10"));
        String before = tracker.revisionsOf(List.of("alpha"), NS);

        secrets.put("alpha", secret("alpha", "11"));
        String after = tracker.revisionsOf(List.of("alpha"), NS);

        assertThat(after).isNotEqualTo(before);
    }

    @Test
    void revisionsOf_emptyOrNullInput_returnsEmpty() {
        assertThat(tracker.revisionsOf(List.of(), NS)).isEmpty();
        assertThat(tracker.revisionsOf(null, NS)).isEmpty();
    }

    @Test
    void revisionsOf_blankAndNullNames_skipped() {
        secrets.put("alpha", secret("alpha", "10"));

        String revs = tracker.revisionsOf(java.util.Arrays.asList("alpha", "", null, "  "), NS);

        assertThat(revs).isEqualTo("alpha=10");
    }

    @Test
    void revisionsOf_duplicateNames_appearOnce() {
        secrets.put("alpha", secret("alpha", "10"));

        String revs = tracker.revisionsOf(List.of("alpha", "alpha"), NS);

        assertThat(revs).isEqualTo("alpha=10");
    }

    private static Secret secret(String name, String resourceVersion) {
        Secret s = new Secret();
        ObjectMeta meta = new ObjectMeta();
        meta.setName(name);
        meta.setResourceVersion(resourceVersion);
        s.setMetadata(meta);
        return s;
    }
}
