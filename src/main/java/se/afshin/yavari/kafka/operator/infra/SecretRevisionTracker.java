package se.afshin.yavari.kafka.operator.infra;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.KubernetesClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.Collection;
import java.util.TreeSet;

/**
 * Reads {@code metadata.resourceVersion} from a set of Secrets and returns a deterministic
 * string that callers fold into a pod's {@code configHash}. When any Secret's content changes
 * Kubernetes increments its {@code resourceVersion}, so the hash flips and the workload rolls.
 *
 * <p>This is the operator's cert-rotation signal: cert-manager (or whoever owns the Secret)
 * writes a new value, the informer wakes the reconciler, the reconciler recomputes the hash,
 * the PodTemplate annotation changes, the Deployment/PodSet rolls. Without this the operator
 * would keep serving the old in-memory cert until something else triggered a restart.
 */
@ApplicationScoped
public class SecretRevisionTracker {

    /** Sentinel used when a referenced Secret is absent. Distinct from any real resourceVersion
     *  so a missing-then-present transition still changes the hash. */
    static final String MISSING = "MISSING";

    @Inject KubernetesClient client;

    /**
     * Returns a deterministic {@code name=revision} string across the given Secret names. Names
     * are deduplicated and sorted so the output is stable across reconciles. Null / blank names
     * are skipped. Missing Secrets contribute {@link #MISSING} so the hash still differs once
     * they appear.
     */
    public String revisionsOf(Collection<String> secretNames, String namespace) {
        if (secretNames == null || secretNames.isEmpty()) return "";
        TreeSet<String> sorted = new TreeSet<>();
        for (String name : secretNames) {
            if (name != null && !name.isBlank()) sorted.add(name);
        }
        StringBuilder sb = new StringBuilder();
        for (String name : sorted) {
            Secret s = client.secrets().inNamespace(namespace).withName(name).get();
            String rev = (s == null || s.getMetadata() == null
                          || s.getMetadata().getResourceVersion() == null)
                    ? MISSING
                    : s.getMetadata().getResourceVersion();
            if (sb.length() > 0) sb.append(',');
            sb.append(name).append('=').append(rev);
        }
        return sb.toString();
    }
}
