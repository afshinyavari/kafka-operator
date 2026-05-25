package se.afshin.yavari.kafka.operator.infra;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudget;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudgetBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.Map;

/**
 * Server-side-applies a {@code maxUnavailable=1} PodDisruptionBudget gated on
 * {@code replicas > 1}. Mirrors the shape already used by {@code KafkaNodePoolReconciler};
 * extracted so the proxy, Connect, MM2, and Apicurio orchestrators can share it instead of
 * re-implementing the same builder per call site.
 *
 * <p>The PDB is named {@code <baseName>-pdb}, lives in {@code namespace}, and is owner-
 * referenced to the parent CR so it gets garbage-collected when the parent is deleted.
 * When {@code replicas <= 1} the PDB is deleted (to avoid deadlocking a single-replica
 * rolling update — a 1-replica PDB with maxUnavailable=1 still allows eviction, but the
 * resource is pointless and clutters {@code kubectl get pdb}).
 */
@ApplicationScoped
public class PdbBuilder {

    public static final String SUFFIX = "-pdb";

    @Inject KubernetesClient client;

    /**
     * Applies or removes a PDB for the named workload.
     *
     * @param baseName       deployment / statefulset name (PDB is named {@code baseName + "-pdb"})
     * @param namespace      target namespace
     * @param labels         metadata labels for the PDB
     * @param podSelector    pod label selector — must match the workload's pod labels
     * @param replicas       desired replica count from the parent CR; PDB is removed when ≤ 1
     * @param owner          parent CR for the owner reference
     */
    public void apply(String baseName, String namespace,
                      Map<String, String> labels, Map<String, String> podSelector,
                      int replicas, HasMetadata owner) {
        String pdbName = baseName + SUFFIX;
        if (replicas <= 1) {
            client.policy().v1().podDisruptionBudget().inNamespace(namespace)
                  .withName(pdbName).delete();
            return;
        }
        PodDisruptionBudget pdb = new PodDisruptionBudgetBuilder()
                .withNewMetadata()
                    .withName(pdbName)
                    .withNamespace(namespace)
                    .withLabels(labels)
                    .withOwnerReferences(OwnerReferences.of(owner))
                .endMetadata()
                .withNewSpec()
                    .withMaxUnavailable(new IntOrString(1))
                    .withNewSelector()
                        .withMatchLabels(podSelector)
                    .endSelector()
                .endSpec()
                .build();
        client.policy().v1().podDisruptionBudget().inNamespace(namespace).resource(pdb).serverSideApply();
    }

    /** Removes the PDB unconditionally — call from {@code cleanup()} paths. */
    public void delete(String baseName, String namespace) {
        client.policy().v1().podDisruptionBudget().inNamespace(namespace)
              .withName(baseName + SUFFIX).delete();
    }
}
