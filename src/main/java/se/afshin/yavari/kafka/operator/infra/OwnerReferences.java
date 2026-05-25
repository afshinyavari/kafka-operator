package se.afshin.yavari.kafka.operator.infra;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;

import java.util.List;

/**
 * Shared helpers for building Kubernetes {@link OwnerReference}s from a CR. Every
 * operator-built resource sets the same controller=true / blockOwnerDeletion=true pair so
 * `kubectl delete` on the parent cascades cleanly; this util centralises that pattern.
 */
public final class OwnerReferences {

    private OwnerReferences() {}

    /** Returns a single-element list with a controller OwnerReference pointing at {@code owner}. */
    public static List<OwnerReference> singleton(HasMetadata owner) {
        return List.of(of(owner));
    }

    /** Returns one controller OwnerReference pointing at {@code owner}. */
    public static OwnerReference of(HasMetadata owner) {
        return new OwnerReferenceBuilder()
                .withApiVersion(owner.getApiVersion())
                .withKind(owner.getKind())
                .withName(owner.getMetadata().getName())
                .withUid(owner.getMetadata().getUid())
                .withController(true)
                .withBlockOwnerDeletion(true)
                .build();
    }
}
