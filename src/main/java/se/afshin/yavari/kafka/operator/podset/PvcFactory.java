package se.afshin.yavari.kafka.operator.podset;

import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.client.KubernetesClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import se.afshin.yavari.kafka.operator.crd.KafkaPodSet;
import se.afshin.yavari.kafka.operator.crd.PodEntry;

import java.util.List;

@ApplicationScoped
public class PvcFactory {

    @Inject
    KubernetesClient client;

    public void ensure(PodEntry entry, String namespace, KafkaPodSet owner) {
        String pvcName = "data-" + entry.getMetadata().getName();
        if (client.persistentVolumeClaims().inNamespace(namespace).withName(pvcName).get() != null) {
            return;
        }
        PersistentVolumeClaim pvc = new PersistentVolumeClaimBuilder()
                .withNewMetadata()
                    .withName(pvcName)
                    .withNamespace(namespace)
                    .withLabels(entry.getMetadata().getLabels())
                    .withOwnerReferences(List.of(new OwnerReferenceBuilder()
                            .withApiVersion(owner.getApiVersion())
                            .withKind(owner.getKind())
                            .withName(owner.getMetadata().getName())
                            .withUid(owner.getMetadata().getUid())
                            .withController(true)
                            .withBlockOwnerDeletion(false)
                            .build()))
                .endMetadata()
                .withNewSpec()
                    .withAccessModes("ReadWriteOnce")
                    .withNewResources()
                        .addToRequests("storage", new Quantity("10Gi"))
                    .endResources()
                .endSpec()
                .build();
        client.persistentVolumeClaims().inNamespace(namespace).resource(pvc).create();
    }
}
