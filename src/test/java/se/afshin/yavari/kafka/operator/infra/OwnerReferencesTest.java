package se.afshin.yavari.kafka.operator.infra;

import io.fabric8.kubernetes.api.model.OwnerReference;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.KafkaCluster;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OwnerReferencesTest {

    @Test
    void buildsControllerReferenceFromCr() {
        KafkaCluster cr = new KafkaCluster();
        cr.setMetadata(new io.fabric8.kubernetes.api.model.ObjectMetaBuilder()
                .withName("my-cluster")
                .withUid("abc-123")
                .build());
        cr.setApiVersion("kafka.yavari.afshin.se/v1alpha1");
        cr.setKind("KafkaCluster");

        OwnerReference ref = OwnerReferences.of(cr);

        assertEquals("kafka.yavari.afshin.se/v1alpha1", ref.getApiVersion());
        assertEquals("KafkaCluster", ref.getKind());
        assertEquals("my-cluster", ref.getName());
        assertEquals("abc-123", ref.getUid());
        assertTrue(ref.getController(), "controller flag");
        assertTrue(ref.getBlockOwnerDeletion(), "blockOwnerDeletion flag");
    }

    @Test
    void singletonWrapsInSingleElementList() {
        KafkaCluster cr = new KafkaCluster();
        cr.setMetadata(new io.fabric8.kubernetes.api.model.ObjectMetaBuilder()
                .withName("c").withUid("u").build());
        cr.setApiVersion("v1alpha1");
        cr.setKind("KafkaCluster");
        List<OwnerReference> refs = OwnerReferences.singleton(cr);
        assertEquals(1, refs.size());
        assertEquals("c", refs.get(0).getName());
    }
}
