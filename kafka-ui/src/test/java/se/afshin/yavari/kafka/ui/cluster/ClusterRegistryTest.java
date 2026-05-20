package se.afshin.yavari.kafka.ui.cluster;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import se.afshin.yavari.kafka.ui.crd.KafkaClusterCr;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@EnableKubernetesMockClient(crud = true)
@Timeout(value = 3, unit = TimeUnit.SECONDS)
class ClusterRegistryTest {

    KubernetesClient client;

    @Test
    void listsClustersAndDerivesServiceNames() {
        createCluster("my-kafka", "kafka");
        createCluster("staging-kafka", "kafka");

        ClusterRegistry reg = newRegistry();

        List<ClusterCoordinates> all = reg.list();
        assertThat(all).extracting(ClusterCoordinates::id)
                .containsExactly("my-kafka", "staging-kafka");
        ClusterCoordinates c = all.get(0);
        assertThat(c.bootstrapUrl()).isEqualTo("kafka-proxy.kafka.svc.clusterset.local:9094");
        assertThat(c.apicurioUrl()).isEqualTo("http://apicurio-rbac-proxy.kafka.svc.clusterset.local:8082");
        assertThat(c.namespace()).isEqualTo("kafka");
    }

    @Test
    void byId_returnsMatchingEntryOrEmpty() {
        createCluster("my-kafka", "kafka");
        ClusterRegistry reg = newRegistry();

        Optional<ClusterCoordinates> hit = reg.byId("my-kafka");
        assertThat(hit).isPresent();
        assertThat(hit.get().id()).isEqualTo("my-kafka");

        assertThat(reg.byId("does-not-exist")).isEmpty();
    }

    @Test
    void emptyCluster_returnsEmptyList() {
        assertThat(newRegistry().list()).isEmpty();
    }

    private ClusterRegistry newRegistry() {
        ClusterRegistry r = new ClusterRegistry();
        r.client = client;
        r.dnsSuffix = ".svc.clusterset.local";
        r.proxyPort = 9094;
        r.apicurioPort = 8082;
        r.operatorNamespace = "kafka";
        r.proxyServiceName = "kafka-proxy";
        r.apicurioServiceName = "apicurio-rbac-proxy";
        return r;
    }

    private void createCluster(String name, String namespace) {
        KafkaClusterCr cr = new KafkaClusterCr();
        cr.setMetadata(new ObjectMetaBuilder().withName(name).withNamespace(namespace).build());
        client.resources(KafkaClusterCr.class).inNamespace(namespace).resource(cr).create();
    }
}
