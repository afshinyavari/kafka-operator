package se.afshin.yavari.kafka.operator.connect;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.KafkaConnectPluginSources;

import java.lang.reflect.Field;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "rawtypes"})
class ConnectPluginResolverTest {

    private KubernetesClient client;
    private ConnectPluginResolver resolver;

    @BeforeEach
    void setup() throws Exception {
        client = mock(KubernetesClient.class);
        resolver = new ConnectPluginResolver();
        Field f = ConnectPluginResolver.class.getDeclaredField("client");
        f.setAccessible(true);
        f.set(resolver, client);
    }

    private void registerPvc(String name, String namespace, PersistentVolumeClaim pvc) {
        MixedOperation mixed = mock(MixedOperation.class);
        NonNamespaceOperation nso = mock(NonNamespaceOperation.class);
        Resource res = mock(Resource.class);
        when(client.persistentVolumeClaims()).thenReturn(mixed);
        when(mixed.inNamespace(namespace)).thenReturn(nso);
        when(nso.withName(name)).thenReturn(res);
        when(res.get()).thenReturn(pvc);
    }

    private void registerConfigMap(String name, String namespace, ConfigMap cm) {
        MixedOperation mixed = mock(MixedOperation.class);
        NonNamespaceOperation nso = mock(NonNamespaceOperation.class);
        Resource res = mock(Resource.class);
        when(client.configMaps()).thenReturn(mixed);
        when(mixed.inNamespace(namespace)).thenReturn(nso);
        when(nso.withName(name)).thenReturn(res);
        when(res.get()).thenReturn(cm);
    }

    private void registerSecret(String name, String namespace, Secret s) {
        MixedOperation mixed = mock(MixedOperation.class);
        NonNamespaceOperation nso = mock(NonNamespaceOperation.class);
        Resource res = mock(Resource.class);
        when(client.secrets()).thenReturn(mixed);
        when(mixed.inNamespace(namespace)).thenReturn(nso);
        when(nso.withName(name)).thenReturn(res);
        when(res.get()).thenReturn(s);
    }

    @Test
    void noSourcesYieldsBakedOnly() {
        ResolvedPluginSources r = resolver.resolve(null, "kafka");

        assertThat(r.pluginPath()).isEqualTo("/opt/kafka/connect-plugins/baked");
        assertThat(r.volumes()).isEmpty();
        assertThat(r.volumeMounts()).isEmpty();
        assertThat(r.referencedSecretNames()).isEmpty();
    }

    @Test
    void pvcAppendedWhenPresent() {
        PersistentVolumeClaim pvc = new PersistentVolumeClaimBuilder()
                .withNewMetadata().withName("my-plugins").withNamespace("kafka").endMetadata()
                .build();
        registerPvc("my-plugins", "kafka", pvc);

        KafkaConnectPluginSources s = new KafkaConnectPluginSources();
        s.setPluginsVolumeClaim("my-plugins");

        ResolvedPluginSources r = resolver.resolve(s, "kafka");
        assertThat(r.pluginPath()).isEqualTo(
                "/opt/kafka/connect-plugins/baked,/opt/kafka/connect-plugins/pvc");
        assertThat(r.volumes()).hasSize(1);
        assertThat(r.volumeMounts()).hasSize(1);
        assertThat(r.volumeMounts().get(0).getMountPath()).isEqualTo("/opt/kafka/connect-plugins/pvc");
    }

    @Test
    void missingPvcThrowsWithPreciseMessage() {
        registerPvc("ghost", "kafka", null);
        KafkaConnectPluginSources s = new KafkaConnectPluginSources();
        s.setPluginsVolumeClaim("ghost");

        assertThatThrownBy(() -> resolver.resolve(s, "kafka"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("pluginsVolumeClaim")
                .hasMessageContaining("'ghost'")
                .hasMessageContaining("namespace kafka");
    }

    @Test
    void configMapAndSecretComposeWithDistinctMountPaths() {
        registerConfigMap("debezium", "kafka", new ConfigMapBuilder()
                .withNewMetadata().withName("debezium").withNamespace("kafka").endMetadata().build());
        registerSecret("jdbc-cred", "kafka", new SecretBuilder()
                .withNewMetadata().withName("jdbc-cred").withNamespace("kafka").endMetadata().build());

        KafkaConnectPluginSources s = new KafkaConnectPluginSources();
        s.setPluginConfigMaps(List.of("debezium"));
        s.setPluginSecrets(List.of("jdbc-cred"));

        ResolvedPluginSources r = resolver.resolve(s, "kafka");
        assertThat(r.pluginPath()).contains(
                "/opt/kafka/connect-plugins/cm-debezium",
                "/opt/kafka/connect-plugins/secret-jdbc-cred");
        assertThat(r.volumes()).hasSize(2);
        assertThat(r.volumeMounts()).hasSize(2);
        assertThat(r.referencedSecretNames()).containsExactly("jdbc-cred");
    }

    @Test
    void missingConfigMapMessageIncludesIndexAndName() {
        registerConfigMap("ghost", "kafka", null);
        KafkaConnectPluginSources s = new KafkaConnectPluginSources();
        s.setPluginConfigMaps(List.of("ghost"));

        assertThatThrownBy(() -> resolver.resolve(s, "kafka"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("pluginConfigMaps[0]='ghost'");
    }

    @Test
    void missingSecretMessageIncludesIndexAndName() {
        registerSecret("ghost", "kafka", null);
        KafkaConnectPluginSources s = new KafkaConnectPluginSources();
        s.setPluginSecrets(List.of("ghost"));

        assertThatThrownBy(() -> resolver.resolve(s, "kafka"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("pluginSecrets[0]='ghost'");
    }
}
