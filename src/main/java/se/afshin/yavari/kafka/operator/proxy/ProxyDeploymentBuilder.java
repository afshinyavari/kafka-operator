package se.afshin.yavari.kafka.operator.proxy;

import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import jakarta.enterprise.context.ApplicationScoped;
import se.afshin.yavari.kafka.operator.crd.KafkaProxy;
import se.afshin.yavari.kafka.operator.crd.KafkaProxySpec;
import se.afshin.yavari.kafka.operator.crd.KafkaProxyTlsConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class ProxyDeploymentBuilder {

    public Deployment build(KafkaProxy proxy, String namespace) {
        String name = proxy.getMetadata().getName();
        KafkaProxySpec spec = proxy.getSpec();
        Map<String, String> labels = labels(name);

        var volumes = new ArrayList<io.fabric8.kubernetes.api.model.Volume>();
        var mounts = new ArrayList<io.fabric8.kubernetes.api.model.VolumeMount>();

        // Config volume
        volumes.add(new VolumeBuilder()
                .withName("config")
                .withNewConfigMap().withName(name + "-config").endConfigMap()
                .build());
        mounts.add(new VolumeMountBuilder()
                .withName("config")
                .withMountPath("/etc/kroxy")
                .build());

        // RBAC rules volume — mounted at a separate path to avoid collision with /etc/kroxy dir mount
        if (spec.getRbacRef() != null) {
            volumes.add(new VolumeBuilder()
                    .withName("rbac-rules")
                    .withNewConfigMap().withName(spec.getRbacRef() + "-kafka-rules").endConfigMap()
                    .build());
            mounts.add(new VolumeMountBuilder()
                    .withName("rbac-rules")
                    .withMountPath("/etc/kroxy-rbac")
                    .build());
        }

        // TLS volumes
        KafkaProxyTlsConfig tls = spec.getTls();
        if (tls != null) {
            if (tls.getProxyKeySecretRef() != null) {
                volumes.add(new VolumeBuilder()
                        .withName("proxy-tls")
                        .withNewSecret().withSecretName(tls.getProxyKeySecretRef()).endSecret()
                        .build());
                mounts.add(new VolumeMountBuilder()
                        .withName("proxy-tls")
                        .withMountPath("/etc/proxy/kafka-tls")
                        .withReadOnly(true)
                        .build());
            }
            if (tls.getClientCaSecretRef() != null) {
                volumes.add(new VolumeBuilder()
                        .withName("client-ca")
                        .withNewSecret().withSecretName(tls.getClientCaSecretRef()).endSecret()
                        .build());
                mounts.add(new VolumeMountBuilder()
                        .withName("client-ca")
                        .withMountPath("/etc/proxy/client-tls")
                        .withReadOnly(true)
                        .build());
            }
        }

        return new DeploymentBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(namespace)
                    .withLabels(labels)
                .endMetadata()
                .withNewSpec()
                    .withReplicas(spec.getReplicas())
                    .withNewSelector()
                        .withMatchLabels(labels)
                    .endSelector()
                    .withNewTemplate()
                        .withNewMetadata().withLabels(labels).endMetadata()
                        .withNewSpec()
                            .withContainers(new ContainerBuilder()
                                    .withName("kroxylicious")
                                    .withImage(spec.getImage())
                                    .withArgs("--config", "/etc/kroxy/config.yaml")
                                    .withVolumeMounts(mounts)
                                    .build())
                            .withVolumes(volumes)
                        .endSpec()
                    .endTemplate()
                .endSpec()
                .build();
    }

    static Map<String, String> labels(String name) {
        return Map.of(
                "app", "kroxylicious",
                "app.instance", name,
                "app.managed-by", "kafka-operator"
        );
    }
}
