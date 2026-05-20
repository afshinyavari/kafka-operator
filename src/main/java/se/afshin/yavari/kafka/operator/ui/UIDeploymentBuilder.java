package se.afshin.yavari.kafka.operator.ui;

import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.ProbeBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import jakarta.enterprise.context.ApplicationScoped;
import se.afshin.yavari.kafka.operator.crd.KafkaUI;
import se.afshin.yavari.kafka.operator.crd.KafkaUIDiscoveryConfig;
import se.afshin.yavari.kafka.operator.crd.KafkaUIEnvVar;
import se.afshin.yavari.kafka.operator.crd.KafkaUIOidcConfig;
import se.afshin.yavari.kafka.operator.crd.KafkaUIProbeConfig;
import se.afshin.yavari.kafka.operator.crd.KafkaUIResourceRequirements;
import se.afshin.yavari.kafka.operator.crd.KafkaUISpec;
import se.afshin.yavari.kafka.operator.crd.KafkaUITlsConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class UIDeploymentBuilder {

    public Deployment build(KafkaUI ui, OwnerReference ownerRef) {
        String name = ui.getMetadata().getName();
        String namespace = ui.getMetadata().getNamespace();
        KafkaUISpec spec = ui.getSpec();
        Map<String, String> labels = UILabels.labels(name);

        int containerPort = spec.getService().getPort();
        String tlsMount = spec.getTls().getMountPath();

        List<EnvVar> envVars = buildEnv(spec, containerPort, tlsMount);

        KafkaUIProbeConfig readiness = spec.getProbes().getReadiness();
        KafkaUIProbeConfig liveness = spec.getProbes().getLiveness();

        KafkaUIResourceRequirements res = spec.getResources();

        var volumes = new ArrayList<io.fabric8.kubernetes.api.model.Volume>();
        var mounts = new ArrayList<io.fabric8.kubernetes.api.model.VolumeMount>();
        KafkaUITlsConfig tls = spec.getTls();
        volumes.add(new VolumeBuilder()
                .withName("kafka-client-tls")
                .withNewSecret().withSecretName(tls.getSecretName()).endSecret()
                .build());
        mounts.add(new VolumeMountBuilder()
                .withName("kafka-client-tls")
                .withMountPath(tls.getMountPath())
                .withReadOnly(true)
                .build());

        return new DeploymentBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(namespace)
                    .withLabels(labels)
                    .withOwnerReferences(ownerRef)
                .endMetadata()
                .withNewSpec()
                    .withReplicas(spec.getReplicas())
                    .withNewSelector()
                        .withMatchLabels(labels)
                    .endSelector()
                    .withNewTemplate()
                        .withNewMetadata()
                            .withLabels(labels)
                        .endMetadata()
                        .withNewSpec()
                            .withServiceAccountName(name)
                            .withContainers(new ContainerBuilder()
                                    .withName("kafka-ui")
                                    .withImage(spec.getImage())
                                    .withImagePullPolicy(spec.getImagePullPolicy())
                                    .withPorts(new ContainerPortBuilder()
                                            .withName("http")
                                            .withContainerPort(containerPort)
                                            .build())
                                    .withEnv(envVars)
                                    .withVolumeMounts(mounts)
                                    .withReadinessProbe(new ProbeBuilder()
                                            .withNewHttpGet()
                                                .withPath(readiness.getPath())
                                                .withNewPort(containerPort)
                                            .endHttpGet()
                                            .withInitialDelaySeconds(readiness.getInitialDelaySeconds())
                                            .withPeriodSeconds(readiness.getPeriodSeconds())
                                            .build())
                                    .withLivenessProbe(new ProbeBuilder()
                                            .withNewHttpGet()
                                                .withPath(liveness.getPath())
                                                .withNewPort(containerPort)
                                            .endHttpGet()
                                            .withInitialDelaySeconds(liveness.getInitialDelaySeconds())
                                            .withPeriodSeconds(liveness.getPeriodSeconds())
                                            .build())
                                    .withResources(new ResourceRequirementsBuilder()
                                            .withRequests(toQuantities(res.getRequests()))
                                            .withLimits(toQuantities(res.getLimits()))
                                            .build())
                                    .build())
                            .withVolumes(volumes)
                        .endSpec()
                    .endTemplate()
                .endSpec()
                .build();
    }

    private List<EnvVar> buildEnv(KafkaUISpec spec, int containerPort, String tlsMount) {
        List<EnvVar> envVars = new ArrayList<>();

        KafkaUIOidcConfig oidc = spec.getOidc();
        if (oidc != null) {
            if (oidc.getIssuerUrl() != null) {
                envVars.add(new EnvVarBuilder().withName("OIDC_ISSUER_URL").withValue(oidc.getIssuerUrl()).build());
            }
            if (oidc.getClientId() != null) {
                envVars.add(new EnvVarBuilder().withName("OIDC_CLIENT_ID").withValue(oidc.getClientId()).build());
            }
            if (oidc.getClientSecretRef() != null) {
                envVars.add(new EnvVarBuilder()
                        .withName("OIDC_CLIENT_SECRET")
                        .withNewValueFrom()
                            .withNewSecretKeyRef(oidc.getClientSecretRef().getKey(),
                                    oidc.getClientSecretRef().getName(), false)
                        .endValueFrom()
                        .build());
            }
        }

        envVars.add(new EnvVarBuilder()
                .withName("KAFKA_UI_KAFKA_SECURITY_PROTOCOL").withValue("SASL_SSL").build());
        envVars.add(new EnvVarBuilder()
                .withName("KAFKA_UI_KAFKA_SSL_TLS_DIR").withValue(tlsMount).build());
        envVars.add(new EnvVarBuilder()
                .withName("UI_PORT").withValue(String.valueOf(containerPort)).build());

        KafkaUIDiscoveryConfig disc = spec.getDiscovery();
        envVars.add(new EnvVarBuilder()
                .withName("KAFKA_UI_CLUSTER_NAMESPACE").withValue(disc.getClusterNamespace()).build());
        envVars.add(new EnvVarBuilder()
                .withName("KAFKA_UI_PROXY_SERVICE_NAME").withValue(disc.getProxyServiceName()).build());
        envVars.add(new EnvVarBuilder()
                .withName("KAFKA_UI_PROXY_PORT").withValue(String.valueOf(disc.getProxyPort())).build());
        envVars.add(new EnvVarBuilder()
                .withName("KAFKA_UI_APICURIO_SERVICE_NAME").withValue(disc.getApicurioServiceName()).build());
        envVars.add(new EnvVarBuilder()
                .withName("KAFKA_UI_APICURIO_PORT").withValue(String.valueOf(disc.getApicurioPort())).build());
        if (disc.getDnsSuffix() != null && !disc.getDnsSuffix().isEmpty()) {
            envVars.add(new EnvVarBuilder()
                    .withName("KAFKA_UI_DNS_SUFFIX").withValue(disc.getDnsSuffix()).build());
        }

        // User-supplied extras override defaults when the name collides.
        if (spec.getEnv() != null) {
            for (KafkaUIEnvVar e : spec.getEnv()) {
                envVars.removeIf(existing -> existing.getName().equals(e.getName()));
                EnvVarBuilder b = new EnvVarBuilder().withName(e.getName());
                if (e.getValueFrom() != null) {
                    b.withNewValueFrom()
                        .withNewSecretKeyRef(e.getValueFrom().getKey(),
                                e.getValueFrom().getName(), false)
                        .endValueFrom();
                } else {
                    b.withValue(e.getValue());
                }
                envVars.add(b.build());
            }
        }
        return envVars;
    }

    private Map<String, Quantity> toQuantities(Map<String, String> raw) {
        if (raw == null) return Map.of();
        var out = new java.util.LinkedHashMap<String, Quantity>();
        raw.forEach((k, v) -> out.put(k, new Quantity(v)));
        return out;
    }
}
