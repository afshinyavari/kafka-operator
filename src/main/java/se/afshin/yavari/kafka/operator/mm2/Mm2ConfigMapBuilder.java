package se.afshin.yavari.kafka.operator.mm2;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.OwnerReference;
import jakarta.enterprise.context.ApplicationScoped;
import se.afshin.yavari.kafka.operator.crd.MirrorMaker2;

import java.util.Map;

@ApplicationScoped
public class Mm2ConfigMapBuilder {

    public static final String PROPERTIES_KEY = "mm2.properties";
    /** Key the bundled JMX exporter config is stored under, when metrics are enabled. */
    public static final String JMX_CONFIG_KEY = "jmx-config.yaml";

    /**
     * @param jmxConfigYaml the bundled JMX exporter config, or {@code null} when metrics are
     *                      disabled — when non-null it is added as a {@code jmx-config.yaml} key.
     */
    public ConfigMap build(MirrorMaker2 cr, String properties, String jmxConfigYaml,
                           OwnerReference ownerRef) {
        String name = cr.getMetadata().getName();
        String namespace = cr.getMetadata().getNamespace();
        Map<String, String> data = new java.util.LinkedHashMap<>();
        data.put(PROPERTIES_KEY, properties);
        if (jmxConfigYaml != null) {
            data.put(JMX_CONFIG_KEY, jmxConfigYaml);
        }
        return new ConfigMapBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(namespace)
                    .withLabels(Mm2Labels.labels(name))
                    .withOwnerReferences(ownerRef)
                .endMetadata()
                .withData(data)
                .build();
    }
}
