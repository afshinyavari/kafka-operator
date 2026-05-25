package se.afshin.yavari.kafka.operator.connect;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.OwnerReference;
import jakarta.enterprise.context.ApplicationScoped;
import se.afshin.yavari.kafka.operator.crd.KafkaConnect;

import java.util.LinkedHashMap;
import java.util.Map;

@ApplicationScoped
public class ConnectConfigMapBuilder {

    public static final String PROPERTIES_KEY = "connect-distributed.properties";
    public static final String JMX_CONFIG_KEY = "jmx-config.yaml";

    public ConfigMap build(KafkaConnect cr, String properties, String jmxConfigYaml,
                           OwnerReference ownerRef) {
        String name = cr.getMetadata().getName();
        String namespace = cr.getMetadata().getNamespace();
        Map<String, String> data = new LinkedHashMap<>();
        data.put(PROPERTIES_KEY, properties);
        if (jmxConfigYaml != null) {
            data.put(JMX_CONFIG_KEY, jmxConfigYaml);
        }
        return new ConfigMapBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(namespace)
                    .withLabels(ConnectLabels.labels(name))
                    .withOwnerReferences(ownerRef)
                .endMetadata()
                .withData(data)
                .build();
    }
}
