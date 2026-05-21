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

    public ConfigMap build(MirrorMaker2 cr, String properties, OwnerReference ownerRef) {
        String name = cr.getMetadata().getName();
        String namespace = cr.getMetadata().getNamespace();
        return new ConfigMapBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(namespace)
                    .withLabels(Mm2Labels.labels(name))
                    .withOwnerReferences(ownerRef)
                .endMetadata()
                .withData(Map.of(PROPERTIES_KEY, properties))
                .build();
    }
}
