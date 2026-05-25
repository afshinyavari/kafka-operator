package se.afshin.yavari.kafka.operator.connect;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.KafkaConnector;
import se.afshin.yavari.kafka.operator.crd.KafkaConnectorSpec;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ConnectorConfigRendererTest {

    private final ConnectorConfigRenderer renderer = new ConnectorConfigRenderer();

    private KafkaConnector cr(String name, String clazz, int tasksMax, Map<String, String> cfg) {
        KafkaConnector cr = new KafkaConnector();
        cr.setMetadata(new ObjectMetaBuilder().withName(name).withNamespace("kafka").build());
        KafkaConnectorSpec spec = new KafkaConnectorSpec();
        spec.setConnectorClass(clazz);
        spec.setTasksMax(tasksMax);
        spec.setConfig(cfg);
        cr.setSpec(spec);
        return cr;
    }

    @Test
    void injectsNameClassAndTasksMax() {
        Map<String, String> rendered = renderer.render(
                cr("file-src", "org.apache.kafka.connect.file.FileStreamSourceConnector", 2,
                        new LinkedHashMap<>(Map.of("file", "/tmp/x", "topic", "t"))),
                Map.of());

        assertThat(rendered).containsEntry("name", "file-src");
        assertThat(rendered).containsEntry("connector.class",
                "org.apache.kafka.connect.file.FileStreamSourceConnector");
        assertThat(rendered).containsEntry("tasks.max", "2");
        assertThat(rendered).containsEntry("file", "/tmp/x");
        assertThat(rendered).containsEntry("topic", "t");
    }

    @Test
    void connectorNameOverrideHonored() {
        KafkaConnector cr = cr("file-src", "X", 1, Map.of());
        cr.getSpec().setConnectorName("explicit-name");
        Map<String, String> rendered = renderer.render(cr, Map.of());
        assertThat(rendered).containsEntry("name", "explicit-name");
    }

    @Test
    void configFromSecretMergesAndWinsOverInline() {
        Map<String, String> inline = new LinkedHashMap<>();
        inline.put("database.user", "from-inline");
        inline.put("database.host", "db.local");

        Map<String, String> fromSecret = Map.of(
                "database.user", "from-secret",
                "database.password", "shh");

        Map<String, String> rendered = renderer.render(
                cr("c", "X", 1, inline), fromSecret);

        assertThat(rendered).containsEntry("database.user", "from-secret"); // secret wins
        assertThat(rendered).containsEntry("database.host", "db.local");
        assertThat(rendered).containsEntry("database.password", "shh");
    }
}
