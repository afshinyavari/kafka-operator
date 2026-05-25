package se.afshin.yavari.kafka.operator.connect;

import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;
import se.afshin.yavari.kafka.operator.crd.KafkaConnector;
import se.afshin.yavari.kafka.operator.crd.KafkaConnectorSpec;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Pure function: render the final Connect connector config from the CR spec and a
 * resolved set of {@code configFrom} secret values.
 *
 * <p>Injection rules:
 * <ul>
 *   <li>{@code name} is the connector's identity ({@link KafkaConnector#resolvedConnectorName()}).
 *   <li>{@code connector.class} comes from the typed field.
 *   <li>{@code tasks.max} comes from the typed field.
 *   <li>{@code spec.config} keys are added next.
 *   <li>{@code configFrom} secret values are merged last (Secret keys win over inline keys).
 * </ul>
 */
@ApplicationScoped
public class ConnectorConfigRenderer {

    private static final Logger LOG = Logger.getLogger(ConnectorConfigRenderer.class);

    public Map<String, String> render(KafkaConnector cr, Map<String, String> resolvedSecretValues) {
        KafkaConnectorSpec spec = cr.getSpec();
        Map<String, String> out = new LinkedHashMap<>();
        out.put("name", cr.resolvedConnectorName());
        out.put("connector.class", spec.getConnectorClass());
        out.put("tasks.max", String.valueOf(spec.getTasksMax()));
        if (spec.getConfig() != null) {
            out.putAll(spec.getConfig());
        }
        if (resolvedSecretValues != null) {
            for (var e : resolvedSecretValues.entrySet()) {
                if (out.containsKey(e.getKey())
                        && !out.get(e.getKey()).equals(e.getValue())) {
                    LOG.debugf("Secret key '%s' overrides inline spec.config value for connector %s",
                            e.getKey(), cr.resolvedConnectorName());
                }
                out.put(e.getKey(), e.getValue());
            }
        }
        return out;
    }
}
