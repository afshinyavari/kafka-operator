package se.afshin.yavari.kafka.operator.crd;

import io.fabric8.generator.annotation.Required;
import io.fabric8.generator.annotation.ValidationRule;

import java.util.LinkedHashMap;
import java.util.Map;

/** Spec for a KafkaConnector CR — one connector running on a parent {@link KafkaConnect}
 *  worker cluster. The operator pushes the resolved config to the Connect REST API and
 *  reconciles drift back to spec on every poll. */
@ValidationRule(
        value = "!has(self.config) || (!('connector.class' in self.config) && !('name' in self.config) && !('tasks.max' in self.config))",
        message = "use the typed fields (connectorClass, tasksMax) and the CR's metadata.name — do not duplicate them in spec.config"
)
public class KafkaConnectorSpec {

    /** Desired runtime state. {@code running} (default), {@code paused}, or {@code stopped}.
     *  Lowercase string matching Strimzi convention. */
    public enum State { running, paused, stopped }

    @Required
    private KafkaConnectClusterRef connectClusterRef;

    /** Override metadata.name when the connector needs an identifier outside K8s
     *  naming rules (rare). Connect REST URL-encodes; keeping ASCII is recommended. */
    private String connectorName;

    @Required
    @ValidationRule(value = "self.size() > 0", message = "connectorClass must not be blank")
    private String connectorClass;

    @ValidationRule(value = "self >= 1", message = "tasksMax must be >= 1")
    private int tasksMax = 1;

    /** Free-form connector configuration. The reconciler injects {@code name},
     *  {@code connector.class}, and {@code tasks.max} from the typed fields above —
     *  putting them here is a CEL error. */
    private Map<String, String> config = new LinkedHashMap<>();

    /** Optional Secret to merge into the rendered config. Secret keys win over inline
     *  {@code config} keys with the same name. */
    private KafkaConnectorConfigFromSource configFrom;

    private State state = State.running;

    private KafkaConnectorAutoRestart autoRestart = new KafkaConnectorAutoRestart();

    public KafkaConnectClusterRef getConnectClusterRef() { return connectClusterRef; }
    public void setConnectClusterRef(KafkaConnectClusterRef connectClusterRef) {
        this.connectClusterRef = connectClusterRef;
    }

    public String getConnectorName() { return connectorName; }
    public void setConnectorName(String connectorName) { this.connectorName = connectorName; }

    public String getConnectorClass() { return connectorClass; }
    public void setConnectorClass(String connectorClass) { this.connectorClass = connectorClass; }

    public int getTasksMax() { return tasksMax; }
    public void setTasksMax(int tasksMax) { this.tasksMax = tasksMax; }

    public Map<String, String> getConfig() { return config; }
    public void setConfig(Map<String, String> config) { this.config = config; }

    public KafkaConnectorConfigFromSource getConfigFrom() { return configFrom; }
    public void setConfigFrom(KafkaConnectorConfigFromSource configFrom) { this.configFrom = configFrom; }

    public State getState() { return state; }
    public void setState(State state) { this.state = state; }

    public KafkaConnectorAutoRestart getAutoRestart() { return autoRestart; }
    public void setAutoRestart(KafkaConnectorAutoRestart autoRestart) { this.autoRestart = autoRestart; }
}
