package se.afshin.yavari.kafka.operator.crd;

import io.fabric8.generator.annotation.ValidationRule;

import java.util.LinkedHashMap;
import java.util.Map;

/** Connect worker tunables (converters, internal-topic replication, escape-hatch
 *  passthrough). Most users should leave these defaults alone. */
public class KafkaConnectWorkerConfig {

    @ValidationRule(value = "self >= 1 && self <= 10", message = "internalReplicationFactor must be in [1,10]")
    private int internalReplicationFactor = 3;

    /** Default Connect key converter. */
    private String keyConverter = "org.apache.kafka.connect.json.JsonConverter";

    /** Default Connect value converter. */
    private String valueConverter = "org.apache.kafka.connect.json.JsonConverter";

    /** When true, Connect ships record schemas alongside values. Most modern setups
     *  want this false and pair with a schema-registry-aware converter. */
    private boolean keyConverterSchemasEnable = false;
    private boolean valueConverterSchemasEnable = false;

    /** Free-form passthrough — wins over every other rendered property except cluster
     *  identity (bootstrap, internal topic names, group.id). Use sparingly. */
    private Map<String, String> additionalProperties = new LinkedHashMap<>();

    public int getInternalReplicationFactor() { return internalReplicationFactor; }
    public void setInternalReplicationFactor(int internalReplicationFactor) {
        this.internalReplicationFactor = internalReplicationFactor;
    }

    public String getKeyConverter() { return keyConverter; }
    public void setKeyConverter(String keyConverter) { this.keyConverter = keyConverter; }

    public String getValueConverter() { return valueConverter; }
    public void setValueConverter(String valueConverter) { this.valueConverter = valueConverter; }

    public boolean isKeyConverterSchemasEnable() { return keyConverterSchemasEnable; }
    public void setKeyConverterSchemasEnable(boolean keyConverterSchemasEnable) {
        this.keyConverterSchemasEnable = keyConverterSchemasEnable;
    }

    public boolean isValueConverterSchemasEnable() { return valueConverterSchemasEnable; }
    public void setValueConverterSchemasEnable(boolean valueConverterSchemasEnable) {
        this.valueConverterSchemasEnable = valueConverterSchemasEnable;
    }

    public Map<String, String> getAdditionalProperties() { return additionalProperties; }
    public void setAdditionalProperties(Map<String, String> additionalProperties) {
        this.additionalProperties = additionalProperties;
    }
}
