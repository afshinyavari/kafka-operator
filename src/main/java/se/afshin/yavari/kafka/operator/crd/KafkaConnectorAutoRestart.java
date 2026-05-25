package se.afshin.yavari.kafka.operator.crd;

import io.fabric8.generator.annotation.ValidationRule;

/** Auto-restart a failed connector / failed tasks by calling
 *  {@code POST /connectors/<name>/restart?includeTasks=true&onlyFailed=true}. The
 *  reconciler tracks retry attempts in memory (reset on operator restart). */
public class KafkaConnectorAutoRestart {

    private boolean enabled = false;

    @ValidationRule(value = "self >= 0 && self <= 100",
            message = "maxRetries must be in [0, 100]")
    private int maxRetries = 3;

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }

    public int getMaxRetries() { return maxRetries; }
    public void setMaxRetries(int maxRetries) { this.maxRetries = maxRetries; }
}
