package se.afshin.yavari.kafka.operator.crd;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.fabric8.generator.annotation.ValidationRule;

/** Discriminated union for one side of an MM2 flow: exactly one of kafkaClusterRef
 *  (operator-managed KafkaCluster CR) or external (raw bootstrap + credentials). */
@ValidationRule(
        value = "(has(self.kafkaClusterRef) && !has(self.external)) || (!has(self.kafkaClusterRef) && has(self.external))",
        message = "exactly one of kafkaClusterRef or external must be set"
)
public class Mm2Endpoint {

    /** Reference to a managed KafkaCluster CR. The operator resolves this to the
     *  cluster's proxy bootstrap (not the broker headless service) so MM2 traffic
     *  flows through the existing Kroxylicious proxy. */
    private KafkaClusterRef kafkaClusterRef;

    /** Raw external endpoint (bootstrap + credentials). */
    private Mm2ExternalEndpoint external;

    public KafkaClusterRef getKafkaClusterRef() { return kafkaClusterRef; }
    public void setKafkaClusterRef(KafkaClusterRef kafkaClusterRef) { this.kafkaClusterRef = kafkaClusterRef; }

    public Mm2ExternalEndpoint getExternal() { return external; }
    public void setExternal(Mm2ExternalEndpoint external) { this.external = external; }

    @JsonIgnore
    public boolean hasManaged() { return kafkaClusterRef != null; }

    @JsonIgnore
    public boolean hasExternal() { return external != null; }
}
