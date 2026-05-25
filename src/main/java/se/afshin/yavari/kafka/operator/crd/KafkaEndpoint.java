package se.afshin.yavari.kafka.operator.crd;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.fabric8.generator.annotation.ValidationRule;

/** Discriminated union describing one Kafka attachment: exactly one of kafkaClusterRef
 *  (operator-managed KafkaCluster CR) or external (raw bootstrap + credentials).
 *
 *  <p>Reused by both MirrorMaker2 (per-side source/target) and KafkaConnect
 *  (the single cluster the worker attaches to). */
@ValidationRule(
        value = "(has(self.kafkaClusterRef) && !has(self.external)) || (!has(self.kafkaClusterRef) && has(self.external))",
        message = "exactly one of kafkaClusterRef or external must be set"
)
public class KafkaEndpoint {

    /** Reference to a managed KafkaCluster CR. The operator resolves this to the
     *  cluster's proxy bootstrap (not the broker headless service) so traffic flows
     *  through the existing Kroxylicious proxy. */
    private KafkaClusterRef kafkaClusterRef;

    /** Raw external endpoint (bootstrap + credentials). */
    private KafkaEndpointExternal external;

    /** Name of a Secret holding OAuth2 client-credentials for authenticating to this
     *  endpoint's schema registry. Required when the registry sits behind an OIDC-gated
     *  proxy — notably a managed KafkaCluster's {@code apicurio-rbac-proxy}, which rejects
     *  unauthenticated schema writes. Expected keys: {@code token-url}, {@code client-id},
     *  {@code client-secret} (and optional {@code scope}). When unset, the schema-sync SMT
     *  accesses this endpoint's registry without authentication. */
    private String schemaRegistryAuthSecretRef;

    public KafkaClusterRef getKafkaClusterRef() { return kafkaClusterRef; }
    public void setKafkaClusterRef(KafkaClusterRef kafkaClusterRef) { this.kafkaClusterRef = kafkaClusterRef; }

    public KafkaEndpointExternal getExternal() { return external; }
    public void setExternal(KafkaEndpointExternal external) { this.external = external; }

    public String getSchemaRegistryAuthSecretRef() { return schemaRegistryAuthSecretRef; }
    public void setSchemaRegistryAuthSecretRef(String schemaRegistryAuthSecretRef) {
        this.schemaRegistryAuthSecretRef = schemaRegistryAuthSecretRef;
    }

    @JsonIgnore
    public boolean hasManaged() { return kafkaClusterRef != null; }

    @JsonIgnore
    public boolean hasExternal() { return external != null; }
}
