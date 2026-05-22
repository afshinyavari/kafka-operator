package se.afshin.yavari.kafka.operator.crd;

import io.fabric8.kubernetes.api.model.ResourceRequirements;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Optional LinkedIn Cruise Control sub-spec on {@link KafkaClusterSpec}. Null = no
 * Cruise Control deployed.
 *
 * <p>Modelled on Strimzi's {@code Kafka.spec.cruiseControl}: when set, the operator
 * deploys ONE Cruise Control Deployment + Service (a singleton for the whole logical
 * Kafka cluster, on the primary cluster only) and injects the Cruise Control Metrics
 * Reporter into every broker.
 *
 * <p><b>One-time broker roll:</b> enabling this sub-spec adds {@code metric.reporters}
 * to broker server.properties, which rolls every broker once via the existing config-hash
 * mechanism. This is expected and only happens on the first enable.
 */
public class KafkaClusterCruiseControlSpec {

    /** Cruise Control container image (compiled from source on a UBI base). */
    private String image = "cruise-control-ubi:2.5.146";

    /** Replica count. Cruise Control is a singleton — the orchestrator hard-pins the
     *  effective replicas to 1 regardless of this value. Kept for shape parity with Strimzi. */
    private Integer replicas = 1;

    /** CPU/memory requests + limits for the Cruise Control container. Null → builder default. */
    private ResourceRequirements resources;

    /** Free-form overrides merged into {@code cruisecontrol.properties} (lowest precedence —
     *  operator-computed keys always win). */
    private Map<String, String> config = new HashMap<>();

    /** Optional ordered Cruise Control goal class names. Empty → Cruise Control defaults. */
    private List<String> goals = new ArrayList<>();

    /** Broker capacity inputs for the generated {@code capacity.json}. Null → derived/defaults. */
    private CruiseControlCapacityConfig capacity;

    /** Optional basic-auth on the Cruise Control REST API. Null/disabled → no auth. */
    private CruiseControlApiSecurity apiSecurity;

    /** Optional tuning for the broker-side metrics reporter. Null → reporter defaults. */
    private CruiseControlMetricsReporterConfig metricsReporter;

    /** Optional cert-manager PEM Secret holding Cruise Control's own AdminClient cert, used
     *  when the broker INTERNAL listener is mTLS ({@code spec.proxyMtls} set). When null,
     *  Cruise Control reuses the operator's AdminClient cert
     *  ({@code spec.proxyMtls.adminClientCertSecretRef}) — already a broker super-user, so
     *  no extra cert or {@code super.users} change is needed. */
    private String brokerClientCertSecretRef;

    /** Optional CN of a dedicated Cruise Control AdminClient cert. Set this only when
     *  {@code brokerClientCertSecretRef} points at a distinct cert — it is then appended to
     *  broker {@code super.users}. Null → Cruise Control uses the shared operator identity. */
    private String principal;

    public String getImage() { return image; }
    public void setImage(String image) { this.image = image; }

    public Integer getReplicas() { return replicas; }
    public void setReplicas(Integer replicas) { this.replicas = replicas; }

    public ResourceRequirements getResources() { return resources; }
    public void setResources(ResourceRequirements resources) { this.resources = resources; }

    public Map<String, String> getConfig() { return config; }
    public void setConfig(Map<String, String> config) { this.config = config; }

    public List<String> getGoals() { return goals; }
    public void setGoals(List<String> goals) { this.goals = goals; }

    public CruiseControlCapacityConfig getCapacity() { return capacity; }
    public void setCapacity(CruiseControlCapacityConfig capacity) { this.capacity = capacity; }

    public CruiseControlApiSecurity getApiSecurity() { return apiSecurity; }
    public void setApiSecurity(CruiseControlApiSecurity apiSecurity) { this.apiSecurity = apiSecurity; }

    public CruiseControlMetricsReporterConfig getMetricsReporter() { return metricsReporter; }
    public void setMetricsReporter(CruiseControlMetricsReporterConfig metricsReporter) {
        this.metricsReporter = metricsReporter;
    }

    public String getBrokerClientCertSecretRef() { return brokerClientCertSecretRef; }
    public void setBrokerClientCertSecretRef(String brokerClientCertSecretRef) {
        this.brokerClientCertSecretRef = brokerClientCertSecretRef;
    }

    public String getPrincipal() { return principal; }
    public void setPrincipal(String principal) { this.principal = principal; }
}
