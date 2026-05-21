package se.afshin.yavari.kafka.operator.crd;

import io.fabric8.crd.generator.annotation.PrinterColumn;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class KafkaClusterStatus {

    public enum Phase { RECONCILING, READY, DEGRADED, FAILED }

    @PrinterColumn(name = "Phase", format = "", priority = 0)
    private Phase phase = Phase.RECONCILING;
    private String message;
    private String lastReconcileTime;
    private Long observedGeneration;

    /** Kubernetes-style conditions ({@code Available}, {@code Progressing}, {@code Degraded}).
     *  Complements the bespoke {@code phase} field — kubectl and generic tooling can react
     *  to these without knowing about the phase enum. */
    private List<Condition> conditions = new ArrayList<>();

    /** Per-pool readiness summary, keyed by pool name. */
    private Map<String, String> poolPhases = new LinkedHashMap<>();

    @PrinterColumn(name = "Kafka", format = "", priority = 0)
    private String currentKafkaVersion;

    @PrinterColumn(name = "Upgrade", format = "", priority = 1)
    private String upgradePhase;

    private Integer currentMetadataVersion;

    /** Proxy sub-status (post-Wave-4 merger of KafkaProxy into KafkaCluster). */
    private KafkaProxyStatus proxy;

    /** Apicurio Registry sub-status (Wave 4c merger). Null when spec.apicurio is unset. */
    private ApicurioRegistryStatus apicurio;

    public Phase getPhase() { return phase; }
    public void setPhase(Phase phase) { this.phase = phase; }

    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }

    public String getLastReconcileTime() { return lastReconcileTime; }
    public void setLastReconcileTime(String lastReconcileTime) { this.lastReconcileTime = lastReconcileTime; }

    public Long getObservedGeneration() { return observedGeneration; }
    public void setObservedGeneration(Long observedGeneration) { this.observedGeneration = observedGeneration; }

    public Map<String, String> getPoolPhases() { return poolPhases; }
    public void setPoolPhases(Map<String, String> poolPhases) { this.poolPhases = poolPhases; }

    public String getCurrentKafkaVersion() { return currentKafkaVersion; }
    public void setCurrentKafkaVersion(String currentKafkaVersion) { this.currentKafkaVersion = currentKafkaVersion; }

    public String getUpgradePhase() { return upgradePhase; }
    public void setUpgradePhase(String upgradePhase) { this.upgradePhase = upgradePhase; }

    public Integer getCurrentMetadataVersion() { return currentMetadataVersion; }
    public void setCurrentMetadataVersion(Integer currentMetadataVersion) { this.currentMetadataVersion = currentMetadataVersion; }

    public KafkaProxyStatus getProxy() { return proxy; }
    public void setProxy(KafkaProxyStatus proxy) { this.proxy = proxy; }

    public ApicurioRegistryStatus getApicurio() { return apicurio; }
    public void setApicurio(ApicurioRegistryStatus apicurio) { this.apicurio = apicurio; }

    public List<Condition> getConditions() { return conditions; }
    public void setConditions(List<Condition> conditions) { this.conditions = conditions; }
}
