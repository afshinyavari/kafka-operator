package se.afshin.yavari.kafka.operator.crd;

/**
 * Standard Kubernetes-style condition (mirrors the {@code metav1.Condition} shape used by
 * native K8s resources like {@code Deployment.status.conditions}). The cluster reconciler
 * sets these alongside the bespoke {@code phase} field — kubectl, dashboards, and
 * controllers can react to {@code conditions[*]} in a polymorphic way without knowing
 * about our specific phase enum.
 *
 * <p>Conventional types: {@code Available}, {@code Progressing}, {@code Degraded}. Status is
 * {@code "True"}, {@code "False"}, or {@code "Unknown"} (Kubernetes uses strings).
 */
public class Condition {

    private String type;
    private String status;
    private String reason;
    private String message;
    private String lastTransitionTime;
    private Long observedGeneration;

    public Condition() {}

    public Condition(String type, String status, String reason, String message,
                     String lastTransitionTime, Long observedGeneration) {
        this.type = type;
        this.status = status;
        this.reason = reason;
        this.message = message;
        this.lastTransitionTime = lastTransitionTime;
        this.observedGeneration = observedGeneration;
    }

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public String getReason() { return reason; }
    public void setReason(String reason) { this.reason = reason; }

    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }

    public String getLastTransitionTime() { return lastTransitionTime; }
    public void setLastTransitionTime(String lastTransitionTime) { this.lastTransitionTime = lastTransitionTime; }

    public Long getObservedGeneration() { return observedGeneration; }
    public void setObservedGeneration(Long observedGeneration) { this.observedGeneration = observedGeneration; }
}
