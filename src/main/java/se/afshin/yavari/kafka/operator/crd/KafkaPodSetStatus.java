package se.afshin.yavari.kafka.operator.crd;

import io.fabric8.crd.generator.annotation.PrinterColumn;

import java.util.ArrayList;
import java.util.List;

public class KafkaPodSetStatus {

    @PrinterColumn(name = "Desired", format = "", priority = 0)
    private int replicas;
    @PrinterColumn(name = "Ready", format = "", priority = 0)
    private int readyReplicas;

    /** Name of the pod currently being rolled; empty string means no roll in progress. */
    private String currentRollingPod = "";

    private List<PodStatus> pods = new ArrayList<>();

    public int getReplicas() { return replicas; }
    public void setReplicas(int replicas) { this.replicas = replicas; }

    public int getReadyReplicas() { return readyReplicas; }
    public void setReadyReplicas(int readyReplicas) { this.readyReplicas = readyReplicas; }

    public String getCurrentRollingPod() { return currentRollingPod; }
    public void setCurrentRollingPod(String currentRollingPod) { this.currentRollingPod = currentRollingPod; }

    public List<PodStatus> getPods() { return pods; }
    public void setPods(List<PodStatus> pods) { this.pods = pods; }
}
