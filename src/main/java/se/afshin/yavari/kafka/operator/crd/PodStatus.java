package se.afshin.yavari.kafka.operator.crd;

public class PodStatus {

    private String name;
    private boolean ready;
    private int nodeId;

    /** SHA-256 of the desired PodSpec JSON — set when the pod is created/updated. */
    private String specHash;

    /** SHA-256 of the PodSpec the currently running pod was created with (from annotation). */
    private String currentSpecHash;

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public boolean isReady() { return ready; }
    public void setReady(boolean ready) { this.ready = ready; }

    public int getNodeId() { return nodeId; }
    public void setNodeId(int nodeId) { this.nodeId = nodeId; }

    public String getSpecHash() { return specHash; }
    public void setSpecHash(String specHash) { this.specHash = specHash; }

    public String getCurrentSpecHash() { return currentSpecHash; }
    public void setCurrentSpecHash(String currentSpecHash) { this.currentSpecHash = currentSpecHash; }
}
