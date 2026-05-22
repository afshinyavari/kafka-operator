package se.afshin.yavari.kafka.operator.crd;

import io.fabric8.generator.annotation.Required;

/** PersistentVolumeClaim storage target — the kafka-backup tool writes to a mounted
 *  filesystem path. Use a {@code ReadWriteMany} PVC, or pin the workload to one
 *  cluster via {@code spec.placement}. */
public class PvcStorageConfig {

    @Required
    private String claimName;

    /** Optional sub-path within the PVC. */
    private String subPath = "";

    public String getClaimName() { return claimName; }
    public void setClaimName(String claimName) { this.claimName = claimName; }

    public String getSubPath() { return subPath; }
    public void setSubPath(String subPath) { this.subPath = subPath; }
}
