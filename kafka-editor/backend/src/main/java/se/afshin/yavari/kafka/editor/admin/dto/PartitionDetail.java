package se.afshin.yavari.kafka.editor.admin.dto;

import java.util.List;

/** One partition of a topic. {@code leader} is -1 when there is no leader. */
public record PartitionDetail(
        int partition,
        int leader,
        List<Integer> replicas,
        List<Integer> isr) {

    /** A partition is under-replicated when its ISR is smaller than its replica set. */
    public boolean underReplicated() {
        return isr.size() < replicas.size();
    }
}
