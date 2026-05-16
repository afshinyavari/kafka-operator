package se.afshin.yavari.kafka.operator.crd;

import io.fabric8.kubernetes.api.model.LabelSelector;

import java.util.ArrayList;
import java.util.List;

public class KafkaPodSetSpec {

    private LabelSelector selector;

    /** Desired pod specs — one entry per replica, ordered by pod index. */
    private List<PodEntry> pods = new ArrayList<>();

    public LabelSelector getSelector() { return selector; }
    public void setSelector(LabelSelector selector) { this.selector = selector; }

    public List<PodEntry> getPods() { return pods; }
    public void setPods(List<PodEntry> pods) { this.pods = pods; }
}
