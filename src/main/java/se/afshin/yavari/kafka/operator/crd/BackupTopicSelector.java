package se.afshin.yavari.kafka.operator.crd;

import java.util.List;

/** Topic include/exclude patterns. Maps to the osodevops config {@code topics.include}
 *  / {@code topics.exclude} lists (literal names or {@code *} globs). */
public class BackupTopicSelector {

    /** Topics to back up / restore. Default: all topics. */
    private List<String> include = List.of("*");

    /** Topics to skip. Default: Kafka/registry internal topics. */
    private List<String> exclude = List.of("__consumer_offsets", "_schemas");

    public List<String> getInclude() { return include; }
    public void setInclude(List<String> include) { this.include = include; }

    public List<String> getExclude() { return exclude; }
    public void setExclude(List<String> exclude) { this.exclude = exclude; }
}
