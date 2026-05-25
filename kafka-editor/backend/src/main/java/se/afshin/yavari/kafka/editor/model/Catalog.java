package se.afshin.yavari.kafka.editor.model;

import java.util.List;

/** The catalog — topics referenced by source and sink nodes. */
public record Catalog(List<TopicDef> topics) {

    public List<TopicDef> topics() {
        return topics != null ? topics : List.of();
    }
}
