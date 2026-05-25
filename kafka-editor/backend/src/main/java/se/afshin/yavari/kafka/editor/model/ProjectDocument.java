package se.afshin.yavari.kafka.editor.model;

import java.util.List;

/** The serialized topology document sent by the editor (schemaVersion 3). */
public record ProjectDocument(
        int schemaVersion,
        String id,
        ProjectMeta meta,
        List<ProjectNode> nodes,
        List<ProjectEdge> edges,
        List<RecordType> recordTypes,
        Catalog catalog) {

    public List<ProjectNode> nodes() {
        return nodes != null ? nodes : List.of();
    }

    public List<ProjectEdge> edges() {
        return edges != null ? edges : List.of();
    }

    public List<RecordType> recordTypes() {
        return recordTypes != null ? recordTypes : List.of();
    }
}
