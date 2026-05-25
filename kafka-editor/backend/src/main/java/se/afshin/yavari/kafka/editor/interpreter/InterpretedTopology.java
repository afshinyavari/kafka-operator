package se.afshin.yavari.kafka.editor.interpreter;

import java.util.List;

import org.apache.kafka.streams.Topology;

/**
 * The result of interpreting a project: a live Topology, its source nodes, and
 * every topic it reads or writes (so the run service can create them upfront).
 */
public record InterpretedTopology(
        Topology topology,
        List<SourceInfo> sources,
        List<String> topics) {
}
