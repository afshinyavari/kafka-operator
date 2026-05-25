package se.afshin.yavari.kafka.editor.interpreter;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.LongNode;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import se.afshin.yavari.kafka.editor.model.AggregateField;
import se.afshin.yavari.kafka.editor.model.Catalog;
import se.afshin.yavari.kafka.editor.model.Predicate;
import se.afshin.yavari.kafka.editor.model.ProjectDocument;
import se.afshin.yavari.kafka.editor.model.ProjectEdge;
import se.afshin.yavari.kafka.editor.model.ProjectNode;
import se.afshin.yavari.kafka.editor.model.TopicDef;
import se.afshin.yavari.kafka.editor.model.ValueExpression;
import se.afshin.yavari.kafka.editor.model.ValueMappingEntry;
import se.afshin.yavari.kafka.editor.model.WindowConfig;
import se.afshin.yavari.kafka.editor.run.MetricsRegistry;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.BranchedKStream;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.TimeWindows;

/**
 * Interprets a topology JSON document into a real Kafka Streams `Topology` —
 * the third consumer of the editor model, after the UI and the TS codegen.
 * Stateless and stateful operators (R1–R3); each node's output is wrapped
 * with a peek that counts records for the run service.
 */
@ApplicationScoped
public class TopologyInterpreter {

    @Inject
    ObjectMapper mapper;

    private Serde<String> keySerde;
    private Serde<JsonNode> valueSerde;

    public InterpretedTopology build(
            ProjectDocument doc, MetricsRegistry metrics, String registryUrl) {
        StreamsBuilder builder = new StreamsBuilder();
        keySerde = Serdes.String();
        valueSerde = new JsonNodeSerde(mapper);
        Consumed<String, JsonNode> consumed = Consumed.with(keySerde, valueSerde);
        // Source topics may carry registry-encoded Avro — decode via the registry.
        Consumed<String, JsonNode> sourceConsumed =
                registryUrl != null && !registryUrl.isBlank()
                        ? Consumed.with(keySerde, new RegistryAvroSerde(mapper, registryUrl))
                        : consumed;
        Produced<String, JsonNode> produced = Produced.with(keySerde, valueSerde);
        Grouped<String, JsonNode> grouped = Grouped.with(keySerde, valueSerde);

        List<ProjectNode> sorted = TopoSort.sort(doc.nodes(), doc.edges());
        List<ProjectEdge> edges = doc.edges();
        Map<String, String> topicNames = topicNamesById(doc.catalog());
        Map<String, NodeOutput> outputs = new HashMap<>();
        Map<String, Map<String, KStream<String, JsonNode>>> branchOutputs =
                new HashMap<>();
        List<SourceInfo> sources = new ArrayList<>();
        Set<String> usedTopics = new LinkedHashSet<>();

        for (ProjectNode node : sorted) {
            String type = node.type() == null ? "" : node.type();
            JsonNode cfg = node.config();
            String id = node.id();

            switch (type) {
                case "source", "table-source", "connect-source" -> {
                    String topic = topicFor(node, topicNames);
                    usedTopics.add(topic);
                    if (type.equals("table-source")) {
                        KTable<String, JsonNode> table =
                                builder.table(topic, sourceConsumed);
                        table.toStream().peek((k, v) -> metrics.increment(id));
                        outputs.put(id, NodeOutput.ofTable(table));
                    } else {
                        KStream<String, JsonNode> stream =
                                builder.stream(topic, sourceConsumed);
                        outputs.put(id, NodeOutput.ofStream(counted(stream, id, metrics)));
                    }
                    sources.add(new SourceInfo(
                            id, topic, effectiveValueRecordTypeId(node, doc)));
                }
                case "sink", "connect-sink" -> {
                    KStream<String, JsonNode> in = stream(node, "in", edges, outputs, branchOutputs);
                    if (in != null) {
                        String topic = topicFor(node, topicNames);
                        usedTopics.add(topic);
                        counted(in, id, metrics).to(topic, produced);
                    }
                }
                case "foreach" -> {
                    KStream<String, JsonNode> in = stream(node, "in", edges, outputs, branchOutputs);
                    if (in != null) {
                        counted(in, id, metrics).foreach((k, v) -> { });
                    }
                }
                case "filter" -> {
                    KStream<String, JsonNode> in = stream(node, "in", edges, outputs, branchOutputs);
                    if (in == null) {
                        continue;
                    }
                    Predicate p = predicate(cfg);
                    outputs.put(id, NodeOutput.ofStream(counted(
                            in.filter((k, v) -> LogicInterpreter.evaluatePredicate(p, v)),
                            id, metrics)));
                }
                case "filter-not" -> {
                    KStream<String, JsonNode> in = stream(node, "in", edges, outputs, branchOutputs);
                    if (in == null) {
                        continue;
                    }
                    Predicate p = predicate(cfg);
                    outputs.put(id, NodeOutput.ofStream(counted(
                            in.filterNot((k, v) -> LogicInterpreter.evaluatePredicate(p, v)),
                            id, metrics)));
                }
                case "map-values" -> {
                    KStream<String, JsonNode> in = stream(node, "in", edges, outputs, branchOutputs);
                    if (in == null) {
                        continue;
                    }
                    List<ValueMappingEntry> vm = valueMapping(cfg, "valueMapping");
                    outputs.put(id, NodeOutput.ofStream(counted(
                            in.mapValues(v -> LogicInterpreter.applyValueMapping(vm, v, mapper)),
                            id, metrics)));
                }
                case "map" -> {
                    KStream<String, JsonNode> in = stream(node, "in", edges, outputs, branchOutputs);
                    if (in == null) {
                        continue;
                    }
                    List<ValueMappingEntry> vm = valueMapping(cfg, "valueMapping");
                    ValueExpression keyExpr = expression(cfg, "keyExpression");
                    outputs.put(id, NodeOutput.ofStream(counted(
                            in.map((k, v) -> KeyValue.pair(keyText(keyExpr, k, v),
                                    LogicInterpreter.applyValueMapping(vm, v, mapper))),
                            id, metrics)));
                }
                case "select-key" -> {
                    KStream<String, JsonNode> in = stream(node, "in", edges, outputs, branchOutputs);
                    if (in == null) {
                        continue;
                    }
                    ValueExpression keyExpr = expression(cfg, "keyExpression");
                    outputs.put(id, NodeOutput.ofStream(counted(
                            in.selectKey((k, v) -> keyText(keyExpr, k, v)), id, metrics)));
                }
                case "peek" -> {
                    KStream<String, JsonNode> in = stream(node, "in", edges, outputs, branchOutputs);
                    if (in != null) {
                        outputs.put(id, NodeOutput.ofStream(counted(in, id, metrics)));
                    }
                }
                case "flat-map-values" -> {
                    KStream<String, JsonNode> in = stream(node, "in", edges, outputs, branchOutputs);
                    if (in != null) {
                        outputs.put(id, NodeOutput.ofStream(counted(
                                in.flatMapValues(v -> List.of(v)), id, metrics)));
                    }
                }
                case "flat-map" -> {
                    KStream<String, JsonNode> in = stream(node, "in", edges, outputs, branchOutputs);
                    if (in != null) {
                        outputs.put(id, NodeOutput.ofStream(counted(
                                in.flatMap((k, v) -> List.of(KeyValue.pair(k, v))),
                                id, metrics)));
                    }
                }
                case "repartition" -> {
                    KStream<String, JsonNode> in = stream(node, "in", edges, outputs, branchOutputs);
                    if (in != null) {
                        outputs.put(id, NodeOutput.ofStream(counted(
                                in.repartition(), id, metrics)));
                    }
                }
                case "to-stream" -> {
                    KStream<String, JsonNode> in = stream(node, "in", edges, outputs, branchOutputs);
                    if (in != null) {
                        outputs.put(id, NodeOutput.ofStream(counted(in, id, metrics)));
                    }
                }
                case "to-table" -> {
                    KStream<String, JsonNode> in = stream(node, "in", edges, outputs, branchOutputs);
                    if (in != null) {
                        outputs.put(id, NodeOutput.ofTable(
                                counted(in, id, metrics).toTable()));
                    }
                }
                case "branch" -> {
                    KStream<String, JsonNode> in = stream(node, "in", edges, outputs, branchOutputs);
                    if (in != null) {
                        branchOutputs.put(id, buildBranches(node, counted(in, id, metrics)));
                    }
                }
                case "group-by-key" -> {
                    KStream<String, JsonNode> in = stream(node, "in", edges, outputs, branchOutputs);
                    if (in != null) {
                        outputs.put(id, NodeOutput.ofGrouped(
                                counted(in, id, metrics).groupByKey(grouped)));
                    }
                }
                case "group-by" -> {
                    KStream<String, JsonNode> in = stream(node, "in", edges, outputs, branchOutputs);
                    if (in != null) {
                        ValueExpression keyExpr = expression(cfg, "keyExpression");
                        outputs.put(id, NodeOutput.ofGrouped(counted(in, id, metrics)
                                .groupBy((k, v) -> keyText(keyExpr, k, v), grouped)));
                    }
                }
                case "count" -> {
                    KGroupedStream<String, JsonNode> g = grouped(node, edges, outputs, branchOutputs);
                    if (g == null) {
                        continue;
                    }
                    WindowConfig w = windowConfig(cfg);
                    KStream<String, JsonNode> result = isWindowed(w)
                            ? g.windowedBy(timeWindows(w)).count().toStream()
                                    .map((wk, v) -> KeyValue.pair(wk.key(),
                                            (JsonNode) LongNode.valueOf(v)))
                            : g.count().toStream()
                                    .mapValues(v -> (JsonNode) LongNode.valueOf(v));
                    outputs.put(id, NodeOutput.ofStream(counted(result, id, metrics)));
                }
                case "reduce" -> {
                    KGroupedStream<String, JsonNode> g = grouped(node, edges, outputs, branchOutputs);
                    if (g == null) {
                        continue;
                    }
                    String strategy = text(cfg, "reducer");
                    WindowConfig w = windowConfig(cfg);
                    KStream<String, JsonNode> result = isWindowed(w)
                            ? g.windowedBy(timeWindows(w))
                                    .reduce((a, b) -> LogicInterpreter.reduceValue(strategy, a, b))
                                    .toStream().map((wk, v) -> KeyValue.pair(wk.key(), v))
                            : g.reduce((a, b) -> LogicInterpreter.reduceValue(strategy, a, b))
                                    .toStream();
                    outputs.put(id, NodeOutput.ofStream(counted(result, id, metrics)));
                }
                case "aggregate" -> {
                    KGroupedStream<String, JsonNode> g = grouped(node, edges, outputs, branchOutputs);
                    if (g == null) {
                        continue;
                    }
                    List<AggregateField> fields = aggregateFields(cfg);
                    WindowConfig w = windowConfig(cfg);
                    Initializer<JsonNode> init =
                            () -> LogicInterpreter.aggregateInit(fields, mapper);
                    Aggregator<String, JsonNode, JsonNode> agg = (k, v, acc) ->
                            LogicInterpreter.aggregateAdd(fields, v, acc, mapper);
                    KStream<String, JsonNode> result = isWindowed(w)
                            ? g.windowedBy(timeWindows(w)).aggregate(init, agg)
                                    .toStream().map((wk, v) -> KeyValue.pair(wk.key(), v))
                            : g.aggregate(init, agg).toStream();
                    outputs.put(id, NodeOutput.ofStream(counted(result, id, metrics)));
                }
                case "stream-stream-join" -> {
                    NodeOutput left = input(node, "left", edges, outputs, branchOutputs);
                    NodeOutput right = input(node, "right", edges, outputs, branchOutputs);
                    if (left == null || right == null
                            || left.asStream() == null || right.asStream() == null) {
                        continue;
                    }
                    List<ValueMappingEntry> joiner = valueMapping(cfg, "valueJoiner");
                    long windowMs = longConfig(cfg, "windowSizeMs", 60_000L);
                    KStream<String, JsonNode> result = left.asStream().join(
                            right.asStream(),
                            (lv, rv) -> LogicInterpreter.joinValues(joiner, lv, rv, mapper),
                            JoinWindows.ofTimeDifferenceWithNoGrace(
                                    Duration.ofMillis(windowMs)),
                            StreamJoined.with(keySerde, valueSerde, valueSerde));
                    outputs.put(id, NodeOutput.ofStream(counted(result, id, metrics)));
                }
                case "stream-table-join" -> {
                    NodeOutput left = input(node, "left", edges, outputs, branchOutputs);
                    NodeOutput right = input(node, "right", edges, outputs, branchOutputs);
                    if (left == null || right == null
                            || left.asStream() == null || right.asTable() == null) {
                        continue;
                    }
                    List<ValueMappingEntry> joiner = valueMapping(cfg, "valueJoiner");
                    KStream<String, JsonNode> result = left.asStream().join(
                            right.asTable(),
                            (lv, rv) -> LogicInterpreter.joinValues(joiner, lv, rv, mapper));
                    outputs.put(id, NodeOutput.ofStream(counted(result, id, metrics)));
                }
                case "table-table-join" -> {
                    NodeOutput left = input(node, "left", edges, outputs, branchOutputs);
                    NodeOutput right = input(node, "right", edges, outputs, branchOutputs);
                    if (left == null || right == null
                            || left.asTable() == null || right.asTable() == null) {
                        continue;
                    }
                    List<ValueMappingEntry> joiner = valueMapping(cfg, "valueJoiner");
                    KTable<String, JsonNode> joined = left.asTable().join(
                            right.asTable(),
                            (lv, rv) -> LogicInterpreter.joinValues(joiner, lv, rv, mapper));
                    joined.toStream().peek((k, v) -> metrics.increment(id));
                    outputs.put(id, NodeOutput.ofTable(joined));
                }
                default -> {
                    KStream<String, JsonNode> in = stream(node, "in", edges, outputs, branchOutputs);
                    if (in != null) {
                        outputs.put(id, NodeOutput.ofStream(counted(in, id, metrics)));
                    }
                }
            }
        }
        return new InterpretedTopology(
                builder.build(), sources, new ArrayList<>(usedTopics));
    }

    /* ---------------------------------------------------------------------- */

    private KStream<String, JsonNode> counted(
            KStream<String, JsonNode> stream, String nodeId, MetricsRegistry metrics) {
        return stream.peek((k, v) -> metrics.increment(nodeId));
    }

    private Map<String, KStream<String, JsonNode>> buildBranches(
            ProjectNode node, KStream<String, JsonNode> in) {
        Map<String, KStream<String, JsonNode>> result = new HashMap<>();
        JsonNode branches = node.config() == null ? null : node.config().get("branches");
        if (branches == null || !branches.isArray()) {
            return result;
        }
        BranchedKStream<String, JsonNode> split = in.split();
        for (JsonNode branch : branches) {
            String branchId = branch.path("id").asText("");
            JsonNode predNode = branch.get("predicate");
            Predicate p = predNode == null || predNode.isNull()
                    ? null
                    : mapper.convertValue(predNode, Predicate.class);
            split = split.branch(
                    (k, v) -> LogicInterpreter.evaluatePredicate(p, v),
                    Branched.withConsumer(ks -> result.put(branchId, ks)));
        }
        split.noDefaultBranch();
        return result;
    }

    /** Resolve a node's input on a handle as a KStream (coercing a table). */
    private KStream<String, JsonNode> stream(
            ProjectNode node, String handle, List<ProjectEdge> edges,
            Map<String, NodeOutput> outputs,
            Map<String, Map<String, KStream<String, JsonNode>>> branchOutputs) {
        NodeOutput out = input(node, handle, edges, outputs, branchOutputs);
        return out == null ? null : out.asStream();
    }

    /** Resolve a node's input as a KGroupedStream (count / reduce / aggregate). */
    private KGroupedStream<String, JsonNode> grouped(
            ProjectNode node, List<ProjectEdge> edges,
            Map<String, NodeOutput> outputs,
            Map<String, Map<String, KStream<String, JsonNode>>> branchOutputs) {
        NodeOutput out = input(node, "in", edges, outputs, branchOutputs);
        return out == null ? null : out.grouped();
    }

    private NodeOutput input(
            ProjectNode node, String handle, List<ProjectEdge> edges,
            Map<String, NodeOutput> outputs,
            Map<String, Map<String, KStream<String, JsonNode>>> branchOutputs) {
        for (ProjectEdge edge : edges) {
            if (!node.id().equals(edge.target())) {
                continue;
            }
            if (handle != null && !handle.equals(edge.targetHandle())) {
                continue;
            }
            Map<String, KStream<String, JsonNode>> branches =
                    branchOutputs.get(edge.source());
            if (branches != null) {
                KStream<String, JsonNode> branchStream =
                        branches.get(edge.sourceHandle());
                return branchStream == null ? null : NodeOutput.ofStream(branchStream);
            }
            return outputs.get(edge.source());
        }
        return null;
    }

    private boolean isWindowed(WindowConfig window) {
        return window != null && window.type() != null
                && !"none".equals(window.type());
    }

    private TimeWindows timeWindows(WindowConfig window) {
        long size = window.sizeMs() > 0 ? window.sizeMs() : 60_000L;
        TimeWindows windows =
                TimeWindows.ofSizeWithNoGrace(Duration.ofMillis(size));
        if ("hopping".equals(window.type()) && window.advanceMs() != null
                && window.advanceMs() > 0 && window.advanceMs() <= size) {
            windows = windows.advanceBy(Duration.ofMillis(window.advanceMs()));
        }
        return windows;
    }

    private Map<String, String> topicNamesById(Catalog catalog) {
        Map<String, String> names = new HashMap<>();
        if (catalog != null) {
            for (TopicDef topic : catalog.topics()) {
                names.put(topic.id(), topic.name());
            }
        }
        return names;
    }

    private String topicFor(ProjectNode node, Map<String, String> topicNames) {
        String topicId = text(node.config(), "topicId");
        String name = topicId == null ? null : topicNames.get(topicId);
        return name != null ? name : "topic-" + node.id();
    }

    /** A source node's value record type — its own override, else the topic's. */
    private String effectiveValueRecordTypeId(ProjectNode node, ProjectDocument doc) {
        String override = text(node.config(), "valueRecordTypeId");
        if (override != null && !override.isBlank()) {
            return override;
        }
        String topicId = text(node.config(), "topicId");
        if (topicId == null) {
            return null;
        }
        for (TopicDef topic : doc.catalog().topics()) {
            if (topicId.equals(topic.id())) {
                return topic.valueRecordTypeId();
            }
        }
        return null;
    }

    private String keyText(ValueExpression keyExpr, String key, JsonNode value) {
        if (keyExpr == null) {
            return key;
        }
        JsonNode result = LogicInterpreter.evaluateExpression(keyExpr, value, mapper);
        return result == null || result.isNull() ? null : result.asText();
    }

    private Predicate predicate(JsonNode cfg) {
        JsonNode p = cfg == null ? null : cfg.get("predicate");
        return p == null || p.isNull() ? null : mapper.convertValue(p, Predicate.class);
    }

    private ValueExpression expression(JsonNode cfg, String key) {
        JsonNode e = cfg == null ? null : cfg.get(key);
        return e == null || e.isNull()
                ? null
                : mapper.convertValue(e, ValueExpression.class);
    }

    private List<ValueMappingEntry> valueMapping(JsonNode cfg, String key) {
        JsonNode vm = cfg == null ? null : cfg.get(key);
        if (vm == null || !vm.isArray()) {
            return List.of();
        }
        return mapper.convertValue(vm, new TypeReference<List<ValueMappingEntry>>() { });
    }

    private List<AggregateField> aggregateFields(JsonNode cfg) {
        JsonNode af = cfg == null ? null : cfg.get("aggregation");
        if (af == null || !af.isArray()) {
            return List.of();
        }
        return mapper.convertValue(af, new TypeReference<List<AggregateField>>() { });
    }

    private WindowConfig windowConfig(JsonNode cfg) {
        JsonNode w = cfg == null ? null : cfg.get("window");
        return w == null || w.isNull()
                ? null
                : mapper.convertValue(w, WindowConfig.class);
    }

    private long longConfig(JsonNode cfg, String key, long fallback) {
        JsonNode v = cfg == null ? null : cfg.get(key);
        return v == null || !v.isNumber() ? fallback : v.asLong();
    }

    private String text(JsonNode cfg, String key) {
        if (cfg == null) {
            return null;
        }
        JsonNode v = cfg.get(key);
        return v == null || v.isNull() ? null : v.asText();
    }
}
