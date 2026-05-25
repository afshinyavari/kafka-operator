package se.afshin.yavari.kafka.editor.interpreter;

import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

import se.afshin.yavari.kafka.editor.model.AggregateField;
import se.afshin.yavari.kafka.editor.model.Condition;
import se.afshin.yavari.kafka.editor.model.Predicate;
import se.afshin.yavari.kafka.editor.model.ValueExpression;
import se.afshin.yavari.kafka.editor.model.ValueMappingEntry;

/**
 * Runtime interpreter for the editor's structured logic — the counterpart of
 * the TS-side codegen compiler. Evaluates predicates, value expressions and
 * value mappings against `JsonNode` records.
 */
public final class LogicInterpreter {

    private LogicInterpreter() {
    }

    /** Evaluate a boolean predicate against a record. Empty predicate = match all. */
    public static boolean evaluatePredicate(Predicate predicate, JsonNode record) {
        if (predicate == null || predicate.conditions().isEmpty()) {
            return true;
        }
        boolean and = !"OR".equalsIgnoreCase(predicate.combinator());
        for (Condition condition : predicate.conditions()) {
            boolean result = evaluateCondition(condition, record);
            if (and && !result) {
                return false;
            }
            if (!and && result) {
                return true;
            }
        }
        return and;
    }

    static boolean evaluateCondition(Condition condition, JsonNode record) {
        JsonNode field = resolvePath(record, condition.field());
        String value = condition.value() == null ? "" : condition.value();
        String op = condition.operator() == null ? "" : condition.operator();
        return switch (op) {
            case "isNull" -> field == null || field.isNull();
            case "isNotNull" -> field != null && !field.isNull();
            case "eq" -> valueEquals(field, value);
            case "neq" -> !valueEquals(field, value);
            case "contains" -> text(field).contains(value);
            case "gt", "gte", "lt", "lte" -> numericCompare(field, value, op);
            default -> false;
        };
    }

    /** Evaluate a value expression — a field reference or a literal. */
    public static JsonNode evaluateExpression(
            ValueExpression expression, JsonNode record, ObjectMapper mapper) {
        if (expression == null) {
            return NullNode.getInstance();
        }
        if ("literal".equals(expression.kind())) {
            return literalNode(expression.value());
        }
        JsonNode resolved = resolvePath(record, expression.path());
        return resolved == null ? NullNode.getInstance() : resolved;
    }

    /** Apply a value mapping, producing a new object (empty mapping = identity). */
    public static JsonNode applyValueMapping(
            List<ValueMappingEntry> entries, JsonNode record, ObjectMapper mapper) {
        if (entries == null || entries.isEmpty()) {
            return record;
        }
        ObjectNode out = mapper.createObjectNode();
        for (ValueMappingEntry entry : entries) {
            if (entry.outputField() == null || entry.outputField().isEmpty()) {
                continue;
            }
            out.set(entry.outputField(),
                    evaluateExpression(entry.expression(), record, mapper));
        }
        return out;
    }

    /* ----- stateful operators (R3) ---------------------------------------- */

    /** Combine two values for a reduce, per the chosen strategy. */
    public static JsonNode reduceValue(String strategy, JsonNode v1, JsonNode v2) {
        return switch (strategy == null ? "latest" : strategy) {
            case "earliest" -> v1;
            case "sum" -> {
                Double a = numberOf(v1);
                Double b = numberOf(v2);
                yield a != null && b != null ? DoubleNode.valueOf(a + b) : v2;
            }
            case "min" -> compareValues(v1, v2) <= 0 ? v1 : v2;
            case "max" -> compareValues(v1, v2) >= 0 ? v1 : v2;
            default -> v2;
        };
    }

    /** The initial accumulator object for an aggregation. */
    public static JsonNode aggregateInit(
            List<AggregateField> fields, ObjectMapper mapper) {
        ObjectNode acc = mapper.createObjectNode();
        for (AggregateField field : fields) {
            if (field.name() == null || field.name().isEmpty()) {
                continue;
            }
            switch (field.op() == null ? "" : field.op()) {
                case "count" -> acc.put(field.name(), 0L);
                case "sum" -> acc.put(field.name(), 0.0);
                default -> acc.putNull(field.name());
            }
        }
        return acc;
    }

    /** Fold one record into the accumulator. */
    public static JsonNode aggregateAdd(
            List<AggregateField> fields,
            JsonNode value,
            JsonNode accumulator,
            ObjectMapper mapper) {
        ObjectNode acc = accumulator instanceof ObjectNode existing
                ? existing.deepCopy()
                : mapper.createObjectNode();
        for (AggregateField field : fields) {
            if (field.name() == null || field.name().isEmpty()) {
                continue;
            }
            String name = field.name();
            JsonNode current = acc.get(name);
            JsonNode source = resolvePath(value, field.sourceField());
            switch (field.op() == null ? "" : field.op()) {
                case "count" ->
                        acc.put(name, (current == null ? 0L : current.asLong()) + 1L);
                case "sum" -> {
                    Double add = numberOf(source);
                    acc.put(name, (current == null ? 0.0 : current.asDouble())
                            + (add == null ? 0.0 : add));
                }
                case "min" -> {
                    if (current == null || current.isNull()
                            || compareValues(source, current) < 0) {
                        acc.set(name, orNull(source));
                    }
                }
                case "max" -> {
                    if (current == null || current.isNull()
                            || compareValues(source, current) > 0) {
                        acc.set(name, orNull(source));
                    }
                }
                case "first" -> {
                    if (current == null || current.isNull()) {
                        acc.set(name, orNull(source));
                    }
                }
                case "last" -> acc.set(name, orNull(source));
                default -> { }
            }
        }
        return acc;
    }

    /** Build the joined value from a join's value-joiner mapping. */
    public static JsonNode joinValues(
            List<ValueMappingEntry> entries,
            JsonNode left,
            JsonNode right,
            ObjectMapper mapper) {
        if (entries == null || entries.isEmpty()) {
            return left;
        }
        ObjectNode out = mapper.createObjectNode();
        for (ValueMappingEntry entry : entries) {
            if (entry.outputField() == null || entry.outputField().isEmpty()) {
                continue;
            }
            out.set(entry.outputField(),
                    evaluateJoinExpression(entry.expression(), left, right));
        }
        return out;
    }

    static JsonNode evaluateJoinExpression(
            ValueExpression expression, JsonNode left, JsonNode right) {
        if (expression == null) {
            return NullNode.getInstance();
        }
        if ("literal".equals(expression.kind())) {
            return literalNode(expression.value());
        }
        String path = expression.path() == null ? "" : expression.path();
        if (path.startsWith("right.")) {
            return orNull(resolvePath(right, path.substring(6)));
        }
        String rest = path.startsWith("left.") ? path.substring(5) : path;
        return orNull(resolvePath(left, rest));
    }

    static int compareValues(JsonNode a, JsonNode b) {
        Double na = numberOf(a);
        Double nb = numberOf(b);
        if (na != null && nb != null) {
            return Double.compare(na, nb);
        }
        return text(a).compareTo(text(b));
    }

    static JsonNode orNull(JsonNode node) {
        return node == null ? NullNode.getInstance() : node;
    }

    /* ---------------------------------------------------------------------- */

    static JsonNode resolvePath(JsonNode record, String path) {
        if (record == null || path == null || path.isEmpty()) {
            return record;
        }
        JsonNode current = record;
        for (String segment : path.split("\\.")) {
            if (current == null || current.isNull()) {
                return null;
            }
            current = current.get(segment);
        }
        return current;
    }

    static boolean valueEquals(JsonNode field, String value) {
        if (field == null || field.isNull()) {
            return value.isEmpty();
        }
        if (field.isNumber()) {
            Double n = parseDouble(value);
            return n != null && field.asDouble() == n;
        }
        if (field.isBoolean()) {
            return field.asBoolean() == Boolean.parseBoolean(value);
        }
        return field.asText().equals(value);
    }

    static boolean numericCompare(JsonNode field, String value, String op) {
        Double a = numberOf(field);
        Double b = parseDouble(value);
        if (a == null || b == null) {
            return false;
        }
        int cmp = Double.compare(a, b);
        return switch (op) {
            case "gt" -> cmp > 0;
            case "gte" -> cmp >= 0;
            case "lt" -> cmp < 0;
            case "lte" -> cmp <= 0;
            default -> false;
        };
    }

    static JsonNode literalNode(String raw) {
        if (raw == null) {
            return NullNode.getInstance();
        }
        if (raw.equals("true") || raw.equals("false")) {
            return BooleanNode.valueOf(Boolean.parseBoolean(raw));
        }
        Double d = parseDouble(raw);
        if (d != null) {
            if (!raw.contains(".") && d == Math.floor(d) && !Double.isInfinite(d)) {
                return LongNode.valueOf((long) (double) d);
            }
            return DoubleNode.valueOf(d);
        }
        return TextNode.valueOf(raw);
    }

    static Double numberOf(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        return node.isNumber() ? node.asDouble() : parseDouble(node.asText());
    }

    static Double parseDouble(String s) {
        if (s == null || s.isBlank()) {
            return null;
        }
        try {
            return Double.parseDouble(s.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    static String text(JsonNode node) {
        return node == null || node.isNull() ? "" : node.asText();
    }
}
