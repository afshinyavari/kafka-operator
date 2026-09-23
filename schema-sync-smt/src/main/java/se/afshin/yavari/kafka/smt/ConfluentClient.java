package se.afshin.yavari.kafka.smt;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import javax.net.ssl.SSLContext;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Minimal Confluent Schema Registry REST client. Also works against Apicurio's
 * Confluent-compatible API when the base URL points at {@code /apis/ccompat/v7}.
 * Endpoints used:
 * <ul>
 *   <li>{@code GET  /schemas/ids/{id}} — schema, {@code schemaType}, {@code references}
 *   <li>{@code GET  /schemas/ids/{id}/versions} — owning subject + version
 *   <li>{@code GET  /subjects/{subject}/versions/{version}} — reference → id
 *   <li>{@code POST /subjects/{subject}/versions} — register (idempotent for identical content)
 *   <li>{@code POST /subjects/{subject}} — lookup by content, yields the assigned version
 * </ul>
 * Subjects map to {@link RegistrySchema#artifactId()} in group {@code "default"}.
 */
public class ConfluentClient extends RestRegistryClient {

    static final String DEFAULT_GROUP = "default";
    private static final String CONTENT_TYPE = "application/vnd.schemaregistry.v1+json";

    public ConfluentClient(String baseUrl, ApicurioClient.AuthProvider auth, SSLContext ssl) {
        super(baseUrl, auth, ssl);
    }

    @Override
    public RegistrySchema fetchById(long id) throws RegistryException {
        JsonNode s = getJsonOrNull("/schemas/ids/" + id);
        if (s == null) throw RegistryException.notFound("No schema for id " + id + " at " + baseUrl);
        JsonNode owners = getJson("/schemas/ids/" + id + "/versions");
        if (!owners.isArray() || owners.isEmpty()) {
            throw RegistryException.notFound("No subject owns schema id " + id + " at " + baseUrl);
        }
        String subject = owningSubject(owners);
        List<SchemaRef> refs = new ArrayList<>();
        JsonNode refNode = s.get("references");
        if (refNode != null && refNode.isArray()) {
            for (JsonNode r : refNode) {
                refs.add(new SchemaRef(textOrNull(r.get("name")), DEFAULT_GROUP,
                        textOrNull(r.get("subject")), textOrNull(r.get("version"))));
            }
        }
        return new RegistrySchema(DEFAULT_GROUP, subject, textOrDefault(s.get("schemaType"), "AVRO"),
                s.get("schema").asText().getBytes(StandardCharsets.UTF_8), refs);
    }

    @Override
    public Registered upsert(RegistrySchema schema) throws RegistryException {
        String subject = pathSegment(schema.artifactId());
        byte[] body = registerBody(schema);
        Map<String, String> headers = Map.of("Content-Type", CONTENT_TYPE);
        JsonNode registered = postJson("/subjects/" + subject + "/versions", body, headers);
        if (!registered.has("id")) {
            throw new RegistryException("Register response missing id: " + registered);
        }
        long id = registered.get("id").asLong();
        // Registration returns only the id; the version comes from a content lookup.
        JsonNode lookup = postJson("/subjects/" + subject, body, headers);
        return new Registered(id, textOrNull(lookup.get("version")));
    }

    @Override
    public Long lookupId(SchemaRef ref) throws RegistryException {
        String version = ref.version() == null ? "latest" : ref.version();
        JsonNode n = getJsonOrNull("/subjects/" + pathSegment(ref.artifactId()) + "/versions/" + version);
        return n == null || !n.has("id") ? null : n.get("id").asLong();
    }

    /** A Confluent schema id can be registered under many subjects (a shared key schema,
     *  RecordNameStrategy). The registry lists them in no documented order, so pick the
     *  lexicographically smallest subject: deterministic across workers and restarts.
     *  Use {@code target.subject.mode=TOPIC} when the record's topic should decide instead. */
    static String owningSubject(JsonNode owners) {
        String best = null;
        for (JsonNode o : owners) {
            String subject = textOrNull(o.get("subject"));
            if (subject != null && (best == null || subject.compareTo(best) < 0)) best = subject;
        }
        if (best == null) throw new IllegalStateException("owners list carries no subject: " + owners);
        return best;
    }

    private static byte[] registerBody(RegistrySchema schema) throws RegistryException {
        ObjectNode body = MAPPER.createObjectNode();
        body.put("schema", new String(schema.content(), StandardCharsets.UTF_8));
        if (schema.type() != null && !"AVRO".equals(schema.type())) {
            body.put("schemaType", schema.type());
        }
        if (schema.references() != null && !schema.references().isEmpty()) {
            ArrayNode refs = body.putArray("references");
            for (SchemaRef ref : schema.references()) {
                ObjectNode r = refs.addObject();
                r.put("name", ref.name());
                r.put("subject", ref.artifactId());
                if (ref.version() == null) {
                    throw new RegistryException("Confluent references need a version; reference '"
                            + ref.name() + "' → " + ref.artifactId() + " has none");
                }
                try {
                    r.put("version", Integer.parseInt(ref.version()));
                } catch (NumberFormatException e) {
                    throw new RegistryException("Confluent reference version must be numeric: "
                            + ref.artifactId() + "@" + ref.version());
                }
            }
        }
        try {
            return MAPPER.writeValueAsBytes(body);
        } catch (Exception e) {
            throw new RegistryException("Failed to serialise register request", e);
        }
    }
}
