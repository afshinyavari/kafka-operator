package se.afshin.yavari.rbac;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.StreamReadFeature;
import se.afshin.yavari.rbac.PolicyEngine.Action;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Maps a registry request to the artifact and action the policy is asked about.
 *
 * <p>Apicurio core API v3 is the primary dialect ({@code /apis/registry/v3}); v2 and the
 * Confluent-compatible API ({@code /apis/ccompat/vN}, subject = artifact) are understood as
 * well. A request that names no single artifact is registry-wide ({@code "*"}). Whenever the
 * artifact cannot be told with certainty (repeated parameters, duplicate JSON keys, malformed
 * input) the request is treated as registry-wide, never as the more specific reading.
 */
final class RegistryRequestClassifier {

    /** The API a request addresses; decides id resolution and the error format. */
    enum Api { CORE_V3, CORE_V2, CCOMPAT, OTHER }

    /** An id that names content rather than an artifact; resolved against the registry
     *  before the policy check. {@code apiRoot} is the request's own API root. */
    record IdLookup(Api api, String apiRoot, String param, String id) {}

    /** {@code artifact} is {@code "*"} for registry-wide requests and whenever {@code lookup} is set. */
    record Classified(Api api, Action action, String artifact, IdLookup lookup) {}

    static final String WILDCARD = "*";

    /** Rejects duplicate keys: the registry keeps the last one, so the first must not be trusted. */
    private static final JsonFactory STRICT_JSON =
            JsonFactory.builder().enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION).build();

    private RegistryRequestClassifier() {}

    /**
     * @param rawPath          request path as received, still percent-encoded
     * @param rawQuery         query string as received, or {@code null}
     * @param artifactIdHeader value of {@code X-Registry-ArtifactId} (v2 create), or {@code null}
     */
    static Classified classify(String method, String rawPath, String rawQuery, String contentType,
                               String artifactIdHeader, byte[] body) {
        String verb = method.toUpperCase();
        Action action = actionFor(verb);
        List<String> seg;
        Map<String, List<String>> query;
        try {
            seg = segments(rawPath);
            query = queryParams(rawQuery);
        } catch (IllegalArgumentException malformedEncoding) {
            return new Classified(Api.OTHER, action, WILDCARD, null);
        }

        if (seg.size() >= 3 && "apis".equals(seg.get(0)) && seg.get(2).matches("v[0-9]+")) {
            String root = "/apis/" + seg.get(1) + "/" + seg.get(2);
            List<String> rest = seg.subList(3, seg.size());
            if ("registry".equals(seg.get(1))) {
                Api api = "v2".equals(seg.get(2)) ? Api.CORE_V2 : Api.CORE_V3;
                return core(api, root, verb, action, rest, query, contentType, artifactIdHeader, body);
            }
            if ("ccompat".equals(seg.get(1))) {
                return ccompat(root, verb, action, rest, query);
            }
        }
        // XML schema service: /schemas/{name}
        if (seg.size() >= 2 && "schemas".equals(seg.get(0))) {
            return new Classified(Api.OTHER, action, seg.get(1), null);
        }
        return new Classified(Api.OTHER, action, WILDCARD, null);
    }

    private static Classified core(Api api, String root, String verb, Action action, List<String> rest,
                                   Map<String, List<String>> query, String contentType,
                                   String artifactIdHeader, byte[] body) {
        String head = rest.isEmpty() ? "" : rest.get(0);
        switch (head) {
            case "groups" -> {
                // groups/{group}/artifacts[/{artifact}/…]
                if (rest.size() >= 4 && "artifacts".equals(rest.get(2))) {
                    return new Classified(api, action, rest.get(3), null);
                }
                if (rest.size() == 3 && "artifacts".equals(rest.get(2)) && "POST".equals(verb)) {
                    String id = api == Api.CORE_V2 ? blankToNull(artifactIdHeader)
                                                   : createdArtifactId(contentType, body);
                    return new Classified(api, action, id != null ? id : WILDCARD, null);
                }
            }
            case "ids" -> {
                // ids/{globalIds|contentIds}/{id}[/references]
                if (rest.size() >= 3 && isNumber(rest.get(2))) {
                    String param = switch (rest.get(1)) {
                        case "globalIds" -> "globalId";
                        case "contentIds" -> "contentId";
                        default -> null;
                    };
                    if (param != null) return lookup(api, root, action, param, rest.get(2));
                }
            }
            case "search" -> {
                // A POST search carries content to look for; it changes nothing.
                Action searchAction = "POST".equals(verb) ? Action.READ : action;
                if (rest.size() == 2 && ("artifacts".equals(rest.get(1)) || "versions".equals(rest.get(1)))) {
                    // v3 filters on the exact artifact id, so the result cannot name another artifact.
                    String artifactId = api == Api.CORE_V3 ? single(query, "artifactId") : null;
                    if (artifactId != null) return new Classified(api, searchAction, artifactId, null);
                    for (String param : List.of("globalId", "contentId")) {
                        String id = single(query, param);
                        if (id != null && isNumber(id)) return lookup(api, root, searchAction, param, id);
                    }
                }
                return new Classified(api, searchAction, WILDCARD, null);
            }
            default -> { }
        }
        return new Classified(api, action, WILDCARD, null);
    }

    private static Classified ccompat(String root, String verb, Action action, List<String> rest,
                                      Map<String, List<String>> query) {
        Api api = Api.CCOMPAT;
        boolean post = "POST".equals(verb);
        String head = rest.isEmpty() ? "" : rest.get(0);
        switch (head) {
            case "subjects" -> {
                // POST subjects/{subject} looks a schema up; POST subjects/{subject}/versions registers one.
                if (rest.size() >= 2) {
                    Action a = post && rest.size() == 2 ? Action.READ : action;
                    return new Classified(api, a, rest.get(1), null);
                }
            }
            case "schemas" -> {
                // The ?subject= hint is not enforced by the registry, so the id is resolved instead.
                if (rest.size() >= 3 && "ids".equals(rest.get(1)) && isNumber(rest.get(2))) {
                    return lookup(api, root, action, "id", rest.get(2));
                }
            }
            case "compatibility" -> {
                if (rest.size() >= 3 && "subjects".equals(rest.get(1))) {
                    return new Classified(api, post ? Action.READ : action, rest.get(2), null);
                }
            }
            case "config", "mode" -> {
                if (rest.size() >= 2) return new Classified(api, action, rest.get(1), null);
            }
            case "associations" -> {
                // associations/resources/{namespace}/{topic}?associationType=value|key
                String type = single(query, "associationType");
                if (rest.size() == 4 && "resources".equals(rest.get(1))
                        && ("value".equals(type) || "key".equals(type))) {
                    return new Classified(api, action, rest.get(3) + "-" + type, null);
                }
            }
            default -> { }
        }
        return new Classified(api, action, WILDCARD, null);
    }

    private static Classified lookup(Api api, String root, Action action, String param, String id) {
        return new Classified(api, action, WILDCARD, new IdLookup(api, root, param, id));
    }

    static Action actionFor(String verb) {
        return switch (verb) {
            case "GET", "HEAD" -> Action.READ;
            case "DELETE"      -> Action.DELETE;
            default            -> Action.WRITE;
        };
    }

    /** Top-level {@code artifactId} of a v3 {@code CreateArtifact} JSON body, or {@code null}
     *  when it is absent, not a non-empty string, or the body is not one well-formed JSON object. */
    private static String createdArtifactId(String contentType, byte[] body) {
        if (body == null || !isJson(contentType)) return null;
        try (JsonParser p = STRICT_JSON.createParser(body)) {
            if (p.nextToken() != JsonToken.START_OBJECT) return null;
            String id = null;
            while (p.nextToken() == JsonToken.FIELD_NAME) {
                String name = p.currentName();
                JsonToken value = p.nextToken();
                if ("artifactId".equals(name)) {
                    if (value != JsonToken.VALUE_STRING) return null;
                    id = p.getText();
                } else {
                    p.skipChildren();
                }
            }
            if (p.currentToken() != JsonToken.END_OBJECT || p.nextToken() != null) return null;
            return blankToNull(id);
        } catch (IOException malformed) {
            return null;
        }
    }

    private static boolean isJson(String contentType) {
        if (contentType == null) return false;
        int params = contentType.indexOf(';');
        String mediaType = params < 0 ? contentType : contentType.substring(0, params);
        return "application/json".equalsIgnoreCase(mediaType.trim());
    }

    /** Path segments, each percent-decoded on its own so an encoded slash stays inside its segment. */
    private static List<String> segments(String rawPath) {
        String path = rawPath.startsWith("/") ? rawPath.substring(1) : rawPath;
        List<String> out = new ArrayList<>();
        for (String s : path.split("/", -1)) {
            out.add(URLDecoder.decode(s.replace("+", "%2B"), StandardCharsets.UTF_8));
        }
        if (out.size() > 1 && out.get(out.size() - 1).isEmpty()) out.remove(out.size() - 1);
        return out;
    }

    private static Map<String, List<String>> queryParams(String rawQuery) {
        Map<String, List<String>> out = new HashMap<>();
        if (rawQuery == null || rawQuery.isEmpty()) return out;
        for (String pair : rawQuery.split("&")) {
            int eq = pair.indexOf('=');
            String key = URLDecoder.decode(eq < 0 ? pair : pair.substring(0, eq), StandardCharsets.UTF_8);
            String value = eq < 0 ? "" : URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8);
            out.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
        }
        return out;
    }

    /** The parameter's value when it occurs exactly once and is not blank, else {@code null}. */
    private static String single(Map<String, List<String>> query, String name) {
        List<String> values = query.get(name);
        return values != null && values.size() == 1 ? blankToNull(values.get(0)) : null;
    }

    private static boolean isNumber(String s) {
        return s.matches("[0-9]{1,19}");
    }

    private static String blankToNull(String s) {
        return s == null || s.isBlank() ? null : s;
    }
}
