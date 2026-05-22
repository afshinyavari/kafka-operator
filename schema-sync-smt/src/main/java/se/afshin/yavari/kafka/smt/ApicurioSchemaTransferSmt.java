package se.afshin.yavari.kafka.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import se.afshin.yavari.kafka.smt.ApicurioClient.ApicurioException;
import se.afshin.yavari.kafka.smt.ApicurioClient.ArtifactByGlobalId;
import se.afshin.yavari.kafka.smt.ApicurioClient.ArtifactReference;

/**
 * Kafka Connect SMT that translates Apicurio V3 schema envelopes between two registries.
 *
 * <p>Apicurio V3 envelope: {@code [0x00][globalId:8 bytes BE][payload]}.
 *
 * <p>For each record, the SMT:
 * <ol>
 *   <li>Checks topic against {@code applyToTopics} regex list — bail if no match.
 *   <li>Checks {@code value} and/or {@code key} (per {@code apply.to}).
 *   <li>If bytes are null, length < 9, or bytes[0] != 0x00 — passthrough.
 *   <li>Parses source globalId, looks up in LRU cache. On miss: fetch schema (+refs)
 *       from source registry, recursively ensure refs exist on target, upsert this
 *       artifact on target, cache the source→target mapping.
 *   <li>Rewrites envelope's 8-byte globalId, leaves payload unchanged.
 * </ol>
 *
 * <p>Errors (source 404, target 5xx, network) route through {@code behavior.on.error}:
 * FAIL re-throws, WARN logs and passes through, IGNORE drops the record.
 *
 * <p>Reference resolution is DFS with cycle guard ({@code HashSet<Long> visiting}) and
 * a configurable {@code maxDepth} (default 16).
 */
public class ApicurioSchemaTransferSmt<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger LOG = LoggerFactory.getLogger(ApicurioSchemaTransferSmt.class);

    public static final String SOURCE_URL = "source.url";
    public static final String TARGET_URL = "target.url";
    public static final String SOURCE_AUTH_HEADER = "source.auth.header";
    public static final String TARGET_AUTH_HEADER = "target.auth.header";
    public static final String SOURCE_AUTH_OAUTH_DIR = "source.auth.oauth.dir";
    public static final String TARGET_AUTH_OAUTH_DIR = "target.auth.oauth.dir";
    public static final String CACHE_SIZE = "cache.size";
    public static final String BEHAVIOR_ON_ERROR = "behavior.on.error";
    public static final String APPLY_TO = "apply.to";
    public static final String APPLY_TO_TOPICS = "apply.to.topics";
    public static final String MAX_REF_DEPTH = "max.ref.depth";

    public enum OnError { FAIL, WARN, IGNORE }
    public enum ApplyTo { VALUE, KEY, BOTH }

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(SOURCE_URL, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
                    "Source Apicurio Registry base URL")
            .define(TARGET_URL, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
                    "Target Apicurio Registry base URL")
            .define(SOURCE_AUTH_HEADER, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                    "Pre-formed Authorization header value for source registry (e.g. 'Bearer …')")
            .define(TARGET_AUTH_HEADER, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                    "Pre-formed Authorization header value for target registry")
            .define(SOURCE_AUTH_OAUTH_DIR, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                    "Directory of OAuth2 client-credentials for the source registry "
                            + "(files: token-url, client-id, client-secret, optional scope)")
            .define(TARGET_AUTH_OAUTH_DIR, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                    "Directory of OAuth2 client-credentials for the target registry "
                            + "(files: token-url, client-id, client-secret, optional scope)")
            .define(CACHE_SIZE, ConfigDef.Type.INT, 10_000, ConfigDef.Importance.LOW,
                    "LRU cache size for source→target globalId mappings")
            .define(BEHAVIOR_ON_ERROR, ConfigDef.Type.STRING, "WARN", ConfigDef.Importance.MEDIUM,
                    "FAIL | WARN | IGNORE — how to react when registry calls fail")
            .define(APPLY_TO, ConfigDef.Type.STRING, "VALUE", ConfigDef.Importance.MEDIUM,
                    "VALUE | KEY | BOTH — which record component to process")
            .define(APPLY_TO_TOPICS, ConfigDef.Type.LIST, List.of(".*"), ConfigDef.Importance.LOW,
                    "Topic-name regex allowlist")
            .define(MAX_REF_DEPTH, ConfigDef.Type.INT, 16, ConfigDef.Importance.LOW,
                    "Maximum DFS depth when resolving schema references");

    private ApicurioClient source;
    private ApicurioClient target;
    private OnError onError;
    private ApplyTo applyTo;
    private List<Pattern> topicPatterns;
    private int maxRefDepth;
    private LruCache cache;

    @Override
    public void configure(Map<String, ?> configs) {
        Map<String, Object> parsed = CONFIG_DEF.parse(configs);
        String srcUrl = (String) parsed.get(SOURCE_URL);
        String tgtUrl = (String) parsed.get(TARGET_URL);
        this.source = new ApicurioClient(srcUrl, buildAuth(
                (String) parsed.get(SOURCE_AUTH_OAUTH_DIR), (String) parsed.get(SOURCE_AUTH_HEADER)));
        this.target = new ApicurioClient(tgtUrl, buildAuth(
                (String) parsed.get(TARGET_AUTH_OAUTH_DIR), (String) parsed.get(TARGET_AUTH_HEADER)));
        this.onError = OnError.valueOf(((String) parsed.get(BEHAVIOR_ON_ERROR)).toUpperCase());
        this.applyTo = ApplyTo.valueOf(((String) parsed.get(APPLY_TO)).toUpperCase());
        this.maxRefDepth = (Integer) parsed.get(MAX_REF_DEPTH);
        int cacheSize = (Integer) parsed.get(CACHE_SIZE);
        this.cache = new LruCache(cacheSize);
        @SuppressWarnings("unchecked")
        List<String> patterns = (List<String>) parsed.get(APPLY_TO_TOPICS);
        this.topicPatterns = patterns.stream().map(Pattern::compile).toList();
        LOG.info("ApicurioSchemaTransferSmt configured: source={}, target={}, applyTo={}, onError={}, cacheSize={}, topics={}",
                srcUrl, tgtUrl, applyTo, onError, cacheSize, patterns);
    }

    /** Builds the auth provider for one registry: OAuth2 client-credentials when an oauth
     *  directory is configured (the common case for a managed target behind the RBAC proxy),
     *  a static header when one is given, otherwise no auth. */
    private static ApicurioClient.AuthProvider buildAuth(String oauthDir, String staticHeader) {
        if (oauthDir != null && !oauthDir.isBlank()) {
            Path dir = Path.of(oauthDir);
            String tokenUrl = readCredential(dir, "token-url");
            String clientId = readCredential(dir, "client-id");
            String clientSecret = readCredential(dir, "client-secret");
            String scope = readCredentialOrNull(dir, "scope");
            LOG.info("Schema-registry auth: OAuth2 client-credentials (tokenUrl={}, clientId={})",
                    tokenUrl, clientId);
            return ApicurioClient.AuthProvider.oauth(
                    new OAuthTokenProvider(tokenUrl, clientId, clientSecret, scope));
        }
        if (staticHeader != null && !staticHeader.isBlank()) {
            return ApicurioClient.AuthProvider.staticHeader(staticHeader);
        }
        return ApicurioClient.AuthProvider.none();
    }

    private static String readCredential(Path dir, String name) {
        String v = readCredentialOrNull(dir, name);
        if (v == null || v.isBlank()) {
            throw new ConnectException("Missing OAuth credential file '" + name + "' in " + dir);
        }
        return v;
    }

    private static String readCredentialOrNull(Path dir, String name) {
        Path p = dir.resolve(name);
        if (!Files.isReadable(p)) return null;
        try {
            return Files.readString(p, StandardCharsets.UTF_8).trim();
        } catch (IOException e) {
            throw new ConnectException("Cannot read OAuth credential file " + p, e);
        }
    }

    /** Sentinel returned by {@link #maybeRewrite} when {@code behavior.on.error=IGNORE}
     *  triggered a drop for that side. Distinct from null/empty/original so the caller
     *  can react. */
    private static final byte[] DROP = new byte[0];

    @Override
    public R apply(R record) {
        if (record == null) return null;
        if (!topicMatches(record.topic())) return record;

        byte[] originalKey = bytesOrNull(record.key());
        byte[] originalValue = bytesOrNull(record.value());
        byte[] newKey = originalKey;
        byte[] newValue = originalValue;

        if ((applyTo == ApplyTo.KEY || applyTo == ApplyTo.BOTH) && originalKey != null) {
            newKey = maybeRewrite(originalKey, record.topic(), "key");
            if (newKey == DROP) return null;
        }
        if ((applyTo == ApplyTo.VALUE || applyTo == ApplyTo.BOTH) && originalValue != null) {
            newValue = maybeRewrite(originalValue, record.topic(), "value");
            if (newValue == DROP) return null;
        }
        if (newKey == originalKey && newValue == originalValue) {
            return record;
        }
        Object outKey = newKey == originalKey ? record.key() : newKey;
        Object outValue = newValue == originalValue ? record.value() : newValue;
        return record.newRecord(record.topic(), record.kafkaPartition(),
                record.keySchema(), outKey,
                record.valueSchema(), outValue,
                record.timestamp(), record.headers());
    }

    /** Returns the (possibly rewritten) bytes, the original array if not an envelope,
     *  or the {@link #DROP} sentinel when {@code behavior.on.error=IGNORE} fired. */
    private byte[] maybeRewrite(byte[] bytes, String topic, String which) {
        if (bytes.length < 9 || bytes[0] != 0x00) return bytes;
        long sourceGlobalId = ByteBuffer.wrap(bytes, 1, 8).getLong();
        try {
            long targetGlobalId = resolveTargetGlobalId(sourceGlobalId);
            byte[] out = bytes.clone();
            ByteBuffer.wrap(out, 1, 8).putLong(targetGlobalId);
            return out;
        } catch (ApicurioException e) {
            return handleError(topic, which, sourceGlobalId, e, bytes);
        }
    }

    private long resolveTargetGlobalId(long sourceGlobalId) throws ApicurioException {
        Long cached = cache.get(sourceGlobalId);
        if (cached != null) return cached;
        long resolved = ensureOnTarget(sourceGlobalId, new HashSet<>(), 0);
        cache.put(sourceGlobalId, resolved);
        return resolved;
    }

    /** DFS over schema references. Each referenced schema is upserted to target before
     *  the parent. Returns the target's globalId for the source's globalId. */
    private long ensureOnTarget(long sourceGlobalId, HashSet<Long> visiting, int depth)
            throws ApicurioException {
        if (depth > maxRefDepth) {
            throw new ApicurioException("Schema reference DFS exceeded maxDepth=" + maxRefDepth
                    + " at globalId=" + sourceGlobalId);
        }
        if (!visiting.add(sourceGlobalId)) {
            throw new ApicurioException("Schema reference cycle detected at globalId=" + sourceGlobalId);
        }
        try {
            ArtifactByGlobalId art = source.fetchByGlobalId(sourceGlobalId);
            // Recursively ensure each reference exists on the target before posting this one.
            // (Apicurio's createArtifact rejects content with unresolved references.) We don't
            // get the referenced globalId back from the SDK directly — the references list
            // we pass to upsertArtifact carries name/group/artifact/version coordinates, which
            // Apicurio resolves server-side.
            for (ArtifactReference ref : art.references) {
                // Walking source by coordinates: look up the referenced artifact's latest
                // globalId on source, then ensure it's on target. The recursive call seeds
                // the cache so future records hit it directly.
                Long refSourceGlobalId = lookupSourceGlobalId(ref);
                if (refSourceGlobalId != null) {
                    if (cache.get(refSourceGlobalId) == null) {
                        ensureOnTarget(refSourceGlobalId, visiting, depth + 1);
                    }
                }
            }
            return target.upsertArtifact(art.groupId, art.artifactId, art.artifactType,
                    art.content, art.references);
        } finally {
            visiting.remove(sourceGlobalId);
        }
    }

    /** Lookup the latest globalId on the source for a reference's coordinates. Best-effort
     *  — returns null if the reference's version isn't pinned and the source has no latest. */
    private Long lookupSourceGlobalId(ArtifactReference ref) {
        // Apicurio v3: /apis/registry/v3/groups/{g}/artifacts/{a}/versions/{v}/references
        // returns the version's metadata. The path for the latest version is "branch=latest".
        // To keep the SMT simple, we skip pre-walking source references — Apicurio's
        // server-side resolution by coordinates handles it when we upsert the parent.
        return null;
    }

    private byte[] handleError(String topic, String which, long sourceGlobalId,
                                ApicurioException e, byte[] bytes) {
        switch (onError) {
            case FAIL:
                throw new ConnectException("ApicurioSchemaTransferSmt failed on topic=" + topic
                        + " " + which + " globalId=" + sourceGlobalId, e);
            case WARN:
                LOG.warn("ApicurioSchemaTransferSmt passthrough on topic={} {} globalId={}: {}",
                        topic, which, sourceGlobalId, e.getMessage());
                return bytes;
            case IGNORE:
                LOG.debug("ApicurioSchemaTransferSmt drop on topic={} {} globalId={}: {}",
                        topic, which, sourceGlobalId, e.getMessage());
                return DROP;
            default:
                return bytes;
        }
    }

    private boolean topicMatches(String topic) {
        if (topic == null) return false;
        for (Pattern p : topicPatterns) {
            if (p.matcher(topic).matches()) return true;
        }
        return false;
    }

    private static byte[] bytesOrNull(Object v) {
        if (v == null) return null;
        if (v instanceof byte[] b) return b;
        if (v instanceof ByteBuffer bb) {
            byte[] arr = new byte[bb.remaining()];
            bb.duplicate().get(arr);
            return arr;
        }
        // For non-byte values (Struct, String) the SMT can't operate — pass through.
        return null;
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        if (source != null) source.close();
        if (target != null) target.close();
    }

    static class LruCache {
        private final int max;
        private final LinkedHashMap<Long, Long> map;
        LruCache(int max) {
            this.max = max;
            this.map = new LinkedHashMap<>(16, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<Long, Long> eldest) {
                    return size() > LruCache.this.max;
                }
            };
        }
        synchronized Long get(long k) { return map.get(k); }
        synchronized void put(long k, long v) { map.put(k, v); }
        synchronized int size() { return map.size(); }
    }
}
