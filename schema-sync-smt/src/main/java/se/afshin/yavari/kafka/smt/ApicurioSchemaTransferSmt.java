package se.afshin.yavari.kafka.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
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

import java.util.ArrayList;

/**
 * Kafka Connect SMT that translates schema-registry envelopes between two registries.
 *
 * <p>Each side is either {@code APICURIO} ({@code [0x00][globalId:8 bytes BE][payload]},
 * Apicurio REST v2) or {@code CONFLUENT} ({@code [0x00][schemaId:4 bytes BE][payload]},
 * Confluent REST — also Apicurio's ccompat API), chosen with {@code source.format} /
 * {@code target.format}. Formats are independent, so a Confluent source can feed an
 * Apicurio target. Optional {@code <side>.ssl.*} keys enable mTLS to the registries.
 *
 * <p>For each record, the SMT:
 * <ol>
 *   <li>Checks topic against {@code applyToTopics} regex list — bail if no match.
 *   <li>Checks {@code value} and/or {@code key} (per {@code apply.to}).
 *   <li>If bytes are null, shorter than the source header, or bytes[0] != 0x00 — passthrough.
 *   <li>Parses the source id, looks it up in the LRU cache, then in the negative cache.
 *       On miss: fetch schema (+refs) from source registry, recursively ensure refs exist
 *       on target (rewriting each reference to the target-assigned version), upsert this
 *       schema on target, cache the source→target mapping.
 *   <li>Re-frames the envelope with the target codec and id, leaves payload unchanged.
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
    public static final String SOURCE_FORMAT = "source.format";
    public static final String TARGET_FORMAT = "target.format";
    public static final String NEGATIVE_CACHE_TTL_MS = "cache.negative.ttl.ms";
    public static final String TARGET_SUBJECT_PREFIX = "target.subject.prefix";

    /** Suffixes of the per-side {@code <side>.ssl.*} keys. */
    static final String SSL_KEYSTORE_LOCATION = ".ssl.keystore.location";
    static final String SSL_KEYSTORE_PASSWORD = ".ssl.keystore.password";
    static final String SSL_KEYSTORE_TYPE = ".ssl.keystore.type";
    static final String SSL_KEY_LOCATION = ".ssl.key.location";
    static final String SSL_TRUSTSTORE_LOCATION = ".ssl.truststore.location";
    static final String SSL_TRUSTSTORE_PASSWORD = ".ssl.truststore.password";
    static final String SSL_TRUSTSTORE_TYPE = ".ssl.truststore.type";

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
                    "Maximum DFS depth when resolving schema references")
            .define(SOURCE_FORMAT, ConfigDef.Type.STRING, "APICURIO", formatValidator(), ConfigDef.Importance.HIGH,
                    "APICURIO | CONFLUENT — wire envelope and REST API of the source registry")
            .define(TARGET_FORMAT, ConfigDef.Type.STRING, "APICURIO", formatValidator(), ConfigDef.Importance.HIGH,
                    "APICURIO | CONFLUENT — wire envelope and REST API of the target registry")
            .define(NEGATIVE_CACHE_TTL_MS, ConfigDef.Type.LONG, 60_000L, ConfigDef.Importance.LOW,
                    "How long (ms) a failed source-id lookup is remembered before the registry is "
                            + "asked again; 0 disables the negative cache")
            .define(TARGET_SUBJECT_PREFIX, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM,
                    "Prefix prepended to the subject/artifactId when registering a record's schema on "
                            + "the target, so subjects mirror MirrorMaker's topic prefix (e.g. 'prod.'). "
                            + "Referenced schemas keep their source subject. Empty keeps the source subject.");

    static {
        defineSsl(CONFIG_DEF, "source");
        defineSsl(CONFIG_DEF, "target");
    }

    private static void defineSsl(ConfigDef def, String side) {
        def.define(side + SSL_KEYSTORE_LOCATION, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                        "Client keystore for mTLS to the " + side + " registry (PKCS12/JKS), or the "
                                + "PEM certificate chain when the type is PEM")
                .define(side + SSL_KEYSTORE_PASSWORD, ConfigDef.Type.PASSWORD, null, ConfigDef.Importance.MEDIUM,
                        "Keystore password (PKCS12/JKS)")
                .define(side + SSL_KEYSTORE_TYPE, ConfigDef.Type.STRING, "PKCS12",
                        ConfigDef.ValidString.in("PKCS12", "JKS", "PEM"), ConfigDef.Importance.LOW,
                        "PKCS12 | JKS | PEM")
                .define(side + SSL_KEY_LOCATION, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                        "PKCS#8 private-key file when the keystore type is PEM")
                .define(side + SSL_TRUSTSTORE_LOCATION, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                        "Truststore (PKCS12/JKS) or CA bundle (PEM) for the " + side
                                + " registry; JDK default trust when unset")
                .define(side + SSL_TRUSTSTORE_PASSWORD, ConfigDef.Type.PASSWORD, null, ConfigDef.Importance.MEDIUM,
                        "Truststore password (PKCS12/JKS)")
                .define(side + SSL_TRUSTSTORE_TYPE, ConfigDef.Type.STRING, "PKCS12",
                        ConfigDef.ValidString.in("PKCS12", "JKS", "PEM"), ConfigDef.Importance.LOW,
                        "PKCS12 | JKS | PEM");
    }

    private static ConfigDef.Validator formatValidator() {
        return (name, value) -> {
            try {
                EnvelopeCodec.of((String) value);
            } catch (IllegalArgumentException e) {
                throw new ConfigException(name, value, "must be one of APICURIO, CONFLUENT");
            }
        };
    }

    private SchemaRegistryClient source;
    private SchemaRegistryClient target;
    private EnvelopeCodec sourceCodec = EnvelopeCodec.APICURIO;
    private EnvelopeCodec targetCodec = EnvelopeCodec.APICURIO;
    private OnError onError;
    private ApplyTo applyTo;
    private List<Pattern> topicPatterns;
    private int maxRefDepth;
    private LruCache cache;
    private NegativeCache negativeCache;
    private String targetSubjectPrefix = "";

    @Override
    public void configure(Map<String, ?> configs) {
        Map<String, Object> parsed = CONFIG_DEF.parse(configs);
        String srcUrl = (String) parsed.get(SOURCE_URL);
        String tgtUrl = (String) parsed.get(TARGET_URL);
        this.sourceCodec = EnvelopeCodec.of((String) parsed.get(SOURCE_FORMAT));
        this.targetCodec = EnvelopeCodec.of((String) parsed.get(TARGET_FORMAT));
        this.source = newClient(sourceCodec, srcUrl,
                buildAuth((String) parsed.get(SOURCE_AUTH_OAUTH_DIR), (String) parsed.get(SOURCE_AUTH_HEADER)),
                RegistryTls.build(sslSettings(parsed, "source")));
        this.target = newClient(targetCodec, tgtUrl,
                buildAuth((String) parsed.get(TARGET_AUTH_OAUTH_DIR), (String) parsed.get(TARGET_AUTH_HEADER)),
                RegistryTls.build(sslSettings(parsed, "target")));
        this.negativeCache = new NegativeCache((Long) parsed.get(NEGATIVE_CACHE_TTL_MS));
        this.targetSubjectPrefix = ((String) parsed.get(TARGET_SUBJECT_PREFIX)).trim();
        this.onError = OnError.valueOf(((String) parsed.get(BEHAVIOR_ON_ERROR)).toUpperCase());
        this.applyTo = ApplyTo.valueOf(((String) parsed.get(APPLY_TO)).toUpperCase());
        this.maxRefDepth = (Integer) parsed.get(MAX_REF_DEPTH);
        int cacheSize = (Integer) parsed.get(CACHE_SIZE);
        this.cache = new LruCache(cacheSize);
        @SuppressWarnings("unchecked")
        List<String> patterns = (List<String>) parsed.get(APPLY_TO_TOPICS);
        this.topicPatterns = patterns.stream().map(Pattern::compile).toList();
        LOG.info("ApicurioSchemaTransferSmt configured: source={} ({}), target={} ({}), applyTo={}, onError={}, "
                        + "cacheSize={}, negativeTtlMs={}, topics={}",
                srcUrl, sourceCodec, tgtUrl, targetCodec, applyTo, onError, cacheSize,
                parsed.get(NEGATIVE_CACHE_TTL_MS), patterns);
        if (!targetSubjectPrefix.isEmpty()) {
            LOG.info("Target subjects will be prefixed with '{}'", targetSubjectPrefix);
        }
    }

    /** The registry client matching a wire format: Apicurio v2 for {@code APICURIO},
     *  Confluent REST (also Apicurio ccompat) for {@code CONFLUENT}. */
    private static SchemaRegistryClient newClient(EnvelopeCodec format, String url,
                                                  ApicurioClient.AuthProvider auth, SSLContext ssl) {
        return format == EnvelopeCodec.CONFLUENT
                ? new ConfluentClient(url, auth, ssl)
                : new ApicurioClient(url, auth, ssl);
    }

    private static RegistryTls.Settings sslSettings(Map<String, Object> parsed, String side) {
        return new RegistryTls.Settings(
                (String) parsed.get(side + SSL_KEYSTORE_LOCATION),
                password(parsed.get(side + SSL_KEYSTORE_PASSWORD)),
                (String) parsed.get(side + SSL_KEYSTORE_TYPE),
                (String) parsed.get(side + SSL_KEY_LOCATION),
                (String) parsed.get(side + SSL_TRUSTSTORE_LOCATION),
                password(parsed.get(side + SSL_TRUSTSTORE_PASSWORD)),
                (String) parsed.get(side + SSL_TRUSTSTORE_TYPE));
    }

    private static String password(Object v) {
        return v instanceof Password pw ? pw.value() : null;
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
        EnvelopeCodec.Parsed parsed = sourceCodec.parse(bytes);
        if (parsed == null) return bytes;
        long sourceId = parsed.id();
        RegistryException remembered = negativeCache.get(sourceId);
        if (remembered != null) {
            return handleError(topic, which, sourceId, remembered, bytes);
        }
        try {
            long targetId = resolveTargetGlobalId(sourceId);
            return targetCodec.encode(targetId, bytes, parsed.payloadOffset());
        } catch (RegistryException e) {
            negativeCache.put(sourceId, e);
            return handleError(topic, which, sourceId, e, bytes);
        }
    }

    private long resolveTargetGlobalId(long sourceGlobalId) throws RegistryException {
        Long cached = cache.get(sourceGlobalId);
        if (cached != null) return cached;
        long resolved = ensureOnTarget(sourceGlobalId, new HashSet<>(), 0).id();
        cache.put(sourceGlobalId, resolved);
        return resolved;
    }

    /** DFS over schema references. Each referenced schema is upserted to the target before
     *  its parent, and the parent's reference is rewritten to the version the target
     *  assigned (registries number versions independently). Returns the target's
     *  registration for the source id. */
    private Registered ensureOnTarget(long sourceGlobalId, HashSet<Long> visiting, int depth)
            throws RegistryException {
        if (depth > maxRefDepth) {
            throw new RegistryException("Schema reference DFS exceeded maxDepth=" + maxRefDepth
                    + " at globalId=" + sourceGlobalId);
        }
        if (!visiting.add(sourceGlobalId)) {
            throw new RegistryException("Schema reference cycle detected at globalId=" + sourceGlobalId);
        }
        try {
            RegistrySchema schema = source.fetchById(sourceGlobalId);
            List<SchemaRef> rewritten = new ArrayList<>(schema.references().size());
            for (SchemaRef ref : schema.references()) {
                Long refSourceId = source.lookupId(ref);
                if (refSourceId == null) {
                    // Source can't resolve the coordinates — pass the reference through and
                    // rely on the target resolving it server-side at upsert time.
                    rewritten.add(ref);
                    continue;
                }
                Registered onTarget = ensureOnTarget(refSourceId, visiting, depth + 1);
                if (targetSubjectPrefix.isEmpty()) {
                    // A reference is registered under its source subject; the same schema seen
                    // top-level must go under the prefixed subject, so only seed the cache when
                    // both registrations would coincide.
                    cache.put(refSourceId, onTarget.id());
                }
                rewritten.add(onTarget.version() != null ? ref.withVersion(onTarget.version()) : ref);
            }
            RegistrySchema toRegister = schema.withReferences(rewritten);
            if (depth == 0 && !targetSubjectPrefix.isEmpty()) {
                toRegister = toRegister.withArtifactId(targetSubjectPrefix + schema.artifactId());
            }
            return target.upsert(toRegister);
        } finally {
            visiting.remove(sourceGlobalId);
        }
    }

    private byte[] handleError(String topic, String which, long sourceGlobalId,
                                RegistryException e, byte[] bytes) {
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

    /** Remembers failed source-id lookups for a TTL so records whose payload merely looks
     *  like an envelope (0x00 prefix, no such schema) don't trigger a registry call each.
     *  Bounded to the same size as the positive cache. */
    static class NegativeCache {
        private final long ttlMs;
        private final LinkedHashMap<Long, Failure> map;
        private record Failure(RegistryException error, long expiresAt) {}

        NegativeCache(long ttlMs) {
            this.ttlMs = ttlMs;
            this.map = new LinkedHashMap<>(16, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<Long, Failure> eldest) {
                    return size() > 10_000;
                }
            };
        }

        synchronized RegistryException get(long id) {
            if (ttlMs <= 0) return null;
            Failure e = map.get(id);
            if (e == null) return null;
            if (System.currentTimeMillis() >= e.expiresAt()) {
                map.remove(id);
                return null;
            }
            return e.error();
        }

        synchronized void put(long id, RegistryException error) {
            if (ttlMs <= 0) return;
            map.put(id, new Failure(error, System.currentTimeMillis() + ttlMs));
        }
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
