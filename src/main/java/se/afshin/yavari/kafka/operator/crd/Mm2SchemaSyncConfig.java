package se.afshin.yavari.kafka.operator.crd;

import io.fabric8.generator.annotation.ValidationRule;

import java.util.List;

/** Apicurio schema-mirroring SMT configuration. When enabled, MM2's
 *  MirrorSourceConnector is wired with the ApicurioSchemaTransferSmt: each record's
 *  Apicurio V3 envelope (magic byte 0x00 + 8-byte globalId) is parsed, the schema is
 *  fetched from the source registry, ensured to exist in the target registry, and the
 *  envelope is rewritten with the target's globalId.
 *
 *  <p>The SMT is designed to coexist with non-Apicurio topics — see the
 *  "Non-Apicurio topics" section of docs/api-reference.md for the six-layer passthrough
 *  rules. Defaults are tuned for mixed-format clusters. */
public class Mm2SchemaSyncConfig {

    /** Master switch. When false, no SMT is added to MM2 properties even if schema
     *  registries are configured. */
    private boolean enabled = false;

    /** LRU cache size for source→target globalId mappings, per worker. */
    @ValidationRule(value = "self >= 16", message = "cacheSize must be at least 16")
    private int cacheSize = 10_000;

    /** Behavior when the SMT encounters an error (e.g. source returns 404 for what
     *  looked like a globalId, or target registry write fails). WARN (default) logs
     *  and passes the record through unchanged — safer in mixed-format clusters. FAIL
     *  bubbles the exception (Connect worker dies). IGNORE drops the record. */
    private OnError behaviorOnError = OnError.WARN;

    /** Which record component to process. Default VALUE matches the common case
     *  where only the value is schema'd. */
    private ApplyTo applyTo = ApplyTo.VALUE;

    /** Topic-name regex allowlist. Only records on matching topics are inspected. */
    private List<String> applyToTopics = List.of(".*");

    public enum OnError { FAIL, WARN, IGNORE }
    public enum ApplyTo { VALUE, KEY, BOTH }

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }

    public int getCacheSize() { return cacheSize; }
    public void setCacheSize(int cacheSize) { this.cacheSize = cacheSize; }

    public OnError getBehaviorOnError() { return behaviorOnError; }
    public void setBehaviorOnError(OnError behaviorOnError) { this.behaviorOnError = behaviorOnError; }

    public ApplyTo getApplyTo() { return applyTo; }
    public void setApplyTo(ApplyTo applyTo) { this.applyTo = applyTo; }

    public List<String> getApplyToTopics() { return applyToTopics; }
    public void setApplyToTopics(List<String> applyToTopics) { this.applyToTopics = applyToTopics; }
}
