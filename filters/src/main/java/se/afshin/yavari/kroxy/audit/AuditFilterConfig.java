package se.afshin.yavari.kroxy.audit;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Set;

/**
 * Config for the audit filter. The filter is wired into the chain unconditionally
 * (stdout sink is always on); the Kafka sink is configured via process env vars
 * picked up by {@link AuditEmitters#fromEnv()} — see that class for the env names.
 *
 * <p>{@code includeOps} is the optional allowlist of operation names to emit
 * ({@code WRITE}, {@code READ}, {@code DESCRIBE}, ...). When unset/empty, all
 * ops are emitted.
 */
public class AuditFilterConfig {

    /** Optional allowlist of ops to emit (WRITE, READ, DESCRIBE, ...). Empty/null = all. */
    @JsonProperty
    private Set<String> includeOps;

    public Set<String> getIncludeOps() { return includeOps; }
    public void setIncludeOps(Set<String> includeOps) { this.includeOps = includeOps; }
}
