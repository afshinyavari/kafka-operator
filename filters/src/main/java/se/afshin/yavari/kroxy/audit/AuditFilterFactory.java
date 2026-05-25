package se.afshin.yavari.kroxy.audit;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * Process-wide singleton: the {@link AuditEmitter} is constructed once per JVM
 * from {@code KAFKA_AUDIT_*} env vars (see {@link AuditEmitters}), then shared
 * across every per-connection {@link AuditFilter} instance.
 */
@Plugin(configType = AuditFilterConfig.class)
public class AuditFilterFactory implements FilterFactory<AuditFilterConfig, AuditFilterFactory.SharedState> {

    private static final Logger log = LoggerFactory.getLogger(AuditFilterFactory.class);

    /** Per-JVM emitter + the parsed includeOps. */
    public record SharedState(AuditEmitter emitter, Set<String> includeOps) {}

    @Override
    public SharedState initialize(FilterFactoryContext context, AuditFilterConfig config)
            throws PluginConfigurationException {
        AuditEmitter emitter = AuditEmitters.fromEnv();
        Set<String> ops = (config == null || config.getIncludeOps() == null)
                ? Set.of() : config.getIncludeOps();
        log.info("Audit filter initialised — sink={}, includeOps={}",
                emitter.getClass().getSimpleName(), ops.isEmpty() ? "<all>" : ops);
        return new SharedState(emitter, ops);
    }

    @Override
    public Filter createFilter(FilterFactoryContext context, SharedState state) {
        return new AuditFilter(state.emitter(), state.includeOps());
    }

    @Override
    public void close(SharedState state) {
        try {
            state.emitter().close();
        } catch (Exception e) {
            log.warn("Error closing audit emitter", e);
        }
    }
}
