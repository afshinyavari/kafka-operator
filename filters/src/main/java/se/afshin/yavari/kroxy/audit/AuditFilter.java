package se.afshin.yavari.kroxy.audit;

import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.authentication.User;
import se.afshin.yavari.kroxy.auth.Group;
import io.kroxylicious.proxy.filter.FetchResponseFilter;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.MetadataResponseFilter;
import io.kroxylicious.proxy.filter.ProduceRequestFilter;
import io.kroxylicious.proxy.filter.ProduceResponseFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;

/**
 * Observation filter. Sits <b>after</b> {@code authorization} in the chain and
 * inspects the response error codes to decide allow vs deny. Does <b>not</b>
 * re-evaluate RBAC.
 *
 * <p>For each Produce or Fetch request the filter captures
 * {@code (startNanos, principal, op, topics)} keyed on {@code correlationId},
 * then on the matching response emits one {@link AuditEvent} per topic with
 * {@code decision=deny} if any partition reports
 * {@link Errors#TOPIC_AUTHORIZATION_FAILED} or
 * {@link Errors#CLUSTER_AUTHORIZATION_FAILED}, else {@code decision=allow}.
 *
 * <p>One instance per connection (Kroxylicious filter scoping). The in-flight
 * map is bounded only by the proxy's per-connection request limit, which is
 * already enforced upstream.
 */
public class AuditFilter implements
        ProduceRequestFilter, ProduceResponseFilter,
        FetchResponseFilter,
        MetadataResponseFilter {

    private static final Logger LOG = LoggerFactory.getLogger(AuditFilter.class);

    private final AuditEmitter emitter;
    private final Set<String> includeOps;
    private final Map<Integer, InFlight> inFlight = new HashMap<>();

    public AuditFilter(AuditEmitter emitter, Set<String> includeOps) {
        this.emitter = emitter;
        this.includeOps = includeOps == null ? Set.of() : Set.copyOf(includeOps);
    }

    @Override
    public CompletionStage<RequestFilterResult> onProduceRequest(short apiVersion,
                                                                 RequestHeaderData header,
                                                                 ProduceRequestData body,
                                                                 FilterContext ctx) {
        // Audit is observational — never block or fail the request path.
        try {
            if (emit("PRODUCE")) {
                List<String> topics = new ArrayList<>(body.topicData().size());
                for (ProduceRequestData.TopicProduceData t : body.topicData()) topics.add(t.name());
                inFlight.put(header.correlationId(),
                        new InFlight(System.nanoTime(), principalOf(ctx), "PRODUCE", topics));
            }
        } catch (Exception e) {
            LOG.warn("Audit pre-request capture failed", e);
        }
        return ctx.forwardRequest(header, body);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onProduceResponse(short apiVersion,
                                                                    ResponseHeaderData header,
                                                                    ProduceResponseData body,
                                                                    FilterContext ctx) {
        try {
            InFlight state = inFlight.remove(header.correlationId());
            if (state != null) {
                long latencyMs = (System.nanoTime() - state.startNanos) / 1_000_000;
                String correlationId = String.valueOf(header.correlationId());
                for (ProduceResponseData.TopicProduceResponse t : body.responses()) {
                    boolean deny = false;
                    if (t.partitionResponses() != null) {
                        for (ProduceResponseData.PartitionProduceResponse p : t.partitionResponses()) {
                            if (isAuthFailure(p.errorCode())) { deny = true; break; }
                        }
                    }
                    emitter.emit(new AuditEvent(Instant.now(), state.principal, "PRODUCE",
                            t.name(), deny ? "deny" : "allow", latencyMs, correlationId));
                }
            }
        } catch (Exception e) {
            LOG.warn("Audit produce-response emit failed", e);
        }
        return ctx.forwardResponse(header, body);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onFetchResponse(short apiVersion,
                                                                  ResponseHeaderData header,
                                                                  FetchResponseData body,
                                                                  FilterContext ctx) {
        try {
            // Audit is observational — emit out-of-band so response forwarding is never
            // blocked or failed. On Fetch v13+ the topic name field is empty and only
            // topicId is present; resolve names via the proxy's metadata cache.
            if (emit("FETCH") && body.responses() != null) {
                List<FetchResponseData.FetchableTopicResponse> responses = body.responses();
                Set<Uuid> ids = new LinkedHashSet<>();
                for (FetchResponseData.FetchableTopicResponse t : responses) {
                    if (t.topicId() != null && !Uuid.ZERO_UUID.equals(t.topicId())) {
                        ids.add(t.topicId());
                    }
                }
                String correlationId = String.valueOf(header.correlationId());
                String principal = principalOf(ctx);
                // Fire-and-forget: resolution runs against the proxy's metadata cache;
                // when it returns we emit one event per topic in the response.
                ctx.topicNames(ids).thenAccept(mapping ->
                        emitFetchEvents(responses, mapping.topicNames(), principal, correlationId)
                ).exceptionally(err -> {
                    // Resolution failed — fall back to the topic id so the audit trail
                    // is never empty.
                    emitFetchEvents(responses, Map.of(), principal, correlationId);
                    LOG.debug("Audit fetch topic-name resolution failed; using topicId", err);
                    return null;
                });
            }
        } catch (Exception e) {
            LOG.warn("Audit fetch-response observation failed", e);
        }
        return ctx.forwardResponse(header, body);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onMetadataResponse(short apiVersion,
                                                                     ResponseHeaderData header,
                                                                     MetadataResponseData body,
                                                                     FilterContext ctx) {
        // Metadata is the first RPC a client makes (DESCRIBE on the topic). When the
        // authorization filter denies a topic, it surfaces here as TOPIC_AUTHORIZATION_FAILED
        // on the per-topic errorCode — and the Produce/Fetch that the client would have done
        // next never reaches the proxy. Auditing Metadata captures these denies.
        try {
            if (emit("DESCRIBE") && body.topics() != null) {
                String correlationId = String.valueOf(header.correlationId());
                String principal = principalOf(ctx);
                Instant now = Instant.now();
                for (MetadataResponseData.MetadataResponseTopic t : body.topics()) {
                    if (t.errorCode() == Errors.NONE.code()) continue;   // only emit interesting outcomes
                    boolean deny = isAuthFailure(t.errorCode());
                    String name = t.name();
                    if ((name == null || name.isEmpty()) && t.topicId() != null) {
                        name = t.topicId().toString();
                    }
                    emitter.emit(new AuditEvent(now, principal, "DESCRIBE",
                            name == null ? "" : name, deny ? "deny" : "error", 0L, correlationId));
                }
            }
        } catch (Exception e) {
            LOG.warn("Audit metadata-response emit failed", e);
        }
        return ctx.forwardResponse(header, body);
    }

    private void emitFetchEvents(List<FetchResponseData.FetchableTopicResponse> responses,
                                 Map<Uuid, String> nameById,
                                 String principal,
                                 String correlationId) {
        Instant now = Instant.now();
        for (FetchResponseData.FetchableTopicResponse t : responses) {
            boolean deny = false;
            if (t.partitions() != null) {
                for (FetchResponseData.PartitionData p : t.partitions()) {
                    if (isAuthFailure(p.errorCode())) { deny = true; break; }
                }
            }
            String name = t.topic();
            if ((name == null || name.isEmpty()) && t.topicId() != null) {
                name = nameById.get(t.topicId());
                if (name == null) name = t.topicId().toString();
            }
            emitter.emit(new AuditEvent(now, principal, "FETCH",
                    name == null ? "" : name, deny ? "deny" : "allow", 0L, correlationId));
        }
    }

    private boolean emit(String op) {
        return includeOps.isEmpty() || includeOps.contains(op);
    }

    static boolean isAuthFailure(short errorCode) {
        return errorCode == Errors.TOPIC_AUTHORIZATION_FAILED.code()
                || errorCode == Errors.CLUSTER_AUTHORIZATION_FAILED.code()
                || errorCode == Errors.GROUP_AUTHORIZATION_FAILED.code()
                || errorCode == Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code()
                || errorCode == Errors.DELEGATION_TOKEN_AUTHORIZATION_FAILED.code();
    }

    /** Subject → "user:<name>" or "group:<g>" or "anonymous".
     *  For JWT-authenticated principals the User name is the JWT {@code sub}
     *  (a UUID for Keycloak). We replace it with the cached {@code preferred_username}
     *  from {@link se.afshin.yavari.kroxy.auth.JwtGroupStore} when available, so the
     *  audit log shows {@code user:alice} instead of {@code user:18d70778-...}.
     *  mTLS clients already have a readable User name (the certificate CN). */
    static String principalOf(FilterContext ctx) {
        Subject subject;
        try {
            subject = ctx.authenticatedSubject();
        } catch (Exception e) {
            return "anonymous";
        }
        if (subject == null || subject.isAnonymous()) return "anonymous";
        Optional<User> user = subject.uniquePrincipalOfType(User.class);
        if (user.isPresent()) {
            String name = user.get().name();
            String preferred = se.afshin.yavari.kroxy.auth.JwtGroupStore.getUsername(name);
            return "user:" + (preferred != null ? preferred : name);
        }
        Set<Group> groups = subject.allPrincipalsOfType(Group.class);
        if (!groups.isEmpty()) return "group:" + groups.iterator().next().name();
        return "anonymous";
    }

    /** Per-request capture, popped on the matching response. */
    private record InFlight(long startNanos, String principal, String op, List<String> topics) {}
}
