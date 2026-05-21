package se.afshin.yavari.kafka.operator.rbac;

import jakarta.enterprise.context.ApplicationScoped;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterClientQuotasOptions;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.jboss.logging.Logger;
import se.afshin.yavari.kafka.operator.crd.KafkaQuotaConfig;
import se.afshin.yavari.kafka.operator.crd.KafkaRbacUser;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Applies {@link KafkaQuotaConfig} on {@link KafkaRbacUser}s via the AdminClient
 * quotas API. Called from {@link KafkaRbacReconciler} after the proxy RBAC ConfigMap
 * is written.
 *
 * <p>Quotas attach to the user principal — under SASL/OAUTHBEARER that's the JWT
 * {@code sub} claim, which is what Kroxylicious forwards to the broker. Listing all
 * four fields as null on a user is a no-op (we don't issue an alteration that
 * clears every entry, so unrelated existing quotas survive).
 *
 * <p>Best-effort: a quota application failure is logged and surfaced via the return
 * value but does not abort the surrounding reconcile (RBAC config is more important
 * than quotas).
 */
@ApplicationScoped
public class KafkaQuotaManager {

    private static final Logger LOG = Logger.getLogger(KafkaQuotaManager.class);

    /** Returns a list of human-readable error strings (empty == all OK). */
    public List<String> applyUserQuotas(AdminClient admin, List<KafkaRbacUser> users) {
        List<String> errors = new ArrayList<>();
        List<ClientQuotaAlteration> alterations = new ArrayList<>();
        for (KafkaRbacUser user : users) {
            if (user.getQuotas() == null || user.getName() == null) continue;
            List<ClientQuotaAlteration.Op> ops = buildOps(user.getQuotas());
            if (ops.isEmpty()) continue;
            ClientQuotaEntity entity = new ClientQuotaEntity(
                    Map.of(ClientQuotaEntity.USER, user.getName()));
            alterations.add(new ClientQuotaAlteration(entity, ops));
        }
        if (alterations.isEmpty()) return errors;
        try {
            admin.alterClientQuotas(alterations, new AlterClientQuotasOptions().validateOnly(false))
                    .all().get(20, TimeUnit.SECONDS);
            LOG.infof("Applied client quotas for %d users", alterations.size());
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            errors.add("interrupted while applying quotas");
        } catch (ExecutionException | java.util.concurrent.TimeoutException e) {
            String msg = "Failed to apply client quotas: " + e.getMessage();
            LOG.warn(msg);
            errors.add(msg);
        }
        return errors;
    }

    private List<ClientQuotaAlteration.Op> buildOps(KafkaQuotaConfig q) {
        List<ClientQuotaAlteration.Op> ops = new ArrayList<>(4);
        if (q.getProducerByteRate() != null) {
            ops.add(new ClientQuotaAlteration.Op(
                    "producer_byte_rate", q.getProducerByteRate().doubleValue()));
        }
        if (q.getConsumerByteRate() != null) {
            ops.add(new ClientQuotaAlteration.Op(
                    "consumer_byte_rate", q.getConsumerByteRate().doubleValue()));
        }
        if (q.getRequestPercentage() != null) {
            ops.add(new ClientQuotaAlteration.Op(
                    "request_percentage", q.getRequestPercentage()));
        }
        if (q.getControllerMutationRate() != null) {
            ops.add(new ClientQuotaAlteration.Op(
                    "controller_mutation_rate", q.getControllerMutationRate()));
        }
        return ops;
    }
}
