package se.afshin.yavari.kafka.operator.rbac;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Cleaner;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import se.afshin.yavari.kafka.operator.crd.KafkaRbac;
import se.afshin.yavari.kafka.operator.crd.KafkaRbacStatus;

@ControllerConfiguration
@ApplicationScoped
public class KafkaRbacReconciler implements Reconciler<KafkaRbac>, Cleaner<KafkaRbac> {

    private static final Logger LOG = Logger.getLogger(KafkaRbacReconciler.class);

    @Inject KubernetesClient client;
    @Inject KafkaRbacConfigMapBuilder configMapBuilder;

    @Override
    public UpdateControl<KafkaRbac> reconcile(KafkaRbac rbac, Context<KafkaRbac> context) {
        String name = rbac.getMetadata().getName();
        String namespace = rbac.getMetadata().getNamespace();
        LOG.infof("Reconciling KafkaRbac %s/%s", namespace, name);

        KafkaRbacStatus status = rbac.getStatus() != null ? rbac.getStatus() : new KafkaRbacStatus();
        status.setPhase(KafkaRbacStatus.Phase.RECONCILING);

        try {
            ConfigMap kafkaCm = configMapBuilder.buildKafkaRules(rbac, namespace);
            client.configMaps().inNamespace(namespace).resource(kafkaCm).serverSideApply();

            ConfigMap apicurioCm = configMapBuilder.buildApicurioPolicy(rbac, namespace);
            client.configMaps().inNamespace(namespace).resource(apicurioCm).serverSideApply();

            status.setPhase(KafkaRbacStatus.Phase.READY);
            status.setMessage(null);
        } catch (Exception e) {
            LOG.errorf("KafkaRbac %s/%s failed: %s", namespace, name, e.getMessage());
            status.setPhase(KafkaRbacStatus.Phase.FAILED);
            status.setMessage(e.getMessage());
        }

        rbac.setStatus(status);
        return UpdateControl.patchStatus(rbac);
    }

    @Override
    public DeleteControl cleanup(KafkaRbac rbac, Context<KafkaRbac> context) {
        LOG.infof("KafkaRbac %s deleted — ConfigMaps cascade via owner reference",
                rbac.getMetadata().getName());
        return DeleteControl.defaultDelete();
    }
}
