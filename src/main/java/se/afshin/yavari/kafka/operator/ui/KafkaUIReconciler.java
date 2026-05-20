package se.afshin.yavari.kafka.operator.ui;

import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
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
import se.afshin.yavari.kafka.operator.crd.KafkaUI;
import se.afshin.yavari.kafka.operator.crd.KafkaUIOidcConfig;
import se.afshin.yavari.kafka.operator.crd.KafkaUIStatus;

import java.time.Duration;

@ControllerConfiguration
@ApplicationScoped
public class KafkaUIReconciler implements Reconciler<KafkaUI>, Cleaner<KafkaUI> {

    private static final Logger LOG = Logger.getLogger(KafkaUIReconciler.class);

    @Inject KubernetesClient client;
    @Inject UIDeploymentBuilder deploymentBuilder;
    @Inject UIServiceBuilder serviceBuilder;
    @Inject UIRbacBuilder rbacBuilder;
    @Inject UIIngressBuilder ingressBuilder;

    @Override
    public UpdateControl<KafkaUI> reconcile(KafkaUI ui, Context<KafkaUI> context) {
        String name = ui.getMetadata().getName();
        String namespace = ui.getMetadata().getNamespace();
        LOG.infof("Reconciling KafkaUI %s/%s", namespace, name);

        KafkaUIStatus status = ui.getStatus() != null ? ui.getStatus() : new KafkaUIStatus();
        status.setPhase(KafkaUIStatus.Phase.RECONCILING);
        status.setObservedGeneration(ui.getMetadata().getGeneration());

        String validationError = validate(ui);
        if (validationError != null) {
            status.setPhase(KafkaUIStatus.Phase.FAILED);
            status.setMessage(validationError);
            ui.setStatus(status);
            return UpdateControl.patchStatus(ui);
        }

        OwnerReference ownerRef = new OwnerReferenceBuilder()
                .withApiVersion(ui.getApiVersion())
                .withKind(ui.getKind())
                .withName(name)
                .withUid(ui.getMetadata().getUid())
                .withController(true)
                .withBlockOwnerDeletion(true)
                .build();

        try {
            client.serviceAccounts().inNamespace(namespace)
                    .resource(rbacBuilder.serviceAccount(ui, ownerRef)).serverSideApply();
            client.rbac().roles().inNamespace(namespace)
                    .resource(rbacBuilder.role(ui, ownerRef)).serverSideApply();
            client.rbac().roleBindings().inNamespace(namespace)
                    .resource(rbacBuilder.roleBinding(ui, ownerRef)).serverSideApply();

            client.apps().deployments().inNamespace(namespace)
                    .resource(deploymentBuilder.build(ui, ownerRef)).serverSideApply();
            client.services().inNamespace(namespace)
                    .resource(serviceBuilder.build(ui, ownerRef)).serverSideApply();

            if (ui.getSpec().getIngress() != null && ui.getSpec().getIngress().isEnabled()) {
                client.network().v1().ingresses().inNamespace(namespace)
                        .resource(ingressBuilder.build(ui, ownerRef)).serverSideApply();
            } else {
                client.network().v1().ingresses().inNamespace(namespace).withName(name).delete();
            }

            int ready = readyReplicas(name, namespace);
            status.setReadyReplicas(ready);
            if (ready >= ui.getSpec().getReplicas()) {
                status.setPhase(KafkaUIStatus.Phase.READY);
                status.setMessage(null);
            } else {
                status.setMessage("Waiting for kafka-ui pods: " + ready + "/" + ui.getSpec().getReplicas());
                ui.setStatus(status);
                return UpdateControl.patchStatus(ui).rescheduleAfter(Duration.ofSeconds(15));
            }
        } catch (Exception e) {
            LOG.errorf("KafkaUI %s/%s failed: %s", namespace, name, e.getMessage());
            status.setPhase(KafkaUIStatus.Phase.FAILED);
            status.setMessage(e.getMessage());
        }

        ui.setStatus(status);
        return UpdateControl.patchStatus(ui);
    }

    @Override
    public DeleteControl cleanup(KafkaUI ui, Context<KafkaUI> context) {
        // Child resources cascade via ownerReferences — no manual cleanup needed.
        LOG.infof("KafkaUI %s/%s deleted", ui.getMetadata().getNamespace(), ui.getMetadata().getName());
        return DeleteControl.defaultDelete();
    }

    private String validate(KafkaUI ui) {
        KafkaUIOidcConfig oidc = ui.getSpec().getOidc();
        if (oidc == null) {
            return "spec.oidc is required";
        }
        if (oidc.getIssuerUrl() == null || oidc.getIssuerUrl().isBlank()) {
            return "spec.oidc.issuerUrl is required";
        }
        if (oidc.getClientId() == null || oidc.getClientId().isBlank()) {
            return "spec.oidc.clientId is required";
        }
        if (oidc.getClientSecretRef() == null
                || oidc.getClientSecretRef().getName() == null
                || oidc.getClientSecretRef().getKey() == null) {
            return "spec.oidc.clientSecretRef.{name,key} are required";
        }
        return null;
    }

    private int readyReplicas(String deploymentName, String namespace) {
        Deployment dep = client.apps().deployments().inNamespace(namespace)
                .withName(deploymentName).get();
        if (dep == null || dep.getStatus() == null || dep.getStatus().getReadyReplicas() == null) {
            return 0;
        }
        return dep.getStatus().getReadyReplicas();
    }
}
