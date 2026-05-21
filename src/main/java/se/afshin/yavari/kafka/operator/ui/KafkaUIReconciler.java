package se.afshin.yavari.kafka.operator.ui;

import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.networking.v1.Ingress;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Cleaner;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import se.afshin.yavari.kafka.operator.crd.ExternalAccessType;
import se.afshin.yavari.kafka.operator.crd.KafkaUI;
import se.afshin.yavari.kafka.operator.crd.KafkaUIOidcConfig;
import se.afshin.yavari.kafka.operator.crd.KafkaUISpec;
import se.afshin.yavari.kafka.operator.crd.KafkaUIStatus;
import se.afshin.yavari.kafka.operator.crd.McsConfig;
import se.afshin.yavari.kafka.operator.externalaccess.HttpExternalAccessConfig;
import se.afshin.yavari.kafka.operator.externalaccess.HttpIngressBuilder;
import se.afshin.yavari.kafka.operator.externalaccess.HttpRouteBuilder;
import se.afshin.yavari.kafka.operator.infra.OptionalResourceApplier;
import se.afshin.yavari.kafka.operator.infra.ServiceExportManager;
import se.afshin.yavari.kafka.operator.proxy.ExternalAccessResolution;
import se.afshin.yavari.kafka.operator.proxy.ExternalAccessResolver;

import java.time.Duration;
import java.util.List;

@ControllerConfiguration
@ApplicationScoped
public class KafkaUIReconciler implements Reconciler<KafkaUI>, Cleaner<KafkaUI> {

    private static final Logger LOG = Logger.getLogger(KafkaUIReconciler.class);

    @Inject KubernetesClient client;
    @Inject UIDeploymentBuilder deploymentBuilder;
    @Inject UIServiceBuilder serviceBuilder;
    @Inject UIRbacBuilder rbacBuilder;
    @Inject ExternalAccessResolver externalAccessResolver;
    @Inject HttpIngressBuilder httpIngressBuilder;
    @Inject HttpRouteBuilder httpRouteBuilder;
    @Inject ServiceExportManager serviceExportManager;
    @Inject OptionalResourceApplier optionalApplier;

    @ConfigProperty(name = "kafka.networking.mcs-enabled")
    boolean mcsEnabled;

    @ConfigProperty(name = "kafka.cluster.id", defaultValue = "")
    String localClusterId;

    @Override
    public UpdateControl<KafkaUI> reconcile(KafkaUI ui, Context<KafkaUI> context) {
        String name = ui.getMetadata().getName();
        String namespace = ui.getMetadata().getNamespace();
        LOG.infof("Reconciling KafkaUI %s/%s", namespace, name);

        KafkaUIStatus status = ui.getStatus() != null ? ui.getStatus() : new KafkaUIStatus();
        status.setPhase(KafkaUIStatus.Phase.RECONCILING);
        status.setObservedGeneration(ui.getMetadata().getGeneration());

        McsConfig mcsCfg = ui.getSpec().getMcs();
        boolean specMcsEnabled = mcsCfg != null && mcsCfg.isEnabled();
        List<String> targetClusters = ui.getSpec().getTargetClusters();

        if (!specMcsEnabled && targetClusters != null && !targetClusters.isEmpty()) {
            status.setPhase(KafkaUIStatus.Phase.FAILED);
            status.setMessage("spec.targetClusters is set but spec.mcs.enabled is false");
            ui.setStatus(status);
            return UpdateControl.patchStatus(ui);
        }

        if (specMcsEnabled) {
            if (targetClusters == null || targetClusters.isEmpty()) {
                status.setPhase(KafkaUIStatus.Phase.FAILED);
                status.setMessage("spec.mcs.enabled requires spec.targetClusters to be non-empty");
                ui.setStatus(status);
                return UpdateControl.patchStatus(ui);
            }
            if (!targetClusters.contains(localClusterId)) {
                LOG.infof("KafkaUI %s/%s: cluster '%s' not in targetClusters %s — skipping",
                        namespace, name, localClusterId, targetClusters);
                status.setPhase(KafkaUIStatus.Phase.SKIPPED);
                status.setMessage("Cluster '" + localClusterId + "' is not a target for this UI");
                ui.setStatus(status);
                return UpdateControl.patchStatus(ui);
            }
        }

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

            // When MCS is enabled the same CR is applied to every target cluster; each operator
            // exports its local kafka-ui Service so cross-cluster clients (e.g. browser landing
            // on a stale LB IP) can fall back to `kafka-ui.<ns>.svc.clusterset.local`.
            if (specMcsEnabled) {
                serviceExportManager.apply(name, namespace);
            }

            HttpExternalAccessConfig ea = ui.getSpec().getExternalAccess();
            ExternalAccessResolution external;
            try {
                external = externalAccessResolver.resolve(ea, name, localClusterId, namespace, client);
            } catch (IllegalStateException e) {
                status.setPhase(KafkaUIStatus.Phase.FAILED);
                status.setMessage(e.getMessage());
                ui.setStatus(status);
                return UpdateControl.patchStatus(ui);
            }

            applyOrDeleteIngress(ui, ownerRef, ea, external);
            applyOrDeleteHttpRoute(ui, ownerRef, ea, external);

            status.setAdvertisedHost(external.advertisedHost());

            // LB pending doesn't block READY — the UI is functional internally; only external
            // clients are affected. Reschedule so advertisedHost is populated when the LB IP arrives.
            boolean rescheduleForLb = external.isPending();
            if (rescheduleForLb) {
                status.setMessage("UI ready; waiting for LoadBalancer ingress address for external access");
            }

            int ready = readyReplicas(name, namespace);
            status.setReadyReplicas(ready);
            if (ready >= ui.getSpec().getReplicas()) {
                status.setPhase(KafkaUIStatus.Phase.READY);
                if (!rescheduleForLb) status.setMessage(null);
            } else {
                status.setMessage("Waiting for kafka-ui pods: " + ready + "/" + ui.getSpec().getReplicas());
                ui.setStatus(status);
                return UpdateControl.patchStatus(ui).rescheduleAfter(Duration.ofSeconds(15));
            }
            if (rescheduleForLb) {
                ui.setStatus(status);
                return UpdateControl.patchStatus(ui).rescheduleAfter(Duration.ofSeconds(5));
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

    private void applyOrDeleteIngress(KafkaUI ui, OwnerReference ownerRef,
                                      HttpExternalAccessConfig ea, ExternalAccessResolution external) {
        String name = ui.getMetadata().getName();
        String namespace = ui.getMetadata().getNamespace();
        boolean shouldExist = ea != null && ea.getType() == ExternalAccessType.INGRESS
                && external.advertisedHost() != null;
        if (shouldExist) {
            Ingress ingress = httpIngressBuilder.build(name, namespace, UILabels.labels(name),
                    ownerRef, external.advertisedHost(), name, KafkaUISpec.PORT, ea.getIngress());
            optionalApplier.applyIngress(ingress, namespace);
        } else {
            optionalApplier.deleteIngress(name, namespace);
        }
    }

    private void applyOrDeleteHttpRoute(KafkaUI ui, OwnerReference ownerRef,
                                        HttpExternalAccessConfig ea, ExternalAccessResolution external) {
        String name = ui.getMetadata().getName();
        String namespace = ui.getMetadata().getNamespace();
        boolean shouldExist = ea != null && ea.getType() == ExternalAccessType.GATEWAY
                && external.advertisedHost() != null;
        if (shouldExist) {
            GenericKubernetesResource route = httpRouteBuilder.build(name, namespace,
                    UILabels.labels(name), ownerRef, external.advertisedHost(), name,
                    KafkaUISpec.PORT, ea.getGateway());
            optionalApplier.applyHttpRoute(route, namespace);
        } else {
            optionalApplier.deleteHttpRoute(name, namespace);
        }
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
