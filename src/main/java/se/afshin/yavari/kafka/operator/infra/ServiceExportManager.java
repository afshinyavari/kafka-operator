package se.afshin.yavari.kafka.operator.infra;

import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

/**
 * One place to apply / delete Submariner-MCS {@code ServiceExport} resources. Reconcilers
 * (NodePool, Proxy, Apicurio, KafkaUI) used to each carry their own ~15-line copy with
 * identical try/catch wrappers; this collapses them onto a single CDI bean.
 *
 * <p>"CRD not installed" is swallowed and logged as a warning — clusters without MCS still
 * reconcile cleanly, just without the export.
 */
@ApplicationScoped
public class ServiceExportManager {

    public static final String API_VERSION = "multicluster.x-k8s.io/v1alpha1";
    public static final String KIND = "ServiceExport";

    private static final Logger LOG = Logger.getLogger(ServiceExportManager.class);

    @Inject KubernetesClient client;

    /** Build a default ServiceExport (name + namespace only) and serverSideApply it. */
    public void apply(String serviceName, String namespace) {
        GenericKubernetesResource export = new GenericKubernetesResource();
        export.setApiVersion(API_VERSION);
        export.setKind(KIND);
        export.setMetadata(new ObjectMetaBuilder()
                .withName(serviceName).withNamespace(namespace).build());
        apply(export, namespace, serviceName);
    }

    /** Apply a pre-built ServiceExport. Use this when the caller needs to attach an
     *  owner reference (e.g. KafkaNodePool's headless export, which is owned by the pool). */
    public void apply(GenericKubernetesResource export, String namespace, String displayName) {
        try {
            client.genericKubernetesResources(API_VERSION, KIND)
                  .inNamespace(namespace).resource(export).serverSideApply();
        } catch (Exception e) {
            LOG.warnf("ServiceExport CRD not available — skipped for %s: %s",
                    displayName, e.getMessage());
        }
    }

    /** Best-effort delete; safe to call from cleanup() paths regardless of whether the
     *  MCS CRD is installed. */
    public void delete(String name, String namespace) {
        try {
            client.genericKubernetesResources(API_VERSION, KIND)
                  .inNamespace(namespace).withName(name).delete();
        } catch (Exception e) {
            LOG.warnf("ServiceExport delete skipped for %s/%s: %s", namespace, name, e.getMessage());
        }
    }
}
