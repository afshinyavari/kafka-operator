package se.afshin.yavari.kafka.operator.infra;

import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.networking.v1.Ingress;
import io.fabric8.kubernetes.client.KubernetesClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

/**
 * Apply / delete resources whose CRDs are not guaranteed to be installed on the cluster
 * (ServiceMonitor, TLSRoute, HTTPRoute) plus the core Ingress that we likewise wrap in
 * defensive try/catch. Centralises the swallow-on-missing-CRD pattern so reconcilers don't
 * each carry their own copy.
 *
 * <p>Behaviour preserved from the per-reconciler implementations: any exception is logged
 * at WARN and the reconcile continues — these resources are optional adornments, not
 * required for the workload to function.
 */
@ApplicationScoped
public class OptionalResourceApplier {

    public static final String SERVICEMONITOR_API = "monitoring.coreos.com/v1";
    public static final String SERVICEMONITOR_KIND = "ServiceMonitor";
    public static final String TLSROUTE_API = "gateway.networking.k8s.io/v1alpha2";
    public static final String TLSROUTE_KIND = "TLSRoute";
    public static final String HTTPROUTE_API = "gateway.networking.k8s.io/v1";
    public static final String HTTPROUTE_KIND = "HTTPRoute";

    private static final Logger LOG = Logger.getLogger(OptionalResourceApplier.class);

    @Inject KubernetesClient client;

    public void applyServiceMonitor(GenericKubernetesResource sm, String namespace) {
        applyGeneric(SERVICEMONITOR_API, SERVICEMONITOR_KIND, sm, namespace);
    }

    public void deleteServiceMonitor(String name, String namespace) {
        deleteGeneric(SERVICEMONITOR_API, SERVICEMONITOR_KIND, name, namespace);
    }

    public void applyTlsRoute(GenericKubernetesResource route, String namespace) {
        applyGeneric(TLSROUTE_API, TLSROUTE_KIND, route, namespace);
    }

    public void deleteTlsRoute(String name, String namespace) {
        deleteGeneric(TLSROUTE_API, TLSROUTE_KIND, name, namespace);
    }

    public void applyHttpRoute(GenericKubernetesResource route, String namespace) {
        applyGeneric(HTTPROUTE_API, HTTPROUTE_KIND, route, namespace);
    }

    public void deleteHttpRoute(String name, String namespace) {
        deleteGeneric(HTTPROUTE_API, HTTPROUTE_KIND, name, namespace);
    }

    /** Ingress is in the K8s core API so the CRD is always present, but the existing
     *  reconcilers all wrap it in try/catch for safety — kept here to match. */
    public void applyIngress(Ingress ingress, String namespace) {
        String displayName = ingress.getMetadata() != null ? ingress.getMetadata().getName() : "<unknown>";
        try {
            client.network().v1().ingresses().inNamespace(namespace).resource(ingress).serverSideApply();
        } catch (Exception e) {
            LOG.warnf("Ingress apply skipped for %s/%s: %s", namespace, displayName, e.getMessage());
        }
    }

    public void deleteIngress(String name, String namespace) {
        try {
            client.network().v1().ingresses().inNamespace(namespace).withName(name).delete();
        } catch (Exception e) {
            LOG.warnf("Ingress delete skipped for %s/%s: %s", namespace, name, e.getMessage());
        }
    }

    private void applyGeneric(String api, String kind, GenericKubernetesResource res, String namespace) {
        String displayName = res.getMetadata() != null ? res.getMetadata().getName() : "<unknown>";
        try {
            client.genericKubernetesResources(api, kind)
                  .inNamespace(namespace).resource(res).serverSideApply();
        } catch (Exception e) {
            LOG.warnf("%s CRD not available — apply skipped for %s/%s: %s",
                    kind, namespace, displayName, e.getMessage());
        }
    }

    private void deleteGeneric(String api, String kind, String name, String namespace) {
        try {
            client.genericKubernetesResources(api, kind)
                  .inNamespace(namespace).withName(name).delete();
        } catch (Exception e) {
            LOG.warnf("%s delete skipped for %s/%s: %s", kind, namespace, name, e.getMessage());
        }
    }
}
