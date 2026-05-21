package se.afshin.yavari.kafka.operator.crd;

import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.ShortNames;
import io.fabric8.kubernetes.model.annotation.Version;

@Version("v1alpha1")
@Group("kafka.yavari.afshin.se")
@ShortNames("mm2")
public class MirrorMaker2 extends CustomResource<MirrorMaker2Spec, MirrorMaker2Status>
        implements Namespaced {

    /** Returns the user-facing flow name: spec.flow.flowName if set, otherwise metadata.name. */
    public String resolvedFlowName() {
        if (getSpec() != null
                && getSpec().getFlow() != null
                && getSpec().getFlow().getFlowName() != null
                && !getSpec().getFlow().getFlowName().isBlank()) {
            return getSpec().getFlow().getFlowName();
        }
        return getMetadata().getName();
    }
}
