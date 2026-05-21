package se.afshin.yavari.kafka.operator.infra;

import io.fabric8.kubernetes.api.model.Capabilities;
import io.fabric8.kubernetes.api.model.CapabilitiesBuilder;
import io.fabric8.kubernetes.api.model.PodSecurityContext;
import io.fabric8.kubernetes.api.model.PodSecurityContextBuilder;
import io.fabric8.kubernetes.api.model.SeccompProfile;
import io.fabric8.kubernetes.api.model.SeccompProfileBuilder;
import io.fabric8.kubernetes.api.model.SecurityContext;
import io.fabric8.kubernetes.api.model.SecurityContextBuilder;

import java.util.List;

/**
 * Pod / container {@code securityContext} defaults applied to every workload the operator
 * generates. Aligned with the Kubernetes "restricted" Pod Security Standard, minus
 * {@code readOnlyRootFilesystem} (Kafka's startup script writes keystores to /tmp and
 * Apicurio's Quarkus app writes to /tmp — covered in a follow-up).
 */
public final class SecurityContextDefaults {

    private SecurityContextDefaults() {}

    /** Pod-level: enforce non-root and the runtime's default seccomp profile. */
    public static PodSecurityContext podDefaults() {
        SeccompProfile seccomp = new SeccompProfileBuilder().withType("RuntimeDefault").build();
        return new PodSecurityContextBuilder()
                .withRunAsNonRoot(true)
                .withSeccompProfile(seccomp)
                .build();
    }

    /** Container-level: drop all caps and bar privilege escalation. */
    public static SecurityContext containerDefaults() {
        Capabilities caps = new CapabilitiesBuilder().withDrop(List.of("ALL")).build();
        return new SecurityContextBuilder()
                .withAllowPrivilegeEscalation(false)
                .withCapabilities(caps)
                .build();
    }
}
