package se.afshin.yavari.kafka.operator.infra;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;

/**
 * Shared init-container helper that converts a cert-manager-shaped PEM Secret
 * ({@code tls.crt}/{@code tls.key}/{@code ca.crt}) into PKCS12 keystore + truststore
 * files. Apicurio's Kafka client config and Kafka Connect's SSL config both only accept
 * keystore <em>file</em> paths, so the init container materializes them onto an
 * emptyDir volume that the main container then mounts read-only.
 *
 * <p>Used by both {@code ApicurioDeploymentBuilder} (kafkasql client TLS) and the MM2
 * worker (dual source/target TLS).
 *
 * <p>The container's image must carry {@code openssl} + {@code keytool}; the project's
 * {@code kafka-ubi} image qualifies.
 *
 * <p>Note on UIDs: the init container's UID (1000 by default in {@code kafka-ubi}) may
 * differ from the workload container's UID. We {@code chmod 0644} the outputs so the
 * workload can read them — they're password-protected anyway.
 */
public final class PemToPkcs12InitContainer {

    private PemToPkcs12InitContainer() {}

    /**
     * @param name container name (unique within the pod — for MM2 use {@code "pem-to-pkcs12-source"} and {@code "pem-to-pkcs12-target"}).
     * @param image image with openssl + keytool (e.g. the project's kafka image).
     * @param tlsVolumeName name of the input volume backed by the PEM Secret.
     * @param tlsMountPath path inside the container where the PEM volume is mounted.
     * @param pkcs12VolumeName name of the shared output emptyDir volume.
     * @param pkcs12MountPath path inside the container where the emptyDir is mounted.
     * @param keystorePassword PKCS12 store/key password. Hardcoded "changeit" is fine —
     *                        the keystore is on an in-pod emptyDir, never persisted.
     */
    public static Container build(String name,
                                   String image,
                                   String tlsVolumeName,
                                   String tlsMountPath,
                                   String pkcs12VolumeName,
                                   String pkcs12MountPath,
                                   String keystorePassword) {
        String script =
                "set -euo pipefail\n"
                + "rm -f " + pkcs12MountPath + "/keystore.p12 " + pkcs12MountPath + "/truststore.p12\n"
                + "openssl pkcs12 -export"
                + " -inkey " + tlsMountPath + "/tls.key"
                + " -in " + tlsMountPath + "/tls.crt"
                + " -out " + pkcs12MountPath + "/keystore.p12"
                + " -passout pass:" + keystorePassword + "\n"
                + "keytool -importcert -noprompt -trustcacerts"
                + " -alias ca -file " + tlsMountPath + "/ca.crt"
                + " -keystore " + pkcs12MountPath + "/truststore.p12"
                + " -storetype PKCS12 -storepass " + keystorePassword + "\n"
                + "chmod 0644 " + pkcs12MountPath + "/keystore.p12 "
                + pkcs12MountPath + "/truststore.p12\n";
        return new ContainerBuilder()
                .withName(name)
                .withImage(image)
                .withCommand("/bin/bash", "-c", script)
                .withVolumeMounts(
                        new VolumeMountBuilder()
                                .withName(tlsVolumeName)
                                .withMountPath(tlsMountPath)
                                .withReadOnly(true)
                                .build(),
                        new VolumeMountBuilder()
                                .withName(pkcs12VolumeName)
                                .withMountPath(pkcs12MountPath)
                                .build())
                .withSecurityContext(SecurityContextDefaults.containerDefaults())
                .build();
    }
}
