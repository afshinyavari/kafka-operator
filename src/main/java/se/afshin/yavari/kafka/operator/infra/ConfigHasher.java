package se.afshin.yavari.kafka.operator.infra;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * Short SHA-256 over the operator-rendered config + mounted Secret revisions, stored on the
 * PodTemplate annotation that drives Kubernetes rolling updates. Twelve hex chars is plenty
 * of collision resistance for an annotation value.
 */
public final class ConfigHasher {

    private ConfigHasher() {}

    public static String sha256(String... parts) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            for (String p : parts) {
                md.update(p == null ? new byte[0] : p.getBytes(StandardCharsets.UTF_8));
                md.update((byte) '\n');
            }
            return HexFormat.of().formatHex(md.digest()).substring(0, 12);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }
}
