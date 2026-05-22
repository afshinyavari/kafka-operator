package se.afshin.yavari.kafka.operator.backup;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SchemaBackupStepTest {

    private final SchemaBackupStep step = new SchemaBackupStep();

    private boolean hasEnv(Container c, String name) {
        return c.getEnv().stream().anyMatch(e -> e.getName().equals(name));
    }

    private EnvVar env(Container c, String name) {
        return c.getEnv().stream().filter(e -> e.getName().equals(name)).findFirst().orElseThrow();
    }

    @Test
    void exportSidecarRunsExportScriptWithSchemaDest() {
        Container c = step.exportSidecar("kafka-backup:dev", "IfNotPresent",
                "http://apicurio", "backups/schemas", "oauth-creds",
                List.of(), List.of(), new ResourceRequirementsBuilder().build());

        assertThat(c.getName()).isEqualTo("schema-export");
        assertThat(c.getCommand()).containsExactly("apicurio-export.sh");
        assertThat(env(c, "APICURIO_URL").getValue()).isEqualTo("http://apicurio");
        assertThat(env(c, "SCHEMA_DEST").getValue()).isEqualTo("backups/schemas");
        assertThat(env(c, "OAUTH_TOKEN_URL").getValueFrom().getSecretKeyRef().getName())
                .isEqualTo("oauth-creds");
    }

    @Test
    void importInitContainerRunsImportScriptWithSchemaSrc() {
        Container c = step.importInitContainer("kafka-backup:dev", "IfNotPresent",
                "http://apicurio", "/backup/schemas", null,
                List.of(), List.of(), new ResourceRequirementsBuilder().build());

        assertThat(c.getName()).isEqualTo("schema-import");
        assertThat(c.getCommand()).containsExactly("apicurio-import.sh");
        assertThat(env(c, "SCHEMA_SRC").getValue()).isEqualTo("/backup/schemas");
    }

    @Test
    void omitsOauthEnvWhenNoAuthSecret() {
        Container c = step.exportSidecar("img", "IfNotPresent", "http://apicurio",
                "/backup/schemas", null, List.of(), List.of(),
                new ResourceRequirementsBuilder().build());
        assertThat(hasEnv(c, "OAUTH_TOKEN_URL")).isFalse();
    }
}
