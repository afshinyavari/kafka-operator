package se.afshin.yavari.kroxy.audit;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class AuditEmittersTest {

    @Test
    void noBootstrap_returnsStdoutOnly() {
        Map<String, String> env = new HashMap<>();
        AuditEmitter emitter = AuditEmitters.fromEnv(env::get);
        assertThat(emitter).isInstanceOf(StdoutAuditEmitter.class);
    }

    @Test
    void producerProps_pinsMaxBlockMsToZero() {
        Properties p = AuditEmitters.producerProps("broker:9092", null, null, null);
        // Non-blocking on calling thread is mandatory — KafkaAuditEmitter relies on this.
        assertThat(p.get("max.block.ms")).isEqualTo(0);
        assertThat(p.get("acks")).isEqualTo("1");
        assertThat(p.get("compression.type")).isEqualTo("zstd");
        // PEM source mode is only enabled when all three paths are non-blank.
        assertThat(p.get("security.protocol")).isNull();
    }
}
