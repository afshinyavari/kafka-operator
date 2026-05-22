package se.afshin.yavari.kafka.smt;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Tiny command-line probe for the MirrorMaker2 mirror e2e test ({@code kind/mm2-mirror-test.sh}).
 *
 * <p>It exists because the Apicurio V3 wire envelope — {@code [0x00][globalId:8 bytes BE][payload]}
 * — is binary, and {@code kafka-console-producer.sh} / {@code kafka-console-consumer.sh} are
 * line/charset-oriented and corrupt arbitrary bytes. This class produces and consumes raw
 * {@code byte[]} records so the test can verify the SMT rewrote the envelope's globalId.
 *
 * <p>It ships inside the {@code schema-sync-smt} JAR (hence in the {@code mm2:dev} image),
 * so the test runs it in-cluster via {@code kafka-run-class.sh}, e.g.:
 * <pre>
 *   kafka-run-class.sh se.afshin.yavari.kafka.smt.Mm2MirrorProbe \
 *       produce --bootstrap localhost:9092 --topic mm2-orders --global-id 1 --count 5
 *   kafka-run-class.sh se.afshin.yavari.kafka.smt.Mm2MirrorProbe \
 *       consume --bootstrap kafka-proxy:9094 --topic source.mm2-orders --group probe \
 *       --timeout-ms 60000 --ssl --keystore ks.p12 --truststore ts.p12 --store-password changeit
 * </pre>
 */
public final class Mm2MirrorProbe {

    private Mm2MirrorProbe() {}

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.err.println("usage: Mm2MirrorProbe <produce|consume> [--flag value ...]");
            System.exit(2);
        }
        Map<String, String> a = parseArgs(args);
        switch (args[0]) {
            case "produce" -> produce(a);
            case "consume" -> consume(a);
            default -> {
                System.err.println("unknown mode: " + args[0]);
                System.exit(2);
            }
        }
    }

    private static void produce(Map<String, String> a) {
        String bootstrap = req(a, "bootstrap");
        String topic = req(a, "topic");
        long globalId = Long.parseLong(req(a, "global-id"));
        int count = Integer.parseInt(a.getOrDefault("count", "5"));
        String prefix = a.getOrDefault("prefix", "mm2-probe-");

        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        p.put(ProducerConfig.CLIENT_ID_CONFIG, "mm2-mirror-probe");

        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(p)) {
            for (int i = 0; i < count; i++) {
                byte[] value = envelope(globalId, prefix + i);
                producer.send(new ProducerRecord<>(topic, null, value));
            }
            producer.flush();
        }
        System.out.println("PRODUCED " + count + " topic=" + topic + " globalId=" + globalId);
    }

    private static void consume(Map<String, String> a) {
        String bootstrap = req(a, "bootstrap");
        String topic = req(a, "topic");
        String group = a.getOrDefault("group", "mm2-mirror-probe");
        long timeoutMs = Long.parseLong(a.getOrDefault("timeout-ms", "60000"));
        int expect = Integer.parseInt(a.getOrDefault("expect", "0"));

        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        p.put(ConsumerConfig.CLIENT_ID_CONFIG, "mm2-mirror-probe");
        if (a.containsKey("ssl")) {
            p.put("security.protocol", "SSL");
            p.put("ssl.keystore.type", "PKCS12");
            p.put("ssl.keystore.location", req(a, "keystore"));
            p.put("ssl.keystore.password", a.getOrDefault("store-password", "changeit"));
            p.put("ssl.key.password", a.getOrDefault("store-password", "changeit"));
            p.put("ssl.truststore.type", "PKCS12");
            p.put("ssl.truststore.location", req(a, "truststore"));
            p.put("ssl.truststore.password", a.getOrDefault("store-password", "changeit"));
            // The proxy/broker cert SANs vary by route — disable hostname verification,
            // matching the repo's other e2e clients.
            p.put("ssl.endpoint.identification.algorithm", "");
        }

        int consumed = 0;
        long deadline = System.currentTimeMillis() + timeoutMs;
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(p)) {
            consumer.subscribe(List.of(topic));
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<byte[], byte[]> r : records) {
                    byte[] v = r.value();
                    long gid = -1;
                    String payload;
                    if (v != null && v.length >= 9 && v[0] == 0x00) {
                        gid = ByteBuffer.wrap(v, 1, 8).getLong();
                        payload = new String(v, 9, v.length - 9, StandardCharsets.UTF_8);
                    } else {
                        payload = v == null ? "<null>" : new String(v, StandardCharsets.UTF_8);
                    }
                    System.out.println("RECORD gid=" + gid + " payload=" + payload);
                    consumed++;
                }
                if (expect > 0 && consumed >= expect) break;
            }
        }
        System.out.println("CONSUMED " + consumed + " topic=" + topic);
    }

    /** Apicurio V3 envelope: {@code [0x00][globalId:8 bytes BE][payload UTF-8]}. */
    private static byte[] envelope(long globalId, String payload) {
        byte[] data = payload.getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.allocate(1 + 8 + data.length)
                .put((byte) 0x00)
                .putLong(globalId)
                .put(data)
                .array();
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> m = new HashMap<>();
        for (int i = 1; i < args.length; i++) {
            if (!args[i].startsWith("--")) continue;
            String key = args[i].substring(2);
            if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
                m.put(key, args[++i]);
            } else {
                m.put(key, "true");   // boolean flag, e.g. --ssl
            }
        }
        return m;
    }

    private static String req(Map<String, String> a, String key) {
        String v = a.get(key);
        if (v == null) {
            System.err.println("missing required --" + key);
            System.exit(2);
        }
        return v;
    }
}
