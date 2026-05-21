package se.afshin.yavari.kafka.ui.messages;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;
import se.afshin.yavari.kafka.ui.kafka.KafkaClientProvider;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Produces a single record per request. Auth is the logged-in user's JWT
 * forwarded via {@link KafkaClientProvider}; permission errors surface as
 * {@code AuthorizationException} from the broker.
 */
@ApplicationScoped
public class ProduceService {

    @Inject KafkaClientProvider clients;

    public record SendResult(String topic, int partition, long offset, long timestamp) {}

    public SendResult send(String clusterId, String topic, Integer partition,
                           String key, String value, Map<String, String> headers)
            throws ExecutionException, InterruptedException, TimeoutException {
        Producer<byte[], byte[]> producer = clients.producer(clusterId);
        byte[] keyBytes = key == null || key.isEmpty() ? null : key.getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = value == null ? null : value.getBytes(StandardCharsets.UTF_8);

        ProducerRecord<byte[], byte[]> record =
                new ProducerRecord<>(topic, partition, keyBytes, valueBytes);
        if (headers != null) {
            for (Map.Entry<String, String> e : headers.entrySet()) {
                if (e.getKey() == null || e.getKey().isBlank()) continue;
                byte[] hv = e.getValue() == null ? new byte[0] : e.getValue().getBytes(StandardCharsets.UTF_8);
                record.headers().add(new RecordHeader(e.getKey(), hv));
            }
        }

        RecordMetadata md = producer.send(record).get(10, TimeUnit.SECONDS);
        return new SendResult(md.topic(), md.partition(), md.offset(), md.timestamp());
    }
}
