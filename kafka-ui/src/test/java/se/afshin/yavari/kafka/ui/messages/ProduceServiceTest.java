package se.afshin.yavari.kafka.ui.messages;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import se.afshin.yavari.kafka.ui.kafka.KafkaClientProvider;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ProduceServiceTest {

    private ProduceService service;
    private Producer<byte[], byte[]> producer;

    @BeforeEach
    void setUp() {
        KafkaClientProvider clients = mock(KafkaClientProvider.class);
        @SuppressWarnings("unchecked")
        Producer<byte[], byte[]> p = (Producer<byte[], byte[]>) mock(Producer.class);
        producer = p;
        when(clients.producer(any())).thenReturn(producer);
        service = new ProduceService();
        service.clients = clients;
    }

    @Test
    void send_setsKeyAndValueAndHeaders() throws Exception {
        stubAck("orders", 2, 42L);

        ProduceService.SendResult r = service.send("c1", "orders", null,
                "user-7", "hello", Map.of("ce-type", "test"));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<ProducerRecord<byte[], byte[]>> cap = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(producer).send(cap.capture());
        ProducerRecord<byte[], byte[]> sent = cap.getValue();
        assertThat(sent.topic()).isEqualTo("orders");
        assertThat(new String(sent.key())).isEqualTo("user-7");
        assertThat(new String(sent.value())).isEqualTo("hello");
        assertThat(sent.headers().lastHeader("ce-type")).isNotNull();
        assertThat(new String(sent.headers().lastHeader("ce-type").value())).isEqualTo("test");

        assertThat(r.partition()).isEqualTo(2);
        assertThat(r.offset()).isEqualTo(42L);
    }

    @Test
    void send_explicitPartition_isHonoured() throws Exception {
        stubAck("orders", 1, 0L);
        service.send("c1", "orders", 1, null, "v", null);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<ProducerRecord<byte[], byte[]>> cap = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(producer).send(cap.capture());
        assertThat(cap.getValue().partition()).isEqualTo(1);
    }

    @Test
    void send_nullKey_isAllowedAndYieldsNullBytes() throws Exception {
        stubAck("orders", 0, 0L);
        service.send("c1", "orders", null, null, "v", null);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<ProducerRecord<byte[], byte[]>> cap = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(producer).send(cap.capture());
        assertThat(cap.getValue().key()).isNull();
    }

    /* helpers */

    private void stubAck(String topic, int partition, long offset) {
        RecordMetadata md = new RecordMetadata(
                new TopicPartition(topic, partition),
                offset, 0, System.currentTimeMillis(), 0, 0);
        CompletableFuture<RecordMetadata> done = CompletableFuture.completedFuture(md);
        // Producer.send returns Future<RecordMetadata>; CompletableFuture suffices.
        when(producer.send(any())).thenAnswer(invocation -> done);
    }
}
