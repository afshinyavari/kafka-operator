package se.afshin.yavari.clientapp.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.clientapp.config.ConsumerConfig;
import se.afshin.yavari.clientapp.config.Format;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ConsumerRunnerTest {

    private static final ConsumerConfig CFG = new ConsumerConfig(true, "orders", "g", "earliest", Format.STRING, null);

    private static MockConsumer<String, Object> mockWithAssignment() {
        MockConsumer<String, Object> mock = new MockConsumer<>("earliest");
        TopicPartition tp = new TopicPartition("orders", 0);
        mock.subscribe(List.of("orders"));
        mock.rebalance(List.of(tp));
        mock.updateBeginningOffsets(Map.of(tp, 0L));
        return mock;
    }

    @Test
    void pollOnceLogsAndCountsRecords() {
        MockConsumer<String, Object> mock = mockWithAssignment();
        ConsumerRunner r = new ConsumerRunner(CFG, mock);
        mock.addRecord(new ConsumerRecord<>("orders", 0, 0L, "k1", "{\"sequence\":0}"));
        mock.addRecord(new ConsumerRecord<>("orders", 0, 1L, "k2", "{\"sequence\":1}"));
        assertEquals(2, r.pollOnce());
        assertEquals(2, r.received());
    }

    @Test
    void pollOnceSwallowsErrors() {
        MockConsumer<String, Object> mock = mockWithAssignment();
        ConsumerRunner r = new ConsumerRunner(CFG, mock);
        mock.setPollException(new org.apache.kafka.common.KafkaException("broker gone"));
        assertDoesNotThrow(r::pollOnce);
        assertEquals(0, r.received());
    }

    @Test
    void startSubscribesAndStopClosesConsumer() throws Exception {
        MockConsumer<String, Object> mock = new MockConsumer<>("earliest");
        ConsumerRunner r = new ConsumerRunner(CFG, mock);
        assertFalse(r.isStarted());
        r.start();
        assertTrue(r.isStarted());
        assertEquals(java.util.Set.of("orders"), mock.subscription());
        Thread.sleep(50);
        r.stop();
        assertTrue(mock.closed());
        assertFalse(r.isStarted());
    }
}
