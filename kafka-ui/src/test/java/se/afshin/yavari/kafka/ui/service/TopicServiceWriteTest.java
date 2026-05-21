package se.afshin.yavari.kafka.ui.service;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import se.afshin.yavari.kafka.ui.kafka.KafkaClientProvider;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TopicServiceWriteTest {

    private TopicService service;
    private AdminClient admin;

    @BeforeEach
    void setUp() {
        KafkaClientProvider clients = mock(KafkaClientProvider.class);
        admin = mock(AdminClient.class);
        when(clients.admin(any())).thenReturn(admin);
        service = new TopicService();
        service.clients = clients;
    }

    @Test
    void create_passesNewTopicToAdminClient() throws Exception {
        CreateTopicsResult result = mock(CreateTopicsResult.class);
        when(result.all()).thenReturn(completed());
        when(admin.createTopics(any())).thenReturn(result);

        service.create("c1", "orders", 6, (short) 3, Map.of("cleanup.policy", "compact"));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<java.util.Collection<NewTopic>> captor =
                ArgumentCaptor.forClass(java.util.Collection.class);
        verify(admin).createTopics(captor.capture());
        NewTopic created = captor.getValue().iterator().next();
        assertThat(created.name()).isEqualTo("orders");
        assertThat(created.numPartitions()).isEqualTo(6);
        assertThat(created.replicationFactor()).isEqualTo((short) 3);
        assertThat(created.configs()).containsEntry("cleanup.policy", "compact");
    }

    @Test
    void create_propagatesAuthorizationException() {
        CreateTopicsResult result = mock(CreateTopicsResult.class);
        when(result.all()).thenReturn(failed(new AuthorizationException("not allowed")));
        when(admin.createTopics(any())).thenReturn(result);

        assertThatThrownBy(() -> service.create("c1", "x", 1, (short) 1, Map.of()))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(AuthorizationException.class);
    }

    @Test
    void create_propagatesTopicExists() {
        CreateTopicsResult result = mock(CreateTopicsResult.class);
        when(result.all()).thenReturn(failed(new TopicExistsException("exists")));
        when(admin.createTopics(any())).thenReturn(result);

        assertThatThrownBy(() -> service.create("c1", "x", 1, (short) 1, Map.of()))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(TopicExistsException.class);
    }

    @Test
    void create_propagatesInvalidTopicException() {
        CreateTopicsResult result = mock(CreateTopicsResult.class);
        when(result.all()).thenReturn(failed(new InvalidTopicException("bad")));
        when(admin.createTopics(any())).thenReturn(result);

        assertThatThrownBy(() -> service.create("c1", "x", 1, (short) 1, Map.of()))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(InvalidTopicException.class);
    }

    @Test
    void alterConfigs_buildsSetAndDeleteOps_perValuePresence() throws Exception {
        AlterConfigsResult result = mock(AlterConfigsResult.class);
        when(result.all()).thenReturn(completed());
        when(admin.incrementalAlterConfigs(any())).thenReturn(result);

        Map<String, String> changes = new java.util.LinkedHashMap<>();
        changes.put("retention.ms", "604800000");
        changes.put("cleanup.policy", ""); // empty -> reset

        service.alterConfigs("c1", "orders", changes);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<ConfigResource, java.util.Collection<org.apache.kafka.clients.admin.AlterConfigOp>>> cap =
                ArgumentCaptor.forClass(Map.class);
        verify(admin).incrementalAlterConfigs(cap.capture());
        var entry = cap.getValue().entrySet().iterator().next();
        assertThat(entry.getKey().type()).isEqualTo(ConfigResource.Type.TOPIC);
        assertThat(entry.getKey().name()).isEqualTo("orders");

        var ops = entry.getValue().stream().toList();
        assertThat(ops).hasSize(2);
        assertThat(ops.get(0).configEntry().name()).isEqualTo("retention.ms");
        assertThat(ops.get(0).opType()).isEqualTo(org.apache.kafka.clients.admin.AlterConfigOp.OpType.SET);
        assertThat(ops.get(1).configEntry().name()).isEqualTo("cleanup.policy");
        assertThat(ops.get(1).opType()).isEqualTo(org.apache.kafka.clients.admin.AlterConfigOp.OpType.DELETE);
    }

    @Test
    void alterConfigs_noOp_whenChangesEmpty() throws Exception {
        service.alterConfigs("c1", "orders", Map.of());
        verify(admin, org.mockito.Mockito.never()).incrementalAlterConfigs(any());
    }

    @Test
    void delete_propagatesUnknownTopic_asExecutionException() {
        DeleteTopicsResult result = mock(DeleteTopicsResult.class);
        when(result.all()).thenReturn(failed(new UnknownTopicOrPartitionException("no such")));
        when(admin.deleteTopics(anyCollection())).thenReturn(result);

        assertThatThrownBy(() -> service.delete("c1", "gone"))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(UnknownTopicOrPartitionException.class);
    }

    @Test
    void delete_happyPath_callsAdminClient() throws Exception {
        DeleteTopicsResult result = mock(DeleteTopicsResult.class);
        when(result.all()).thenReturn(completed());
        when(admin.deleteTopics(anyCollection())).thenReturn(result);

        service.delete("c1", "orders");

        verify(admin).deleteTopics(anyCollection());
    }

    /* ---------- helpers ---------- */

    private static KafkaFuture<Void> completed() {
        KafkaFutureImpl<Void> f = new KafkaFutureImpl<>();
        f.complete(null);
        return f;
    }

    private static KafkaFuture<Void> failed(Throwable t) {
        KafkaFutureImpl<Void> f = new KafkaFutureImpl<>();
        f.completeExceptionally(t);
        return f;
    }
}
