package se.afshin.yavari.kafka.ui.service;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.DeleteConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthorizationException;
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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class GroupServiceWriteTest {

    private GroupService service;
    private AdminClient admin;

    @BeforeEach
    void setUp() {
        KafkaClientProvider clients = mock(KafkaClientProvider.class);
        admin = mock(AdminClient.class);
        when(clients.admin(any())).thenReturn(admin);
        service = new GroupService();
        service.clients = clients;
    }

    @Test
    void deleteGroup_callsAdminClient_andSucceeds() throws Exception {
        DeleteConsumerGroupsResult dr = mock(DeleteConsumerGroupsResult.class);
        when(dr.all()).thenReturn(completed());
        when(admin.deleteConsumerGroups(any())).thenReturn(dr);

        service.deleteGroup("c1", "my-group");

        verify(admin).deleteConsumerGroups(any());
    }

    @Test
    void deleteGroup_propagatesAuthorizationException() {
        DeleteConsumerGroupsResult dr = mock(DeleteConsumerGroupsResult.class);
        when(dr.all()).thenReturn(failed(new AuthorizationException("nope")));
        when(admin.deleteConsumerGroups(any())).thenReturn(dr);

        assertThatThrownBy(() -> service.deleteGroup("c1", "g"))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(AuthorizationException.class);
    }

    @Test
    void resetOffsets_latest_resolvesViaListOffsets() throws Exception {
        stubListOffsets(OffsetSpec.latest(), 100L);
        stubAlterCommit();

        long applied = service.resetOffsets("c1", "g", "topic-a", 2,
                GroupService.ResetTarget.LATEST, 0L);

        assertThat(applied).isEqualTo(100L);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> cap =
                ArgumentCaptor.forClass(Map.class);
        verify(admin).alterConsumerGroupOffsets(anyString(), cap.capture());
        var entry = cap.getValue().entrySet().iterator().next();
        assertThat(entry.getKey()).isEqualTo(new TopicPartition("topic-a", 2));
        assertThat(entry.getValue().offset()).isEqualTo(100L);
    }

    @Test
    void resetOffsets_earliest_resolvesViaListOffsets() throws Exception {
        stubListOffsets(OffsetSpec.earliest(), 5L);
        stubAlterCommit();

        long applied = service.resetOffsets("c1", "g", "t", 0,
                GroupService.ResetTarget.EARLIEST, 0L);

        assertThat(applied).isEqualTo(5L);
    }

    @Test
    void resetOffsets_offset_usesExplicitValue() throws Exception {
        stubAlterCommit();

        long applied = service.resetOffsets("c1", "g", "t", 0,
                GroupService.ResetTarget.OFFSET, 42L);

        assertThat(applied).isEqualTo(42L);
    }

    @Test
    void resetOffsets_offset_rejectsNegative() {
        assertThatThrownBy(() -> service.resetOffsets("c1", "g", "t", 0,
                GroupService.ResetTarget.OFFSET, -1L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /* helpers */

    private void stubListOffsets(OffsetSpec spec, long offset) {
        ListOffsetsResult res = mock(ListOffsetsResult.class);
        @SuppressWarnings("unchecked")
        KafkaFutureImpl<ListOffsetsResult.ListOffsetsResultInfo> f = new KafkaFutureImpl<>();
        f.complete(new ListOffsetsResult.ListOffsetsResultInfo(offset, 0L, java.util.Optional.empty()));
        when(res.partitionResult(any())).thenReturn(f);
        when(admin.listOffsets(any(Map.class))).thenReturn(res);
    }

    private void stubAlterCommit() {
        AlterConsumerGroupOffsetsResult ar = mock(AlterConsumerGroupOffsetsResult.class);
        when(ar.all()).thenReturn(completed());
        when(admin.alterConsumerGroupOffsets(anyString(), any())).thenReturn(ar);
    }

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
