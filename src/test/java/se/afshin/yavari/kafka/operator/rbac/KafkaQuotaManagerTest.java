package se.afshin.yavari.kafka.operator.rbac;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterClientQuotasOptions;
import org.apache.kafka.clients.admin.AlterClientQuotasResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import se.afshin.yavari.kafka.operator.crd.KafkaQuotaConfig;
import se.afshin.yavari.kafka.operator.crd.KafkaRbacUser;

import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaQuotaManagerTest {

    @Test
    void noUsersWithQuotas_doesNotCallAdminClient() {
        AdminClient admin = mock(AdminClient.class);
        KafkaRbacUser u = user("alice", null);

        List<String> errors = new KafkaQuotaManager().applyUserQuotas(admin, List.of(u));

        assertThat(errors).isEmpty();
        verify(admin, never()).alterClientQuotas(any(), any());
    }

    @Test
    void usersWithQuotas_buildsAlterationsForEachUser() throws Exception {
        AdminClient admin = mock(AdminClient.class);
        AlterClientQuotasResult result = mock(AlterClientQuotasResult.class);
        KafkaFuture<Void> done = KafkaFuture.completedFuture(null);
        when(result.all()).thenReturn(done);
        ArgumentCaptor<Collection<ClientQuotaAlteration>> captor =
                ArgumentCaptor.forClass(Collection.class);
        when(admin.alterClientQuotas(captor.capture(), any(AlterClientQuotasOptions.class)))
                .thenReturn(result);

        KafkaQuotaConfig q = new KafkaQuotaConfig();
        q.setProducerByteRate(1_048_576L);
        q.setConsumerByteRate(2_097_152L);
        q.setRequestPercentage(0.5);
        q.setControllerMutationRate(10.0);
        KafkaRbacUser u = user("alice", q);

        List<String> errors = new KafkaQuotaManager().applyUserQuotas(admin, List.of(u));

        assertThat(errors).isEmpty();
        List<ClientQuotaAlteration> alts = List.copyOf(captor.getValue());
        assertThat(alts).hasSize(1);
        ClientQuotaAlteration alt = alts.get(0);
        assertThat(alt.entity().entries())
                .containsEntry(ClientQuotaEntity.USER, "alice");
        assertThat(alt.ops()).extracting(ClientQuotaAlteration.Op::key)
                .containsExactlyInAnyOrder(
                        "producer_byte_rate", "consumer_byte_rate",
                        "request_percentage", "controller_mutation_rate");
        assertThat(alt.ops()).extracting(ClientQuotaAlteration.Op::value)
                .containsExactlyInAnyOrder(1_048_576.0, 2_097_152.0, 0.5, 10.0);
    }

    @Test
    void partialQuotas_emitsOnlyPopulatedFields() throws Exception {
        AdminClient admin = mock(AdminClient.class);
        AlterClientQuotasResult result = mock(AlterClientQuotasResult.class);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(null));
        ArgumentCaptor<Collection<ClientQuotaAlteration>> captor =
                ArgumentCaptor.forClass(Collection.class);
        when(admin.alterClientQuotas(captor.capture(), any(AlterClientQuotasOptions.class)))
                .thenReturn(result);

        KafkaQuotaConfig q = new KafkaQuotaConfig();
        q.setProducerByteRate(1_000_000L);
        KafkaRbacUser u = user("bob", q);

        new KafkaQuotaManager().applyUserQuotas(admin, List.of(u));

        ClientQuotaAlteration alt = List.copyOf(captor.getValue()).get(0);
        assertThat(alt.ops()).hasSize(1);
        assertThat(alt.ops().iterator().next().key()).isEqualTo("producer_byte_rate");
    }

    private KafkaRbacUser user(String name, KafkaQuotaConfig quotas) {
        KafkaRbacUser u = new KafkaRbacUser();
        u.setName(name);
        u.setQuotas(quotas);
        return u;
    }
}
