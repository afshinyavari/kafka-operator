package se.afshin.yavari.kafka.operator.acl;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateAclsResult;
import org.apache.kafka.clients.admin.DeleteAclsResult;
import org.apache.kafka.clients.admin.DescribeAclsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import se.afshin.yavari.kafka.operator.topic.AdminClientTlsLoader;
import se.afshin.yavari.kafka.operator.topic.BrokerBootstrapResolver;
import se.afshin.yavari.kafka.operator.topic.KafkaTopicService;

import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaAclManagerTest {

    private static final String NS = "kafka";
    private static final String CLUSTER = "my-cluster";
    private static final String ADMIN_SECRET = "kafka-operator-client-tls";
    private static final String PRINCIPAL = "User:CN=apicurio-registry";

    private AdminClient admin;
    private KafkaAclManager manager;

    @BeforeEach
    void setup() throws Exception {
        admin = mock(AdminClient.class);
        BrokerBootstrapResolver bootstrap = mock(BrokerBootstrapResolver.class);
        AdminClientTlsLoader tls = mock(AdminClientTlsLoader.class);
        KafkaTopicService svc = mock(KafkaTopicService.class);
        when(bootstrap.resolve(anyString(), anyString())).thenReturn("b:9092");
        when(tls.loadAsAdminClientSslProps(anyString(), anyString())).thenReturn(new Properties());
        when(svc.newAdmin(anyString(), any())).thenReturn(admin);

        manager = new KafkaAclManager();
        inject("bootstrapResolver", bootstrap);
        inject("tlsLoader", tls);
        inject("adminFactory", svc);
    }

    @Test
    void apply_createsMissingBindings() throws Exception {
        AclBinding desired = topicAcl(PRINCIPAL, "journal", AclOperation.WRITE);
        mockDescribe(Set.of());          // no existing ACLs
        mockCreateSuccess();

        manager.apply(CLUSTER, NS, ADMIN_SECRET, List.of(desired));

        ArgumentCaptor<Collection<AclBinding>> captor = ArgumentCaptor.forClass(Collection.class);
        verify(admin).createAcls(captor.capture());
        assertThat(captor.getValue()).containsExactly(desired);
    }

    @Test
    void apply_isIdempotent_whenBindingAlreadyExists() throws Exception {
        AclBinding desired = topicAcl(PRINCIPAL, "journal", AclOperation.WRITE);
        mockDescribe(Set.of(desired));   // exact match already exists

        manager.apply(CLUSTER, NS, ADMIN_SECRET, List.of(desired));

        verify(admin, never()).createAcls(any());
    }

    @Test
    void apply_createsOnlyMissingSubset() throws Exception {
        AclBinding a = topicAcl(PRINCIPAL, "journal", AclOperation.WRITE);
        AclBinding b = topicAcl(PRINCIPAL, "journal", AclOperation.READ);
        mockDescribe(Set.of(a));         // only WRITE exists
        mockCreateSuccess();

        manager.apply(CLUSTER, NS, ADMIN_SECRET, List.of(a, b));

        ArgumentCaptor<Collection<AclBinding>> captor = ArgumentCaptor.forClass(Collection.class);
        verify(admin).createAcls(captor.capture());
        assertThat(captor.getValue()).containsExactly(b);
    }

    @Test
    void apply_emptyDesiredSkipsAdminClient() {
        manager.apply(CLUSTER, NS, ADMIN_SECRET, List.of());
        verify(admin, never()).describeAcls(any(AclBindingFilter.class));
        verify(admin, never()).createAcls(any());
    }

    @Test
    @SuppressWarnings("unchecked")
    void delete_deletesByPrincipal() throws Exception {
        DeleteAclsResult result = mock(DeleteAclsResult.class);
        KafkaFuture<Collection<AclBinding>> fut = KafkaFuture.completedFuture(List.of());
        when(result.all()).thenReturn(fut);
        when(admin.deleteAcls(any(List.class))).thenReturn(result);

        manager.delete(CLUSTER, NS, ADMIN_SECRET, PRINCIPAL);

        ArgumentCaptor<List<AclBindingFilter>> captor = ArgumentCaptor.forClass(List.class);
        verify(admin).deleteAcls(captor.capture());
        assertThat(captor.getValue()).hasSize(1);
        AclBindingFilter filter = captor.getValue().get(0);
        assertThat(filter.entryFilter().principal()).isEqualTo(PRINCIPAL);
    }

    @Test
    void apply_wrapsExecutionException() throws Exception {
        AclBinding desired = topicAcl(PRINCIPAL, "journal", AclOperation.WRITE);
        mockDescribe(Set.of());

        CreateAclsResult result = mock(CreateAclsResult.class);
        KafkaFuture<Void> fut = mock(KafkaFuture.class);
        when(fut.get(anyLong(), any())).thenThrow(new java.util.concurrent.ExecutionException(new RuntimeException("boom")));
        when(result.all()).thenReturn(fut);
        when(admin.createAcls(any())).thenReturn(result);

        assertThatThrownBy(() -> manager.apply(CLUSTER, NS, ADMIN_SECRET, List.of(desired)))
                .isInstanceOf(KafkaAclManager.AclProvisioningException.class)
                .hasMessageContaining("Failed to apply ACLs");
    }

    private void mockDescribe(Set<AclBinding> existing) throws Exception {
        DescribeAclsResult result = mock(DescribeAclsResult.class);
        KafkaFuture<Collection<AclBinding>> fut = KafkaFuture.completedFuture(existing);
        when(result.values()).thenReturn(fut);
        when(admin.describeAcls(any(AclBindingFilter.class))).thenReturn(result);
    }

    private void mockCreateSuccess() {
        CreateAclsResult result = mock(CreateAclsResult.class);
        KafkaFuture<Void> fut = KafkaFuture.completedFuture(null);
        when(result.all()).thenReturn(fut);
        when(admin.createAcls(any())).thenReturn(result);
    }

    private static AclBinding topicAcl(String principal, String topic, AclOperation op) {
        return new AclBinding(
                new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL),
                new AccessControlEntry(principal, "*", op, AclPermissionType.ALLOW));
    }

    private void inject(String fieldName, Object value) {
        try {
            var f = KafkaAclManager.class.getDeclaredField(fieldName);
            f.setAccessible(true);
            f.set(manager, value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
