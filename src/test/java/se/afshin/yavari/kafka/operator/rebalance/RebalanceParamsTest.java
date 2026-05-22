package se.afshin.yavari.kafka.operator.rebalance;

import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.KafkaRebalanceMode;
import se.afshin.yavari.kafka.operator.crd.KafkaRebalanceSpec;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class RebalanceParamsTest {

    @Test
    void fullModeMapsToRebalanceEndpoint() {
        KafkaRebalanceSpec spec = new KafkaRebalanceSpec();
        spec.setMode(KafkaRebalanceMode.FULL);
        RebalanceParams p = RebalanceParams.from(spec);

        assertThat(p.endpointPath()).isEqualTo("/kafkacruisecontrol/rebalance");
        assertThat(p.queryString(true)).isEqualTo("dryrun=true&json=true");
    }

    @Test
    void addBrokersMapsToAddBrokerEndpointWithBrokerIds() {
        KafkaRebalanceSpec spec = new KafkaRebalanceSpec();
        spec.setMode(KafkaRebalanceMode.ADD_BROKERS);
        spec.setBrokers(List.of(3, 4));
        RebalanceParams p = RebalanceParams.from(spec);

        assertThat(p.endpointPath()).isEqualTo("/kafkacruisecontrol/add_broker");
        assertThat(p.queryString(false)).contains("dryrun=false").contains("brokerid=3,4");
    }

    @Test
    void removeBrokersMapsToRemoveBrokerEndpoint() {
        KafkaRebalanceSpec spec = new KafkaRebalanceSpec();
        spec.setMode(KafkaRebalanceMode.REMOVE_BROKERS);
        spec.setBrokers(List.of(7));
        assertThat(RebalanceParams.from(spec).endpointPath())
                .isEqualTo("/kafkacruisecontrol/remove_broker");
    }

    @Test
    void optionalParametersAreRendered() {
        KafkaRebalanceSpec spec = new KafkaRebalanceSpec();
        spec.setMode(KafkaRebalanceMode.FULL);
        spec.setGoals(List.of("GoalA", "GoalB"));
        spec.setSkipHardGoalCheck(true);
        spec.setRebalanceDisk(true);
        spec.setConcurrentLeaderMovements(5);
        spec.setReplicationThrottle(1048576L);
        spec.setExcludedTopics("internal-.*");

        String q = RebalanceParams.from(spec).queryString(true);

        assertThat(q).contains("goals=GoalA,GoalB");
        assertThat(q).contains("skip_hard_goal_check=true");
        assertThat(q).contains("rebalance_disk=true");
        assertThat(q).contains("concurrent_leader_movements=5");
        assertThat(q).contains("replication_throttle=1048576");
        assertThat(q).contains("excluded_topics=internal-");
    }

    @Test
    void rebalanceDiskIsIgnoredForBrokerModes() {
        KafkaRebalanceSpec spec = new KafkaRebalanceSpec();
        spec.setMode(KafkaRebalanceMode.ADD_BROKERS);
        spec.setBrokers(List.of(1));
        spec.setRebalanceDisk(true);
        assertThat(RebalanceParams.from(spec).queryString(false)).doesNotContain("rebalance_disk");
    }

    @Test
    void queryStringIsDeterministic() {
        KafkaRebalanceSpec spec = new KafkaRebalanceSpec();
        spec.setMode(KafkaRebalanceMode.FULL);
        spec.setGoals(List.of("GoalA"));
        RebalanceParams p = RebalanceParams.from(spec);
        // Re-issued requests carrying a User-Task-ID must match byte-for-byte.
        assertThat(p.queryString(true)).isEqualTo(p.queryString(true));
    }
}
