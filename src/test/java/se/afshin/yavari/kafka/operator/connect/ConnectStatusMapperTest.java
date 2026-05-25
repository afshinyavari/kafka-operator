package se.afshin.yavari.kafka.operator.connect;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.KafkaConnectorStatus;

import static org.assertj.core.api.Assertions.assertThat;

class ConnectStatusMapperTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private final ConnectStatusMapper sm = new ConnectStatusMapper();

    @Test
    void parsesRunningConnectorWithAllTasksRunning() throws Exception {
        String json = """
                {
                  "name": "file-src",
                  "connector": {"state": "RUNNING", "worker_id": "10.0.0.5:8083"},
                  "tasks": [
                    {"id": 0, "state": "RUNNING", "worker_id": "10.0.0.5:8083"},
                    {"id": 1, "state": "RUNNING", "worker_id": "10.0.0.6:8083"}
                  ]
                }
                """;
        JsonNode root = mapper.readTree(json);
        KafkaConnectorStatus s = new KafkaConnectorStatus();
        sm.map(root, s);

        assertThat(s.getConnectorState()).isEqualTo("RUNNING");
        assertThat(s.getWorkerId()).isEqualTo("10.0.0.5:8083");
        assertThat(s.getTasks()).hasSize(2);
        assertThat(s.getTasksTotal()).isEqualTo(2);
        assertThat(s.getTasksRunning()).isEqualTo("2/2");
    }

    @Test
    void capturesFailedTaskAndTrace() throws Exception {
        String json = """
                {
                  "connector": {"state": "RUNNING", "worker_id": "w1"},
                  "tasks": [
                    {"id": 0, "state": "FAILED", "worker_id": "w1", "trace": "java.lang.IllegalStateException: bad"}
                  ]
                }
                """;
        JsonNode root = mapper.readTree(json);
        KafkaConnectorStatus s = new KafkaConnectorStatus();
        sm.map(root, s);

        assertThat(s.getTasks()).hasSize(1);
        assertThat(s.getTasks().get(0).getState()).isEqualTo("FAILED");
        assertThat(s.getTasks().get(0).getTrace()).contains("IllegalStateException");
        assertThat(s.getTasksRunning()).isEqualTo("0/1");
    }

    @Test
    void truncatesLongTrace() throws Exception {
        String trace = "x".repeat(5_000);
        String json = "{\"tasks\":[{\"id\":0,\"state\":\"FAILED\",\"trace\":\"" + trace + "\"}]}";
        JsonNode root = mapper.readTree(json);
        KafkaConnectorStatus s = new KafkaConnectorStatus();
        sm.map(root, s);

        assertThat(s.getTasks().get(0).getTrace()).contains("(truncated)");
        assertThat(s.getTasks().get(0).getTrace().length()).isLessThan(2_500);
    }
}
