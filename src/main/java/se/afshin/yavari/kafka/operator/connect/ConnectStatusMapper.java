package se.afshin.yavari.kafka.operator.connect;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.enterprise.context.ApplicationScoped;
import se.afshin.yavari.kafka.operator.crd.KafkaConnectorStatus;
import se.afshin.yavari.kafka.operator.crd.KafkaConnectorTaskStatus;

import java.util.ArrayList;
import java.util.List;

/** Pure function: parse Connect's {@code /connectors/<name>/status} response into the
 *  shape we surface on {@code KafkaConnectorStatus}. */
@ApplicationScoped
public class ConnectStatusMapper {

    private static final int MAX_TRACE_BYTES = 2_048;

    public void map(JsonNode root, KafkaConnectorStatus status) {
        JsonNode connector = root.path("connector");
        if (!connector.isMissingNode()) {
            status.setConnectorState(connector.path("state").asText(null));
            status.setWorkerId(connector.path("worker_id").asText(null));
        }
        JsonNode tasks = root.path("tasks");
        List<KafkaConnectorTaskStatus> out = new ArrayList<>();
        int running = 0;
        if (tasks.isArray()) {
            for (JsonNode t : tasks) {
                int id = t.path("id").asInt();
                String state = t.path("state").asText(null);
                String workerId = t.path("worker_id").asText(null);
                String trace = t.path("trace").asText(null);
                if (trace != null && trace.length() > MAX_TRACE_BYTES) {
                    trace = trace.substring(0, MAX_TRACE_BYTES) + "...(truncated)";
                }
                if ("RUNNING".equals(state)) running++;
                out.add(new KafkaConnectorTaskStatus(id, state, workerId, trace));
            }
        }
        status.setTasks(out);
        status.setTasksTotal(out.size());
        status.setTasksRunning(running + "/" + out.size());
    }
}
