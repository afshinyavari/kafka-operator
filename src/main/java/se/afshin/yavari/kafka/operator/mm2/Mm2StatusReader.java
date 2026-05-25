package se.afshin.yavari.kafka.operator.mm2;

import jakarta.enterprise.context.ApplicationScoped;
import se.afshin.yavari.kafka.operator.crd.MirrorMaker2;
import se.afshin.yavari.kafka.operator.crd.MirrorMaker2Status;
import se.afshin.yavari.kafka.operator.endpoint.ResolvedKafkaEndpoint;

import java.util.List;

/**
 * Per-connector telemetry reader for the {@code mm2-status.{flow}} topic on the target.
 *
 * <p>v1 placeholder: returns an empty connector list. The dedicated-mode worker emits
 * status records as Connect JSON; the full implementation will use an AdminClient +
 * KafkaConsumer against the target proxy bootstrap (reusing
 * {@code AdminClientTlsLoader}) and parse the records into {@link MirrorMaker2Status.ConnectorStatus}.
 *
 * <p>TODO(distributed-mode): once a future {@code spec.connectClusterRef} ships, replace
 * this with REST {@code /connectors/{n}/status} polling against the Connect cluster.
 */
@ApplicationScoped
public class Mm2StatusReader {

    public List<MirrorMaker2Status.ConnectorStatus> read(MirrorMaker2 cr, ResolvedKafkaEndpoint target) {
        return List.of();
    }
}
