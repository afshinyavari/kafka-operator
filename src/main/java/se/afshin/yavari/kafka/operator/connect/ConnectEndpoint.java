package se.afshin.yavari.kafka.operator.connect;

/** Resolved REST endpoint of a parent {@link se.afshin.yavari.kafka.operator.crd.KafkaConnect}
 *  cluster. */
public record ConnectEndpoint(
        String baseUrl,
        String tlsSecretRef,
        String authSecretRef,
        boolean ready,
        String message
) {}
