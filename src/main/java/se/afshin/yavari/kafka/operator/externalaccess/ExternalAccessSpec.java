package se.afshin.yavari.kafka.operator.externalaccess;

import se.afshin.yavari.kafka.operator.crd.ExternalAccessType;

/**
 * Common shape of the {@code externalAccess} CRD sub-spec across resources that expose
 * themselves outside the cluster. Implemented both by the Kafka-proxy variant (TLS SNI
 * passthrough) and by the HTTP variant (KafkaUI, Apicurio, etc.).
 *
 * <p>The {@link se.afshin.yavari.kafka.operator.proxy.ExternalAccessResolver} only needs the
 * type and the advertised-host template to resolve a hostname for the local cluster — both
 * variants supply those, so the resolver can be shared without coupling to the proxy CRD.
 */
public interface ExternalAccessSpec {
    ExternalAccessType getType();
    String getAdvertisedHostTemplate();
}
