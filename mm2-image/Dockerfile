ARG KAFKA_BASE=kafka-ubi:4.0.0

FROM ${KAFKA_BASE}

# Apicurio schema-mirroring SMT — sits on /opt/kafka/libs so Kafka Connect's
# PluginClassLoader picks it up automatically (the SMT is on the plugin.path).
COPY target/schema-sync-smt-1.0-SNAPSHOT.jar /opt/kafka/libs/

USER 1000
