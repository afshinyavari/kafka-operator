# Strimzi MirrorMaker2 image with the schema-sync SMT baked in.
#
# Build (from the schema-sync-smt/ directory):
#   mvn -q package -DskipTests
#   docker build -f strimzi/Dockerfile -t <registry>/strimzi-kafka-schema-sync:0.47.0-kafka-4.0.0 .
#
# Pick STRIMZI_IMAGE to match the Strimzi operator version you run; the Kafka
# version in the tag must be one that operator supports. The JAR targets Java 17,
# which all current Strimzi images ship.
ARG STRIMZI_IMAGE=quay.io/strimzi/kafka:0.47.0-kafka-4.0.0

FROM ${STRIMZI_IMAGE}

# Strimzi's Connect/MM2 workers use /opt/kafka/plugins as plugin.path. Each plugin
# lives in its own sub-directory so Connect's PluginClassLoader isolates it.
USER root
COPY target/schema-sync-smt-1.0-SNAPSHOT.jar /opt/kafka/plugins/schema-sync-smt/
RUN chown -R 1001:0 /opt/kafka/plugins/schema-sync-smt && chmod -R g+rX /opt/kafka/plugins/schema-sync-smt
USER 1001
