ARG KAFKA_VERSION=4.0.0
ARG JMX_EXPORTER_VERSION=1.1.0
ARG CC_VERSION=2.5.146

FROM apache/kafka:${KAFKA_VERSION} AS kafka-source

# Build the Cruise Control Metrics Reporter from source. It runs inside the broker JVM
# and publishes broker/partition metrics to __CruiseControlMetrics for Cruise Control to
# sample. Cruise Control ships no Maven Central artifact, so we compile the one subproject
# we need (releases 2.5.145+ dropped Java 11 — hence a JDK 17 builder).
FROM registry.access.redhat.com/ubi9/openjdk-17:latest AS cc-reporter
ARG CC_VERSION
USER root
RUN microdnf install -y git && microdnf clean all
WORKDIR /src
RUN git clone --depth 1 --branch "${CC_VERSION}" \
        https://github.com/linkedin/cruise-control.git .
RUN ./gradlew --no-daemon -x test :cruise-control-metrics-reporter:jar

FROM registry.access.redhat.com/ubi9/openjdk-21:latest

ARG JMX_EXPORTER_VERSION

USER root

RUN microdnf install -y openssl && microdnf clean all

RUN mkdir -p /opt/jmx-exporter /var/lib/kafka/data && \
    useradd -u 1000 -r -g 0 -s /sbin/nologin kafka

COPY --from=kafka-source /opt/kafka /opt/kafka

# Cruise Control Metrics Reporter onto the broker classpath. Inert unless the operator
# sets metric.reporters — it only does so when KafkaCluster.spec.cruiseControl is enabled.
COPY --from=cc-reporter \
    /src/cruise-control-metrics-reporter/build/libs/cruise-control-metrics-reporter-*.jar \
    /opt/kafka/libs/cruise-control-metrics-reporter.jar

RUN curl -sSfL \
    https://github.com/prometheus/jmx_exporter/releases/download/${JMX_EXPORTER_VERSION}/jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar \
    -o /opt/jmx-exporter/jmx-exporter.jar && \
    chown -R 1000:0 /opt/kafka /var/lib/kafka /opt/jmx-exporter && \
    chmod -R g=u /opt/kafka /var/lib/kafka /opt/jmx-exporter

ENV PATH="$PATH:/opt/kafka/bin"

USER 1000
