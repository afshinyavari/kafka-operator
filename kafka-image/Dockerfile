ARG KAFKA_VERSION=4.0.0
ARG JMX_EXPORTER_VERSION=1.1.0

FROM apache/kafka:${KAFKA_VERSION} AS kafka-source

FROM registry.access.redhat.com/ubi9/openjdk-21:latest

ARG JMX_EXPORTER_VERSION

USER root

RUN mkdir -p /opt/jmx-exporter /var/lib/kafka/data && \
    useradd -u 1000 -r -g 0 -s /sbin/nologin kafka

COPY --from=kafka-source /opt/kafka /opt/kafka

RUN curl -sSfL \
    https://github.com/prometheus/jmx_exporter/releases/download/${JMX_EXPORTER_VERSION}/jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar \
    -o /opt/jmx-exporter/jmx-exporter.jar && \
    chown -R 1000:0 /opt/kafka /var/lib/kafka /opt/jmx-exporter && \
    chmod -R g=u /opt/kafka /var/lib/kafka /opt/jmx-exporter

ENV PATH="$PATH:/opt/kafka/bin"

USER 1000
