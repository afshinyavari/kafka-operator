# LinkedIn Cruise Control, compiled from source onto a Red Hat UBI base.
#
# Cruise Control publishes neither a container image nor a Maven Central artifact, so we
# clone and build it. Releases 2.5.145+ target Apache Kafka 4.0 (KRaft, no ZooKeeper) and
# dropped Java 11 support — hence a JDK 17 builder stage. Project policy: every image we
# build uses a UBI base.
#
# Cruise Control is BSD-2-Clause licensed (https://github.com/linkedin/cruise-control).
ARG CC_VERSION=2.5.146

# ---- builder: compile Cruise Control with Gradle on a UBI JDK-17 base -------
FROM registry.access.redhat.com/ubi9/openjdk-17:latest AS build
ARG CC_VERSION
USER root
RUN microdnf install -y git && microdnf clean all
WORKDIR /src
RUN git clone --depth 1 --branch "${CC_VERSION}" \
        https://github.com/linkedin/cruise-control.git .
# `jar` builds the server JAR; `copyDependantLibs` stages its full runtime classpath
# (including the cruise-control-core / metrics-reporter project JARs). Tests are skipped.
RUN ./gradlew --no-daemon -x test :cruise-control:jar :cruise-control:copyDependantLibs

# ---- runtime: UBI9 JDK-21 + the built classpath ----------------------------
FROM registry.access.redhat.com/ubi9/openjdk-21:latest
ARG CC_VERSION
# Prometheus JMX exporter version — kept in lock-step with kafka-image/Dockerfile so the
# operator's -javaagent wiring (path + version) is identical across both images.
ARG JMX_EXPORTER_VERSION=1.1.0
LABEL org.opencontainers.image.title="cruise-control" \
      org.opencontainers.image.description="LinkedIn Cruise Control compiled on Red Hat UBI" \
      org.opencontainers.image.source="https://github.com/linkedin/cruise-control" \
      org.opencontainers.image.licenses="BSD-2-Clause"
USER root
RUN mkdir -p /opt/cruise-control \
    && useradd -u 1000 -r -g 0 -s /sbin/nologin cruise-control
# Preserve the repo-relative layout the start script computes its classpath from.
COPY --from=build /src/kafka-cruise-control-start.sh        /opt/cruise-control/
COPY --from=build /src/cruise-control/build/libs            /opt/cruise-control/cruise-control/build/libs
COPY --from=build /src/cruise-control/build/dependant-libs  /opt/cruise-control/cruise-control/build/dependant-libs
COPY --from=build /src/config                              /opt/cruise-control/config
RUN chmod 0755 /opt/cruise-control/kafka-cruise-control-start.sh \
    && chown -R 1000:0 /opt/cruise-control \
    && chmod -R g=u /opt/cruise-control
# Prometheus JMX exporter agent. Cruise Control exposes metrics only via JMX; the operator
# attaches this jar as a -javaagent when KafkaCluster.spec.metricsConfig is set. Same jar
# name and path (/opt/jmx-exporter/jmx-exporter.jar) as the Kafka image.
RUN mkdir -p /opt/jmx-exporter \
    && curl -sSfL \
       https://github.com/prometheus/jmx_exporter/releases/download/${JMX_EXPORTER_VERSION}/jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar \
       -o /opt/jmx-exporter/jmx-exporter.jar \
    && chown -R 1000:0 /opt/jmx-exporter \
    && chmod -R g=u /opt/jmx-exporter
USER 1000
WORKDIR /opt/cruise-control
# The operator-rendered cruisecontrol.properties path is supplied as the container arg.
ENTRYPOINT ["/bin/bash", "/opt/cruise-control/kafka-cruise-control-start.sh"]
