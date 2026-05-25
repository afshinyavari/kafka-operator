# Kroxylicious 0.21.0 compiled from source onto a Red Hat UBI base, with our custom
# filters JAR overlaid on top. Mirrors the from-source UBI build pattern already used
# for Cruise Control (cruise-control-image/Dockerfile) — project policy is that every
# image we build uses a UBI base.
#
# Kroxylicious is Apache 2.0 licensed (https://github.com/kroxylicious/kroxylicious).
ARG KROXY_VERSION=0.21.0

# ---- builder: compile Kroxylicious with Maven on a UBI JDK-21 base ----------
FROM registry.access.redhat.com/ubi9/openjdk-21:latest AS build
ARG KROXY_VERSION
ARG MAVEN_VERSION=3.9.9
USER root
RUN microdnf install -y git tar gzip && microdnf clean all
# Kroxylicious doesn't bundle the maven-wrapper (./mvnw is .gitignored), so install
# Apache Maven directly from the Apache archive. The ubi9 repos don't ship a recent
# enough Maven to skip this.
RUN curl -sSfL \
        https://archive.apache.org/dist/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz \
        -o /tmp/maven.tar.gz \
    && tar -xzf /tmp/maven.tar.gz -C /opt \
    && ln -s /opt/apache-maven-${MAVEN_VERSION}/bin/mvn /usr/local/bin/mvn \
    && rm /tmp/maven.tar.gz
WORKDIR /src
RUN git clone --depth 1 --branch "v${KROXY_VERSION}" \
        https://github.com/kroxylicious/kroxylicious.git .
# Skip tests + javadoc + format checks to keep the build under a couple of minutes.
# The kroxylicious-app module produces the runnable distribution tarball.
# The `dist` profile activates the maven-assembly-plugin execution that produces the
# kroxylicious-app-${KROXY_VERSION}-bin.tar.gz distribution. Without it `package` only
# builds the bare JAR + target/libs/.
# `-Pdist` activates the binary-distribution assembly but also a fabric8 docker plugin
# bound to `package`; the kroxylicious build exposes `-DskipContainerImageBuild=true` to
# disable just the docker-image step while keeping the assembly tarball.
RUN mvn --no-transfer-progress -B -DskipTests -Dquickly -Pdist \
        -DskipContainerImageBuild=true \
        -pl kroxylicious-app -am package

# ---- runtime: UBI9 JDK-21 + the unpacked distribution ----------------------
FROM registry.access.redhat.com/ubi9/openjdk-21:latest
ARG KROXY_VERSION
LABEL org.opencontainers.image.title="kroxylicious" \
      org.opencontainers.image.description="Kroxylicious compiled on Red Hat UBI + custom filters" \
      org.opencontainers.image.source="https://github.com/kroxylicious/kroxylicious" \
      org.opencontainers.image.licenses="Apache-2.0"
USER root
# UID 185 already exists in ubi9/openjdk-21 (jboss user). We reuse it for the runtime
# user rather than allocating a new one, so file ownership stays consistent with the
# base image's expectations.
RUN microdnf install -y tar gzip && microdnf clean all \
    && mkdir -p /opt/kroxylicious /opt/filters
# The kroxylicious-app build produces .tar.gz and .zip distributions; pick the tar.
COPY --from=build \
    /src/kroxylicious-app/target/kroxylicious-app-${KROXY_VERSION}-bin.tar.gz \
    /tmp/kroxylicious.tar.gz
RUN tar -xzf /tmp/kroxylicious.tar.gz -C /opt/kroxylicious --strip-components=1 \
    && rm /tmp/kroxylicious.tar.gz \
    && chown -R 185:0 /opt/kroxylicious /opt/filters \
    && chmod -R g=u /opt/kroxylicious /opt/filters

# Custom filters JAR — operator-built. Loaded onto Kroxylicious' classpath via the
# KROXYLICIOUS_CLASSPATH env below.
COPY target/kroxy-filters-1.0-SNAPSHOT.jar /opt/filters/

# Override the upstream log4j2.yaml so custom-filter and audit channel logs are visible.
# The upstream config only whitelists `io.kroxylicious` at INFO; everything else inherits
# root=WARN, which hides `se.afshin.yavari.*` and the `kafka-audit` channel by default.
COPY log4j2.yaml /opt/kroxylicious/config/log4j2.yaml

USER 185
ENV KROXYLICIOUS_CLASSPATH=/opt/filters/*
ENTRYPOINT ["/opt/kroxylicious/bin/kroxylicious-start.sh"]
CMD ["--config", "/opt/kroxylicious/config/config.yaml"]
