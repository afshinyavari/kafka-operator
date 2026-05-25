FROM quay.io/kroxylicious/kroxylicious:0.21.0

COPY target/kroxy-filters-1.0-SNAPSHOT.jar /opt/filters/

# Override the upstream log4j2.yaml so custom-filter and audit channel logs are visible.
# The upstream config only whitelists `io.kroxylicious` at INFO; everything else inherits
# root=WARN, which hides `se.afshin.yavari.*` and the `kafka-audit` channel by default.
USER 0
COPY log4j2.yaml /opt/kroxylicious/config/log4j2.yaml
USER 185

ENV KROXYLICIOUS_CLASSPATH=/opt/filters/*

CMD ["--config", "/opt/kroxylicious/config/config.yaml"]
