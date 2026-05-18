FROM quay.io/kroxylicious/kroxylicious:0.21.0

COPY target/kroxy-filters-1.0-SNAPSHOT.jar /opt/filters/

ENV KROXYLICIOUS_CLASSPATH=/opt/filters/*

CMD ["--config", "/opt/kroxylicious/config/config.yaml"]
