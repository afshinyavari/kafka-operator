ARG KAFKA_BASE=kafka-ubi:4.0.0

FROM ${KAFKA_BASE}

# Operator-shipped plugin landing zone. The reconciler always appends this dir to
# plugin.path, so a user-supplied custom image can drop JARs here without changing
# the operator config.
USER root
RUN mkdir -p /opt/kafka/connect-plugins/baked \
    && chown -R 1000:0 /opt/kafka/connect-plugins \
    && chmod -R g=u /opt/kafka/connect-plugins

# Expose Kafka's bundled FileStream connector as a plugin. In distributed mode Connect
# only discovers connectors via plugin.path (PluginClassLoader isolation), and connect-file
# lives on the system classpath. The smoke test + simple file-based examples need it.
RUN cp /opt/kafka/libs/connect-file-*.jar /opt/kafka/connect-plugins/baked/ \
    && chown 1000:0 /opt/kafka/connect-plugins/baked/connect-file-*.jar

USER 1000
