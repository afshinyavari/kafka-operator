FROM registry.access.redhat.com/ubi9/openjdk-21-runtime:latest
WORKDIR /deployments
COPY target/quarkus-app/lib/ lib/
COPY target/quarkus-app/*.jar ./
COPY target/quarkus-app/app/ app/
COPY target/quarkus-app/quarkus/ quarkus/
EXPOSE 8080
# Avro 1.12 refuses to deserialize into a class it doesn't trust; Main sets this property too if unset,
# but it's set explicitly here so the behaviour is visible in the image and overridable at deploy time.
ENTRYPOINT ["java", "-Dorg.apache.avro.SERIALIZABLE_PACKAGES=se.afshin.yavari.clientapp.avro", "-jar", "quarkus-run.jar"]
