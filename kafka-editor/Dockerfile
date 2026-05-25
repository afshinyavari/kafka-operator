FROM registry.access.redhat.com/ubi9/openjdk-21-runtime:latest
WORKDIR /deployments
COPY backend/target/quarkus-app/lib/ lib/
COPY backend/target/quarkus-app/*.jar ./
COPY backend/target/quarkus-app/app/ app/
COPY backend/target/quarkus-app/quarkus/ quarkus/
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "quarkus-run.jar"]
