FROM eclipse-temurin:21-jre-alpine
WORKDIR /deployment
COPY target/quarkus-app/lib/ lib/
COPY target/quarkus-app/*.jar ./
COPY target/quarkus-app/app/ app/
COPY target/quarkus-app/quarkus/ quarkus/
COPY policy.yaml /opt/rbac/policy.yaml
EXPOSE 8082
ENTRYPOINT ["java", "-jar", "quarkus-run.jar"]
