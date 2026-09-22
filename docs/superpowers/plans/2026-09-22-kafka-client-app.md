# kafka-client-app Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A standalone Quarkus app in `test-clients/kafka-client-app` that produces and/or consumes Kafka records (JSON string or Avro via Apicurio or Confluent registry), configured only through environment variables, with PKCS12/JKS TLS everywhere.

**Architecture:** Plain `kafka-clients` `KafkaProducer`/`KafkaConsumer` built from `Properties` that pure, unit-tested config classes derive from env. Registry serdes are configured by property name only (Apicurio serde 3.3.3 and Confluent 8.3.2 both support keystore/truststore config natively). A `@QuarkusMain` validates config, starts runner threads, and exposes readiness.

**Tech Stack:** Java 21, Quarkus 3.39.4 (`quarkus-kafka-client`, `quarkus-smallrye-health`), `kafka-clients` 4.2.1 (BOM-managed), `io.apicurio:apicurio-registry-avro-serde-kafka:3.3.3`, `io.confluent:kafka-avro-serializer:8.3.2`, `io.strimzi:kafka-oauth-client:0.18.0`, Avro 1.12.2 + `avro-maven-plugin` 1.12.2, JUnit 5, UBI 9 `openjdk-21-runtime`.

**Spec:** `docs/superpowers/specs/2026-09-22-kafka-client-app-design.md`

## Global Constraints

- Base package `se.afshin.yavari.clientapp` (CLAUDE.md rule 1).
- Module is standalone: its own `pom.xml`, not referenced from the root `pom.xml`. All `mvn` commands run inside `test-clients/kafka-client-app`.
- Only keystore/truststore TLS (PKCS12 default, JKS supported). No PEM.
- Every config class reads env through `Env` (a `Function<String,String>` wrapper). Never call `System.getenv` outside `Main`.
- Exact env variable names and defaults as in the spec's tables. Enumerated values are case-insensitive.
- Startup validation collects all problems and exits 1; runtime errors are logged and retried, never fatal.
- Serializer/deserializer classes are passed by class name in properties, never constructed in code.
- Image base: `registry.access.redhat.com/ubi9/openjdk-21-runtime:latest` (UBI rule).
- This module is not part of `mcs-setup` or `make e2e`; the user explicitly skipped e2e for it (2026-09-22). Do not run `make teardown` / `mcs-setup` for this work.
- CLAUDE.md rule 5: the user must approve commits. The commit steps below assume the user has granted per-task commit approval for this branch at execution start; if not, stop at each commit step and ask.
- Commit messages end with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`.
- Branch: `feat/kafka-client-app`.

---

## File structure

```
test-clients/kafka-client-app/
  pom.xml
  Dockerfile
  README.md
  deploy/
    producer-deployment.yaml
    consumer-deployment.yaml
    tls-secrets.example.yaml
    configmaps/
      string-plaintext.yaml
      avro-apicurio-mtls-proxy.yaml
      avro-apicurio-oidc.yaml
      avro-apicurio-none.yaml
      avro-confluent-tls.yaml
      avro-confluent-none.yaml
      kafka-oauth.yaml
  src/main/avro/Event.avsc
  src/main/resources/application.properties
  src/main/java/se/afshin/yavari/clientapp/
    Main.java                       @QuarkusMain: load+validate config, build clients, start runners, wait
    config/Env.java                 env accessor with defaults/enum/long parsing
    config/Problems.java            error collector used by all config parsers
    config/ConfigException.java     thrown by AppConfig.load when Problems is non-empty
    config/TlsStores.java           keystore/truststore record + prefix→global→default resolution
    config/KafkaClientConfig.java   KAFKA_* → Properties
    config/SchemaRegistryConfig.java <PREFIX>_SCHEMA_* → validated record
    config/Format.java              STRING | AVRO
    config/ProducerConfig.java      PRODUCER_*
    config/ConsumerConfig.java      CONSUMER_*
    config/AppConfig.java           aggregate + load(Env)
    serde/SerdeProps.java           SchemaRegistryConfig → serde property map (+ class names)
    producer/PayloadGenerator.java  Event factory + JSON rendering
    producer/ProducerRunner.java    scheduled send loop
    consumer/ConsumerRunner.java    poll loop thread
    runtime/RunnerRegistry.java     @ApplicationScoped holder of started flags
    runtime/RunnersReadyCheck.java  @Readiness health check
  src/test/java/se/afshin/yavari/clientapp/
    config/EnvTest.java
    config/TlsStoresTest.java
    config/KafkaClientConfigTest.java
    config/SchemaRegistryConfigTest.java
    config/ProducerConfigTest.java
    config/ConsumerConfigTest.java
    config/AppConfigTest.java
    serde/SerdePropsTest.java
    producer/PayloadGeneratorTest.java
    producer/ProducerRunnerTest.java
    consumer/ConsumerRunnerTest.java
```

`Event` (Avro `SpecificRecord`) is generated into `target/generated-sources/avro` as `se.afshin.yavari.clientapp.avro.Event`.

---

### Task 1: Module scaffold that builds

**Files:**
- Create: `test-clients/kafka-client-app/pom.xml`
- Create: `test-clients/kafka-client-app/src/main/avro/Event.avsc`
- Create: `test-clients/kafka-client-app/src/main/resources/application.properties`
- Create: `test-clients/kafka-client-app/src/main/java/se/afshin/yavari/clientapp/Main.java`
- Create: `test-clients/kafka-client-app/Dockerfile`
- Create: `test-clients/kafka-client-app/.gitignore`

**Interfaces:**
- Produces: generated class `se.afshin.yavari.clientapp.avro.Event` with builder and getters `getId()`, `getSequence()`, `getTimestamp()`, `getMessage()`; `Main` placeholder replaced in Task 10.

- [x] **Step 1: Write `pom.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>se.afshin.yavari</groupId>
    <artifactId>kafka-client-app</artifactId>
    <version>1.0.0-SNAPSHOT</version>

    <description>
        Env-configured Quarkus producer/consumer test client: JSON string or Avro via
        Apicurio (mTLS through the RBAC proxy, OIDC, or none) or Confluent Schema Registry.
    </description>

    <properties>
        <maven.compiler.release>21</maven.compiler.release>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <quarkus.platform.version>3.39.4</quarkus.platform.version>
        <apicurio.version>3.3.3</apicurio.version>
        <confluent.version>8.3.2</confluent.version>
        <strimzi-oauth.version>0.18.0</strimzi-oauth.version>
        <avro.version>1.12.2</avro.version>
        <surefire.version>3.5.4</surefire.version>
    </properties>

    <repositories>
        <repository>
            <id>confluent</id>
            <url>https://packages.confluent.io/maven/</url>
        </repository>
    </repositories>

    <dependencyManagement>
        <dependencies>
            <dependency>
                <groupId>io.quarkus.platform</groupId>
                <artifactId>quarkus-bom</artifactId>
                <version>${quarkus.platform.version}</version>
                <type>pom</type>
                <scope>import</scope>
            </dependency>
        </dependencies>
    </dependencyManagement>

    <dependencies>
        <dependency>
            <groupId>io.quarkus</groupId>
            <artifactId>quarkus-arc</artifactId>
        </dependency>
        <dependency>
            <groupId>io.quarkus</groupId>
            <artifactId>quarkus-kafka-client</artifactId>
        </dependency>
        <dependency>
            <groupId>io.quarkus</groupId>
            <artifactId>quarkus-smallrye-health</artifactId>
        </dependency>
        <dependency>
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>jackson-databind</artifactId>
        </dependency>
        <dependency>
            <groupId>org.apache.avro</groupId>
            <artifactId>avro</artifactId>
            <version>${avro.version}</version>
        </dependency>

        <!-- Apicurio Registry 3.x Avro serde (TLS keystore/truststore + OIDC via config) -->
        <dependency>
            <groupId>io.apicurio</groupId>
            <artifactId>apicurio-registry-avro-serde-kafka</artifactId>
            <version>${apicurio.version}</version>
        </dependency>

        <!-- Confluent Schema Registry Avro serde -->
        <dependency>
            <groupId>io.confluent</groupId>
            <artifactId>kafka-avro-serializer</artifactId>
            <version>${confluent.version}</version>
            <exclusions>
                <exclusion>
                    <groupId>org.apache.kafka</groupId>
                    <artifactId>*</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>org.slf4j</groupId>
                    <artifactId>slf4j-log4j12</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>log4j</groupId>
                    <artifactId>log4j</artifactId>
                </exclusion>
            </exclusions>
        </dependency>

        <!-- Strimzi OAuth login callback handler for SASL_SSL/OAUTHBEARER -->
        <dependency>
            <groupId>io.strimzi</groupId>
            <artifactId>kafka-oauth-client</artifactId>
            <version>${strimzi-oauth.version}</version>
        </dependency>

        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <finalName>kafka-client-app</finalName>
        <plugins>
            <plugin>
                <groupId>org.apache.avro</groupId>
                <artifactId>avro-maven-plugin</artifactId>
                <version>${avro.version}</version>
                <executions>
                    <execution>
                        <phase>generate-sources</phase>
                        <goals><goal>schema</goal></goals>
                        <configuration>
                            <sourceDirectory>${project.basedir}/src/main/avro</sourceDirectory>
                            <outputDirectory>${project.build.directory}/generated-sources/avro</outputDirectory>
                            <stringType>String</stringType>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
            <plugin>
                <groupId>io.quarkus.platform</groupId>
                <artifactId>quarkus-maven-plugin</artifactId>
                <version>${quarkus.platform.version}</version>
                <extensions>true</extensions>
                <executions>
                    <execution>
                        <goals>
                            <goal>build</goal>
                            <goal>generate-code</goal>
                            <goal>generate-code-tests</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-surefire-plugin</artifactId>
                <version>${surefire.version}</version>
                <configuration>
                    <systemPropertyVariables>
                        <java.util.logging.manager>org.jboss.logmanager.LogManager</java.util.logging.manager>
                    </systemPropertyVariables>
                </configuration>
            </plugin>
        </plugins>
    </build>
</project>
```

- [x] **Step 2: Write `src/main/avro/Event.avsc`**

```json
{
  "type": "record",
  "name": "Event",
  "namespace": "se.afshin.yavari.clientapp.avro",
  "doc": "Test event produced by kafka-client-app",
  "fields": [
    { "name": "id", "type": "string", "doc": "UUID" },
    { "name": "sequence", "type": "long", "doc": "0,1,2,... from process start" },
    { "name": "timestamp", "type": "long", "doc": "epoch millis" },
    { "name": "message", "type": "string", "doc": "hello from <hostname> #<sequence>" }
  ]
}
```

- [x] **Step 3: Write `src/main/resources/application.properties`**

```properties
quarkus.application.name=kafka-client-app
quarkus.http.port=8080
quarkus.log.level=INFO
quarkus.log.console.format=%d{HH:mm:ss.SSS} %-5p [%c{2.}] %s%e%n
# We build our own clients; do not let the extension probe a broker from its own config.
quarkus.kafka.health.enabled=false
quarkus.kafka.devservices.enabled=false
# Kafka client internals are chatty at INFO
quarkus.log.category."org.apache.kafka".level=WARN
quarkus.log.category."io.apicurio".level=INFO
quarkus.log.category."io.confluent".level=WARN
```

- [x] **Step 4: Write placeholder `Main.java`**

```java
package se.afshin.yavari.clientapp;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;

/** Replaced with the real startup sequence in Task 10. */
@QuarkusMain
public class Main implements QuarkusApplication {
    @Override
    public int run(String... args) {
        Quarkus.waitForExit();
        return 0;
    }
}
```

- [x] **Step 5: Write `Dockerfile` and `.gitignore`**

```dockerfile
FROM registry.access.redhat.com/ubi9/openjdk-21-runtime:latest
WORKDIR /deployments
COPY target/quarkus-app/lib/ lib/
COPY target/quarkus-app/*.jar ./
COPY target/quarkus-app/app/ app/
COPY target/quarkus-app/quarkus/ quarkus/
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "quarkus-run.jar"]
```

`.gitignore`:
```
target/
```

- [x] **Step 6: Build and confirm the Avro class is generated**

Run: `cd test-clients/kafka-client-app && mvn -q package -DskipTests && ls target/generated-sources/avro/se/afshin/yavari/clientapp/avro/Event.java && ls target/quarkus-app/quarkus-run.jar`
Expected: both files listed, exit 0. If dependency convergence fails (typically Vert.x or Jackson between Apicurio and Quarkus), run `mvn dependency:tree -Dincludes=io.vertx,com.fasterxml.jackson.core` and add the conflicting artifact to `dependencyManagement` pinned to the Quarkus BOM version.

- [x] **Step 7: Commit**

```bash
git add test-clients/kafka-client-app
git commit -m "feat(kafka-client-app): module scaffold with Quarkus 3.39, Apicurio/Confluent serdes and Avro Event

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 2: `Env`, `Problems`, `ConfigException`, `TlsStores`

**Files:**
- Create: `src/main/java/se/afshin/yavari/clientapp/config/Env.java`
- Create: `src/main/java/se/afshin/yavari/clientapp/config/Problems.java`
- Create: `src/main/java/se/afshin/yavari/clientapp/config/ConfigException.java`
- Create: `src/main/java/se/afshin/yavari/clientapp/config/TlsStores.java`
- Test: `src/test/java/se/afshin/yavari/clientapp/config/EnvTest.java`
- Test: `src/test/java/se/afshin/yavari/clientapp/config/TlsStoresTest.java`

**Interfaces:**
- Produces:
  - `Env(Function<String,String> source)`; `Optional<String> get(String)` (blank → empty); `String get(String, String def)`; `boolean getBoolean(String, boolean def)`; `long getLong(String, long def, Problems)`; `<E extends Enum<E>> E getEnum(String, Class<E>, E def, Problems)`; `String require(String, Problems)` (returns null and records a problem when missing).
  - `Problems`: `void add(String)`, `boolean isEmpty()`, `List<String> list()`, `String message()` (lines joined with `\n`).
  - `ConfigException extends RuntimeException` with `List<String> problems()`.
  - `record TlsStores(String keystorePath, String keystorePassword, String keystoreType, String truststorePath, String truststorePassword, String truststoreType)`; `static TlsStores resolve(Env, String prefix)`; `boolean hasKeystore()`; `boolean hasTruststore()`. Constants `GLOBAL_PREFIX = "TLS_"`, `DEFAULT_TYPE = "PKCS12"`.

- [x] **Step 1: Write failing tests**

`EnvTest.java`:
```java
package se.afshin.yavari.clientapp.config;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class EnvTest {

    private static Env env(Map<String, String> m) { return new Env(m::get); }

    @Test
    void blankIsAbsent() {
        Env e = env(Map.of("A", "  "));
        assertTrue(e.get("A").isEmpty());
        assertEquals("d", e.get("A", "d"));
    }

    @Test
    void booleanParsesCaseInsensitive() {
        Env e = env(Map.of("A", "TRUE", "B", "no"));
        assertTrue(e.getBoolean("A", false));
        assertFalse(e.getBoolean("B", true));
        assertTrue(e.getBoolean("MISSING", true));
    }

    @Test
    void longRecordsProblemOnGarbage() {
        Problems p = new Problems();
        Env e = env(Map.of("A", "12x"));
        assertEquals(5L, e.getLong("A", 5L, p));
        assertEquals(1, p.list().size());
        assertTrue(p.list().get(0).contains("A"));
    }

    @Test
    void enumIsCaseInsensitiveAndRecordsProblem() {
        enum Color { RED, BLUE }
        Problems p = new Problems();
        Env e = env(Map.of("A", "blue", "B", "green"));
        assertEquals(Color.BLUE, e.getEnum("A", Color.class, Color.RED, p));
        assertEquals(Color.RED, e.getEnum("B", Color.class, Color.RED, p));
        assertEquals(Color.RED, e.getEnum("MISSING", Color.class, Color.RED, p));
        assertEquals(1, p.list().size());
        assertTrue(p.list().get(0).contains("B") && p.list().get(0).contains("RED, BLUE"));
    }

    @Test
    void requireRecordsProblemAndReturnsNull() {
        Problems p = new Problems();
        Env e = env(Map.of());
        assertNull(e.require("A", p));
        assertEquals("A is required", p.list().get(0));
        assertEquals("A is required", p.message());
    }
}
```

`TlsStoresTest.java`:
```java
package se.afshin.yavari.clientapp.config;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class TlsStoresTest {

    private static Env env(Map<String, String> m) { return new Env(m::get); }

    @Test
    void globalOnlyWithDefaultTypes() {
        TlsStores t = TlsStores.resolve(env(Map.of(
                "TLS_KEYSTORE_PATH", "/etc/tls/keystore.p12", "TLS_KEYSTORE_PASSWORD", "kp",
                "TLS_TRUSTSTORE_PATH", "/etc/tls/truststore.p12", "TLS_TRUSTSTORE_PASSWORD", "tp")),
                "KAFKA_TLS_");
        assertEquals("/etc/tls/keystore.p12", t.keystorePath());
        assertEquals("kp", t.keystorePassword());
        assertEquals("PKCS12", t.keystoreType());
        assertEquals("/etc/tls/truststore.p12", t.truststorePath());
        assertEquals("tp", t.truststorePassword());
        assertEquals("PKCS12", t.truststoreType());
        assertTrue(t.hasKeystore());
        assertTrue(t.hasTruststore());
    }

    @Test
    void componentOverridesSingleVariableOnly() {
        Map<String, String> m = new HashMap<>();
        m.put("TLS_KEYSTORE_PATH", "/g/ks.p12");
        m.put("TLS_KEYSTORE_PASSWORD", "gk");
        m.put("TLS_TRUSTSTORE_PATH", "/g/ts.p12");
        m.put("TLS_TRUSTSTORE_PASSWORD", "gt");
        m.put("PRODUCER_SCHEMA_TLS_TRUSTSTORE_PATH", "/p/ts.jks");
        m.put("PRODUCER_SCHEMA_TLS_TRUSTSTORE_TYPE", "jks");
        TlsStores t = TlsStores.resolve(env(m), "PRODUCER_SCHEMA_TLS_");
        assertEquals("/g/ks.p12", t.keystorePath());
        assertEquals("gk", t.keystorePassword());
        assertEquals("/p/ts.jks", t.truststorePath());
        assertEquals("gt", t.truststorePassword());
        assertEquals("JKS", t.truststoreType());
    }

    @Test
    void nothingSetMeansNoStores() {
        TlsStores t = TlsStores.resolve(env(Map.of()), "KAFKA_TLS_");
        assertFalse(t.hasKeystore());
        assertFalse(t.hasTruststore());
        assertNull(t.keystorePath());
        assertEquals("PKCS12", t.keystoreType());
    }
}
```

- [x] **Step 2: Run tests to verify they fail**

Run: `cd test-clients/kafka-client-app && mvn -q test -Dtest='EnvTest,TlsStoresTest'`
Expected: compilation failure (classes missing).

- [x] **Step 3: Implement**

`Problems.java`:
```java
package se.afshin.yavari.clientapp.config;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Collects configuration problems so startup can report all of them at once. */
public final class Problems {
    private final List<String> items = new ArrayList<>();

    public void add(String problem) { items.add(problem); }
    public boolean isEmpty() { return items.isEmpty(); }
    public List<String> list() { return Collections.unmodifiableList(items); }
    public String message() { return String.join("\n", items); }
}
```

`ConfigException.java`:
```java
package se.afshin.yavari.clientapp.config;

import java.util.List;

public final class ConfigException extends RuntimeException {
    private final List<String> problems;

    public ConfigException(Problems problems) {
        super(problems.message());
        this.problems = problems.list();
    }

    public List<String> problems() { return problems; }
}
```

`Env.java`:
```java
package se.afshin.yavari.clientapp.config;

import java.util.Arrays;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Environment accessor. Blank values count as absent. Parse failures go to {@link Problems}. */
public final class Env {
    private final Function<String, String> source;

    public Env(Function<String, String> source) { this.source = source; }

    public Optional<String> get(String name) {
        String v = source.apply(name);
        return (v == null || v.isBlank()) ? Optional.empty() : Optional.of(v.trim());
    }

    public String get(String name, String def) { return get(name).orElse(def); }

    public boolean getBoolean(String name, boolean def) {
        return get(name).map(v -> v.equalsIgnoreCase("true") || v.equalsIgnoreCase("yes")).orElse(def);
    }

    public long getLong(String name, long def, Problems problems) {
        Optional<String> v = get(name);
        if (v.isEmpty()) return def;
        try {
            return Long.parseLong(v.get());
        } catch (NumberFormatException e) {
            problems.add(name + " must be a number, got '" + v.get() + "'");
            return def;
        }
    }

    public <E extends Enum<E>> E getEnum(String name, Class<E> type, E def, Problems problems) {
        Optional<String> v = get(name);
        if (v.isEmpty()) return def;
        for (E c : type.getEnumConstants()) {
            if (c.name().equalsIgnoreCase(v.get())) return c;
        }
        String allowed = Arrays.stream(type.getEnumConstants()).map(Enum::name).collect(Collectors.joining(", "));
        problems.add(name + " must be one of " + allowed + ", got '" + v.get() + "'");
        return def;
    }

    /** Returns the value, or records "<name> is required" and returns null. */
    public String require(String name, Problems problems) {
        Optional<String> v = get(name);
        if (v.isEmpty()) {
            problems.add(name + " is required");
            return null;
        }
        return v.get();
    }
}
```

`TlsStores.java`:
```java
package se.afshin.yavari.clientapp.config;

import java.util.Locale;

/**
 * Keystore/truststore locations. {@link #resolve} looks each variable up under the
 * component prefix first (e.g. {@code KAFKA_TLS_}), then the global {@code TLS_}
 * prefix, then the default. Types default to PKCS12 and are upper-cased.
 */
public record TlsStores(String keystorePath, String keystorePassword, String keystoreType,
                        String truststorePath, String truststorePassword, String truststoreType) {

    public static final String GLOBAL_PREFIX = "TLS_";
    public static final String DEFAULT_TYPE = "PKCS12";

    public static TlsStores resolve(Env env, String prefix) {
        return new TlsStores(
                lookup(env, prefix, "KEYSTORE_PATH", null),
                lookup(env, prefix, "KEYSTORE_PASSWORD", null),
                lookup(env, prefix, "KEYSTORE_TYPE", DEFAULT_TYPE).toUpperCase(Locale.ROOT),
                lookup(env, prefix, "TRUSTSTORE_PATH", null),
                lookup(env, prefix, "TRUSTSTORE_PASSWORD", null),
                lookup(env, prefix, "TRUSTSTORE_TYPE", DEFAULT_TYPE).toUpperCase(Locale.ROOT));
    }

    private static String lookup(Env env, String prefix, String suffix, String def) {
        return env.get(prefix + suffix).or(() -> env.get(GLOBAL_PREFIX + suffix)).orElse(def);
    }

    public boolean hasKeystore() { return keystorePath != null; }
    public boolean hasTruststore() { return truststorePath != null; }
}
```

- [x] **Step 4: Run tests to verify they pass**

Run: `cd test-clients/kafka-client-app && mvn -q test -Dtest='EnvTest,TlsStoresTest'`
Expected: `Tests run: 8, Failures: 0`.

- [x] **Step 5: Commit**

```bash
git add test-clients/kafka-client-app/src
git commit -m "feat(kafka-client-app): Env accessor, Problems collector and TlsStores with global fallback

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 3: `KafkaClientConfig`

**Files:**
- Create: `src/main/java/se/afshin/yavari/clientapp/config/KafkaClientConfig.java`
- Test: `src/test/java/se/afshin/yavari/clientapp/config/KafkaClientConfigTest.java`

**Interfaces:**
- Consumes: `Env`, `Problems`, `TlsStores` from Task 2.
- Produces: `record KafkaClientConfig(String bootstrapServers, SecurityProtocol protocol, TlsStores tls, OAuth oauth, String clientId)`; nested `enum SecurityProtocol { PLAINTEXT, SSL, SASL_SSL }`; nested `record OAuth(String tokenEndpoint, String clientId, String clientSecret, String scope)`; `static KafkaClientConfig from(Env, Problems)`; `Properties toProperties(String clientIdSuffix)`. Constant `DEFAULT_CLIENT_ID = "kafka-client-app"`.

- [x] **Step 1: Write failing tests**

```java
package se.afshin.yavari.clientapp.config;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class KafkaClientConfigTest {

    private static Env env(Map<String, String> m) { return new Env(m::get); }

    private static Map<String, String> tls() {
        Map<String, String> m = new HashMap<>();
        m.put("TLS_KEYSTORE_PATH", "/etc/tls/keystore.p12");
        m.put("TLS_KEYSTORE_PASSWORD", "kp");
        m.put("TLS_TRUSTSTORE_PATH", "/etc/tls/truststore.p12");
        m.put("TLS_TRUSTSTORE_PASSWORD", "tp");
        return m;
    }

    @Test
    void plaintextEmitsNoSslKeys() {
        Map<String, String> m = tls();
        m.put("KAFKA_BOOTSTRAP_SERVERS", "b:9092");
        m.put("KAFKA_SECURITY_PROTOCOL", "plaintext");
        Problems p = new Problems();
        Properties props = KafkaClientConfig.from(env(m), p).toProperties("-producer");
        assertTrue(p.isEmpty(), p.message());
        assertEquals("b:9092", props.get("bootstrap.servers"));
        assertEquals("PLAINTEXT", props.get("security.protocol"));
        assertEquals("kafka-client-app-producer", props.get("client.id"));
        assertTrue(props.keySet().stream().noneMatch(k -> k.toString().startsWith("ssl.")));
        assertTrue(props.keySet().stream().noneMatch(k -> k.toString().startsWith("sasl.")));
    }

    @Test
    void sslIsDefaultAndEmitsKeystoreAndTruststore() {
        Map<String, String> m = tls();
        m.put("KAFKA_BOOTSTRAP_SERVERS", "b:9093");
        m.put("KAFKA_CLIENT_ID", "myapp");
        Problems p = new Problems();
        Properties props = KafkaClientConfig.from(env(m), p).toProperties("-consumer");
        assertTrue(p.isEmpty(), p.message());
        assertEquals("SSL", props.get("security.protocol"));
        assertEquals("myapp-consumer", props.get("client.id"));
        assertEquals("/etc/tls/keystore.p12", props.get("ssl.keystore.location"));
        assertEquals("kp", props.get("ssl.keystore.password"));
        assertEquals("PKCS12", props.get("ssl.keystore.type"));
        assertEquals("/etc/tls/truststore.p12", props.get("ssl.truststore.location"));
        assertEquals("tp", props.get("ssl.truststore.password"));
        assertEquals("PKCS12", props.get("ssl.truststore.type"));
    }

    @Test
    void sslWithoutTruststoreIsAProblem() {
        Map<String, String> m = new HashMap<>();
        m.put("KAFKA_BOOTSTRAP_SERVERS", "b:9093");
        Problems p = new Problems();
        KafkaClientConfig.from(env(m), p);
        assertEquals(1, p.list().size());
        assertTrue(p.list().get(0).contains("KAFKA_TLS_TRUSTSTORE_PATH"));
    }

    @Test
    void saslSslEmitsOauthJaasAndTruststoreButNoKeystore() {
        Map<String, String> m = tls();
        m.put("KAFKA_BOOTSTRAP_SERVERS", "b:9094");
        m.put("KAFKA_SECURITY_PROTOCOL", "SASL_SSL");
        m.put("KAFKA_OAUTH_TOKEN_ENDPOINT", "https://kc/realms/r/protocol/openid-connect/token");
        m.put("KAFKA_OAUTH_CLIENT_ID", "cid");
        m.put("KAFKA_OAUTH_CLIENT_SECRET", "sec\"ret");
        m.put("KAFKA_OAUTH_SCOPE", "kafka");
        Problems p = new Problems();
        Properties props = KafkaClientConfig.from(env(m), p).toProperties("-producer");
        assertTrue(p.isEmpty(), p.message());
        assertEquals("SASL_SSL", props.get("security.protocol"));
        assertEquals("OAUTHBEARER", props.get("sasl.mechanism"));
        assertEquals("io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler",
                props.get("sasl.login.callback.handler.class"));
        String jaas = props.getProperty("sasl.jaas.config");
        assertTrue(jaas.startsWith("org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required "));
        assertTrue(jaas.contains("oauth.token.endpoint.uri=\"https://kc/realms/r/protocol/openid-connect/token\""));
        assertTrue(jaas.contains("oauth.client.id=\"cid\""));
        assertTrue(jaas.contains("oauth.client.secret=\"sec\\\"ret\""));
        assertTrue(jaas.contains("oauth.scope=\"kafka\""));
        assertTrue(jaas.contains("oauth.ssl.truststore.location=\"/etc/tls/truststore.p12\""));
        assertTrue(jaas.contains("oauth.ssl.truststore.password=\"tp\""));
        assertTrue(jaas.contains("oauth.ssl.truststore.type=\"PKCS12\""));
        assertTrue(jaas.endsWith(";"));
        assertEquals("/etc/tls/truststore.p12", props.get("ssl.truststore.location"));
        assertNull(props.get("ssl.keystore.location"));
    }

    @Test
    void saslSslMissingOauthVarsAreAllReported() {
        Map<String, String> m = tls();
        m.put("KAFKA_BOOTSTRAP_SERVERS", "b:9094");
        m.put("KAFKA_SECURITY_PROTOCOL", "SASL_SSL");
        Problems p = new Problems();
        KafkaClientConfig.from(env(m), p);
        assertEquals(3, p.list().size());
        assertTrue(p.message().contains("KAFKA_OAUTH_TOKEN_ENDPOINT is required"));
        assertTrue(p.message().contains("KAFKA_OAUTH_CLIENT_ID is required"));
        assertTrue(p.message().contains("KAFKA_OAUTH_CLIENT_SECRET is required"));
    }

    @Test
    void missingBootstrapIsReported() {
        Problems p = new Problems();
        KafkaClientConfig.from(env(Map.of("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")), p);
        assertEquals("KAFKA_BOOTSTRAP_SERVERS is required", p.message());
    }
}
```

- [x] **Step 2: Run tests to verify they fail**

Run: `cd test-clients/kafka-client-app && mvn -q test -Dtest=KafkaClientConfigTest`
Expected: compilation failure.

- [x] **Step 3: Implement**

```java
package se.afshin.yavari.clientapp.config;

import java.util.Properties;

/** {@code KAFKA_*} environment → kafka-clients {@link Properties}. */
public record KafkaClientConfig(String bootstrapServers, SecurityProtocol protocol, TlsStores tls,
                                OAuth oauth, String clientId) {

    public static final String DEFAULT_CLIENT_ID = "kafka-client-app";
    public static final String TLS_PREFIX = "KAFKA_TLS_";
    static final String OAUTH_CALLBACK_HANDLER = "io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler";
    static final String OAUTH_LOGIN_MODULE = "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule";

    public enum SecurityProtocol { PLAINTEXT, SSL, SASL_SSL }

    public record OAuth(String tokenEndpoint, String clientId, String clientSecret, String scope) {}

    public static KafkaClientConfig from(Env env, Problems problems) {
        String bootstrap = env.require("KAFKA_BOOTSTRAP_SERVERS", problems);
        SecurityProtocol protocol = env.getEnum("KAFKA_SECURITY_PROTOCOL", SecurityProtocol.class,
                SecurityProtocol.SSL, problems);
        TlsStores tls = TlsStores.resolve(env, TLS_PREFIX);
        String clientId = env.get("KAFKA_CLIENT_ID", DEFAULT_CLIENT_ID);
        OAuth oauth = null;

        if (protocol != SecurityProtocol.PLAINTEXT && !tls.hasTruststore()) {
            problems.add(TLS_PREFIX + "TRUSTSTORE_PATH (or TLS_TRUSTSTORE_PATH) is required for KAFKA_SECURITY_PROTOCOL=" + protocol);
        }
        if (protocol == SecurityProtocol.SASL_SSL) {
            oauth = new OAuth(
                    env.require("KAFKA_OAUTH_TOKEN_ENDPOINT", problems),
                    env.require("KAFKA_OAUTH_CLIENT_ID", problems),
                    env.require("KAFKA_OAUTH_CLIENT_SECRET", problems),
                    env.get("KAFKA_OAUTH_SCOPE").orElse(null));
        }
        return new KafkaClientConfig(bootstrap, protocol, tls, oauth, clientId);
    }

    /** Base properties for a producer or consumer; {@code clientIdSuffix} is e.g. {@code -producer}. */
    public Properties toProperties(String clientIdSuffix) {
        Properties p = new Properties();
        p.put("bootstrap.servers", bootstrapServers);
        p.put("client.id", clientId + clientIdSuffix);
        p.put("security.protocol", protocol.name());
        switch (protocol) {
            case PLAINTEXT -> { }
            case SSL -> {
                putTruststore(p);
                if (tls.hasKeystore()) {
                    p.put("ssl.keystore.location", tls.keystorePath());
                    p.put("ssl.keystore.password", tls.keystorePassword());
                    p.put("ssl.keystore.type", tls.keystoreType());
                }
            }
            case SASL_SSL -> {
                putTruststore(p);
                p.put("sasl.mechanism", "OAUTHBEARER");
                p.put("sasl.login.callback.handler.class", OAUTH_CALLBACK_HANDLER);
                p.put("sasl.jaas.config", jaasConfig());
            }
        }
        return p;
    }

    private void putTruststore(Properties p) {
        p.put("ssl.truststore.location", tls.truststorePath());
        p.put("ssl.truststore.password", tls.truststorePassword());
        p.put("ssl.truststore.type", tls.truststoreType());
    }

    private String jaasConfig() {
        StringBuilder sb = new StringBuilder(OAUTH_LOGIN_MODULE).append(" required");
        option(sb, "oauth.token.endpoint.uri", oauth.tokenEndpoint());
        option(sb, "oauth.client.id", oauth.clientId());
        option(sb, "oauth.client.secret", oauth.clientSecret());
        option(sb, "oauth.scope", oauth.scope());
        option(sb, "oauth.ssl.truststore.location", tls.truststorePath());
        option(sb, "oauth.ssl.truststore.password", tls.truststorePassword());
        option(sb, "oauth.ssl.truststore.type", tls.truststoreType());
        return sb.append(';').toString();
    }

    private static void option(StringBuilder sb, String key, String value) {
        if (value == null) return;
        sb.append(' ').append(key).append("=\"")
          .append(value.replace("\\", "\\\\").replace("\"", "\\\""))
          .append('"');
    }
}
```

- [x] **Step 4: Run tests to verify they pass**

Run: `cd test-clients/kafka-client-app && mvn -q test -Dtest=KafkaClientConfigTest`
Expected: `Tests run: 6, Failures: 0`.

- [x] **Step 5: Commit**

```bash
git add test-clients/kafka-client-app/src
git commit -m "feat(kafka-client-app): KafkaClientConfig for PLAINTEXT, SSL and SASL_SSL/OAUTHBEARER

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 4: `SchemaRegistryConfig`

**Files:**
- Create: `src/main/java/se/afshin/yavari/clientapp/config/SchemaRegistryConfig.java`
- Test: `src/test/java/se/afshin/yavari/clientapp/config/SchemaRegistryConfigTest.java`

**Interfaces:**
- Consumes: `Env`, `Problems`, `TlsStores`.
- Produces: `record SchemaRegistryConfig(RegistryType type, String url, AuthMode auth, TlsStores tls, Oidc oidc, boolean autoRegister, String group)`; `enum RegistryType { APICURIO, CONFLUENT }`; `enum AuthMode { NONE, MTLS, OIDC }`; `record Oidc(String clientId, String clientSecret, String tokenEndpoint, String scope)`; `static SchemaRegistryConfig from(Env, String prefix, Problems)` where prefix is `"PRODUCER_SCHEMA_"` or `"CONSUMER_SCHEMA_"`; `boolean https()`.

- [x] **Step 1: Write failing tests**

```java
package se.afshin.yavari.clientapp.config;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static se.afshin.yavari.clientapp.config.SchemaRegistryConfig.AuthMode;
import static se.afshin.yavari.clientapp.config.SchemaRegistryConfig.RegistryType;

class SchemaRegistryConfigTest {

    private static final String P = "PRODUCER_SCHEMA_";

    private static Env env(Map<String, String> m) { return new Env(m::get); }

    private static Map<String, String> tls() {
        Map<String, String> m = new HashMap<>();
        m.put("TLS_KEYSTORE_PATH", "/etc/tls/keystore.p12");
        m.put("TLS_KEYSTORE_PASSWORD", "kp");
        m.put("TLS_TRUSTSTORE_PATH", "/etc/tls/truststore.p12");
        m.put("TLS_TRUSTSTORE_PASSWORD", "tp");
        return m;
    }

    @Test
    void defaultsAreApicurioNoneAutoRegisterDefaultGroup() {
        Map<String, String> m = new HashMap<>();
        m.put(P + "URL", "http://registry:8080/apis/registry/v3");
        Problems p = new Problems();
        SchemaRegistryConfig c = SchemaRegistryConfig.from(env(m), P, p);
        assertTrue(p.isEmpty(), p.message());
        assertEquals(RegistryType.APICURIO, c.type());
        assertEquals(AuthMode.NONE, c.auth());
        assertTrue(c.autoRegister());
        assertEquals("default", c.group());
        assertFalse(c.https());
        assertNull(c.oidc());
    }

    @Test
    void urlIsRequired() {
        Problems p = new Problems();
        SchemaRegistryConfig.from(env(Map.of()), P, p);
        assertEquals("PRODUCER_SCHEMA_URL is required", p.message());
    }

    @Test
    void mtlsRequiresKeystore() {
        Map<String, String> m = new HashMap<>();
        m.put(P + "URL", "https://proxy:8443/apis/registry/v3");
        m.put(P + "AUTH", "mtls");
        m.put("TLS_TRUSTSTORE_PATH", "/etc/tls/truststore.p12");
        m.put("TLS_TRUSTSTORE_PASSWORD", "tp");
        Problems p = new Problems();
        SchemaRegistryConfig.from(env(m), P, p);
        assertEquals(1, p.list().size());
        assertTrue(p.list().get(0).contains("PRODUCER_SCHEMA_TLS_KEYSTORE_PATH"));
    }

    @Test
    void mtlsUsesComponentPrefixForTls() {
        Map<String, String> m = tls();
        m.put(P + "URL", "https://proxy:8443/apis/registry/v3");
        m.put(P + "AUTH", "MTLS");
        m.put("CONSUMER_SCHEMA_TLS_KEYSTORE_PATH", "/other.p12");
        Problems p = new Problems();
        SchemaRegistryConfig c = SchemaRegistryConfig.from(env(m), "CONSUMER_SCHEMA_", p);
        assertTrue(p.isEmpty(), p.message());
        assertEquals("/other.p12", c.tls().keystorePath());
        assertTrue(c.https());
    }

    @Test
    void oidcRequiresThreeVarsAndReportsAll() {
        Map<String, String> m = new HashMap<>();
        m.put(P + "URL", "https://registry/apis/registry/v3");
        m.put(P + "AUTH", "oidc");
        Problems p = new Problems();
        SchemaRegistryConfig.from(env(m), P, p);
        assertEquals(3, p.list().size());
        assertTrue(p.message().contains("PRODUCER_SCHEMA_CLIENT_ID is required"));
        assertTrue(p.message().contains("PRODUCER_SCHEMA_CLIENT_SECRET is required"));
        assertTrue(p.message().contains("PRODUCER_SCHEMA_TOKEN_ENDPOINT is required"));
    }

    @Test
    void oidcHappyPath() {
        Map<String, String> m = new HashMap<>();
        m.put(P + "URL", "https://registry/apis/registry/v3");
        m.put(P + "AUTH", "oidc");
        m.put(P + "CLIENT_ID", "cid");
        m.put(P + "CLIENT_SECRET", "sec");
        m.put(P + "TOKEN_ENDPOINT", "https://kc/token");
        m.put(P + "SCOPE", "registry");
        m.put(P + "AUTO_REGISTER", "false");
        m.put(P + "GROUP", "orders");
        Problems p = new Problems();
        SchemaRegistryConfig c = SchemaRegistryConfig.from(env(m), P, p);
        assertTrue(p.isEmpty(), p.message());
        assertEquals(new SchemaRegistryConfig.Oidc("cid", "sec", "https://kc/token", "registry"), c.oidc());
        assertFalse(c.autoRegister());
        assertEquals("orders", c.group());
    }

    @Test
    void oidcRejectedForConfluent() {
        Map<String, String> m = new HashMap<>();
        m.put(P + "URL", "https://sr:8081");
        m.put(P + "REGISTRY_TYPE", "confluent");
        m.put(P + "AUTH", "oidc");
        m.put(P + "CLIENT_ID", "cid");
        m.put(P + "CLIENT_SECRET", "sec");
        m.put(P + "TOKEN_ENDPOINT", "https://kc/token");
        Problems p = new Problems();
        SchemaRegistryConfig.from(env(m), P, p);
        assertEquals(1, p.list().size());
        assertTrue(p.list().get(0).contains("PRODUCER_SCHEMA_AUTH=oidc"));
        assertTrue(p.list().get(0).contains("confluent"));
    }

    @Test
    void invalidEnumValuesReported() {
        Map<String, String> m = new HashMap<>();
        m.put(P + "URL", "http://x");
        m.put(P + "REGISTRY_TYPE", "glue");
        m.put(P + "AUTH", "basic");
        Problems p = new Problems();
        SchemaRegistryConfig.from(env(m), P, p);
        assertEquals(2, p.list().size());
    }
}
```

- [x] **Step 2: Run tests to verify they fail**

Run: `cd test-clients/kafka-client-app && mvn -q test -Dtest=SchemaRegistryConfigTest`
Expected: compilation failure.

- [x] **Step 3: Implement**

```java
package se.afshin.yavari.clientapp.config;

import java.util.Locale;

/**
 * {@code <PREFIX>_SCHEMA_*} environment for one side (producer or consumer).
 * {@code prefix} is {@code PRODUCER_SCHEMA_} or {@code CONSUMER_SCHEMA_}.
 */
public record SchemaRegistryConfig(RegistryType type, String url, AuthMode auth, TlsStores tls,
                                   Oidc oidc, boolean autoRegister, String group) {

    public enum RegistryType { APICURIO, CONFLUENT }
    public enum AuthMode { NONE, MTLS, OIDC }
    public record Oidc(String clientId, String clientSecret, String tokenEndpoint, String scope) {}

    public static final String DEFAULT_GROUP = "default";

    public static SchemaRegistryConfig from(Env env, String prefix, Problems problems) {
        RegistryType type = env.getEnum(prefix + "REGISTRY_TYPE", RegistryType.class, RegistryType.APICURIO, problems);
        String url = env.require(prefix + "URL", problems);
        AuthMode auth = env.getEnum(prefix + "AUTH", AuthMode.class, AuthMode.NONE, problems);
        TlsStores tls = TlsStores.resolve(env, prefix + "TLS_");
        boolean autoRegister = env.getBoolean(prefix + "AUTO_REGISTER", true);
        String group = env.get(prefix + "GROUP", DEFAULT_GROUP);
        Oidc oidc = null;

        if (auth == AuthMode.MTLS && !tls.hasKeystore()) {
            problems.add(prefix + "TLS_KEYSTORE_PATH (or TLS_KEYSTORE_PATH) is required for " + prefix + "AUTH=mtls");
        }
        if (auth == AuthMode.OIDC) {
            if (type == RegistryType.CONFLUENT) {
                problems.add(prefix + "AUTH=oidc is not supported for " + prefix + "REGISTRY_TYPE=confluent (use none or mtls)");
            }
            oidc = new Oidc(
                    env.require(prefix + "CLIENT_ID", problems),
                    env.require(prefix + "CLIENT_SECRET", problems),
                    env.require(prefix + "TOKEN_ENDPOINT", problems),
                    env.get(prefix + "SCOPE").orElse(null));
        }
        return new SchemaRegistryConfig(type, url, auth, tls, oidc, autoRegister, group);
    }

    public boolean https() {
        return url != null && url.toLowerCase(Locale.ROOT).startsWith("https://");
    }
}
```

- [x] **Step 4: Run tests to verify they pass**

Run: `cd test-clients/kafka-client-app && mvn -q test -Dtest=SchemaRegistryConfigTest`
Expected: `Tests run: 8, Failures: 0`. (The Confluent+OIDC test: CLIENT_ID/SECRET/TOKEN_ENDPOINT are set, so exactly one problem.)

- [x] **Step 5: Commit**

```bash
git add test-clients/kafka-client-app/src
git commit -m "feat(kafka-client-app): SchemaRegistryConfig with apicurio/confluent and none/mtls/oidc validation

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 5: `SerdeProps`

**Files:**
- Create: `src/main/java/se/afshin/yavari/clientapp/serde/SerdeProps.java`
- Test: `src/test/java/se/afshin/yavari/clientapp/serde/SerdePropsTest.java`

**Interfaces:**
- Consumes: `SchemaRegistryConfig`, `TlsStores`.
- Produces: `final class SerdeProps` with `static Map<String,Object> producer(SchemaRegistryConfig)` (includes `value.serializer`) and `static Map<String,Object> consumer(SchemaRegistryConfig)` (includes `value.deserializer` and the specific-reader flag). Constants for class names: `APICURIO_SERIALIZER`, `APICURIO_DESERIALIZER`, `CONFLUENT_SERIALIZER`, `CONFLUENT_DESERIALIZER`.

Property names (verified against the 3.3.3 and 8.3.2 jars):

| Apicurio | Confluent |
|---|---|
| `apicurio.registry.url` | `schema.registry.url` |
| `apicurio.registry.tls.truststore.location` / `.password` / `.type` | `schema.registry.ssl.truststore.location` / `.password` / `.type` |
| `apicurio.registry.tls.keystore.location` / `.password` / `.type` | `schema.registry.ssl.keystore.location` / `.password` / `.type` |
| `apicurio.registry.auth.service.token.endpoint`, `apicurio.registry.auth.client.id`, `apicurio.registry.auth.client.secret`, `apicurio.registry.auth.client.scope` | – |
| `apicurio.registry.auto-register` | `auto.register.schemas` |
| `apicurio.registry.artifact.group-id` | – |
| `apicurio.registry.use-specific-avro-reader` (consumer) | `specific.avro.reader` (consumer) |

- [x] **Step 1: Write failing tests**

```java
package se.afshin.yavari.clientapp.serde;

import org.junit.jupiter.api.Test;
import se.afshin.yavari.clientapp.config.SchemaRegistryConfig;
import se.afshin.yavari.clientapp.config.SchemaRegistryConfig.AuthMode;
import se.afshin.yavari.clientapp.config.SchemaRegistryConfig.Oidc;
import se.afshin.yavari.clientapp.config.SchemaRegistryConfig.RegistryType;
import se.afshin.yavari.clientapp.config.TlsStores;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class SerdePropsTest {

    private static final TlsStores BOTH = new TlsStores("/ks.p12", "kp", "PKCS12", "/ts.p12", "tp", "PKCS12");
    private static final TlsStores NONE = new TlsStores(null, null, "PKCS12", null, null, "PKCS12");

    private static SchemaRegistryConfig cfg(RegistryType t, String url, AuthMode a, TlsStores tls, Oidc oidc) {
        return new SchemaRegistryConfig(t, url, a, tls, oidc, true, "grp");
    }

    @Test
    void apicurioHttpNoneEmitsOnlyUrlRegisterGroupAndClass() {
        Map<String, Object> p = SerdeProps.producer(cfg(RegistryType.APICURIO, "http://r/apis/registry/v3", AuthMode.NONE, BOTH, null));
        assertEquals("http://r/apis/registry/v3", p.get("apicurio.registry.url"));
        assertEquals(true, p.get("apicurio.registry.auto-register"));
        assertEquals("grp", p.get("apicurio.registry.artifact.group-id"));
        assertEquals(SerdeProps.APICURIO_SERIALIZER, p.get("value.serializer"));
        assertTrue(p.keySet().stream().noneMatch(k -> k.contains(".tls.")));
        assertTrue(p.keySet().stream().noneMatch(k -> k.contains(".auth.")));
    }

    @Test
    void apicurioHttpsNoneEmitsTruststoreOnly() {
        Map<String, Object> p = SerdeProps.producer(cfg(RegistryType.APICURIO, "https://r/apis/registry/v3", AuthMode.NONE, BOTH, null));
        assertEquals("/ts.p12", p.get("apicurio.registry.tls.truststore.location"));
        assertEquals("tp", p.get("apicurio.registry.tls.truststore.password"));
        assertEquals("PKCS12", p.get("apicurio.registry.tls.truststore.type"));
        assertNull(p.get("apicurio.registry.tls.keystore.location"));
    }

    @Test
    void apicurioMtlsEmitsKeystoreAndTruststore() {
        Map<String, Object> p = SerdeProps.producer(cfg(RegistryType.APICURIO, "https://proxy:8443/apis/registry/v3", AuthMode.MTLS, BOTH, null));
        assertEquals("/ks.p12", p.get("apicurio.registry.tls.keystore.location"));
        assertEquals("kp", p.get("apicurio.registry.tls.keystore.password"));
        assertEquals("PKCS12", p.get("apicurio.registry.tls.keystore.type"));
        assertEquals("/ts.p12", p.get("apicurio.registry.tls.truststore.location"));
    }

    @Test
    void apicurioOidcEmitsAuthKeysAndTruststore() {
        Oidc o = new Oidc("cid", "sec", "https://kc/token", "reg");
        Map<String, Object> p = SerdeProps.producer(cfg(RegistryType.APICURIO, "https://r/apis/registry/v3", AuthMode.OIDC, BOTH, o));
        assertEquals("https://kc/token", p.get("apicurio.registry.auth.service.token.endpoint"));
        assertEquals("cid", p.get("apicurio.registry.auth.client.id"));
        assertEquals("sec", p.get("apicurio.registry.auth.client.secret"));
        assertEquals("reg", p.get("apicurio.registry.auth.client.scope"));
        assertEquals("/ts.p12", p.get("apicurio.registry.tls.truststore.location"));
        assertNull(p.get("apicurio.registry.tls.keystore.location"));
    }

    @Test
    void apicurioOidcWithoutScopeOmitsScopeKey() {
        Oidc o = new Oidc("cid", "sec", "https://kc/token", null);
        Map<String, Object> p = SerdeProps.producer(cfg(RegistryType.APICURIO, "https://r", AuthMode.OIDC, NONE, o));
        assertFalse(p.containsKey("apicurio.registry.auth.client.scope"));
        assertFalse(p.containsKey("apicurio.registry.tls.truststore.location"));
    }

    @Test
    void apicurioConsumerSetsSpecificReaderAndDeserializer() {
        Map<String, Object> p = SerdeProps.consumer(cfg(RegistryType.APICURIO, "http://r", AuthMode.NONE, NONE, null));
        assertEquals(SerdeProps.APICURIO_DESERIALIZER, p.get("value.deserializer"));
        assertEquals(true, p.get("apicurio.registry.use-specific-avro-reader"));
        assertFalse(p.containsKey("value.serializer"));
        assertFalse(p.containsKey("apicurio.registry.auto-register"));
    }

    @Test
    void confluentNoneHttp() {
        Map<String, Object> p = SerdeProps.producer(cfg(RegistryType.CONFLUENT, "http://sr:8081", AuthMode.NONE, BOTH, null));
        assertEquals("http://sr:8081", p.get("schema.registry.url"));
        assertEquals(true, p.get("auto.register.schemas"));
        assertEquals(SerdeProps.CONFLUENT_SERIALIZER, p.get("value.serializer"));
        assertTrue(p.keySet().stream().noneMatch(k -> k.startsWith("schema.registry.ssl.")));
        assertTrue(p.keySet().stream().noneMatch(k -> k.startsWith("apicurio.")));
    }

    @Test
    void confluentMtlsHttps() {
        Map<String, Object> p = SerdeProps.producer(cfg(RegistryType.CONFLUENT, "https://sr:8081", AuthMode.MTLS, BOTH, null));
        assertEquals("/ts.p12", p.get("schema.registry.ssl.truststore.location"));
        assertEquals("tp", p.get("schema.registry.ssl.truststore.password"));
        assertEquals("PKCS12", p.get("schema.registry.ssl.truststore.type"));
        assertEquals("/ks.p12", p.get("schema.registry.ssl.keystore.location"));
        assertEquals("kp", p.get("schema.registry.ssl.keystore.password"));
        assertEquals("PKCS12", p.get("schema.registry.ssl.keystore.type"));
    }

    @Test
    void confluentConsumer() {
        Map<String, Object> p = SerdeProps.consumer(cfg(RegistryType.CONFLUENT, "https://sr:8081", AuthMode.NONE, BOTH, null));
        assertEquals(SerdeProps.CONFLUENT_DESERIALIZER, p.get("value.deserializer"));
        assertEquals(true, p.get("specific.avro.reader"));
        assertEquals("/ts.p12", p.get("schema.registry.ssl.truststore.location"));
        assertFalse(p.containsKey("auto.register.schemas"));
    }
}
```

- [x] **Step 2: Run tests to verify they fail**

Run: `cd test-clients/kafka-client-app && mvn -q test -Dtest=SerdePropsTest`
Expected: compilation failure.

- [x] **Step 3: Implement**

```java
package se.afshin.yavari.clientapp.serde;

import se.afshin.yavari.clientapp.config.SchemaRegistryConfig;
import se.afshin.yavari.clientapp.config.SchemaRegistryConfig.AuthMode;
import se.afshin.yavari.clientapp.config.TlsStores;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Translates a {@link SchemaRegistryConfig} into the property names the chosen registry's
 * Avro serde understands. Truststore keys are emitted whenever the URL is https; keystore
 * keys only for {@link AuthMode#MTLS}; OIDC keys only for {@link AuthMode#OIDC}.
 */
public final class SerdeProps {

    public static final String APICURIO_SERIALIZER = "io.apicurio.registry.serde.avro.AvroKafkaSerializer";
    public static final String APICURIO_DESERIALIZER = "io.apicurio.registry.serde.avro.AvroKafkaDeserializer";
    public static final String CONFLUENT_SERIALIZER = "io.confluent.kafka.serializers.KafkaAvroSerializer";
    public static final String CONFLUENT_DESERIALIZER = "io.confluent.kafka.serializers.KafkaAvroDeserializer";

    private SerdeProps() {}

    public static Map<String, Object> producer(SchemaRegistryConfig c) {
        Map<String, Object> p = common(c);
        switch (c.type()) {
            case APICURIO -> {
                p.put("value.serializer", APICURIO_SERIALIZER);
                p.put("apicurio.registry.auto-register", c.autoRegister());
                p.put("apicurio.registry.artifact.group-id", c.group());
            }
            case CONFLUENT -> {
                p.put("value.serializer", CONFLUENT_SERIALIZER);
                p.put("auto.register.schemas", c.autoRegister());
            }
        }
        return p;
    }

    public static Map<String, Object> consumer(SchemaRegistryConfig c) {
        Map<String, Object> p = common(c);
        switch (c.type()) {
            case APICURIO -> {
                p.put("value.deserializer", APICURIO_DESERIALIZER);
                p.put("apicurio.registry.use-specific-avro-reader", true);
            }
            case CONFLUENT -> {
                p.put("value.deserializer", CONFLUENT_DESERIALIZER);
                p.put("specific.avro.reader", true);
            }
        }
        return p;
    }

    private static Map<String, Object> common(SchemaRegistryConfig c) {
        Map<String, Object> p = new LinkedHashMap<>();
        TlsStores tls = c.tls();
        switch (c.type()) {
            case APICURIO -> {
                p.put("apicurio.registry.url", c.url());
                if (c.https() && tls.hasTruststore()) {
                    p.put("apicurio.registry.tls.truststore.location", tls.truststorePath());
                    p.put("apicurio.registry.tls.truststore.password", tls.truststorePassword());
                    p.put("apicurio.registry.tls.truststore.type", tls.truststoreType());
                }
                if (c.auth() == AuthMode.MTLS) {
                    p.put("apicurio.registry.tls.keystore.location", tls.keystorePath());
                    p.put("apicurio.registry.tls.keystore.password", tls.keystorePassword());
                    p.put("apicurio.registry.tls.keystore.type", tls.keystoreType());
                }
                if (c.auth() == AuthMode.OIDC) {
                    p.put("apicurio.registry.auth.service.token.endpoint", c.oidc().tokenEndpoint());
                    p.put("apicurio.registry.auth.client.id", c.oidc().clientId());
                    p.put("apicurio.registry.auth.client.secret", c.oidc().clientSecret());
                    if (c.oidc().scope() != null) {
                        p.put("apicurio.registry.auth.client.scope", c.oidc().scope());
                    }
                }
            }
            case CONFLUENT -> {
                p.put("schema.registry.url", c.url());
                if (c.https() && tls.hasTruststore()) {
                    p.put("schema.registry.ssl.truststore.location", tls.truststorePath());
                    p.put("schema.registry.ssl.truststore.password", tls.truststorePassword());
                    p.put("schema.registry.ssl.truststore.type", tls.truststoreType());
                }
                if (c.auth() == AuthMode.MTLS) {
                    p.put("schema.registry.ssl.keystore.location", tls.keystorePath());
                    p.put("schema.registry.ssl.keystore.password", tls.keystorePassword());
                    p.put("schema.registry.ssl.keystore.type", tls.keystoreType());
                }
            }
        }
        return p;
    }
}
```

- [x] **Step 4: Run tests to verify they pass**

Run: `cd test-clients/kafka-client-app && mvn -q test -Dtest=SerdePropsTest`
Expected: `Tests run: 9, Failures: 0`.

- [x] **Step 5: Commit**

```bash
git add test-clients/kafka-client-app/src
git commit -m "feat(kafka-client-app): SerdeProps mapping registry config to Apicurio and Confluent serde keys

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 6: `Format`, `ProducerConfig`, `ConsumerConfig`, `AppConfig`

**Files:**
- Create: `src/main/java/se/afshin/yavari/clientapp/config/Format.java`
- Create: `src/main/java/se/afshin/yavari/clientapp/config/ProducerConfig.java`
- Create: `src/main/java/se/afshin/yavari/clientapp/config/ConsumerConfig.java`
- Create: `src/main/java/se/afshin/yavari/clientapp/config/AppConfig.java`
- Test: `src/test/java/se/afshin/yavari/clientapp/config/ProducerConfigTest.java`
- Test: `src/test/java/se/afshin/yavari/clientapp/config/ConsumerConfigTest.java`
- Test: `src/test/java/se/afshin/yavari/clientapp/config/AppConfigTest.java`

**Interfaces:**
- Consumes: `Env`, `Problems`, `ConfigException`, `KafkaClientConfig`, `SchemaRegistryConfig`.
- Produces:
  - `enum Format { STRING, AVRO }`
  - `record ProducerConfig(boolean enabled, String topic, long intervalMs, Format format, SchemaRegistryConfig schema)`; `static ProducerConfig from(Env, Problems)`; `schema` is null unless format is AVRO.
  - `record ConsumerConfig(boolean enabled, String topic, String groupId, String autoOffsetReset, Format format, SchemaRegistryConfig schema)`; `static ConsumerConfig from(Env, Problems)`.
  - `record AppConfig(KafkaClientConfig kafka, ProducerConfig producer, ConsumerConfig consumer)`; `static AppConfig load(Env)` throws `ConfigException`.

- [x] **Step 1: Write failing tests**

`ProducerConfigTest.java`:
```java
package se.afshin.yavari.clientapp.config;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ProducerConfigTest {

    private static Env env(Map<String, String> m) { return new Env(m::get); }

    @Test
    void disabledByDefaultReadsNothingElse() {
        Problems p = new Problems();
        ProducerConfig c = ProducerConfig.from(env(Map.of()), p);
        assertFalse(c.enabled());
        assertTrue(p.isEmpty());
        assertNull(c.schema());
    }

    @Test
    void enabledStringDefaults() {
        Problems p = new Problems();
        ProducerConfig c = ProducerConfig.from(env(Map.of("PRODUCER_ENABLED", "true", "PRODUCER_TOPIC", "orders")), p);
        assertTrue(p.isEmpty(), p.message());
        assertEquals("orders", c.topic());
        assertEquals(1000L, c.intervalMs());
        assertEquals(Format.STRING, c.format());
        assertNull(c.schema());
    }

    @Test
    void enabledWithoutTopicIsAProblem() {
        Problems p = new Problems();
        ProducerConfig.from(env(Map.of("PRODUCER_ENABLED", "true")), p);
        assertEquals("PRODUCER_TOPIC is required", p.message());
    }

    @Test
    void avroReadsSchemaWithProducerPrefix() {
        Map<String, String> m = new HashMap<>();
        m.put("PRODUCER_ENABLED", "true");
        m.put("PRODUCER_TOPIC", "orders");
        m.put("PRODUCER_FORMAT", "avro");
        m.put("PRODUCER_INTERVAL_MS", "250");
        m.put("PRODUCER_SCHEMA_URL", "http://r/apis/registry/v3");
        Problems p = new Problems();
        ProducerConfig c = ProducerConfig.from(env(m), p);
        assertTrue(p.isEmpty(), p.message());
        assertEquals(Format.AVRO, c.format());
        assertEquals(250L, c.intervalMs());
        assertNotNull(c.schema());
        assertEquals("http://r/apis/registry/v3", c.schema().url());
    }

    @Test
    void avroWithoutUrlIsAProblem() {
        Map<String, String> m = new HashMap<>();
        m.put("PRODUCER_ENABLED", "true");
        m.put("PRODUCER_TOPIC", "orders");
        m.put("PRODUCER_FORMAT", "avro");
        Problems p = new Problems();
        ProducerConfig.from(env(m), p);
        assertEquals("PRODUCER_SCHEMA_URL is required", p.message());
    }

    @Test
    void intervalMustBePositive() {
        Map<String, String> m = new HashMap<>();
        m.put("PRODUCER_ENABLED", "true");
        m.put("PRODUCER_TOPIC", "orders");
        m.put("PRODUCER_INTERVAL_MS", "0");
        Problems p = new Problems();
        ProducerConfig.from(env(m), p);
        assertTrue(p.message().contains("PRODUCER_INTERVAL_MS must be > 0"));
    }
}
```

`ConsumerConfigTest.java`:
```java
package se.afshin.yavari.clientapp.config;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ConsumerConfigTest {

    private static Env env(Map<String, String> m) { return new Env(m::get); }

    @Test
    void disabledByDefault() {
        Problems p = new Problems();
        ConsumerConfig c = ConsumerConfig.from(env(Map.of()), p);
        assertFalse(c.enabled());
        assertTrue(p.isEmpty());
    }

    @Test
    void enabledStringDefaults() {
        Problems p = new Problems();
        ConsumerConfig c = ConsumerConfig.from(env(Map.of("CONSUMER_ENABLED", "true", "CONSUMER_TOPIC", "orders")), p);
        assertTrue(p.isEmpty(), p.message());
        assertEquals("orders", c.topic());
        assertEquals("kafka-client-app", c.groupId());
        assertEquals("earliest", c.autoOffsetReset());
        assertEquals(Format.STRING, c.format());
        assertNull(c.schema());
    }

    @Test
    void avroReadsSchemaWithConsumerPrefix() {
        Map<String, String> m = new HashMap<>();
        m.put("CONSUMER_ENABLED", "true");
        m.put("CONSUMER_TOPIC", "orders");
        m.put("CONSUMER_GROUP_ID", "g1");
        m.put("CONSUMER_AUTO_OFFSET_RESET", "latest");
        m.put("CONSUMER_FORMAT", "AVRO");
        m.put("CONSUMER_SCHEMA_URL", "https://sr:8081");
        m.put("CONSUMER_SCHEMA_REGISTRY_TYPE", "confluent");
        Problems p = new Problems();
        ConsumerConfig c = ConsumerConfig.from(env(m), p);
        assertTrue(p.isEmpty(), p.message());
        assertEquals("g1", c.groupId());
        assertEquals("latest", c.autoOffsetReset());
        assertEquals(SchemaRegistryConfig.RegistryType.CONFLUENT, c.schema().type());
    }

    @Test
    void enabledWithoutTopicIsAProblem() {
        Problems p = new Problems();
        ConsumerConfig.from(env(Map.of("CONSUMER_ENABLED", "true")), p);
        assertEquals("CONSUMER_TOPIC is required", p.message());
    }
}
```

`AppConfigTest.java`:
```java
package se.afshin.yavari.clientapp.config;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class AppConfigTest {

    private static Env env(Map<String, String> m) { return new Env(m::get); }

    @Test
    void neitherRunnerEnabledIsAnError() {
        ConfigException ex = assertThrows(ConfigException.class,
                () -> AppConfig.load(env(Map.of("KAFKA_BOOTSTRAP_SERVERS", "b:9092", "KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"))));
        assertEquals("at least one of PRODUCER_ENABLED or CONSUMER_ENABLED must be true", ex.getMessage());
    }

    @Test
    void allProblemsAreReportedTogether() {
        Map<String, String> m = new HashMap<>();
        m.put("PRODUCER_ENABLED", "true");
        m.put("PRODUCER_FORMAT", "avro");
        m.put("CONSUMER_ENABLED", "true");
        ConfigException ex = assertThrows(ConfigException.class, () -> AppConfig.load(env(m)));
        assertTrue(ex.problems().contains("KAFKA_BOOTSTRAP_SERVERS is required"));
        assertTrue(ex.problems().contains("PRODUCER_TOPIC is required"));
        assertTrue(ex.problems().contains("PRODUCER_SCHEMA_URL is required"));
        assertTrue(ex.problems().contains("CONSUMER_TOPIC is required"));
        assertTrue(ex.problems().stream().anyMatch(s -> s.contains("KAFKA_TLS_TRUSTSTORE_PATH")));
    }

    @Test
    void happyPathBothEnabled() {
        Map<String, String> m = new HashMap<>();
        m.put("KAFKA_BOOTSTRAP_SERVERS", "b:9092");
        m.put("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT");
        m.put("PRODUCER_ENABLED", "true");
        m.put("PRODUCER_TOPIC", "t");
        m.put("CONSUMER_ENABLED", "true");
        m.put("CONSUMER_TOPIC", "t");
        AppConfig c = AppConfig.load(env(m));
        assertTrue(c.producer().enabled());
        assertTrue(c.consumer().enabled());
        assertEquals("b:9092", c.kafka().bootstrapServers());
    }
}
```

- [x] **Step 2: Run tests to verify they fail**

Run: `cd test-clients/kafka-client-app && mvn -q test -Dtest='ProducerConfigTest,ConsumerConfigTest,AppConfigTest'`
Expected: compilation failure.

- [x] **Step 3: Implement**

`Format.java`:
```java
package se.afshin.yavari.clientapp.config;

public enum Format { STRING, AVRO }
```

`ProducerConfig.java`:
```java
package se.afshin.yavari.clientapp.config;

/** {@code PRODUCER_*} environment. {@code schema} is non-null only for {@link Format#AVRO}. */
public record ProducerConfig(boolean enabled, String topic, long intervalMs, Format format,
                             SchemaRegistryConfig schema) {

    public static final String SCHEMA_PREFIX = "PRODUCER_SCHEMA_";
    public static final long DEFAULT_INTERVAL_MS = 1000L;

    public static ProducerConfig from(Env env, Problems problems) {
        boolean enabled = env.getBoolean("PRODUCER_ENABLED", false);
        if (!enabled) {
            return new ProducerConfig(false, null, DEFAULT_INTERVAL_MS, Format.STRING, null);
        }
        String topic = env.require("PRODUCER_TOPIC", problems);
        long interval = env.getLong("PRODUCER_INTERVAL_MS", DEFAULT_INTERVAL_MS, problems);
        if (interval <= 0) {
            problems.add("PRODUCER_INTERVAL_MS must be > 0, got " + interval);
        }
        Format format = env.getEnum("PRODUCER_FORMAT", Format.class, Format.STRING, problems);
        SchemaRegistryConfig schema = format == Format.AVRO
                ? SchemaRegistryConfig.from(env, SCHEMA_PREFIX, problems) : null;
        return new ProducerConfig(true, topic, interval, format, schema);
    }
}
```

`ConsumerConfig.java`:
```java
package se.afshin.yavari.clientapp.config;

/** {@code CONSUMER_*} environment. {@code schema} is non-null only for {@link Format#AVRO}. */
public record ConsumerConfig(boolean enabled, String topic, String groupId, String autoOffsetReset,
                             Format format, SchemaRegistryConfig schema) {

    public static final String SCHEMA_PREFIX = "CONSUMER_SCHEMA_";
    public static final String DEFAULT_GROUP_ID = "kafka-client-app";
    public static final String DEFAULT_AUTO_OFFSET_RESET = "earliest";

    public static ConsumerConfig from(Env env, Problems problems) {
        boolean enabled = env.getBoolean("CONSUMER_ENABLED", false);
        if (!enabled) {
            return new ConsumerConfig(false, null, DEFAULT_GROUP_ID, DEFAULT_AUTO_OFFSET_RESET, Format.STRING, null);
        }
        String topic = env.require("CONSUMER_TOPIC", problems);
        String groupId = env.get("CONSUMER_GROUP_ID", DEFAULT_GROUP_ID);
        String reset = env.get("CONSUMER_AUTO_OFFSET_RESET", DEFAULT_AUTO_OFFSET_RESET);
        Format format = env.getEnum("CONSUMER_FORMAT", Format.class, Format.STRING, problems);
        SchemaRegistryConfig schema = format == Format.AVRO
                ? SchemaRegistryConfig.from(env, SCHEMA_PREFIX, problems) : null;
        return new ConsumerConfig(true, topic, groupId, reset, format, schema);
    }
}
```

`AppConfig.java`:
```java
package se.afshin.yavari.clientapp.config;

/** Whole configuration. {@link #load} reports every problem at once via {@link ConfigException}. */
public record AppConfig(KafkaClientConfig kafka, ProducerConfig producer, ConsumerConfig consumer) {

    public static AppConfig load(Env env) {
        Problems problems = new Problems();
        KafkaClientConfig kafka = KafkaClientConfig.from(env, problems);
        ProducerConfig producer = ProducerConfig.from(env, problems);
        ConsumerConfig consumer = ConsumerConfig.from(env, problems);
        if (!producer.enabled() && !consumer.enabled()) {
            problems.add("at least one of PRODUCER_ENABLED or CONSUMER_ENABLED must be true");
        }
        if (!problems.isEmpty()) {
            throw new ConfigException(problems);
        }
        return new AppConfig(kafka, producer, consumer);
    }
}
```

- [x] **Step 4: Run tests to verify they pass**

Run: `cd test-clients/kafka-client-app && mvn -q test -Dtest='ProducerConfigTest,ConsumerConfigTest,AppConfigTest'`
Expected: `Tests run: 13, Failures: 0`.

- [x] **Step 5: Commit**

```bash
git add test-clients/kafka-client-app/src
git commit -m "feat(kafka-client-app): ProducerConfig, ConsumerConfig and AppConfig with collected validation

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 7: `PayloadGenerator`

**Files:**
- Create: `src/main/java/se/afshin/yavari/clientapp/producer/PayloadGenerator.java`
- Test: `src/test/java/se/afshin/yavari/clientapp/producer/PayloadGeneratorTest.java`

**Interfaces:**
- Consumes: generated `se.afshin.yavari.clientapp.avro.Event`.
- Produces: `final class PayloadGenerator` with constructor `PayloadGenerator(String hostname, Clock clock)`, `static PayloadGenerator forThisHost()`, `Event next()`, `static String toJson(Event)`.

- [x] **Step 1: Write failing tests**

```java
package se.afshin.yavari.clientapp.producer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.clientapp.avro.Event;

import java.io.ByteArrayOutputStream;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class PayloadGeneratorTest {

    private static final Clock FIXED = Clock.fixed(Instant.ofEpochMilli(1_700_000_000_000L), ZoneOffset.UTC);

    @Test
    void sequenceIncrementsAndFieldsAreFilled() {
        PayloadGenerator g = new PayloadGenerator("pod-1", FIXED);
        Event a = g.next();
        Event b = g.next();
        assertEquals(0L, a.getSequence());
        assertEquals(1L, b.getSequence());
        assertEquals(1_700_000_000_000L, a.getTimestamp());
        assertEquals("hello from pod-1 #0", a.getMessage());
        assertEquals("hello from pod-1 #1", b.getMessage());
        assertDoesNotThrow(() -> UUID.fromString(a.getId()));
        assertNotEquals(a.getId(), b.getId());
    }

    @Test
    void jsonHasExactlyTheFourFields() throws Exception {
        Event e = new PayloadGenerator("h", FIXED).next();
        JsonNode n = new ObjectMapper().readTree(PayloadGenerator.toJson(e));
        assertEquals(4, n.size());
        assertEquals(e.getId(), n.get("id").asText());
        assertEquals(0L, n.get("sequence").asLong());
        assertEquals(1_700_000_000_000L, n.get("timestamp").asLong());
        assertEquals("hello from h #0", n.get("message").asText());
    }

    @Test
    void avroRecordRoundTripsThroughItsOwnSchema() throws Exception {
        Event e = new PayloadGenerator("h", FIXED).next();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder enc = EncoderFactory.get().binaryEncoder(out, null);
        new SpecificDatumWriter<>(Event.class).write(e, enc);
        enc.flush();
        BinaryDecoder dec = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
        Event back = new SpecificDatumReader<>(Event.class).read(null, dec);
        assertEquals(e, back);
    }
}
```

- [x] **Step 2: Run tests to verify they fail**

Run: `cd test-clients/kafka-client-app && mvn -q test -Dtest=PayloadGeneratorTest`
Expected: compilation failure.

- [x] **Step 3: Implement**

```java
package se.afshin.yavari.clientapp.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import se.afshin.yavari.clientapp.avro.Event;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Clock;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/** Produces {@link Event}s with an increasing sequence, and renders them as JSON. */
public final class PayloadGenerator {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String hostname;
    private final Clock clock;
    private final AtomicLong sequence = new AtomicLong();

    public PayloadGenerator(String hostname, Clock clock) {
        this.hostname = hostname;
        this.clock = clock;
    }

    public static PayloadGenerator forThisHost() {
        String host;
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            host = "unknown";
        }
        return new PayloadGenerator(host, Clock.systemUTC());
    }

    public Event next() {
        long seq = sequence.getAndIncrement();
        return Event.newBuilder()
                .setId(UUID.randomUUID().toString())
                .setSequence(seq)
                .setTimestamp(clock.millis())
                .setMessage("hello from " + hostname + " #" + seq)
                .build();
    }

    /** Only the four business fields; the Avro class's schema/specificData getters are not serialized. */
    public static String toJson(Event e) {
        ObjectNode n = MAPPER.createObjectNode();
        n.put("id", e.getId());
        n.put("sequence", e.getSequence());
        n.put("timestamp", e.getTimestamp());
        n.put("message", e.getMessage());
        return n.toString();
    }
}
```

- [x] **Step 4: Run tests to verify they pass**

Run: `cd test-clients/kafka-client-app && mvn -q test -Dtest=PayloadGeneratorTest`
Expected: `Tests run: 3, Failures: 0`.

- [x] **Step 5: Commit**

```bash
git add test-clients/kafka-client-app/src
git commit -m "feat(kafka-client-app): PayloadGenerator producing Event records and JSON

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 8: `ProducerRunner`

**Files:**
- Create: `src/main/java/se/afshin/yavari/clientapp/producer/ProducerRunner.java`
- Test: `src/test/java/se/afshin/yavari/clientapp/producer/ProducerRunnerTest.java`

**Interfaces:**
- Consumes: `ProducerConfig`, `Format`, `PayloadGenerator`, `org.apache.kafka.clients.producer.Producer<String,Object>`.
- Produces: `final class ProducerRunner` with constructor `ProducerRunner(ProducerConfig, Producer<String,Object>, PayloadGenerator)`, `void start()`, `void stop()`, `boolean isStarted()`, package-private `void tick()`, `long sent()`, `long failed()`.

Semantics: `tick()` builds one `Event`, converts to JSON string when format is STRING, otherwise passes the `Event` itself, sends with key = id. Success logs `INFO` with `topic-partition@offset`; failure (callback exception or synchronous exception from `send`, e.g. `SerializationException` when the registry rejects) logs `WARN` with the cause and increments `failed`. Never throws. `start()` schedules `tick` on a single-thread `ScheduledExecutorService` at fixed rate `intervalMs`; `stop()` shuts the executor down, awaits 5 s, flushes and closes the producer.

- [x] **Step 1: Write failing tests**

```java
package se.afshin.yavari.clientapp.producer;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.clientapp.avro.Event;
import se.afshin.yavari.clientapp.config.Format;
import se.afshin.yavari.clientapp.config.ProducerConfig;

import java.time.Clock;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ProducerRunnerTest {

    /** Serializer that accepts String or Event without touching a registry. */
    private static final Serializer<Object> ANY = (topic, data) -> String.valueOf(data).getBytes();

    private static ProducerConfig cfg(Format f) {
        return new ProducerConfig(true, "orders", 1000L, f, null);
    }

    @Test
    void stringFormatSendsJsonWithIdKey() {
        MockProducer<String, Object> mock = new MockProducer<>(true, null, new StringSerializer(), ANY);
        ProducerRunner r = new ProducerRunner(cfg(Format.STRING), mock, new PayloadGenerator("h", Clock.systemUTC()));
        r.tick();
        List<ProducerRecord<String, Object>> hist = mock.history();
        assertEquals(1, hist.size());
        ProducerRecord<String, Object> rec = hist.get(0);
        assertEquals("orders", rec.topic());
        assertInstanceOf(String.class, rec.value());
        assertTrue(((String) rec.value()).contains("\"sequence\":0"));
        assertTrue(((String) rec.value()).contains("\"id\":\"" + rec.key() + "\""));
        assertEquals(1, r.sent());
        assertEquals(0, r.failed());
    }

    @Test
    void avroFormatSendsEventObject() {
        MockProducer<String, Object> mock = new MockProducer<>(true, null, new StringSerializer(), ANY);
        ProducerRunner r = new ProducerRunner(cfg(Format.AVRO), mock, new PayloadGenerator("h", Clock.systemUTC()));
        r.tick();
        Object v = mock.history().get(0).value();
        assertInstanceOf(Event.class, v);
        assertEquals(mock.history().get(0).key(), ((Event) v).getId());
    }

    @Test
    void sendFailureIsCountedNotThrown() {
        MockProducer<String, Object> mock = new MockProducer<>(false, null, new StringSerializer(), ANY);
        ProducerRunner r = new ProducerRunner(cfg(Format.STRING), mock, new PayloadGenerator("h", Clock.systemUTC()));
        r.tick();
        mock.errorNext(new RuntimeException("boom"));
        assertEquals(1, r.failed());
        assertEquals(0, r.sent());
    }

    @Test
    void synchronousSerializationErrorIsCountedNotThrown() {
        Serializer<Object> exploding = (topic, data) -> { throw new org.apache.kafka.common.errors.SerializationException("registry said no"); };
        MockProducer<String, Object> mock = new MockProducer<>(true, null, new StringSerializer(), exploding);
        ProducerRunner r = new ProducerRunner(cfg(Format.STRING), mock, new PayloadGenerator("h", Clock.systemUTC()));
        assertDoesNotThrow(r::tick);
        assertEquals(1, r.failed());
    }

    @Test
    void startAndStopLifecycle() throws Exception {
        MockProducer<String, Object> mock = new MockProducer<>(true, null, new StringSerializer(), ANY);
        ProducerConfig fast = new ProducerConfig(true, "orders", 10L, Format.STRING, null);
        ProducerRunner r = new ProducerRunner(fast, mock, new PayloadGenerator("h", Clock.systemUTC()));
        assertFalse(r.isStarted());
        r.start();
        assertTrue(r.isStarted());
        Thread.sleep(100);
        r.stop();
        assertTrue(mock.closed());
        assertTrue(r.sent() >= 2, "expected several ticks, got " + r.sent());
    }
}
```

- [x] **Step 2: Run tests to verify they fail**

Run: `cd test-clients/kafka-client-app && mvn -q test -Dtest=ProducerRunnerTest`
Expected: compilation failure.

- [x] **Step 3: Implement**

```java
package se.afshin.yavari.clientapp.producer;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jboss.logging.Logger;
import se.afshin.yavari.clientapp.avro.Event;
import se.afshin.yavari.clientapp.config.Format;
import se.afshin.yavari.clientapp.config.ProducerConfig;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** Sends one {@link Event} per interval. Failures are logged and counted, never fatal. */
public final class ProducerRunner {

    private static final Logger LOG = Logger.getLogger(ProducerRunner.class);

    private final ProducerConfig config;
    private final Producer<String, Object> producer;
    private final PayloadGenerator generator;
    private final AtomicLong sent = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private volatile ScheduledExecutorService executor;

    public ProducerRunner(ProducerConfig config, Producer<String, Object> producer, PayloadGenerator generator) {
        this.config = config;
        this.producer = producer;
        this.generator = generator;
    }

    public synchronized void start() {
        if (executor != null) return;
        executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "producer");
            t.setDaemon(true);
            return t;
        });
        executor.scheduleAtFixedRate(this::tick, 0, config.intervalMs(), TimeUnit.MILLISECONDS);
        LOG.infof("Producer started: topic=%s format=%s interval=%dms", config.topic(), config.format(), config.intervalMs());
    }

    public synchronized void stop() {
        ScheduledExecutorService ex = executor;
        if (ex == null) return;
        executor = null;
        ex.shutdown();
        try {
            ex.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        try {
            producer.flush();
        } finally {
            producer.close();
        }
        LOG.infof("Producer stopped: sent=%d failed=%d", sent.get(), failed.get());
    }

    public boolean isStarted() { return executor != null; }
    public long sent() { return sent.get(); }
    public long failed() { return failed.get(); }

    void tick() {
        Event event = generator.next();
        Object value = config.format() == Format.STRING ? PayloadGenerator.toJson(event) : event;
        ProducerRecord<String, Object> record = new ProducerRecord<>(config.topic(), event.getId(), value);
        try {
            producer.send(record, (meta, err) -> {
                if (err != null) {
                    failed.incrementAndGet();
                    LOG.warnf("Send failed for seq=%d: %s", event.getSequence(), err.toString());
                } else {
                    sent.incrementAndGet();
                    LOG.infof("Sent seq=%d key=%s to %s-%d@%d", event.getSequence(), event.getId(),
                            meta.topic(), meta.partition(), meta.offset());
                }
            });
        } catch (Exception e) {
            // Serializer errors (e.g. registry 403 through the RBAC proxy) surface synchronously.
            failed.incrementAndGet();
            LOG.warnf("Send failed for seq=%d before dispatch: %s", event.getSequence(), e.toString());
        }
    }
}
```

- [x] **Step 4: Run tests to verify they pass**

Run: `cd test-clients/kafka-client-app && mvn -q test -Dtest=ProducerRunnerTest`
Expected: `Tests run: 5, Failures: 0`.

- [x] **Step 5: Commit**

```bash
git add test-clients/kafka-client-app/src
git commit -m "feat(kafka-client-app): ProducerRunner with scheduled sends and non-fatal failures

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 9: `ConsumerRunner`

**Files:**
- Create: `src/main/java/se/afshin/yavari/clientapp/consumer/ConsumerRunner.java`
- Test: `src/test/java/se/afshin/yavari/clientapp/consumer/ConsumerRunnerTest.java`

**Interfaces:**
- Consumes: `ConsumerConfig`, `org.apache.kafka.clients.consumer.Consumer<String,Object>`.
- Produces: `final class ConsumerRunner` with constructor `ConsumerRunner(ConsumerConfig, Consumer<String,Object>)`, `void start()`, `void stop()`, `boolean isStarted()`, package-private `int pollOnce()` (returns records handled), `long received()`.

Semantics: `start()` subscribes and starts a daemon thread that loops `pollOnce()` until stopped. `pollOnce()` polls with a 1 s timeout, logs each record at INFO (`key`, `partition`, `offset`, `value.toString()`), catches any non-`WakeupException` and logs WARN, and returns the number of records. `stop()` sets the stop flag, calls `consumer.wakeup()`, joins the thread (5 s), and closes the consumer.

- [x] **Step 1: Write failing tests**

```java
package se.afshin.yavari.clientapp.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.clientapp.config.ConsumerConfig;
import se.afshin.yavari.clientapp.config.Format;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ConsumerRunnerTest {

    private static final ConsumerConfig CFG = new ConsumerConfig(true, "orders", "g", "earliest", Format.STRING, null);

    private static MockConsumer<String, Object> mockWithAssignment() {
        MockConsumer<String, Object> mock = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        TopicPartition tp = new TopicPartition("orders", 0);
        mock.subscribe(List.of("orders"));
        mock.rebalance(List.of(tp));
        mock.updateBeginningOffsets(Map.of(tp, 0L));
        return mock;
    }

    @Test
    void pollOnceLogsAndCountsRecords() {
        MockConsumer<String, Object> mock = mockWithAssignment();
        ConsumerRunner r = new ConsumerRunner(CFG, mock);
        mock.addRecord(new ConsumerRecord<>("orders", 0, 0L, "k1", "{\"sequence\":0}"));
        mock.addRecord(new ConsumerRecord<>("orders", 0, 1L, "k2", "{\"sequence\":1}"));
        assertEquals(2, r.pollOnce());
        assertEquals(2, r.received());
    }

    @Test
    void pollOnceSwallowsErrors() {
        MockConsumer<String, Object> mock = mockWithAssignment();
        ConsumerRunner r = new ConsumerRunner(CFG, mock);
        mock.setPollException(new org.apache.kafka.common.KafkaException("broker gone"));
        assertDoesNotThrow(r::pollOnce);
        assertEquals(0, r.received());
    }

    @Test
    void startSubscribesAndStopClosesConsumer() throws Exception {
        MockConsumer<String, Object> mock = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        ConsumerRunner r = new ConsumerRunner(CFG, mock);
        assertFalse(r.isStarted());
        r.start();
        assertTrue(r.isStarted());
        assertEquals(java.util.Set.of("orders"), mock.subscription());
        Thread.sleep(50);
        r.stop();
        assertTrue(mock.closed());
        assertFalse(r.isStarted());
    }
}
```

- [x] **Step 2: Run tests to verify they fail**

Run: `cd test-clients/kafka-client-app && mvn -q test -Dtest=ConsumerRunnerTest`
Expected: compilation failure.

- [x] **Step 3: Implement**

```java
package se.afshin.yavari.clientapp.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.jboss.logging.Logger;
import se.afshin.yavari.clientapp.config.ConsumerConfig;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/** Polls a topic on a dedicated thread and logs every record. Errors are logged and retried. */
public final class ConsumerRunner {

    private static final Logger LOG = Logger.getLogger(ConsumerRunner.class);
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(1);

    private final ConsumerConfig config;
    private final Consumer<String, Object> consumer;
    private final AtomicLong received = new AtomicLong();
    private volatile boolean running;
    private Thread thread;

    public ConsumerRunner(ConsumerConfig config, Consumer<String, Object> consumer) {
        this.config = config;
        this.consumer = consumer;
    }

    public synchronized void start() {
        if (running) return;
        running = true;
        consumer.subscribe(List.of(config.topic()));
        thread = new Thread(this::loop, "consumer");
        thread.setDaemon(true);
        thread.start();
        LOG.infof("Consumer started: topic=%s group=%s format=%s", config.topic(), config.groupId(), config.format());
    }

    public synchronized void stop() {
        if (!running) return;
        running = false;
        consumer.wakeup();
        try {
            thread.join(5_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        consumer.close();
        LOG.infof("Consumer stopped: received=%d", received.get());
    }

    public boolean isStarted() { return running; }
    public long received() { return received.get(); }

    private void loop() {
        while (running) {
            pollOnce();
        }
    }

    /** One poll; returns the number of records handled. Never throws except on wakeup during shutdown. */
    int pollOnce() {
        try {
            ConsumerRecords<String, Object> records = consumer.poll(POLL_TIMEOUT);
            for (ConsumerRecord<String, Object> r : records) {
                received.incrementAndGet();
                LOG.infof("Received key=%s %s-%d@%d value=%s", r.key(), r.topic(), r.partition(), r.offset(), r.value());
            }
            return records.count();
        } catch (WakeupException e) {
            if (running) LOG.debug("Wakeup while running; ignoring");
            return 0;
        } catch (Exception e) {
            LOG.warnf("Poll failed: %s", e.toString());
            return 0;
        }
    }
}
```

- [x] **Step 4: Run tests to verify they pass**

Run: `cd test-clients/kafka-client-app && mvn -q test -Dtest=ConsumerRunnerTest`
Expected: `Tests run: 3, Failures: 0`.

- [x] **Step 5: Commit**

```bash
git add test-clients/kafka-client-app/src
git commit -m "feat(kafka-client-app): ConsumerRunner poll loop with non-fatal error handling

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 10: `Main`, `RunnerRegistry`, readiness, client construction

**Files:**
- Modify: `src/main/java/se/afshin/yavari/clientapp/Main.java` (replace placeholder)
- Create: `src/main/java/se/afshin/yavari/clientapp/runtime/RunnerRegistry.java`
- Create: `src/main/java/se/afshin/yavari/clientapp/runtime/RunnersReadyCheck.java`
- Create: `src/main/java/se/afshin/yavari/clientapp/runtime/ClientFactory.java`
- Test: `src/test/java/se/afshin/yavari/clientapp/runtime/ClientFactoryTest.java`

**Interfaces:**
- Consumes: everything above.
- Produces:
  - `ClientFactory`: `static Properties producerProperties(KafkaClientConfig, ProducerConfig)`, `static Properties consumerProperties(KafkaClientConfig, ConsumerConfig)`. String format sets `StringSerializer`/`StringDeserializer` for the value; Avro format merges `SerdeProps`. Key serde is always String. Consumer sets `group.id`, `auto.offset.reset`, `enable.auto.commit=true`.
  - `RunnerRegistry` (`@ApplicationScoped`): `void register(Supplier<Boolean> started)`, `boolean allStarted()`.
  - `RunnersReadyCheck` (`@Readiness`).
  - `Main.run`: load config (exit 1 with all problems on stderr), build clients, start runners, register, `Quarkus.waitForExit()`, stop runners.

- [x] **Step 1: Write failing test for `ClientFactory`**

```java
package se.afshin.yavari.clientapp.runtime;

import org.junit.jupiter.api.Test;
import se.afshin.yavari.clientapp.config.*;
import se.afshin.yavari.clientapp.config.SchemaRegistryConfig.AuthMode;
import se.afshin.yavari.clientapp.config.SchemaRegistryConfig.RegistryType;
import se.afshin.yavari.clientapp.serde.SerdeProps;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class ClientFactoryTest {

    private static final TlsStores NONE = new TlsStores(null, null, "PKCS12", null, null, "PKCS12");
    private static final KafkaClientConfig KAFKA =
            new KafkaClientConfig("b:9092", KafkaClientConfig.SecurityProtocol.PLAINTEXT, NONE, null, "app");

    @Test
    void stringProducerUsesStringSerializers() {
        ProducerConfig pc = new ProducerConfig(true, "t", 1000L, Format.STRING, null);
        Properties p = ClientFactory.producerProperties(KAFKA, pc);
        assertEquals("org.apache.kafka.common.serialization.StringSerializer", p.get("key.serializer"));
        assertEquals("org.apache.kafka.common.serialization.StringSerializer", p.get("value.serializer"));
        assertEquals("app-producer", p.get("client.id"));
        assertEquals("b:9092", p.get("bootstrap.servers"));
    }

    @Test
    void avroProducerMergesSerdeProps() {
        SchemaRegistryConfig s = new SchemaRegistryConfig(RegistryType.APICURIO, "http://r/apis/registry/v3",
                AuthMode.NONE, NONE, null, true, "default");
        ProducerConfig pc = new ProducerConfig(true, "t", 1000L, Format.AVRO, s);
        Properties p = ClientFactory.producerProperties(KAFKA, pc);
        assertEquals("org.apache.kafka.common.serialization.StringSerializer", p.get("key.serializer"));
        assertEquals(SerdeProps.APICURIO_SERIALIZER, p.get("value.serializer"));
        assertEquals("http://r/apis/registry/v3", p.get("apicurio.registry.url"));
    }

    @Test
    void consumerSetsGroupResetAndDeserializers() {
        SchemaRegistryConfig s = new SchemaRegistryConfig(RegistryType.CONFLUENT, "http://sr:8081",
                AuthMode.NONE, NONE, null, true, "default");
        ConsumerConfig cc = new ConsumerConfig(true, "t", "g1", "latest", Format.AVRO, s);
        Properties p = ClientFactory.consumerProperties(KAFKA, cc);
        assertEquals("g1", p.get("group.id"));
        assertEquals("latest", p.get("auto.offset.reset"));
        assertEquals("true", p.get("enable.auto.commit"));
        assertEquals("app-consumer", p.get("client.id"));
        assertEquals("org.apache.kafka.common.serialization.StringDeserializer", p.get("key.deserializer"));
        assertEquals(SerdeProps.CONFLUENT_DESERIALIZER, p.get("value.deserializer"));
        assertEquals(true, p.get("specific.avro.reader"));
    }

    @Test
    void stringConsumerUsesStringDeserializer() {
        ConsumerConfig cc = new ConsumerConfig(true, "t", "g1", "earliest", Format.STRING, null);
        Properties p = ClientFactory.consumerProperties(KAFKA, cc);
        assertEquals("org.apache.kafka.common.serialization.StringDeserializer", p.get("value.deserializer"));
    }
}
```

- [x] **Step 2: Run test to verify it fails**

Run: `cd test-clients/kafka-client-app && mvn -q test -Dtest=ClientFactoryTest`
Expected: compilation failure.

- [x] **Step 3: Implement `ClientFactory`, `RunnerRegistry`, `RunnersReadyCheck`, `Main`**

`ClientFactory.java`:
```java
package se.afshin.yavari.clientapp.runtime;

import se.afshin.yavari.clientapp.config.ConsumerConfig;
import se.afshin.yavari.clientapp.config.Format;
import se.afshin.yavari.clientapp.config.KafkaClientConfig;
import se.afshin.yavari.clientapp.config.ProducerConfig;
import se.afshin.yavari.clientapp.serde.SerdeProps;

import java.util.Properties;

/** Assembles the final kafka-clients {@link Properties} for producer and consumer. */
public final class ClientFactory {

    static final String STRING_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    static final String STRING_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";

    private ClientFactory() {}

    public static Properties producerProperties(KafkaClientConfig kafka, ProducerConfig pc) {
        Properties p = kafka.toProperties("-producer");
        p.put("key.serializer", STRING_SERIALIZER);
        if (pc.format() == Format.STRING) {
            p.put("value.serializer", STRING_SERIALIZER);
        } else {
            p.putAll(SerdeProps.producer(pc.schema()));
        }
        return p;
    }

    public static Properties consumerProperties(KafkaClientConfig kafka, ConsumerConfig cc) {
        Properties p = kafka.toProperties("-consumer");
        p.put("group.id", cc.groupId());
        p.put("auto.offset.reset", cc.autoOffsetReset());
        p.put("enable.auto.commit", "true");
        p.put("key.deserializer", STRING_DESERIALIZER);
        if (cc.format() == Format.STRING) {
            p.put("value.deserializer", STRING_DESERIALIZER);
        } else {
            p.putAll(SerdeProps.consumer(cc.schema()));
        }
        return p;
    }
}
```

`RunnerRegistry.java`:
```java
package se.afshin.yavari.clientapp.runtime;

import jakarta.enterprise.context.ApplicationScoped;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;

/** Started-state of every enabled runner, for the readiness probe. */
@ApplicationScoped
public class RunnerRegistry {
    private final List<BooleanSupplier> runners = new CopyOnWriteArrayList<>();

    public void register(BooleanSupplier started) { runners.add(started); }

    /** True once every registered runner reports started (and at least one is registered). */
    public boolean allStarted() {
        return !runners.isEmpty() && runners.stream().allMatch(BooleanSupplier::getAsBoolean);
    }
}
```

`RunnersReadyCheck.java`:
```java
package se.afshin.yavari.clientapp.runtime;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Readiness;

@Readiness
@ApplicationScoped
public class RunnersReadyCheck implements HealthCheck {
    @Inject
    RunnerRegistry registry;

    @Override
    public HealthCheckResponse call() {
        return HealthCheckResponse.named("runners").status(registry.allStarted()).build();
    }
}
```

`Main.java` (replace placeholder):
```java
package se.afshin.yavari.clientapp;

import io.quarkus.arc.Arc;
import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.jboss.logging.Logger;
import se.afshin.yavari.clientapp.config.AppConfig;
import se.afshin.yavari.clientapp.config.ConfigException;
import se.afshin.yavari.clientapp.config.Env;
import se.afshin.yavari.clientapp.consumer.ConsumerRunner;
import se.afshin.yavari.clientapp.producer.PayloadGenerator;
import se.afshin.yavari.clientapp.producer.ProducerRunner;
import se.afshin.yavari.clientapp.runtime.ClientFactory;
import se.afshin.yavari.clientapp.runtime.RunnerRegistry;

/**
 * Loads and validates configuration from the environment, builds the Kafka clients,
 * starts the enabled runners and blocks until shutdown.
 */
@QuarkusMain
public class Main implements QuarkusApplication {

    private static final Logger LOG = Logger.getLogger(Main.class);

    @Override
    public int run(String... args) {
        AppConfig config;
        try {
            config = AppConfig.load(new Env(System::getenv));
        } catch (ConfigException e) {
            System.err.println("Invalid configuration:");
            e.problems().forEach(p -> System.err.println("  - " + p));
            return 1;
        }

        RunnerRegistry registry = Arc.container().instance(RunnerRegistry.class).get();
        ProducerRunner producer = null;
        ConsumerRunner consumer = null;

        if (config.producer().enabled()) {
            producer = new ProducerRunner(config.producer(),
                    new KafkaProducer<>(ClientFactory.producerProperties(config.kafka(), config.producer())),
                    PayloadGenerator.forThisHost());
            ProducerRunner p = producer;
            registry.register(p::isStarted);
            producer.start();
        }
        if (config.consumer().enabled()) {
            consumer = new ConsumerRunner(config.consumer(),
                    new KafkaConsumer<>(ClientFactory.consumerProperties(config.kafka(), config.consumer())));
            ConsumerRunner c = consumer;
            registry.register(c::isStarted);
            consumer.start();
        }
        LOG.infof("kafka-client-app up: bootstrap=%s protocol=%s producer=%s consumer=%s",
                config.kafka().bootstrapServers(), config.kafka().protocol(),
                config.producer().enabled(), config.consumer().enabled());

        Quarkus.waitForExit();

        if (producer != null) producer.stop();
        if (consumer != null) consumer.stop();
        return 0;
    }
}
```

- [x] **Step 4: Run the whole test suite and the build**

Run: `cd test-clients/kafka-client-app && mvn -q package`
Expected: all tests pass (`Tests run: 59` across the module, `Failures: 0`), `target/quarkus-app/quarkus-run.jar` exists.

- [x] **Step 5: Smoke-run startup validation locally**

Run: `cd test-clients/kafka-client-app && java -jar target/quarkus-app/quarkus-run.jar; echo "exit=$?"`
Expected: stderr lists `Invalid configuration:` followed by `KAFKA_BOOTSTRAP_SERVERS is required`, the truststore requirement, and `at least one of PRODUCER_ENABLED or CONSUMER_ENABLED must be true`; `exit=1`.

Run: `cd test-clients/kafka-client-app && KAFKA_BOOTSTRAP_SERVERS=localhost:1 KAFKA_SECURITY_PROTOCOL=PLAINTEXT PRODUCER_ENABLED=true PRODUCER_TOPIC=t timeout 8 java -jar target/quarkus-app/quarkus-run.jar; echo "exit=$?"`
Expected: `Producer started: topic=t format=STRING interval=1000ms`, then repeated `Send failed ... ` WARN lines (no broker), process still alive until timeout kills it (`exit=124`). No stack-trace crash.

- [x] **Step 6: Commit**

```bash
git add test-clients/kafka-client-app/src
git commit -m "feat(kafka-client-app): Main startup, client assembly and readiness check

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 11: Container image, deploy manifests, README

**Files:**
- Create: `test-clients/kafka-client-app/deploy/tls-secrets.example.yaml`
- Create: `test-clients/kafka-client-app/deploy/producer-deployment.yaml`
- Create: `test-clients/kafka-client-app/deploy/consumer-deployment.yaml`
- Create: `test-clients/kafka-client-app/deploy/configmaps/string-plaintext.yaml`
- Create: `test-clients/kafka-client-app/deploy/configmaps/avro-apicurio-mtls-proxy.yaml`
- Create: `test-clients/kafka-client-app/deploy/configmaps/avro-apicurio-oidc.yaml`
- Create: `test-clients/kafka-client-app/deploy/configmaps/avro-apicurio-none.yaml`
- Create: `test-clients/kafka-client-app/deploy/configmaps/avro-confluent-tls.yaml`
- Create: `test-clients/kafka-client-app/deploy/configmaps/avro-confluent-none.yaml`
- Create: `test-clients/kafka-client-app/deploy/configmaps/kafka-oauth.yaml`
- Create: `test-clients/kafka-client-app/README.md`

Conventions: namespace `kafka`; image `image-registry.openshift-image-registry.svc:5000/kafka/kafka-client-app:1.0.0` (the user replaces it); the Deployments reference ConfigMap `kafka-client-app-producer` / `kafka-client-app-consumer` via `envFrom`, so a scenario is applied by writing one of the scenario ConfigMaps under that name. Passwords live in Secret `kafka-client-app-tls-passwords`; stores in Secret `kafka-client-app-tls` mounted at `/etc/tls`.

- [x] **Step 1: Build the image to confirm the Dockerfile works**

Run: `cd test-clients/kafka-client-app && mvn -q package -DskipTests && docker build -t kafka-client-app:dev . && docker run --rm kafka-client-app:dev; echo "exit=$?"`
Expected: image builds on UBI; container prints `Invalid configuration:` and exits 1.

- [x] **Step 2: Write `deploy/tls-secrets.example.yaml`**

```yaml
# Example only. On OpenShift, let cert-manager issue the keystore (Certificate.spec.keystores.pkcs12)
# and create the truststore from your CA bundle; then only the password Secret is hand-made.
apiVersion: v1
kind: Secret
metadata:
  name: kafka-client-app-tls
  namespace: kafka
type: Opaque
data:
  keystore.p12: ""     # base64 of a PKCS12 with the client cert + key
  truststore.p12: ""   # base64 of a PKCS12 with the CA(s) for Kafka, the registry and Keycloak
---
apiVersion: v1
kind: Secret
metadata:
  name: kafka-client-app-tls-passwords
  namespace: kafka
type: Opaque
stringData:
  TLS_KEYSTORE_PASSWORD: changeit
  TLS_TRUSTSTORE_PASSWORD: changeit
```

- [x] **Step 3: Write `deploy/producer-deployment.yaml`**

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka-client-app-producer
  namespace: kafka
  labels:
    app: kafka-client-app
    role: producer
spec:
  replicas: 1
  selector:
    matchLabels:
      app: kafka-client-app
      role: producer
  template:
    metadata:
      labels:
        app: kafka-client-app
        role: producer
    spec:
      containers:
        - name: app
          image: image-registry.openshift-image-registry.svc:5000/kafka/kafka-client-app:1.0.0
          ports:
            - name: http
              containerPort: 8080
          env:
            - name: TLS_KEYSTORE_PATH
              value: /etc/tls/keystore.p12
            - name: TLS_TRUSTSTORE_PATH
              value: /etc/tls/truststore.p12
          envFrom:
            - secretRef:
                name: kafka-client-app-tls-passwords
            - configMapRef:
                name: kafka-client-app-producer
          volumeMounts:
            - name: tls
              mountPath: /etc/tls
              readOnly: true
          readinessProbe:
            httpGet:
              path: /q/health/ready
              port: http
            initialDelaySeconds: 5
            periodSeconds: 10
          livenessProbe:
            httpGet:
              path: /q/health/live
              port: http
            initialDelaySeconds: 10
            periodSeconds: 20
          resources:
            requests: { cpu: 100m, memory: 256Mi }
            limits: { memory: 512Mi }
      volumes:
        - name: tls
          secret:
            secretName: kafka-client-app-tls
```

- [x] **Step 4: Write `deploy/consumer-deployment.yaml`**

Same as the producer file with every `producer` replaced by `consumer` (`metadata.name: kafka-client-app-consumer`, labels `role: consumer`, `configMapRef.name: kafka-client-app-consumer`).

- [x] **Step 5: Write the scenario ConfigMaps**

`configmaps/string-plaintext.yaml`:
```yaml
# JSON strings over PLAINTEXT. No registry, no TLS.
apiVersion: v1
kind: ConfigMap
metadata:
  name: kafka-client-app-producer
  namespace: kafka
data:
  KAFKA_BOOTSTRAP_SERVERS: my-cluster-kafka-bootstrap:9092
  KAFKA_SECURITY_PROTOCOL: PLAINTEXT
  PRODUCER_ENABLED: "true"
  PRODUCER_TOPIC: orders
  PRODUCER_FORMAT: string
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: kafka-client-app-consumer
  namespace: kafka
data:
  KAFKA_BOOTSTRAP_SERVERS: my-cluster-kafka-bootstrap:9092
  KAFKA_SECURITY_PROTOCOL: PLAINTEXT
  CONSUMER_ENABLED: "true"
  CONSUMER_TOPIC: orders
  CONSUMER_GROUP_ID: kafka-client-app
  CONSUMER_FORMAT: string
```

`configmaps/avro-apicurio-mtls-proxy.yaml`:
```yaml
# Avro through the Apicurio RBAC proxy: the client cert's CN is authorized by its Kafka ACLs
# (WRITE on topic `orders` => may register/read artifact `orders-value`). Kafka itself is mTLS.
apiVersion: v1
kind: ConfigMap
metadata:
  name: kafka-client-app-producer
  namespace: kafka
data:
  KAFKA_BOOTSTRAP_SERVERS: my-cluster-kafka-bootstrap:9093
  KAFKA_SECURITY_PROTOCOL: SSL
  PRODUCER_ENABLED: "true"
  PRODUCER_TOPIC: orders
  PRODUCER_FORMAT: avro
  PRODUCER_SCHEMA_REGISTRY_TYPE: apicurio
  PRODUCER_SCHEMA_URL: https://apicurio-proxy.kafka.svc:8443/apis/registry/v3
  PRODUCER_SCHEMA_AUTH: mtls
  PRODUCER_SCHEMA_AUTO_REGISTER: "true"
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: kafka-client-app-consumer
  namespace: kafka
data:
  KAFKA_BOOTSTRAP_SERVERS: my-cluster-kafka-bootstrap:9093
  KAFKA_SECURITY_PROTOCOL: SSL
  CONSUMER_ENABLED: "true"
  CONSUMER_TOPIC: orders
  CONSUMER_GROUP_ID: kafka-client-app
  CONSUMER_FORMAT: avro
  CONSUMER_SCHEMA_REGISTRY_TYPE: apicurio
  CONSUMER_SCHEMA_URL: https://apicurio-proxy.kafka.svc:8443/apis/registry/v3
  CONSUMER_SCHEMA_AUTH: mtls
```

`configmaps/avro-apicurio-oidc.yaml`:
```yaml
# Avro against an Apicurio secured by Keycloak (no proxy). Client credentials flow.
# Put PRODUCER_SCHEMA_CLIENT_SECRET / CONSUMER_SCHEMA_CLIENT_SECRET in a Secret and add it to envFrom.
apiVersion: v1
kind: ConfigMap
metadata:
  name: kafka-client-app-producer
  namespace: kafka
data:
  KAFKA_BOOTSTRAP_SERVERS: my-cluster-kafka-bootstrap:9093
  KAFKA_SECURITY_PROTOCOL: SSL
  PRODUCER_ENABLED: "true"
  PRODUCER_TOPIC: orders
  PRODUCER_FORMAT: avro
  PRODUCER_SCHEMA_REGISTRY_TYPE: apicurio
  PRODUCER_SCHEMA_URL: https://apicurio.kafka.svc:8443/apis/registry/v3
  PRODUCER_SCHEMA_AUTH: oidc
  PRODUCER_SCHEMA_CLIENT_ID: kafka-client-app
  PRODUCER_SCHEMA_TOKEN_ENDPOINT: https://keycloak.example.com/realms/kafka/protocol/openid-connect/token
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: kafka-client-app-consumer
  namespace: kafka
data:
  KAFKA_BOOTSTRAP_SERVERS: my-cluster-kafka-bootstrap:9093
  KAFKA_SECURITY_PROTOCOL: SSL
  CONSUMER_ENABLED: "true"
  CONSUMER_TOPIC: orders
  CONSUMER_GROUP_ID: kafka-client-app
  CONSUMER_FORMAT: avro
  CONSUMER_SCHEMA_REGISTRY_TYPE: apicurio
  CONSUMER_SCHEMA_URL: https://apicurio.kafka.svc:8443/apis/registry/v3
  CONSUMER_SCHEMA_AUTH: oidc
  CONSUMER_SCHEMA_CLIENT_ID: kafka-client-app
  CONSUMER_SCHEMA_TOKEN_ENDPOINT: https://keycloak.example.com/realms/kafka/protocol/openid-connect/token
```

`configmaps/avro-apicurio-none.yaml`:
```yaml
# Avro against an unsecured Apicurio over plain HTTP.
apiVersion: v1
kind: ConfigMap
metadata:
  name: kafka-client-app-producer
  namespace: kafka
data:
  KAFKA_BOOTSTRAP_SERVERS: my-cluster-kafka-bootstrap:9092
  KAFKA_SECURITY_PROTOCOL: PLAINTEXT
  PRODUCER_ENABLED: "true"
  PRODUCER_TOPIC: orders
  PRODUCER_FORMAT: avro
  PRODUCER_SCHEMA_REGISTRY_TYPE: apicurio
  PRODUCER_SCHEMA_URL: http://apicurio.kafka.svc:8080/apis/registry/v3
  PRODUCER_SCHEMA_AUTH: none
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: kafka-client-app-consumer
  namespace: kafka
data:
  KAFKA_BOOTSTRAP_SERVERS: my-cluster-kafka-bootstrap:9092
  KAFKA_SECURITY_PROTOCOL: PLAINTEXT
  CONSUMER_ENABLED: "true"
  CONSUMER_TOPIC: orders
  CONSUMER_GROUP_ID: kafka-client-app
  CONSUMER_FORMAT: avro
  CONSUMER_SCHEMA_REGISTRY_TYPE: apicurio
  CONSUMER_SCHEMA_URL: http://apicurio.kafka.svc:8080/apis/registry/v3
  CONSUMER_SCHEMA_AUTH: none
```

`configmaps/avro-confluent-tls.yaml`:
```yaml
# Avro against Confluent Schema Registry over HTTPS with client certificate (mTLS).
# Use PRODUCER_SCHEMA_AUTH: none for server-side TLS only (truststore, no client cert).
apiVersion: v1
kind: ConfigMap
metadata:
  name: kafka-client-app-producer
  namespace: kafka
data:
  KAFKA_BOOTSTRAP_SERVERS: my-cluster-kafka-bootstrap:9093
  KAFKA_SECURITY_PROTOCOL: SSL
  PRODUCER_ENABLED: "true"
  PRODUCER_TOPIC: orders
  PRODUCER_FORMAT: avro
  PRODUCER_SCHEMA_REGISTRY_TYPE: confluent
  PRODUCER_SCHEMA_URL: https://schema-registry.kafka.svc:8081
  PRODUCER_SCHEMA_AUTH: mtls
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: kafka-client-app-consumer
  namespace: kafka
data:
  KAFKA_BOOTSTRAP_SERVERS: my-cluster-kafka-bootstrap:9093
  KAFKA_SECURITY_PROTOCOL: SSL
  CONSUMER_ENABLED: "true"
  CONSUMER_TOPIC: orders
  CONSUMER_GROUP_ID: kafka-client-app
  CONSUMER_FORMAT: avro
  CONSUMER_SCHEMA_REGISTRY_TYPE: confluent
  CONSUMER_SCHEMA_URL: https://schema-registry.kafka.svc:8081
  CONSUMER_SCHEMA_AUTH: mtls
```

`configmaps/avro-confluent-none.yaml`:
```yaml
# Avro against an unsecured Confluent Schema Registry over plain HTTP.
apiVersion: v1
kind: ConfigMap
metadata:
  name: kafka-client-app-producer
  namespace: kafka
data:
  KAFKA_BOOTSTRAP_SERVERS: my-cluster-kafka-bootstrap:9092
  KAFKA_SECURITY_PROTOCOL: PLAINTEXT
  PRODUCER_ENABLED: "true"
  PRODUCER_TOPIC: orders
  PRODUCER_FORMAT: avro
  PRODUCER_SCHEMA_REGISTRY_TYPE: confluent
  PRODUCER_SCHEMA_URL: http://schema-registry.kafka.svc:8081
  PRODUCER_SCHEMA_AUTH: none
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: kafka-client-app-consumer
  namespace: kafka
data:
  KAFKA_BOOTSTRAP_SERVERS: my-cluster-kafka-bootstrap:9092
  KAFKA_SECURITY_PROTOCOL: PLAINTEXT
  CONSUMER_ENABLED: "true"
  CONSUMER_TOPIC: orders
  CONSUMER_GROUP_ID: kafka-client-app
  CONSUMER_FORMAT: avro
  CONSUMER_SCHEMA_REGISTRY_TYPE: confluent
  CONSUMER_SCHEMA_URL: http://schema-registry.kafka.svc:8081
  CONSUMER_SCHEMA_AUTH: none
```

`configmaps/kafka-oauth.yaml`:
```yaml
# Kafka secured with SASL_SSL / OAUTHBEARER (Strimzi oauth listener + Keycloak). JSON strings.
# Put KAFKA_OAUTH_CLIENT_SECRET in a Secret and add it to envFrom.
apiVersion: v1
kind: ConfigMap
metadata:
  name: kafka-client-app-producer
  namespace: kafka
data:
  KAFKA_BOOTSTRAP_SERVERS: my-cluster-kafka-bootstrap:9094
  KAFKA_SECURITY_PROTOCOL: SASL_SSL
  KAFKA_OAUTH_TOKEN_ENDPOINT: https://keycloak.example.com/realms/kafka/protocol/openid-connect/token
  KAFKA_OAUTH_CLIENT_ID: kafka-client-app
  PRODUCER_ENABLED: "true"
  PRODUCER_TOPIC: orders
  PRODUCER_FORMAT: string
```

- [x] **Step 6: Write `README.md`**

Contents (verbatim, the tables mirror the spec):

````markdown
# kafka-client-app

Env-configured Quarkus producer/consumer for verifying Kafka + schema-registry setups end to
end: PLAINTEXT / mTLS / OAuth to Kafka; JSON strings or Avro; Apicurio (through the RBAC proxy
with mTLS, with OIDC, or unsecured) or Confluent Schema Registry (TLS/mTLS or unsecured).

One image, two roles. Enable the producer, the consumer, or both, with environment variables.

## Build

```bash
cd test-clients/kafka-client-app
mvn -q package
docker build -t <registry>/kafka-client-app:1.0.0 .
docker push <registry>/kafka-client-app:1.0.0
```

The module is standalone. Confluent serdes come from `https://packages.confluent.io/maven/`.

## TLS stores

Only keystore/truststore files, PKCS12 by default (JKS supported). Set the global variables
once; override per component only when something differs.

| Variable | Default |
|---|---|
| `TLS_KEYSTORE_PATH`, `TLS_KEYSTORE_PASSWORD`, `TLS_KEYSTORE_TYPE` | `PKCS12` |
| `TLS_TRUSTSTORE_PATH`, `TLS_TRUSTSTORE_PASSWORD`, `TLS_TRUSTSTORE_TYPE` | `PKCS12` |

Component prefixes with the same six suffixes: `KAFKA_TLS_`, `PRODUCER_SCHEMA_TLS_`,
`CONSUMER_SCHEMA_TLS_`. Resolution per variable: component → global → default.

## Kafka

| Variable | Default | Notes |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | required | |
| `KAFKA_SECURITY_PROTOCOL` | `SSL` | `PLAINTEXT`, `SSL`, `SASL_SSL` |
| `KAFKA_CLIENT_ID` | `kafka-client-app` | suffixed `-producer` / `-consumer` |
| `KAFKA_OAUTH_TOKEN_ENDPOINT`, `KAFKA_OAUTH_CLIENT_ID`, `KAFKA_OAUTH_CLIENT_SECRET` | required for `SASL_SSL` | OAUTHBEARER via Strimzi `kafka-oauth-client` |
| `KAFKA_OAUTH_SCOPE` | | optional |

`SSL` uses keystore (if set) + truststore. `SASL_SSL` uses the truststore for the broker and
for the token endpoint; the keystore is ignored.

## Producer

| Variable | Default | Notes |
|---|---|---|
| `PRODUCER_ENABLED` | `false` | |
| `PRODUCER_TOPIC` | required if enabled | |
| `PRODUCER_INTERVAL_MS` | `1000` | |
| `PRODUCER_FORMAT` | `string` | `string` (JSON) or `avro` |
| `PRODUCER_SCHEMA_REGISTRY_TYPE` | `apicurio` | `apicurio` or `confluent` |
| `PRODUCER_SCHEMA_URL` | required for `avro` | Apicurio: `https://host/apis/registry/v3`; Confluent: `https://host:8081` |
| `PRODUCER_SCHEMA_AUTH` | `none` | `none`, `mtls`, `oidc` (`oidc` not allowed with `confluent`) |
| `PRODUCER_SCHEMA_CLIENT_ID`, `PRODUCER_SCHEMA_CLIENT_SECRET`, `PRODUCER_SCHEMA_TOKEN_ENDPOINT` | required for `oidc` | |
| `PRODUCER_SCHEMA_SCOPE` | | optional |
| `PRODUCER_SCHEMA_AUTO_REGISTER` | `true` | |
| `PRODUCER_SCHEMA_GROUP` | `default` | Apicurio artifact group |

An `https` URL always applies the truststore. `mtls` additionally applies the keystore.

## Consumer

Same variables with the `CONSUMER_` prefix (`CONSUMER_SCHEMA_AUTO_REGISTER` is not used), plus:

| Variable | Default |
|---|---|
| `CONSUMER_GROUP_ID` | `kafka-client-app` |
| `CONSUMER_AUTO_OFFSET_RESET` | `earliest` |

## Payload

`Event { id: string (UUID), sequence: long, timestamp: long (epoch ms), message: string }`.
Avro schema in `src/main/avro/Event.avsc`; JSON uses the same four fields. Record key = `id`.
Registry artifact/subject is `<topic>-value` for both registry types, so through the RBAC proxy
a certificate with WRITE on the topic can register and read the schema.

## Deploy

```bash
kubectl apply -f deploy/tls-secrets.example.yaml         # or cert-manager-issued Secrets
kubectl apply -f deploy/configmaps/avro-apicurio-mtls-proxy.yaml   # pick a scenario
kubectl apply -f deploy/producer-deployment.yaml
kubectl apply -f deploy/consumer-deployment.yaml
kubectl logs -f deploy/kafka-client-app-producer
```

| Scenario file | Kafka | Format | Registry |
|---|---|---|---|
| `string-plaintext.yaml` | PLAINTEXT | string | – |
| `avro-apicurio-mtls-proxy.yaml` | SSL | avro | Apicurio via RBAC proxy, mTLS |
| `avro-apicurio-oidc.yaml` | SSL | avro | Apicurio, Keycloak client credentials |
| `avro-apicurio-none.yaml` | PLAINTEXT | avro | Apicurio, http, no auth |
| `avro-confluent-tls.yaml` | SSL | avro | Confluent, https + client cert |
| `avro-confluent-none.yaml` | PLAINTEXT | avro | Confluent, http, no auth |
| `kafka-oauth.yaml` | SASL_SSL | string | – |

Secrets (`*_CLIENT_SECRET`) go in a Secret referenced from `envFrom`, not in the ConfigMap.

## What the logs show

- Startup: `Invalid configuration:` + one line per problem, exit 1, if anything is missing.
- Producer: `Sent seq=<n> key=<uuid> to <topic>-<p>@<offset>` per record; `Send failed ...` WARN
  and retry on the next tick when the broker or registry rejects (e.g. 403 from the RBAC proxy).
- Consumer: `Received key=<uuid> <topic>-<p>@<offset> value=...`.
- Readiness `/q/health/ready` is UP once every enabled runner has started.
````

- [x] **Step 7: Validate the manifests parse**

Run: `cd test-clients/kafka-client-app && for f in deploy/*.yaml deploy/configmaps/*.yaml; do python3 -c "import sys,yaml; list(yaml.safe_load_all(open('$f')))" && echo "ok $f"; done`
Expected: `ok` for every file.

- [x] **Step 8: Commit**

```bash
git add test-clients/kafka-client-app
git commit -m "docs(kafka-client-app): README, OpenShift deployment manifests and scenario ConfigMaps

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 12: Final verification and repo docs pointer

**Files:**
- Modify: `README.md` (repository root) — add one line under the section that lists `test-clients` / tooling, pointing to `test-clients/kafka-client-app/README.md`. If no such section exists, add a bullet "Test clients" at the end of the existing table of contents/overview.
- Modify: `docs/superpowers/plans/2026-09-22-kafka-client-app.md` — tick all boxes.

- [x] **Step 1: Full clean build with tests**

Run: `cd test-clients/kafka-client-app && mvn -q clean package && docker build -q -t kafka-client-app:dev .`
Expected: `BUILD SUCCESS`, 59 tests, image built.

- [x] **Step 2: Add the root README pointer**

Run: `grep -n "test-clients\|schema-producer" README.md` to find the right place; add:

```markdown
- `test-clients/kafka-client-app/` — env-configured Quarkus producer/consumer (string or Avro via Apicurio/Confluent, PLAINTEXT/mTLS/OAuth) for verifying registry and proxy setups. See its [README](test-clients/kafka-client-app/README.md).
```

- [x] **Step 3: Confirm the operator build is untouched**

Run: `cd /home/afshin/dev/code/kroxy-test/kafka-operator && git status --short && git diff --stat HEAD~7 -- src pom.xml`
Expected: no changes under the operator's `src/` or root `pom.xml` (only `README.md` and the new module across the branch).

- [x] **Step 4: Commit**

```bash
git add README.md docs/superpowers/plans/2026-09-22-kafka-client-app.md
git commit -m "docs: point root README at kafka-client-app

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

## Self-review notes

- Spec coverage: env tables → Tasks 2–6; serde mapping → Task 5; components → Tasks 7–10; runtime behaviour (validation exit 1, non-fatal runtime errors, readiness) → Tasks 8–10; deploy + README → Task 11; testing list → each task's tests (`TlsStoresTest`, `KafkaClientConfigTest`, `SchemaRegistryConfigTest`, `SerdePropsTest`, `PayloadGeneratorTest`, `AppConfigTest`, plus runner and factory tests beyond the spec's minimum).
- Not in the spec, added deliberately: `KafkaClientConfig` treats the keystore as optional for `SSL` (server-TLS-only listeners are valid Kafka); the spec's `SSL: keystore + truststore` wording is satisfied when both are present.
- Test count: Env 5 + TlsStores 3 + Kafka 6 + Schema 8 + Serde 9 + Producer/Consumer/App 13 + Payload 3 + ProducerRunner 5 + ConsumerRunner 3 + ClientFactory 4 = 59. Tasks 10 and 12 expect 59.
