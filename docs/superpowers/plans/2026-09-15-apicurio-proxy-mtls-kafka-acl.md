# Apicurio RBAC proxy: mTLS + Kafka-ACL authorization — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let the `apicurio-rbac-proxy` authenticate services by mTLS client certificate and authorize them from Kafka ACLs, while humans keep OIDC + the role policy file, and make the module build standalone for deployment as an Apicurio sidecar in Strimzi.

**Architecture:** Quarkus terminates TLS with `client-auth=request` so OIDC bearer and mTLS coexist per request. `PolicyEngine` dispatches on the identity's credential type: JWT → existing role rules, certificate → new `KafkaAclPolicySource`, which keeps a periodically refreshed snapshot of `Admin.describeAcls` and evaluates topic ACLs with Kafka semantics, mapping artifact `orders-value` → topic `orders`. Key material (server TLS, client-CA trust, Kafka client) is accepted as PKCS12, JKS or PEM through env vars translated by a MicroProfile `ConfigSource` (for Quarkus TLS) and a shared `KafkaSslProps` helper (for Kafka clients).

**Tech Stack:** Java 21, Quarkus 3.15 LTS (quarkus-rest, quarkus-oidc, TLS registry, smallrye-health, test-security), kafka-clients 3.9 (`Admin`, `AclBinding`), snakeyaml, JUnit 5 + AssertJ + REST-assured.

**Spec:** `docs/superpowers/specs/2026-09-15-apicurio-proxy-mtls-kafka-acl-design.md`

## Global Constraints

- Base package `se.afshin.yavari` (CLAUDE.md); new code under `se.afshin.yavari.rbac` and `se.afshin.yavari.rbac.audit`.
- Work on branch `feat/apicurio-proxy-mtls-acl`; **never merge to `main`**; ask before each commit (CLAUDE.md), no `make teardown` / e2e (user-skipped for this work).
- `apicurio-proxy/` must build with `mvn -q package` **without** any other module of this repo.
- Quarkus `3.15.4` (fall back to the newest available `3.15.x` if Maven cannot resolve it); `maven.compiler.release` stays `21`.
- All key material inputs accept `PKCS12` (default), `JKS`, `PEM`; PEM private keys are PKCS#8.
- No cross-mapping: OIDC identities never consult Kafka ACLs; certificate identities never consult the role file.
- Existing tests in `PolicyEngineTest` and `ProxyResourceTest` must stay green throughout.
- Run tests with `cd apicurio-proxy && mvn -q test` (Quarkus tests take ~20 s to boot).

---

### Task 1: Inline the audit classes and add the shared Kafka SSL helper

**Files:**
- Create: `apicurio-proxy/src/main/java/se/afshin/yavari/rbac/audit/{AuditEmitter,AuditEmitters,AuditEvent,AuditEventJson,StdoutAuditEmitter,KafkaAuditEmitter,CompositeAuditEmitter}.java` (copied from `filters/src/main/java/se/afshin/yavari/kroxy/audit/`, package renamed)
- Create: `apicurio-proxy/src/main/java/se/afshin/yavari/rbac/KafkaSslProps.java`
- Modify: `apicurio-proxy/src/main/java/se/afshin/yavari/rbac/{AuditConfig,ProxyResource}.java` (imports)
- Modify: `apicurio-proxy/pom.xml` (drop `kroxy-filters`)
- Test: `apicurio-proxy/src/test/java/se/afshin/yavari/rbac/KafkaSslPropsTest.java`

**Interfaces:**
- Produces: `KafkaSslProps.Store` record `(String type, String path, String password, String certPath, String keyPath, String caPath)`; `static void apply(Properties p, Store keystore, Store truststore)`; `static Store fromEnv(Function<String,String> env, String prefix)` reading `<prefix>_KEYSTORE`, `<prefix>_KEYSTORE_PASSWORD`, `<prefix>_KEYSTORE_TYPE`, `<prefix>_CERT`, `<prefix>_KEY`, `<prefix>_CA` (for a keystore) and `static Store trustFromEnv(env, prefix)` reading `<prefix>_TRUSTSTORE`, `<prefix>_TRUSTSTORE_PASSWORD`, `<prefix>_TRUSTSTORE_TYPE`, `<prefix>_CA`.

- [ ] **Step 1: Write the failing test**

```java
package se.afshin.yavari.rbac;

import org.apache.kafka.common.config.SslConfigs;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaSslPropsTest {

    @Test
    void pkcs12StoresMapToLocationPasswordType() {
        Properties p = new Properties();
        KafkaSslProps.apply(p,
                new KafkaSslProps.Store("PKCS12", "/k/user.p12", "kpw", null, null, null),
                new KafkaSslProps.Store("PKCS12", "/t/ca.p12", "tpw", null, null, null));
        assertThat(p).containsEntry(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "/k/user.p12")
                .containsEntry(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "kpw")
                .containsEntry(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PKCS12")
                .containsEntry(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/t/ca.p12")
                .containsEntry(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "tpw")
                .containsEntry(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PKCS12");
    }

    @Test
    void jksTypeIsPassedThrough() {
        Properties p = new Properties();
        KafkaSslProps.apply(p, new KafkaSslProps.Store("JKS", "/k/user.jks", "kpw", null, null, null), null);
        assertThat(p).containsEntry(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "JKS")
                .doesNotContainKey(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
    }

    @Test
    void pemStoresAreInlinedAsPemSourceMode(@TempDir Path dir) throws Exception {
        Path cert = Files.writeString(dir.resolve("tls.crt"), "CERT");
        Path key = Files.writeString(dir.resolve("tls.key"), "KEY");
        Path ca = Files.writeString(dir.resolve("ca.crt"), "CA");
        Properties p = new Properties();
        KafkaSslProps.apply(p,
                new KafkaSslProps.Store("PEM", null, null, cert.toString(), key.toString(), null),
                new KafkaSslProps.Store("PEM", null, null, null, null, ca.toString()));
        assertThat(p).containsEntry(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PEM")
                .containsEntry(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, "CERT")
                .containsEntry(SslConfigs.SSL_KEYSTORE_KEY_CONFIG, "KEY")
                .containsEntry(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PEM")
                .containsEntry(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, "CA");
    }

    @Test
    void fromEnvReadsPrefixedVariables() {
        Map<String, String> env = Map.of(
                "X_KEYSTORE", "/k.p12", "X_KEYSTORE_PASSWORD", "pw",
                "X_TRUSTSTORE", "/t.jks", "X_TRUSTSTORE_PASSWORD", "tpw", "X_TRUSTSTORE_TYPE", "JKS");
        KafkaSslProps.Store ks = KafkaSslProps.fromEnv(env::get, "X");
        KafkaSslProps.Store ts = KafkaSslProps.trustFromEnv(env::get, "X");
        assertThat(ks.type()).isEqualTo("PKCS12");
        assertThat(ks.path()).isEqualTo("/k.p12");
        assertThat(ts.type()).isEqualTo("JKS");
        assertThat(ts.password()).isEqualTo("tpw");
    }

    @Test
    void emptyStoreIsNullAndApplyIgnoresIt() {
        assertThat(KafkaSslProps.fromEnv(k -> null, "X")).isNull();
        Properties p = new Properties();
        KafkaSslProps.apply(p, null, null);
        assertThat(p).isEmpty();
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd apicurio-proxy && mvn -q test -Dtest=KafkaSslPropsTest`
Expected: compilation error `cannot find symbol: KafkaSslProps`

- [ ] **Step 3: Copy the audit classes, remove the kroxy-filters dependency, write KafkaSslProps**

```bash
cd apicurio-proxy
mkdir -p src/main/java/se/afshin/yavari/rbac/audit
for f in AuditEmitter AuditEmitters AuditEvent AuditEventJson StdoutAuditEmitter KafkaAuditEmitter CompositeAuditEmitter; do
  sed 's/^package se\.afshin\.yavari\.kroxy\.audit;/package se.afshin.yavari.rbac.audit;/' \
    ../filters/src/main/java/se/afshin/yavari/kroxy/audit/$f.java > src/main/java/se/afshin/yavari/rbac/audit/$f.java
done
sed -i 's/se\.afshin\.yavari\.kroxy\.audit/se.afshin.yavari.rbac.audit/g' \
  src/main/java/se/afshin/yavari/rbac/AuditConfig.java src/main/java/se/afshin/yavari/rbac/ProxyResource.java
```

In `pom.xml` delete the whole `<dependency>` block for `se.afshin.yavari:kroxy-filters` (with its comment and exclusions). Keep `kafka-clients`.

Then in the copied `AuditEmitters.java`, replace `producerProps(bootstrap, cert, key, ca)` and its PEM block with the shared helper, and extend `fromEnv` to read keystores too:

```java
    static AuditEmitter fromEnv(EnvLookup env) {
        AuditEmitter stdout = new StdoutAuditEmitter();
        String bootstrap = env.get("KAFKA_AUDIT_BOOTSTRAP");
        if (bootstrap == null || bootstrap.isBlank()) {
            return stdout;
        }
        String topic = env.get("KAFKA_AUDIT_TOPIC");
        if (topic == null || topic.isBlank()) topic = DEFAULT_TOPIC;
        Properties props = producerProps(bootstrap,
                KafkaSslProps.fromEnv(env::get, "KAFKA_AUDIT_TLS"),
                KafkaSslProps.trustFromEnv(env::get, "KAFKA_AUDIT_TLS"));
        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props,
                new ByteArraySerializer(), new ByteArraySerializer());
        AuditEmitter kafka = new KafkaAuditEmitter(producer, topic, stdout);
        return new CompositeAuditEmitter(List.of(stdout, kafka));
    }

    /** Producer config tuned for a never-block, fire-and-forget audit stream. */
    static Properties producerProps(String bootstrap, KafkaSslProps.Store keystore, KafkaSslProps.Store truststore) {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ProducerConfig.CLIENT_ID_CONFIG, "kafka-audit-emitter");
        p.put(ProducerConfig.ACKS_CONFIG, "1");
        p.put(ProducerConfig.LINGER_MS_CONFIG, 20);
        p.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
        p.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 0);
        p.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 60_000);
        if (keystore != null || truststore != null) {
            p.put("security.protocol", "SSL");
            KafkaSslProps.apply(p, keystore, truststore);
        }
        return p;
    }
```

(`KAFKA_AUDIT_TLS_CERT` / `_KEY` / `_CA` keep working: `fromEnv` maps them to a PEM store when `KAFKA_AUDIT_TLS_KEYSTORE` is unset — see `fromEnv` below. Delete the old `readPem` helper and the `SslConfigs` import from `AuditEmitters`.)

`src/main/java/se/afshin/yavari/rbac/KafkaSslProps.java`:

```java
package se.afshin.yavari.rbac;

import org.apache.kafka.common.config.SslConfigs;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.function.Function;

/**
 * Translates key material supplied as PKCS12 / JKS files or PEM files into Kafka client
 * {@code ssl.*} properties. Used by both the audit producer and the ACL admin client.
 *
 * <p>Env contract for a keystore with prefix {@code P}: {@code P_KEYSTORE},
 * {@code P_KEYSTORE_PASSWORD}, {@code P_KEYSTORE_TYPE} (PKCS12 default | JKS | PEM); in
 * PEM mode {@code P_CERT} + {@code P_KEY} (PKCS#8) are used instead of a store file.
 * For a truststore: {@code P_TRUSTSTORE}, {@code P_TRUSTSTORE_PASSWORD},
 * {@code P_TRUSTSTORE_TYPE}; in PEM mode {@code P_CA}.
 */
public final class KafkaSslProps {

    /** One store. For PKCS12/JKS {@code path}+{@code password} are set; for PEM the
     *  {@code certPath}/{@code keyPath} (keystore) or {@code caPath} (truststore). */
    public record Store(String type, String path, String password,
                        String certPath, String keyPath, String caPath) {
        boolean isPem() { return "PEM".equalsIgnoreCase(type); }
    }

    private KafkaSslProps() {}

    public static Store fromEnv(Function<String, String> env, String prefix) {
        String type = orDefault(env.apply(prefix + "_KEYSTORE_TYPE"), "PKCS12");
        String path = env.apply(prefix + "_KEYSTORE");
        String cert = env.apply(prefix + "_CERT");
        String key = env.apply(prefix + "_KEY");
        if (isBlank(path) && (isBlank(cert) || isBlank(key))) return null;
        if (isBlank(path)) type = "PEM";
        return new Store(type, path, env.apply(prefix + "_KEYSTORE_PASSWORD"), cert, key, null);
    }

    public static Store trustFromEnv(Function<String, String> env, String prefix) {
        String type = orDefault(env.apply(prefix + "_TRUSTSTORE_TYPE"), "PKCS12");
        String path = env.apply(prefix + "_TRUSTSTORE");
        String ca = env.apply(prefix + "_CA");
        if (isBlank(path) && isBlank(ca)) return null;
        if (isBlank(path)) type = "PEM";
        return new Store(type, path, env.apply(prefix + "_TRUSTSTORE_PASSWORD"), null, null, ca);
    }

    public static void apply(Properties p, Store keystore, Store truststore) {
        if (keystore != null) {
            if (keystore.isPem()) {
                p.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PEM");
                p.put(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, read(keystore.certPath()));
                p.put(SslConfigs.SSL_KEYSTORE_KEY_CONFIG, read(keystore.keyPath()));
            } else {
                p.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, keystore.type().toUpperCase());
                p.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keystore.path());
                if (keystore.password() != null) p.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, keystore.password());
            }
        }
        if (truststore != null) {
            if (truststore.isPem()) {
                p.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PEM");
                p.put(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, read(truststore.caPath()));
            } else {
                p.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, truststore.type().toUpperCase());
                p.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststore.path());
                if (truststore.password() != null) p.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, truststore.password());
            }
        }
    }

    private static String read(String path) {
        try {
            return Files.readString(Path.of(path));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read PEM file at " + path, e);
        }
    }

    private static boolean isBlank(String s) { return s == null || s.isBlank(); }
    private static String orDefault(String s, String d) { return isBlank(s) ? d : s; }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd apicurio-proxy && mvn -q test`
Expected: PASS (5 new + all existing tests); `mvn -q package -DskipTests` succeeds with no `kroxy-filters` on the classpath (`grep -c kroxy target/quarkus-app/lib/main/* ` → 0 files).

- [ ] **Step 5: Commit (after asking the user)**

```bash
git add apicurio-proxy/pom.xml apicurio-proxy/src
git commit -m "refactor(apicurio-proxy): inline audit emitters, shared KafkaSslProps (PKCS12/JKS/PEM)"
```

---

### Task 2: Upgrade Quarkus to 3.15 and enable optional client certificates

**Files:**
- Modify: `apicurio-proxy/pom.xml` (`quarkus.platform.version`)
- Modify: `apicurio-proxy/src/main/resources/application.properties`

**Interfaces:**
- Produces: build-time setting `quarkus.http.ssl.client-auth=request`, so the mTLS mechanism is registered whenever an HTTPS listener exists.

- [ ] **Step 1: Bump the version and add the client-auth setting**

`pom.xml`: `<quarkus.platform.version>3.15.4</quarkus.platform.version>` (if Maven fails to resolve, use the newest `3.15.x` that resolves: `mvn -q dependency:get -Dartifact=io.quarkus.platform:quarkus-bom:3.15.4:pom`).

`application.properties` — append:

```properties
# TLS + mTLS. The HTTPS listener itself is configured at runtime by ProxyTlsConfigSource
# from PROXY_TLS_* env vars (Task 3). client-auth is build-time: "request" makes a client
# certificate optional, so OIDC bearer requests and mTLS requests coexist on one listener.
quarkus.http.ssl.client-auth=request
```

- [ ] **Step 2: Build and run the existing tests**

Run: `cd apicurio-proxy && mvn -q test`
Expected: PASS. If the Quarkus upgrade breaks compilation (e.g. `@RunOnVirtualThread` package), fix imports only; do not change behaviour.

- [ ] **Step 3: Commit (after asking the user)**

```bash
git add apicurio-proxy/pom.xml apicurio-proxy/src/main/resources/application.properties
git commit -m "build(apicurio-proxy): Quarkus 3.15 LTS, optional client-auth on the HTTPS listener"
```

---

### Task 3: ProxyTlsConfigSource — PROXY_TLS_* env → Quarkus TLS registry properties

**Files:**
- Create: `apicurio-proxy/src/main/java/se/afshin/yavari/rbac/ProxyTlsConfigSource.java`
- Create: `apicurio-proxy/src/main/resources/META-INF/services/org.eclipse.microprofile.config.spi.ConfigSource` (one line: `se.afshin.yavari.rbac.ProxyTlsConfigSource`)
- Test: `apicurio-proxy/src/test/java/se/afshin/yavari/rbac/ProxyTlsConfigSourceTest.java`

**Interfaces:**
- Produces: `static Map<String,String> properties(Function<String,String> env)` — pure translation used by the ConfigSource's `getProperties()`.

- [ ] **Step 1: Write the failing test**

```java
package se.afshin.yavari.rbac;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ProxyTlsConfigSourceTest {

    @Test
    void disabledByDefaultYieldsNoProperties() {
        assertThat(ProxyTlsConfigSource.properties(k -> null)).isEmpty();
    }

    @Test
    void pkcs12ServerAndTrustStores() {
        Map<String, String> env = Map.of(
                "PROXY_TLS_ENABLED", "true",
                "PROXY_TLS_KEYSTORE", "/tls/server.p12", "PROXY_TLS_KEYSTORE_PASSWORD", "spw",
                "PROXY_TLS_TRUSTSTORE", "/tls/clients-ca.p12", "PROXY_TLS_TRUSTSTORE_PASSWORD", "tpw");
        Map<String, String> p = ProxyTlsConfigSource.properties(env::get);
        assertThat(p).containsEntry("quarkus.tls.key-store.p12.path", "/tls/server.p12")
                .containsEntry("quarkus.tls.key-store.p12.password", "spw")
                .containsEntry("quarkus.tls.trust-store.p12.path", "/tls/clients-ca.p12")
                .containsEntry("quarkus.tls.trust-store.p12.password", "tpw")
                .containsEntry("quarkus.http.insecure-requests", "disabled")
                .containsEntry("quarkus.http.ssl-port", "8443");
    }

    @Test
    void jksAndCustomPort() {
        Map<String, String> env = Map.of(
                "PROXY_TLS_ENABLED", "true", "PROXY_TLS_PORT", "9443",
                "PROXY_TLS_KEYSTORE", "/tls/server.jks", "PROXY_TLS_KEYSTORE_PASSWORD", "spw",
                "PROXY_TLS_KEYSTORE_TYPE", "JKS",
                "PROXY_TLS_TRUSTSTORE", "/tls/ca.jks", "PROXY_TLS_TRUSTSTORE_PASSWORD", "tpw",
                "PROXY_TLS_TRUSTSTORE_TYPE", "JKS");
        Map<String, String> p = ProxyTlsConfigSource.properties(env::get);
        assertThat(p).containsEntry("quarkus.tls.key-store.jks.path", "/tls/server.jks")
                .containsEntry("quarkus.tls.trust-store.jks.path", "/tls/ca.jks")
                .containsEntry("quarkus.http.ssl-port", "9443")
                .doesNotContainKey("quarkus.tls.key-store.p12.path");
    }

    @Test
    void pemFiles() {
        Map<String, String> env = Map.of(
                "PROXY_TLS_ENABLED", "true", "PROXY_TLS_KEYSTORE_TYPE", "PEM",
                "PROXY_TLS_CERT", "/tls/tls.crt", "PROXY_TLS_KEY", "/tls/tls.key",
                "PROXY_TLS_TRUSTSTORE_TYPE", "PEM", "PROXY_TLS_CA", "/tls/ca.crt");
        Map<String, String> p = ProxyTlsConfigSource.properties(env::get);
        assertThat(p).containsEntry("quarkus.tls.key-store.pem.server.cert", "/tls/tls.crt")
                .containsEntry("quarkus.tls.key-store.pem.server.key", "/tls/tls.key")
                .containsEntry("quarkus.tls.trust-store.pem.certs", "/tls/ca.crt");
    }

    @Test
    void enabledWithoutKeystoreIsAnError() {
        Map<String, String> env = Map.of("PROXY_TLS_ENABLED", "true");
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> ProxyTlsConfigSource.properties(env::get))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("PROXY_TLS_KEYSTORE");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd apicurio-proxy && mvn -q test -Dtest=ProxyTlsConfigSourceTest`
Expected: compilation error `cannot find symbol: ProxyTlsConfigSource`

- [ ] **Step 3: Write the ConfigSource**

```java
package se.afshin.yavari.rbac;

import org.eclipse.microprofile.config.spi.ConfigSource;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Turns the {@code PROXY_TLS_*} environment into Quarkus TLS-registry + HTTP properties so
 * the proxy can terminate TLS and request client certificates. Registered through
 * {@code META-INF/services}. Keys are runtime config, so reading env at runtime is fine.
 *
 * <pre>
 * PROXY_TLS_ENABLED=true
 * PROXY_TLS_PORT=8443                       (default)
 * PROXY_TLS_KEYSTORE=/tls/server.p12        PROXY_TLS_KEYSTORE_PASSWORD  PROXY_TLS_KEYSTORE_TYPE=PKCS12|JKS|PEM
 *   PEM: PROXY_TLS_CERT=/tls/tls.crt        PROXY_TLS_KEY=/tls/tls.key (PKCS#8)
 * PROXY_TLS_TRUSTSTORE=/tls/clients-ca.p12  PROXY_TLS_TRUSTSTORE_PASSWORD PROXY_TLS_TRUSTSTORE_TYPE
 *   PEM: PROXY_TLS_CA=/tls/ca.crt
 * </pre>
 */
public class ProxyTlsConfigSource implements ConfigSource {

    private final Map<String, String> props;

    public ProxyTlsConfigSource() {
        this.props = properties(System::getenv);
    }

    static Map<String, String> properties(Function<String, String> env) {
        Map<String, String> p = new LinkedHashMap<>();
        if (!"true".equalsIgnoreCase(env.apply("PROXY_TLS_ENABLED"))) return p;

        KafkaSslProps.Store ks = KafkaSslProps.fromEnv(env, "PROXY_TLS");
        if (ks == null) {
            throw new IllegalStateException("PROXY_TLS_ENABLED=true requires PROXY_TLS_KEYSTORE "
                    + "(or PROXY_TLS_CERT + PROXY_TLS_KEY for PEM)");
        }
        if (ks.isPem()) {
            p.put("quarkus.tls.key-store.pem.server.cert", ks.certPath());
            p.put("quarkus.tls.key-store.pem.server.key", ks.keyPath());
        } else {
            String kind = "JKS".equalsIgnoreCase(ks.type()) ? "jks" : "p12";
            p.put("quarkus.tls.key-store." + kind + ".path", ks.path());
            if (ks.password() != null) p.put("quarkus.tls.key-store." + kind + ".password", ks.password());
        }

        KafkaSslProps.Store ts = KafkaSslProps.trustFromEnv(env, "PROXY_TLS");
        if (ts != null) {
            if (ts.isPem()) {
                p.put("quarkus.tls.trust-store.pem.certs", ts.caPath());
            } else {
                String kind = "JKS".equalsIgnoreCase(ts.type()) ? "jks" : "p12";
                p.put("quarkus.tls.trust-store." + kind + ".path", ts.path());
                if (ts.password() != null) p.put("quarkus.tls.trust-store." + kind + ".password", ts.password());
            }
        }

        String port = env.apply("PROXY_TLS_PORT");
        p.put("quarkus.http.ssl-port", port == null || port.isBlank() ? "8443" : port);
        p.put("quarkus.http.insecure-requests", "disabled");
        return p;
    }

    @Override public Map<String, String> getProperties() { return props; }
    @Override public Set<String> getPropertyNames() { return props.keySet(); }
    @Override public String getValue(String key) { return props.get(key); }
    @Override public String getName() { return "proxy-tls-env"; }
    @Override public int getOrdinal() { return 275; }
}
```

- [ ] **Step 4: Run tests to verify they pass; smoke the listener**

Run: `cd apicurio-proxy && mvn -q test`
Expected: PASS. Then a manual smoke (no test): generate a server PKCS12 with `keytool -genkeypair -alias server -keyalg RSA -dname CN=localhost -keystore /tmp/s.p12 -storetype PKCS12 -storepass pw`, run `PROXY_TLS_ENABLED=true PROXY_TLS_KEYSTORE=/tmp/s.p12 PROXY_TLS_KEYSTORE_PASSWORD=pw OIDC_ISSUER_URL=http://localhost:1 java -jar target/quarkus-app/quarkus-run.jar` and confirm `curl -k https://localhost:8443/q/health` answers and `curl http://localhost:8082/` is refused.

- [ ] **Step 5: Commit (after asking the user)**

```bash
git add apicurio-proxy/src
git commit -m "feat(apicurio-proxy): TLS listener from PROXY_TLS_* (PKCS12/JKS/PEM) with optional client certs"
```

---

### Task 4: MtlsPrincipal — DN or CN from the client certificate

**Files:**
- Create: `apicurio-proxy/src/main/java/se/afshin/yavari/rbac/MtlsPrincipal.java`
- Test: `apicurio-proxy/src/test/java/se/afshin/yavari/rbac/MtlsPrincipalTest.java`

**Interfaces:**
- Produces: `enum MtlsPrincipal.Mode { DN, CN }`; `static String of(X509Certificate cert, Mode mode)`; `static String of(String rfc2253Dn, Mode mode)`; `static boolean isCertificateIdentity(SecurityIdentity id)` (true when the identity carries a `io.quarkus.security.credential.CertificateCredential`, or the attribute `proxy.auth` equals `mtls`, which tests use).

- [ ] **Step 1: Write the failing test**

```java
package se.afshin.yavari.rbac;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MtlsPrincipalTest {

    @Test
    void dnModeKeepsRfc2253Subject() {
        assertThat(MtlsPrincipal.of("CN=orders-service,O=Acme", MtlsPrincipal.Mode.DN))
                .isEqualTo("CN=orders-service,O=Acme");
    }

    @Test
    void cnModeExtractsCommonName() {
        assertThat(MtlsPrincipal.of("CN=orders-service,O=Acme", MtlsPrincipal.Mode.CN))
                .isEqualTo("orders-service");
        assertThat(MtlsPrincipal.of("O=Acme,CN=orders-service", MtlsPrincipal.Mode.CN))
                .isEqualTo("orders-service");
    }

    @Test
    void cnModeFallsBackToDnWhenNoCn() {
        assertThat(MtlsPrincipal.of("O=Acme", MtlsPrincipal.Mode.CN)).isEqualTo("O=Acme");
    }

    @Test
    void strimziKafkaUserSubjectIsJustCn() {
        // Strimzi issues "CN=<user>"; Kafka's principal is "User:CN=<user>" → DN mode matches.
        assertThat(MtlsPrincipal.of("CN=orders-service", MtlsPrincipal.Mode.DN)).isEqualTo("CN=orders-service");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd apicurio-proxy && mvn -q test -Dtest=MtlsPrincipalTest`
Expected: compilation error `cannot find symbol: MtlsPrincipal`

- [ ] **Step 3: Write the class**

```java
package se.afshin.yavari.rbac;

import io.quarkus.security.credential.CertificateCredential;
import io.quarkus.security.identity.SecurityIdentity;

import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import java.security.cert.X509Certificate;

/** Derives the Kafka principal name from an mTLS client certificate. */
public final class MtlsPrincipal {

    /** {@code DN}: RFC 2253 subject (Strimzi KafkaUser default, e.g. {@code CN=orders-service}).
     *  {@code CN}: only the common name, for clusters using {@code ssl.principal.mapping.rules}. */
    public enum Mode { DN, CN }

    /** Attribute tests set on a {@code @TestSecurity} identity to mark it as mTLS. */
    static final String AUTH_ATTRIBUTE = "proxy.auth";
    static final String AUTH_MTLS = "mtls";

    private MtlsPrincipal() {}

    public static String of(X509Certificate cert, Mode mode) {
        return of(cert.getSubjectX500Principal().getName(), mode);
    }

    public static String of(String rfc2253Dn, Mode mode) {
        if (mode == Mode.DN) return rfc2253Dn;
        try {
            for (Rdn rdn : new LdapName(rfc2253Dn).getRdns()) {
                if ("CN".equalsIgnoreCase(rdn.getType())) return rdn.getValue().toString();
            }
        } catch (Exception ignored) {
            // fall through
        }
        return rfc2253Dn;
    }

    /** True when the request was authenticated with a client certificate. */
    public static boolean isCertificateIdentity(SecurityIdentity id) {
        if (id == null || id.isAnonymous()) return false;
        if (id.getCredential(CertificateCredential.class) != null) return true;
        return AUTH_MTLS.equals(id.getAttribute(AUTH_ATTRIBUTE));
    }

    /** Principal for an mTLS identity: from the certificate when present, otherwise the
     *  identity's principal name (test identities). */
    public static String principalOf(SecurityIdentity id, Mode mode) {
        CertificateCredential cc = id.getCredential(CertificateCredential.class);
        if (cc != null && cc.getCertificate() != null) return of(cc.getCertificate(), mode);
        return of(id.getPrincipal().getName(), mode);
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd apicurio-proxy && mvn -q test -Dtest=MtlsPrincipalTest`
Expected: PASS

- [ ] **Step 5: Commit (after asking the user)**

```bash
git add apicurio-proxy/src
git commit -m "feat(apicurio-proxy): mTLS principal extraction (DN|CN)"
```

---

### Task 5: KafkaAclPolicySource — evaluation over an ACL snapshot

**Files:**
- Create: `apicurio-proxy/src/main/java/se/afshin/yavari/rbac/KafkaAclPolicySource.java`
- Test: `apicurio-proxy/src/test/java/se/afshin/yavari/rbac/KafkaAclPolicySourceTest.java`

**Interfaces:**
- Produces: `static String topicForArtifact(String artifact, List<String> suffixes)`; `static boolean evaluate(Collection<AclBinding> acls, String principal, String topic, PolicyEngine.Action action)`; `static Set<AclOperation> grantingOperations(PolicyEngine.Action)`. Instance API (Task 6): `boolean isAllowed(String principal, String artifact, PolicyEngine.Action action)`, `boolean isEnabled()`, `boolean isLoaded()`, `void replaceSnapshot(Collection<AclBinding>)` (package-private, tests).

- [ ] **Step 1: Write the failing test**

```java
package se.afshin.yavari.rbac;

import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static se.afshin.yavari.rbac.KafkaAclPolicySource.evaluate;
import static se.afshin.yavari.rbac.KafkaAclPolicySource.topicForArtifact;
import static se.afshin.yavari.rbac.PolicyEngine.Action.*;

class KafkaAclPolicySourceTest {

    private static final List<String> SUFFIXES = List.of("-value", "-key");
    private static final String SVC = "CN=orders-service";

    private static AclBinding acl(String principal, ResourceType type, String name, PatternType pattern,
                                  AclOperation op, AclPermissionType perm) {
        return new AclBinding(new ResourcePattern(type, name, pattern),
                new AccessControlEntry("User:" + principal, "*", op, perm));
    }

    private static AclBinding allow(String name, PatternType pattern, AclOperation op) {
        return acl(SVC, ResourceType.TOPIC, name, pattern, op, AclPermissionType.ALLOW);
    }

    // ── artifact → topic ──────────────────────────────────────────────────────

    @Test
    void stripsValueAndKeySuffixes() {
        assertThat(topicForArtifact("orders-value", SUFFIXES)).isEqualTo("orders");
        assertThat(topicForArtifact("orders-key", SUFFIXES)).isEqualTo("orders");
    }

    @Test
    void unknownSuffixMapsToSameName() {
        assertThat(topicForArtifact("orders", SUFFIXES)).isEqualTo("orders");
        assertThat(topicForArtifact("orders-schema", SUFFIXES)).isEqualTo("orders-schema");
    }

    @Test
    void wildcardArtifactMapsToWildcardTopic() {
        assertThat(topicForArtifact("*", SUFFIXES)).isEqualTo("*");
    }

    // ── operation table ───────────────────────────────────────────────────────

    @Test
    void writeOnTopicGrantsReadAndWrite() {
        List<AclBinding> acls = List.of(allow("orders", PatternType.LITERAL, AclOperation.WRITE));
        assertThat(evaluate(acls, SVC, "orders", READ)).isTrue();
        assertThat(evaluate(acls, SVC, "orders", WRITE)).isTrue();
        assertThat(evaluate(acls, SVC, "orders", DELETE)).isFalse();
    }

    @Test
    void readOrDescribeOnTopicGrantsReadOnly() {
        assertThat(evaluate(List.of(allow("orders", PatternType.LITERAL, AclOperation.READ)), SVC, "orders", READ)).isTrue();
        assertThat(evaluate(List.of(allow("orders", PatternType.LITERAL, AclOperation.DESCRIBE)), SVC, "orders", READ)).isTrue();
        assertThat(evaluate(List.of(allow("orders", PatternType.LITERAL, AclOperation.READ)), SVC, "orders", WRITE)).isFalse();
    }

    @Test
    void deleteAndAllMapAsExpected() {
        assertThat(evaluate(List.of(allow("orders", PatternType.LITERAL, AclOperation.DELETE)), SVC, "orders", DELETE)).isTrue();
        assertThat(evaluate(List.of(allow("orders", PatternType.LITERAL, AclOperation.DELETE)), SVC, "orders", READ)).isFalse();
        List<AclBinding> all = List.of(allow("orders", PatternType.LITERAL, AclOperation.ALL));
        assertThat(evaluate(all, SVC, "orders", READ)).isTrue();
        assertThat(evaluate(all, SVC, "orders", WRITE)).isTrue();
        assertThat(evaluate(all, SVC, "orders", DELETE)).isTrue();
    }

    // ── pattern matching ──────────────────────────────────────────────────────

    @Test
    void prefixedPatternMatchesTopicsWithThatPrefix() {
        List<AclBinding> acls = List.of(allow("orders", PatternType.PREFIXED, AclOperation.WRITE));
        assertThat(evaluate(acls, SVC, "orders.created", WRITE)).isTrue();
        assertThat(evaluate(acls, SVC, "payments", WRITE)).isFalse();
    }

    @Test
    void literalWildcardNameMatchesEveryTopicIncludingStar() {
        List<AclBinding> acls = List.of(allow("*", PatternType.LITERAL, AclOperation.ALL));
        assertThat(evaluate(acls, SVC, "anything", DELETE)).isTrue();
        assertThat(evaluate(acls, SVC, "*", READ)).isTrue();
    }

    @Test
    void starTopicRequiresWildcardAcl() {
        List<AclBinding> acls = List.of(allow("orders", PatternType.PREFIXED, AclOperation.ALL));
        assertThat(evaluate(acls, SVC, "*", READ)).isFalse();
    }

    @Test
    void wildcardPrincipalApplies() {
        List<AclBinding> acls = List.of(acl("*", ResourceType.TOPIC, "orders", PatternType.LITERAL,
                AclOperation.READ, AclPermissionType.ALLOW));
        assertThat(evaluate(acls, "CN=someone-else", "orders", READ)).isTrue();
    }

    @Test
    void otherPrincipalsAndResourceTypesAreIgnored() {
        List<AclBinding> acls = List.of(
                acl("CN=other", ResourceType.TOPIC, "orders", PatternType.LITERAL, AclOperation.ALL, AclPermissionType.ALLOW),
                acl(SVC, ResourceType.GROUP, "orders", PatternType.LITERAL, AclOperation.ALL, AclPermissionType.ALLOW));
        assertThat(evaluate(acls, SVC, "orders", READ)).isFalse();
    }

    // ── deny precedence ───────────────────────────────────────────────────────

    @Test
    void denyOnOperationBeatsAllow() {
        List<AclBinding> acls = List.of(
                allow("*", PatternType.LITERAL, AclOperation.ALL),
                acl(SVC, ResourceType.TOPIC, "orders", PatternType.LITERAL, AclOperation.WRITE, AclPermissionType.DENY));
        assertThat(evaluate(acls, SVC, "orders", WRITE)).isFalse();
        // READ is still granted through ALL (the DENY targets WRITE only)
        assertThat(evaluate(acls, SVC, "orders", READ)).isTrue();
        assertThat(evaluate(acls, SVC, "payments", WRITE)).isTrue();
    }

    @Test
    void denyAllBeatsEverything() {
        List<AclBinding> acls = List.of(
                allow("orders", PatternType.LITERAL, AclOperation.WRITE),
                acl(SVC, ResourceType.TOPIC, "orders", PatternType.PREFIXED, AclOperation.ALL, AclPermissionType.DENY));
        assertThat(evaluate(acls, SVC, "orders", READ)).isFalse();
        assertThat(evaluate(acls, SVC, "orders", WRITE)).isFalse();
    }

    @Test
    void emptySnapshotDeniesEverything() {
        assertThat(evaluate(List.of(), SVC, "orders", READ)).isFalse();
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd apicurio-proxy && mvn -q test -Dtest=KafkaAclPolicySourceTest`
Expected: compilation error `cannot find symbol: KafkaAclPolicySource`

- [ ] **Step 3: Write the evaluation core (instance/loading parts come in Task 6)**

```java
package se.afshin.yavari.rbac;

import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;

import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * Authorizes mTLS principals from Kafka topic ACLs. Artifact {@code orders-value} maps to
 * topic {@code orders}; the principal's topic permissions decide the artifact actions:
 * READ/DESCRIBE → READ, WRITE → READ+WRITE, DELETE → DELETE, ALL → everything. Evaluation
 * follows Kafka's authorizer: a matching DENY for an operation (or ALL) wins over any
 * ALLOW; LITERAL patterns match exactly (or via the {@code *} wildcard name), PREFIXED
 * patterns match by prefix; principal {@code User:*} matches everyone. Host is ignored.
 */
public class KafkaAclPolicySource {

    static final String WILDCARD = "*";

    /** Maps an artifact id to the topic it belongs to by stripping the first matching suffix. */
    public static String topicForArtifact(String artifact, List<String> suffixes) {
        if (artifact == null || WILDCARD.equals(artifact)) return WILDCARD;
        for (String suffix : suffixes) {
            if (artifact.length() > suffix.length() && artifact.endsWith(suffix)) {
                return artifact.substring(0, artifact.length() - suffix.length());
            }
        }
        return artifact;
    }

    /** Topic operations any one of which grants the artifact action. */
    public static Set<AclOperation> grantingOperations(PolicyEngine.Action action) {
        return switch (action) {
            case READ -> EnumSet.of(AclOperation.READ, AclOperation.DESCRIBE, AclOperation.WRITE, AclOperation.ALL);
            case WRITE -> EnumSet.of(AclOperation.WRITE, AclOperation.ALL);
            case DELETE -> EnumSet.of(AclOperation.DELETE, AclOperation.ALL);
        };
    }

    /** Kafka-style evaluation: for each granting operation, allowed if some ALLOW matches
     *  and no DENY (for that operation or ALL) matches. */
    public static boolean evaluate(Collection<AclBinding> acls, String principal, String topic,
                                   PolicyEngine.Action action) {
        String kafkaPrincipal = "User:" + principal;
        for (AclOperation op : grantingOperations(action)) {
            boolean allowed = false;
            boolean denied = false;
            for (AclBinding b : acls) {
                if (!matches(b, kafkaPrincipal, topic)) continue;
                AccessControlEntry e = b.entry();
                boolean opMatches = e.operation() == op || e.operation() == AclOperation.ALL;
                if (!opMatches) continue;
                if (e.permissionType() == AclPermissionType.DENY) denied = true;
                else if (e.permissionType() == AclPermissionType.ALLOW) allowed = true;
            }
            if (allowed && !denied) return true;
        }
        return false;
    }

    private static boolean matches(AclBinding b, String kafkaPrincipal, String topic) {
        ResourcePattern r = b.pattern();
        if (r.resourceType() != ResourceType.TOPIC) return false;
        String p = b.entry().principal();
        if (!p.equals(kafkaPrincipal) && !p.equals("User:" + WILDCARD)) return false;
        if (r.patternType() == PatternType.LITERAL) {
            return WILDCARD.equals(r.name()) || r.name().equals(topic);
        }
        if (r.patternType() == PatternType.PREFIXED) {
            return !WILDCARD.equals(topic) && topic.startsWith(r.name());
        }
        return false;
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd apicurio-proxy && mvn -q test -Dtest=KafkaAclPolicySourceTest`
Expected: PASS (15 tests)

- [ ] **Step 5: Commit (after asking the user)**

```bash
git add apicurio-proxy/src
git commit -m "feat(apicurio-proxy): Kafka ACL evaluation for artifact actions"
```

---

### Task 6: KafkaAclPolicySource — snapshot loading, refresh and readiness

**Files:**
- Modify: `apicurio-proxy/src/main/java/se/afshin/yavari/rbac/KafkaAclPolicySource.java`
- Modify: `apicurio-proxy/src/main/java/se/afshin/yavari/rbac/PolicyReadinessCheck.java`
- Modify: `apicurio-proxy/src/main/resources/application.properties`
- Test: `apicurio-proxy/src/test/java/se/afshin/yavari/rbac/KafkaAclPolicySourceLoadingTest.java`

**Interfaces:**
- Consumes: `KafkaSslProps.fromEnv/trustFromEnv/apply` (Task 1), `evaluate`/`topicForArtifact` (Task 5).
- Produces: `KafkaAclPolicySource` as an `@ApplicationScoped` bean with `boolean isEnabled()`, `boolean isLoaded()`, `boolean isAllowed(String principal, String artifact, PolicyEngine.Action)`, package-private `void replaceSnapshot(Collection<AclBinding>)`, `static Properties adminProps(Function<String,String> env)`, and a `Supplier<Collection<AclBinding>>` loader seam (`KafkaAclPolicySource(Supplier<Collection<AclBinding>> loader, List<String> suffixes)` package-private constructor for tests).

- [ ] **Step 1: Write the failing test**

```java
package se.afshin.yavari.rbac;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static se.afshin.yavari.rbac.PolicyEngine.Action.WRITE;

class KafkaAclPolicySourceLoadingTest {

    private static final AclBinding ORDERS_WRITE = new AclBinding(
            new ResourcePattern(ResourceType.TOPIC, "orders", PatternType.LITERAL),
            new AccessControlEntry("User:CN=svc", "*", AclOperation.WRITE, AclPermissionType.ALLOW));

    @Test
    void notLoadedUntilFirstSuccessfulRefresh() {
        KafkaAclPolicySource src = new KafkaAclPolicySource(() -> List.of(ORDERS_WRITE), List.of("-value", "-key"));
        assertThat(src.isLoaded()).isFalse();
        assertThat(src.isAllowed("CN=svc", "orders-value", WRITE)).isFalse();
        src.refresh();
        assertThat(src.isLoaded()).isTrue();
        assertThat(src.isAllowed("CN=svc", "orders-value", WRITE)).isTrue();
    }

    @Test
    void failedRefreshKeepsLastSnapshot() {
        AtomicInteger calls = new AtomicInteger();
        KafkaAclPolicySource src = new KafkaAclPolicySource(() -> {
            if (calls.incrementAndGet() > 1) throw new RuntimeException("kafka down");
            return List.of(ORDERS_WRITE);
        }, List.of("-value", "-key"));
        src.refresh();
        src.refresh(); // throws inside, must be swallowed
        assertThat(src.isLoaded()).isTrue();
        assertThat(src.isAllowed("CN=svc", "orders-value", WRITE)).isTrue();
    }

    @Test
    void disabledSourceDeniesAndReportsDisabled() {
        KafkaAclPolicySource src = new KafkaAclPolicySource(null, List.of("-value"));
        assertThat(src.isEnabled()).isFalse();
        assertThat(src.isAllowed("CN=svc", "orders-value", WRITE)).isFalse();
    }

    @Test
    void adminPropsFromEnvWithPkcs12() {
        Map<String, String> env = Map.of(
                "PROXY_KAFKA_BOOTSTRAP", "kafka:9093",
                "PROXY_KAFKA_SSL_KEYSTORE", "/k/user.p12", "PROXY_KAFKA_SSL_KEYSTORE_PASSWORD", "pw",
                "PROXY_KAFKA_SSL_TRUSTSTORE", "/k/ca.p12", "PROXY_KAFKA_SSL_TRUSTSTORE_PASSWORD", "cpw");
        Properties p = KafkaAclPolicySource.adminProps(env::get);
        assertThat(p).containsEntry(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "kafka:9093")
                .containsEntry(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL")
                .containsEntry(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "/k/user.p12")
                .containsEntry(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/k/ca.p12");
    }

    @Test
    void adminPropsPlaintextWhenRequested() {
        Map<String, String> env = Map.of("PROXY_KAFKA_BOOTSTRAP", "kafka:9092",
                "PROXY_KAFKA_SECURITY_PROTOCOL", "PLAINTEXT");
        Properties p = KafkaAclPolicySource.adminProps(env::get);
        assertThat(p).containsEntry(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT")
                .doesNotContainKey(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd apicurio-proxy && mvn -q test -Dtest=KafkaAclPolicySourceLoadingTest`
Expected: compilation errors (`refresh`, constructor, `adminProps` missing)

- [ ] **Step 3: Add the bean lifecycle to KafkaAclPolicySource**

Add these imports and members to `KafkaAclPolicySource` (keep everything from Task 5):

```java
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
```

```java
@ApplicationScoped
public class KafkaAclPolicySource {

    // ... static helpers from Task 5 ...

    @ConfigProperty(name = "proxy.kafka.acl.refresh-seconds", defaultValue = "30")
    long refreshSeconds;

    private Supplier<Collection<AclBinding>> loader;
    private List<String> suffixes;
    private Admin admin;
    private final AtomicReference<List<AclBinding>> snapshot = new AtomicReference<>(null);
    private volatile boolean running = true;
    private Thread refresher;

    /** CDI constructor: wiring happens in {@link #onStart}. */
    public KafkaAclPolicySource() {}

    /** Test seam: explicit loader (null = disabled) and suffix list. */
    KafkaAclPolicySource(Supplier<Collection<AclBinding>> loader, List<String> suffixes) {
        this.loader = loader;
        this.suffixes = suffixes;
    }

    void onStart(@Observes StartupEvent ev,
                 @ConfigProperty(name = "proxy.artifact.suffixes", defaultValue = "-value,-key") String suffixCsv) {
        this.suffixes = Arrays.stream(suffixCsv.split(",")).map(String::trim).filter(s -> !s.isEmpty()).toList();
        String bootstrap = System.getenv("PROXY_KAFKA_BOOTSTRAP");
        if (bootstrap == null || bootstrap.isBlank()) {
            System.out.println("[KafkaAclPolicySource] PROXY_KAFKA_BOOTSTRAP unset — mTLS identities will be denied");
            return;
        }
        this.admin = Admin.create(adminProps(System::getenv));
        this.loader = () -> {
            try {
                return admin.describeAcls(AclBindingFilter.ANY).values().get();
            } catch (Exception e) {
                throw new IllegalStateException("describeAcls failed: " + e.getMessage(), e);
            }
        };
        refresh();
        refresher = new Thread(() -> {
            while (running) {
                try {
                    Thread.sleep(refreshSeconds * 1000L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                refresh();
            }
        }, "kafka-acl-refresh");
        refresher.setDaemon(true);
        refresher.start();
    }

    @PreDestroy
    void onStop() {
        running = false;
        if (refresher != null) refresher.interrupt();
        if (admin != null) admin.close();
    }

    /** Loads a new snapshot; a failure keeps the previous one and logs. */
    void refresh() {
        if (loader == null) return;
        try {
            List<AclBinding> loaded = new ArrayList<>(loader.get());
            snapshot.set(loaded);
            System.out.println("[KafkaAclPolicySource] Loaded " + loaded.size() + " ACL bindings");
        } catch (Exception e) {
            System.err.println("[KafkaAclPolicySource] ACL refresh failed, keeping "
                    + (snapshot.get() == null ? "nothing" : snapshot.get().size() + " bindings") + ": " + e.getMessage());
        }
    }

    void replaceSnapshot(Collection<AclBinding> acls) {
        snapshot.set(new ArrayList<>(acls));
    }

    public boolean isEnabled() { return loader != null; }

    public boolean isLoaded() { return snapshot.get() != null; }

    public boolean isAllowed(String principal, String artifact, PolicyEngine.Action action) {
        List<AclBinding> acls = snapshot.get();
        if (acls == null) return false;
        return evaluate(acls, principal, topicForArtifact(artifact, suffixes), action);
    }

    /** Admin client config from {@code PROXY_KAFKA_*} (bootstrap, protocol, PKCS12/JKS/PEM stores). */
    static Properties adminProps(Function<String, String> env) {
        Properties p = new Properties();
        p.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, env.apply("PROXY_KAFKA_BOOTSTRAP"));
        p.put(CommonClientConfigs.CLIENT_ID_CONFIG, "apicurio-rbac-proxy-acl");
        p.put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, 15_000);
        p.put(CommonClientConfigs.DEFAULT_API_TIMEOUT_MS_CONFIG, 20_000);
        String protocol = Optional.ofNullable(env.apply("PROXY_KAFKA_SECURITY_PROTOCOL")).filter(s -> !s.isBlank()).orElse("SSL");
        p.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, protocol);
        if ("SSL".equalsIgnoreCase(protocol)) {
            KafkaSslProps.apply(p, KafkaSslProps.fromEnv(env, "PROXY_KAFKA_SSL"),
                    KafkaSslProps.trustFromEnv(env, "PROXY_KAFKA_SSL"));
        }
        return p;
    }
}
```

Note the CDI `onStart` observer injects `proxy.artifact.suffixes` as a method parameter; if CDI rejects the `@ConfigProperty` parameter on an observer, make it a field with `@ConfigProperty(name = "proxy.artifact.suffixes", defaultValue = "-value,-key") String suffixCsv;` and read it in `onStart`.

`application.properties` — append:

```properties
# Kafka-ACL authorization for mTLS identities (Task 6). Enabled when PROXY_KAFKA_BOOTSTRAP is set.
proxy.kafka.acl.refresh-seconds=${PROXY_KAFKA_ACL_REFRESH_SECONDS:30}
proxy.artifact.suffixes=${PROXY_ARTIFACT_SUFFIXES:-value,-key}
proxy.mtls.principal=${PROXY_MTLS_PRINCIPAL:DN}
```

`PolicyReadinessCheck` — also require the ACL snapshot when the source is enabled:

```java
    @Inject PolicyEngine engine;
    @Inject KafkaAclPolicySource acls;

    @Override
    public HealthCheckResponse call() {
        boolean up = engine.isInitialized() && (!acls.isEnabled() || acls.isLoaded());
        return up ? HealthCheckResponse.up("policy-loaded") : HealthCheckResponse.down("policy-loaded");
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd apicurio-proxy && mvn -q test`
Expected: PASS (the `@QuarkusTest` boots with `PROXY_KAFKA_BOOTSTRAP` unset → source disabled, readiness unaffected).

- [ ] **Step 5: Commit (after asking the user)**

```bash
git add apicurio-proxy/src
git commit -m "feat(apicurio-proxy): Kafka ACL snapshot loading with periodic refresh and readiness"
```

---

### Task 7: PolicyEngine dispatch (OIDC → roles, mTLS → ACLs) and ProxyResource wiring

**Files:**
- Modify: `apicurio-proxy/src/main/java/se/afshin/yavari/rbac/PolicyEngine.java`
- Modify: `apicurio-proxy/src/main/java/se/afshin/yavari/rbac/ProxyResource.java:82-100` (`proxy(...)`) and `principalOf`
- Test: `apicurio-proxy/src/test/java/se/afshin/yavari/rbac/PolicyEngineDispatchTest.java`
- Test: `apicurio-proxy/src/test/java/se/afshin/yavari/rbac/ProxyResourceTest.java` (append)

**Interfaces:**
- Consumes: `MtlsPrincipal.isCertificateIdentity/principalOf` (Task 4), `KafkaAclPolicySource.isAllowed` (Task 6).
- Produces: `PolicyEngine.isAllowed(SecurityIdentity identity, String artifact, Action action)`; `PolicyEngine.principalOf(SecurityIdentity)` returning `user:<name>` (mTLS name per mode) or `anonymous`.

- [ ] **Step 1: Write the failing tests**

`PolicyEngineDispatchTest.java` (plain JUnit, builds identities with `QuarkusSecurityIdentity`):

```java
package se.afshin.yavari.rbac;

import io.quarkus.security.runtime.QuarkusPrincipal;
import io.quarkus.security.runtime.QuarkusSecurityIdentity;
import io.quarkus.security.identity.SecurityIdentity;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static se.afshin.yavari.rbac.PolicyEngine.Action.*;

class PolicyEngineDispatchTest {

    private PolicyEngine engine;
    private KafkaAclPolicySource acls;

    @BeforeEach
    void setUp() throws Exception {
        Path policy = Files.createTempFile("policy", ".yaml");
        Files.writeString(policy, """
            rules:
              - roles: [orders-team]
                resources:
                  - artifact: orders-value
                    actions: [READ, WRITE]
            """);
        engine = new PolicyEngine();
        engine.reload(policy);
        acls = new KafkaAclPolicySource(() -> List.of(new AclBinding(
                new ResourcePattern(ResourceType.TOPIC, "orders", PatternType.LITERAL),
                new AccessControlEntry("User:CN=orders-service", "*", AclOperation.WRITE, AclPermissionType.ALLOW))),
                List.of("-value", "-key"));
        acls.refresh();
        engine.acls = acls;
        engine.principalMode = MtlsPrincipal.Mode.DN;
    }

    private static SecurityIdentity oidc(String user, String... roles) {
        return QuarkusSecurityIdentity.builder().setPrincipal(new QuarkusPrincipal(user)).addRoles(Set.of(roles)).build();
    }

    private static SecurityIdentity mtls(String dn) {
        return QuarkusSecurityIdentity.builder().setPrincipal(new QuarkusPrincipal(dn))
                .addAttribute(MtlsPrincipal.AUTH_ATTRIBUTE, MtlsPrincipal.AUTH_MTLS).build();
    }

    @Test
    void oidcIdentityUsesRoleRules() {
        assertThat(engine.isAllowed(oidc("alice", "orders-team"), "orders-value", WRITE)).isTrue();
        assertThat(engine.isAllowed(oidc("alice", "orders-team"), "payments-value", READ)).isFalse();
    }

    @Test
    void oidcIdentityNeverConsultsAcls() {
        // "CN=orders-service" as an OIDC user with no roles: ACLs would allow, roles don't.
        assertThat(engine.isAllowed(oidc("CN=orders-service"), "orders-value", WRITE)).isFalse();
    }

    @Test
    void mtlsIdentityUsesKafkaAcls() {
        assertThat(engine.isAllowed(mtls("CN=orders-service"), "orders-value", WRITE)).isTrue();
        assertThat(engine.isAllowed(mtls("CN=orders-service"), "orders-key", READ)).isTrue();
        assertThat(engine.isAllowed(mtls("CN=orders-service"), "payments-value", READ)).isFalse();
    }

    @Test
    void mtlsIdentityNeverConsultsRoleFile() {
        SecurityIdentity withRole = QuarkusSecurityIdentity.builder()
                .setPrincipal(new QuarkusPrincipal("CN=nobody")).addRole("orders-team")
                .addAttribute(MtlsPrincipal.AUTH_ATTRIBUTE, MtlsPrincipal.AUTH_MTLS).build();
        assertThat(engine.isAllowed(withRole, "orders-value", WRITE)).isFalse();
    }

    @Test
    void mtlsPrincipalRespectsCnMode() {
        engine.principalMode = MtlsPrincipal.Mode.CN;
        acls.replaceSnapshot(List.of(new AclBinding(
                new ResourcePattern(ResourceType.TOPIC, "orders", PatternType.LITERAL),
                new AccessControlEntry("User:orders-service", "*", AclOperation.READ, AclPermissionType.ALLOW))));
        assertThat(engine.isAllowed(mtls("CN=orders-service,O=Acme"), "orders-value", READ)).isTrue();
        assertThat(PolicyEngine.principalOf(mtls("CN=orders-service,O=Acme"), MtlsPrincipal.Mode.CN))
                .isEqualTo("user:orders-service");
    }
}
```

Append to `ProxyResourceTest.java` (inside the class; add imports `io.quarkus.test.security.SecurityAttribute`, `jakarta.inject.Inject`, and the kafka acl imports used below):

```java
    // ── mTLS identities are authorized from Kafka ACLs ────────────────────────

    @Inject KafkaAclPolicySource aclSource;

    private void aclSnapshot(AclBinding... bindings) {
        aclSource.replaceSnapshot(java.util.List.of(bindings));
    }

    private static AclBinding topicAcl(String principal, String topic, AclOperation op, AclPermissionType perm) {
        return new AclBinding(new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL),
                new AccessControlEntry("User:" + principal, "*", op, perm));
    }

    @Test
    @TestSecurity(user = "CN=orders-service", attributes = @SecurityAttribute(key = "proxy.auth", value = "mtls"))
    void mtlsServiceWithWriteAclPassesPolicyForItsSubjects() {
        aclSnapshot(topicAcl("CN=orders-service", "orders", AclOperation.WRITE, AclPermissionType.ALLOW));
        int status = given().body("{}").contentType("application/json")
            .post("/apis/registry/v2/groups/default/artifacts?ifExists=RETURN_OR_UPDATE")
            .then().extract().statusCode();
        // "*" artifact on create-without-id → needs wildcard; use the explicit artifact path instead
        int put = given().body("{}").contentType("application/json")
            .put("/apis/registry/v2/groups/default/artifacts/orders-value")
            .then().extract().statusCode();
        assertThat(put).isNotEqualTo(403);
        assertThat(status).isEqualTo(403);
    }

    @Test
    @TestSecurity(user = "CN=orders-service", attributes = @SecurityAttribute(key = "proxy.auth", value = "mtls"))
    void mtlsServiceIsBlockedFromOtherTopicsSubjects() {
        aclSnapshot(topicAcl("CN=orders-service", "orders", AclOperation.WRITE, AclPermissionType.ALLOW));
        given().get("/apis/registry/v2/groups/default/artifacts/invoices-value").then().statusCode(403);
    }

    @Test
    @TestSecurity(user = "CN=orders-service", attributes = @SecurityAttribute(key = "proxy.auth", value = "mtls"))
    void mtlsServiceWithDenyIsRefused() {
        aclSnapshot(topicAcl("*", "*", AclOperation.ALL, AclPermissionType.ALLOW),
                    topicAcl("CN=orders-service", "orders", AclOperation.ALL, AclPermissionType.DENY));
        given().get("/apis/registry/v2/groups/default/artifacts/orders-value").then().statusCode(403);
    }

    @Test
    @TestSecurity(user = "CN=orders-service", attributes = @SecurityAttribute(key = "proxy.auth", value = "mtls"))
    void mtlsServiceIgnoresRoleFile() {
        aclSnapshot(); // no ACLs at all
        given().get("/apis/registry/v2/groups/default/artifacts/orders").then().statusCode(403);
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd apicurio-proxy && mvn -q test -Dtest='PolicyEngineDispatchTest,ProxyResourceTest'`
Expected: compilation errors (`engine.acls`, `engine.principalMode`, `isAllowed(SecurityIdentity, …)`, `principalOf(…, Mode)` missing)

- [ ] **Step 3: Implement the dispatch**

In `PolicyEngine` add:

```java
import io.quarkus.security.identity.SecurityIdentity;
import jakarta.inject.Inject;
```

```java
    @Inject
    KafkaAclPolicySource acls;

    @ConfigProperty(name = "proxy.mtls.principal", defaultValue = "DN")
    MtlsPrincipal.Mode principalMode;

    /**
     * Either/or dispatch: a certificate-authenticated identity is judged only by Kafka
     * ACLs; any other (OIDC) identity only by the role rules. No fallback between them.
     */
    public boolean isAllowed(SecurityIdentity identity, String artifact, Action action) {
        if (MtlsPrincipal.isCertificateIdentity(identity)) {
            if (acls == null || !acls.isEnabled()) return false;
            return acls.isAllowed(MtlsPrincipal.principalOf(identity, principalMode), artifact, action);
        }
        return isAllowed(identity.getRoles(), artifact, action);
    }

    /** Identity → "user:<name>" (certificate subject per mode for mTLS) or "anonymous". */
    public static String principalOf(SecurityIdentity identity, MtlsPrincipal.Mode mode) {
        if (identity == null || identity.isAnonymous()) return "anonymous";
        if (MtlsPrincipal.isCertificateIdentity(identity)) {
            return "user:" + MtlsPrincipal.principalOf(identity, mode);
        }
        return identity.getPrincipal() != null && identity.getPrincipal().getName() != null
                ? "user:" + identity.getPrincipal().getName() : "anonymous";
    }
```

MicroProfile Config converts `DN`/`CN` to the enum automatically. In `ProxyResource.proxy(...)` replace the role lookup and the two uses:

```java
        String artifact      = resolveArtifact(fullPath, uriInfo.getRequestUri().getRawQuery());
        PolicyEngine.Action action = resolveAction(method, fullPath);
        // ...
            if (!policy.isAllowed(identity, artifact, action)) {
        // ...
            audit.emit(new AuditEvent(Instant.now(), PolicyEngine.principalOf(identity, policy.principalMode),
                    action.name(), artifact, decision, latencyMs, MDC.get("correlationId")));
```

Delete the now-unused `roles` local and the old `ProxyResource.principalOf` (move any callers to `PolicyEngine.principalOf`).

- [ ] **Step 4: Run all tests to verify they pass**

Run: `cd apicurio-proxy && mvn -q test`
Expected: PASS. If `@TestSecurity` `attributes` is not on the classpath, confirm `quarkus-test-security` is in the pom (it is) and the import is `io.quarkus.test.security.SecurityAttribute`.

- [ ] **Step 5: Commit (after asking the user)**

```bash
git add apicurio-proxy/src
git commit -m "feat(apicurio-proxy): dispatch OIDC identities to role rules, mTLS identities to Kafka ACLs"
```

---

### Task 8: Strimzi sidecar manifests, README, Dockerfile verification, docs pointer

**Files:**
- Create: `apicurio-proxy/strimzi/apicurio-with-proxy-deployment.yaml`
- Create: `apicurio-proxy/strimzi/kafkauser.yaml`
- Create: `apicurio-proxy/strimzi/README.md`
- Modify: `apicurio-proxy/Dockerfile` (expose 8443 too)
- Modify: `docs/security.md` (one paragraph + link) and `README.md` tree line for `apicurio-proxy/`

**Interfaces:** none (deployment artifacts).

- [ ] **Step 1: Write the manifests**

`apicurio-with-proxy-deployment.yaml`:

```yaml
# Apicurio Registry with the RBAC proxy as a sidecar. Only the proxy's TLS port is exposed;
# Apicurio listens on localhost:8080 inside the pod.
#
# Secrets expected in the namespace:
#   apicurio-proxy-server-tls    server.p12 + password   (proxy's own server certificate)
#   my-cluster-clients-ca-cert   ca.p12 + ca.password    (Strimzi clients CA — trust for client certs)
#   apicurio-proxy-kafka         user.p12 + user.password (Strimzi KafkaUser secret for the proxy)
#   my-cluster-cluster-ca-cert   ca.p12 + ca.password    (Strimzi cluster CA — trust for Kafka)
#   apicurio-proxy-policy        policy.yaml             (role rules for OIDC users)
apiVersion: apps/v1
kind: Deployment
metadata:
  name: apicurio-registry
  namespace: kafka
spec:
  replicas: 1
  selector:
    matchLabels: { app: apicurio-registry }
  template:
    metadata:
      labels: { app: apicurio-registry }
    spec:
      containers:
        - name: registry
          image: quay.io/apicurio/apicurio-registry-kafkasql:2.6.5.Final
          env:
            - { name: QUARKUS_HTTP_HOST, value: "127.0.0.1" }   # not reachable from outside the pod
            - { name: QUARKUS_HTTP_PORT, value: "8080" }
            - { name: KAFKA_BOOTSTRAP_SERVERS, value: my-cluster-kafka-bootstrap:9093 }
            # ... your existing kafkasql TLS settings ...
        - name: rbac-proxy
          image: registry.example.com/apicurio-rbac-proxy:1.0
          ports:
            - { name: https, containerPort: 8443 }
          env:
            - { name: APICURIO_URL, value: "http://127.0.0.1:8080" }
            - { name: XML_SCHEMA_URL, value: "http://127.0.0.1:8080" }
            - { name: POLICY_FILE, value: /opt/rbac/policy.yaml }
            # OIDC for humans
            - { name: OIDC_ISSUER_URL, value: https://keycloak.example.com/realms/kafka }
            - { name: OIDC_ROLE_CLAIM, value: realm_access/roles }
            # TLS listener + optional client certs (mTLS for services)
            - { name: PROXY_TLS_ENABLED, value: "true" }
            - { name: PROXY_TLS_PORT, value: "8443" }
            - { name: PROXY_TLS_KEYSTORE, value: /tls/server/server.p12 }
            - name: PROXY_TLS_KEYSTORE_PASSWORD
              valueFrom: { secretKeyRef: { name: apicurio-proxy-server-tls, key: password } }
            - { name: PROXY_TLS_TRUSTSTORE, value: /tls/clients-ca/ca.p12 }
            - name: PROXY_TLS_TRUSTSTORE_PASSWORD
              valueFrom: { secretKeyRef: { name: my-cluster-clients-ca-cert, key: ca.password } }
            - { name: PROXY_MTLS_PRINCIPAL, value: DN }          # Strimzi KafkaUser → "CN=<name>"
            # Kafka ACL source (the proxy's own KafkaUser, needs Describe on Cluster)
            - { name: PROXY_KAFKA_BOOTSTRAP, value: my-cluster-kafka-bootstrap:9093 }
            - { name: PROXY_KAFKA_SSL_KEYSTORE, value: /tls/kafka-user/user.p12 }
            - name: PROXY_KAFKA_SSL_KEYSTORE_PASSWORD
              valueFrom: { secretKeyRef: { name: apicurio-proxy-kafka, key: user.password } }
            - { name: PROXY_KAFKA_SSL_TRUSTSTORE, value: /tls/cluster-ca/ca.p12 }
            - name: PROXY_KAFKA_SSL_TRUSTSTORE_PASSWORD
              valueFrom: { secretKeyRef: { name: my-cluster-cluster-ca-cert, key: ca.password } }
            - { name: PROXY_KAFKA_ACL_REFRESH_SECONDS, value: "30" }
            - { name: PROXY_ARTIFACT_SUFFIXES, value: "-value,-key" }
          volumeMounts:
            - { name: server-tls,  mountPath: /tls/server,     readOnly: true }
            - { name: clients-ca,  mountPath: /tls/clients-ca, readOnly: true }
            - { name: kafka-user,  mountPath: /tls/kafka-user, readOnly: true }
            - { name: cluster-ca,  mountPath: /tls/cluster-ca, readOnly: true }
            - { name: policy,      mountPath: /opt/rbac,       readOnly: true }
          readinessProbe:
            httpGet: { path: /q/health/ready, port: 8443, scheme: HTTPS }
            initialDelaySeconds: 5
          livenessProbe:
            httpGet: { path: /q/health/live, port: 8443, scheme: HTTPS }
            initialDelaySeconds: 15
      volumes:
        - { name: server-tls, secret: { secretName: apicurio-proxy-server-tls } }
        - { name: clients-ca, secret: { secretName: my-cluster-clients-ca-cert } }
        - { name: kafka-user, secret: { secretName: apicurio-proxy-kafka } }
        - { name: cluster-ca, secret: { secretName: my-cluster-cluster-ca-cert } }
        - { name: policy,     secret: { secretName: apicurio-proxy-policy } }
---
apiVersion: v1
kind: Service
metadata:
  name: apicurio-registry
  namespace: kafka
spec:
  selector: { app: apicurio-registry }
  ports:
    - { name: https, port: 8443, targetPort: 8443 }
```

`kafkauser.yaml`:

```yaml
# The proxy's own Kafka identity: reads ACLs via describeAcls, which needs Describe on Cluster.
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaUser
metadata:
  name: apicurio-proxy-kafka
  namespace: kafka
  labels:
    strimzi.io/cluster: my-cluster
spec:
  authentication:
    type: tls
  authorization:
    type: simple
    acls:
      - resource:
          type: cluster
        operations: [Describe]
```

`README.md`:

````markdown
# Apicurio RBAC proxy as a Strimzi sidecar

Humans authenticate with OIDC bearer tokens and are authorized by `policy.yaml` (roles →
artifacts). Services authenticate with mTLS client certificates and are authorized by their
**Kafka ACLs**: WRITE on topic `orders` ⇒ READ+WRITE on artifacts `orders-key` / `orders-value`;
READ/DESCRIBE ⇒ READ; DELETE ⇒ DELETE; ALL ⇒ everything. DENY wins, PREFIXED and `*` patterns
work as in Kafka. The two paths never mix.

## Build

```bash
cd apicurio-proxy
mvn -q package -DskipTests
docker build -t registry.example.com/apicurio-rbac-proxy:1.0 .
docker push registry.example.com/apicurio-rbac-proxy:1.0
```

## Secrets

| Secret | Keys | Source |
|---|---|---|
| `apicurio-proxy-server-tls` | `server.p12`, `password` | Your PKI / cert-manager (server cert for the proxy's hostname) |
| `my-cluster-clients-ca-cert` | `ca.p12`, `ca.password` | Created by Strimzi — trust anchor for client certificates |
| `apicurio-proxy-kafka` | `user.p12`, `user.password` | Created by Strimzi from `kafkauser.yaml` |
| `my-cluster-cluster-ca-cert` | `ca.p12`, `ca.password` | Created by Strimzi — trust anchor for Kafka |
| `apicurio-proxy-policy` | `policy.yaml` | `kubectl create secret generic apicurio-proxy-policy --from-file=policy.yaml` |

All stores can be JKS (`*_TYPE=JKS`) or PEM (`*_TYPE=PEM` with `PROXY_TLS_CERT`/`PROXY_TLS_KEY`/`PROXY_TLS_CA`,
`PROXY_KAFKA_SSL_CERT`/`_KEY`/`_CA`; PEM keys must be PKCS#8).

## Apply and verify

```bash
kubectl apply -f kafkauser.yaml
kubectl apply -f apicurio-with-proxy-deployment.yaml
kubectl -n kafka logs deploy/apicurio-registry -c rbac-proxy | grep -E 'PolicyEngine|KafkaAclPolicySource'
# Service with a KafkaUser cert (Strimzi secret "orders-service"):
kubectl -n kafka get secret orders-service -o jsonpath='{.data.user\.p12}' | base64 -d > user.p12
curl --cert-type P12 --cert user.p12:"$(kubectl -n kafka get secret orders-service -o jsonpath='{.data.user\.password}' | base64 -d)" \
     --cacert ca.crt https://apicurio-registry.kafka.svc:8443/apis/registry/v2/groups/default/artifacts/orders-value
# Human with a token:
curl -H "Authorization: Bearer $TOKEN" --cacert ca.crt https://apicurio-registry.kafka.svc:8443/apis/registry/v2/search/artifacts
```

Readiness is down until the policy file is loaded and (when `PROXY_KAFKA_BOOTSTRAP` is set) the
first ACL snapshot has been fetched. Every decision is audited as one JSON line on stdout
(`principal` is `user:CN=…` for mTLS, `user:<oidc name>` for OIDC).

## Environment reference

| Variable | Default | Meaning |
|---|---|---|
| `PROXY_TLS_ENABLED` | `false` | Enable the HTTPS listener with optional client certificates. |
| `PROXY_TLS_PORT` | `8443` | HTTPS port. Plain HTTP is disabled when TLS is enabled. |
| `PROXY_TLS_KEYSTORE`, `_PASSWORD`, `_TYPE` | — / PKCS12 | Server identity. PEM: `PROXY_TLS_CERT` + `PROXY_TLS_KEY`. |
| `PROXY_TLS_TRUSTSTORE`, `_PASSWORD`, `_TYPE` | — / PKCS12 | CAs client certificates must chain to. PEM: `PROXY_TLS_CA`. |
| `PROXY_MTLS_PRINCIPAL` | `DN` | `DN` (RFC 2253 subject, Strimzi default) or `CN`. |
| `PROXY_KAFKA_BOOTSTRAP` | — | Enables the Kafka ACL source. |
| `PROXY_KAFKA_SECURITY_PROTOCOL` | `SSL` | `SSL` or `PLAINTEXT`. |
| `PROXY_KAFKA_SSL_KEYSTORE`, `_PASSWORD`, `_TYPE` | — / PKCS12 | Proxy's Kafka client identity. PEM: `PROXY_KAFKA_SSL_CERT` + `_KEY`. |
| `PROXY_KAFKA_SSL_TRUSTSTORE`, `_PASSWORD`, `_TYPE` | — / PKCS12 | Kafka cluster CA. PEM: `PROXY_KAFKA_SSL_CA`. |
| `PROXY_KAFKA_ACL_REFRESH_SECONDS` | `30` | ACL snapshot refresh interval. |
| `PROXY_ARTIFACT_SUFFIXES` | `-value,-key` | Suffixes stripped from an artifact id to find its topic. |
| `OIDC_ISSUER_URL`, `OIDC_ROLE_CLAIM`, `POLICY_FILE`, `APICURIO_URL`, `XML_SCHEMA_URL`, `KAFKA_AUDIT_*` | as before | Unchanged. |
````

`Dockerfile`: change `EXPOSE 8082` to `EXPOSE 8082 8443`.

- [ ] **Step 2: Verify the image builds and the manifests parse**

Run:
```bash
cd apicurio-proxy && mvn -q package -DskipTests && docker build -q -t apicurio-rbac-proxy:test . && docker rmi apicurio-rbac-proxy:test
python3 -c "import yaml; list(yaml.safe_load_all(open('strimzi/apicurio-with-proxy-deployment.yaml'))); yaml.safe_load(open('strimzi/kafkauser.yaml')); print('yaml ok')"
```
Expected: image builds; `yaml ok`.

- [ ] **Step 3: Docs pointer**

In `docs/security.md`, under the Apicurio RBAC proxy section, add:

> **mTLS + Kafka-ACL mode (branch `feat/apicurio-proxy-mtls-acl`, not merged):** the proxy can also terminate TLS, accept client certificates alongside OIDC, and authorize certificate identities from Kafka topic ACLs (`orders-value` ⇒ topic `orders`). See `apicurio-proxy/strimzi/README.md`.

In `README.md`'s tree, extend the `apicurio-proxy/` lines with `strimzi/  # sidecar Deployment + KafkaUser + README for Strimzi (mTLS + Kafka-ACL mode)`.

- [ ] **Step 4: Commit (after asking the user)**

```bash
git add apicurio-proxy/Dockerfile apicurio-proxy/strimzi docs/security.md README.md
git commit -m "docs(apicurio-proxy): Strimzi sidecar manifests + README for mTLS/Kafka-ACL mode"
```

---

## Self-review

- **Spec coverage:** TLS listener + optional client cert (Task 2, 3); mTLS principal DN/CN (Task 4); either/or dispatch (Task 7); ACL snapshot, refresh, readiness, `PROXY_KAFKA_*` (Task 6); ACL semantics and operation table (Task 5); PKCS12/JKS/PEM everywhere (Tasks 1, 3, 6); audit inlined and standalone build (Task 1); sidecar manifests + README (Task 8); tests as listed in the spec (Tasks 3–7). Manual HTTPS smoke in Task 3 covers the listener since `@QuarkusTest` runs without TLS.
- **Placeholders:** none; every step has code or an exact command.
- **Type consistency:** `PolicyEngine.Action` used throughout; `KafkaSslProps.Store` record fields `(type, path, password, certPath, keyPath, caPath)` consistent between Tasks 1, 3, 6; `MtlsPrincipal.AUTH_ATTRIBUTE`/`AUTH_MTLS` used identically in Tasks 4 and 7; `KafkaAclPolicySource(Supplier, List)` constructor and `refresh()`/`replaceSnapshot()` consistent between Tasks 6 and 7.
