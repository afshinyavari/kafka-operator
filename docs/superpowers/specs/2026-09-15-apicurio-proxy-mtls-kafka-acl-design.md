# Apicurio RBAC proxy: mTLS authentication + Kafka-ACL authorization

**Date:** 2026-09-15
**Module:** `apicurio-proxy/`
**Branch:** `feat/apicurio-proxy-mtls-acl` (never merged to `main`; the module is lifted into
the user's Strimzi environment)
**Status:** approved (chat), implementing

## Goal

Run the existing `apicurio-rbac-proxy` as a **sidecar next to an existing Apicurio Registry**
in a Strimzi-managed environment where:

- **humans** authenticate with **OIDC bearer tokens** and are authorized by today's
  role → artifact policy file (unchanged), and
- **services** authenticate with **mTLS client certificates** and are authorized by the
  **Kafka ACLs** of the certificate's principal: WRITE on topic `orders` ⇒ may create/edit
  artifacts `orders-key` and `orders-value`.

Both mechanisms are active at the same time. There is no cross-mapping (OIDC users are
never checked against Kafka ACLs; mTLS principals never use the role file).

The module must build standalone (no dependency on the rest of this repo) and accept
PKCS12 / JKS / PEM key material everywhere.

## Authentication

Quarkus (upgraded 3.10 → **3.15 LTS** for the TLS registry) terminates TLS itself:

| Env | Meaning |
|---|---|
| `PROXY_TLS_KEYSTORE` / `PROXY_TLS_KEYSTORE_PASSWORD` / `PROXY_TLS_KEYSTORE_TYPE` | Server identity. Type `PKCS12` (default), `JKS` or `PEM`. With `PEM`, `PROXY_TLS_CERT` + `PROXY_TLS_KEY` (PKCS#8) are used instead. |
| `PROXY_TLS_TRUSTSTORE` / `PROXY_TLS_TRUSTSTORE_PASSWORD` / `PROXY_TLS_TRUSTSTORE_TYPE` | CA(s) that client certificates must chain to. With `PEM`, `PROXY_TLS_CA` (bundle) is used instead. |
| `PROXY_TLS_ENABLED` | `true` enables the HTTPS listener with `client-auth=request`; `false` (default) keeps today's plain-HTTP, OIDC-only behaviour. |

With `client-auth=request` a certificate is optional: a request carrying
`Authorization: Bearer …` is authenticated by OIDC as today; a request carrying a trusted
client certificate is authenticated by mTLS; a request with neither gets 401.
`quarkus.http.insecure-requests=disabled` when TLS is enabled.

### mTLS principal

`PROXY_MTLS_PRINCIPAL=dn|cn` (default `dn`): the certificate subject in RFC 2253 form
(`CN=orders-service`), which is what a Strimzi `KafkaUser` with TLS authentication gets as
its Kafka principal; `cn` yields only the CN value for clusters using
`ssl.principal.mapping.rules`. The audit log records `user:<principal>`.

## Authorization

`PolicyEngine.isAllowed(identity, artifact, action)` dispatches on how the identity was
established:

- identity carries a JWT (OIDC) → existing role rules from `proxy.policy.file`;
- identity carries a client certificate (mTLS) → `KafkaAclPolicySource`.

No fallback between the two.

### KafkaAclPolicySource

- Holds a snapshot of all ACL bindings from `Admin.describeAcls(AclBindingFilter.ANY)`,
  refreshed every `PROXY_KAFKA_ACL_REFRESH_SECONDS` (default 30) on a daemon thread. A
  failed refresh keeps the last snapshot and logs; the readiness check reports not-ready
  until the first successful load.
- Artifact → topic: strip the first matching suffix from `PROXY_ARTIFACT_SUFFIXES`
  (default `-value,-key`); an artifact without a known suffix maps to a topic of the same
  name. Artifact `*` maps to topic `*` (registry-wide operations need ALL on `*`).
- Topic operation → artifact action:

  | Kafka operation on the topic | Artifact actions granted |
  |---|---|
  | READ, DESCRIBE | READ |
  | WRITE | READ, WRITE |
  | DELETE | DELETE |
  | ALL | READ, WRITE, DELETE |

- Evaluation follows Kafka: a matching **DENY** for the required operation (or ALL) wins;
  otherwise any matching ALLOW grants. Resource patterns: `LITERAL` (exact, or the `*`
  wildcard name), `PREFIXED`. Principal match: exact `User:<principal>` or `User:*`. Host
  is ignored (proxy cannot see the client's Kafka host). Only `TOPIC` resource type is
  consulted.

### Kafka connection

| Env | Meaning |
|---|---|
| `PROXY_KAFKA_BOOTSTRAP` | Required to enable the ACL source. |
| `PROXY_KAFKA_SSL_KEYSTORE` / `_PASSWORD` / `_TYPE` | Client identity (PKCS12 default, JKS, PEM). With `PEM`: `PROXY_KAFKA_SSL_CERT` + `PROXY_KAFKA_SSL_KEY`. |
| `PROXY_KAFKA_SSL_TRUSTSTORE` / `_PASSWORD` / `_TYPE` | Cluster CA. With `PEM`: `PROXY_KAFKA_SSL_CA`. |
| `PROXY_KAFKA_SECURITY_PROTOCOL` | `SSL` (default), `PLAINTEXT`. |

The proxy's own `KafkaUser` needs `Describe` on `Cluster`. When `PROXY_KAFKA_BOOTSTRAP` is
unset, mTLS identities are denied (there is nothing to authorize them against) and a
startup warning is logged.

## Portability

- The audit classes used by the proxy (`AuditEmitter`, `AuditEmitters`, `AuditEvent`,
  `AuditEventJson`, `StdoutAuditEmitter`, `KafkaAuditEmitter`, `CompositeAuditEmitter`)
  are copied into `apicurio-proxy/src/main/java/se/afshin/yavari/rbac/audit/`; the
  `kroxy-filters` dependency is removed. Kroxylicious-specific filter classes are not
  copied. The audit Kafka sink also accepts PKCS12/JKS via `KAFKA_AUDIT_TLS_KEYSTORE*`.
- `apicurio-proxy/strimzi/` gains: `apicurio-with-proxy-deployment.yaml` (Apicurio +
  proxy sidecar on `localhost:8080`, Service exposing the proxy's TLS port), `kafkauser.yaml`
  (proxy's `KafkaUser` with `Describe` on `Cluster`), `README.md` (build, secrets in PKCS12
  form from Strimzi's `user.p12` / `ca.p12`, verification).

## Testing

- `KafkaAclPolicySourceTest`: pure unit tests over an injected ACL snapshot — literal,
  prefixed, wildcard name, `User:*`, DENY precedence, suffix stripping, unknown suffix,
  `*` artifact, operation → action table.
- `MtlsPrincipalTest`: DN vs CN extraction from a keytool-generated certificate.
- `PolicyEngineTest` (existing) + new either/or tests: a JWT identity never consults ACLs,
  a certificate identity never consults the role file.
- `ProxyResourceTest` (existing, `@TestSecurity`) stays green; add one test that an mTLS
  identity with a matching ACL is allowed and one with a DENY is refused, using a stubbed
  ACL snapshot.
- Build verified with `mvn package` and the Dockerfile.

## Out of scope

- OIDC users evaluated against Kafka ACLs; group/role ACLs; writing ACLs.
- Any operator change; this branch is not merged to `main`.
