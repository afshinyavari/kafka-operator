# Apicurio RBAC proxy as a Strimzi sidecar

Humans authenticate with OIDC bearer tokens and are authorized by `policy.yaml` (roles →
artifacts). Services authenticate with mTLS client certificates and are authorized by their
**Kafka ACLs**: WRITE on topic `orders` ⇒ READ+WRITE on artifacts `orders-key` / `orders-value`;
READ/DESCRIBE ⇒ READ; DELETE ⇒ DELETE; ALL ⇒ everything. DENY wins, PREFIXED and `*` patterns
work as in Kafka. The two paths never mix: a certificate identity is never checked against the
role file, and an OIDC identity never against ACLs.

Both mechanisms are active on one HTTPS listener: a request with `Authorization: Bearer …`
goes the OIDC way, a request presenting a trusted client certificate goes the mTLS way, a
request with neither gets 401.

Files:

- `apicurio-with-proxy-deployment.yaml` — Apicurio + proxy sidecar (Apicurio on `127.0.0.1:8080`,
  proxy on `:8443`) and the Service exposing only the proxy.
- `kafkauser.yaml` — the proxy's `KafkaUser` (`Describe` on `Cluster`, needed for `describeAcls`).

## Build

```bash
cd apicurio-proxy
mvn -q package -DskipTests
docker build -t registry.example.com/apicurio-rbac-proxy:1.0 .
docker push registry.example.com/apicurio-rbac-proxy:1.0
```

The module builds standalone (no other part of this repository is needed).

## Secrets

| Secret | Keys | Source |
|---|---|---|
| `apicurio-proxy-server-tls` | `server.p12`, `password` | Your PKI / cert-manager: server certificate for the proxy's hostname |
| `my-cluster-clients-ca-cert` | `ca.p12`, `ca.password` | Created by Strimzi — trust anchor for client certificates |
| `apicurio-proxy-kafka` | `user.p12`, `user.password` | Created by Strimzi from `kafkauser.yaml` |
| `my-cluster-cluster-ca-cert` | `ca.p12`, `ca.password` | Created by Strimzi — trust anchor for Kafka |
| `apicurio-proxy-policy` | `policy.yaml` | `kubectl -n kafka create secret generic apicurio-proxy-policy --from-file=policy.yaml` |

All stores can be JKS (`*_TYPE=JKS`) or PEM (`*_TYPE=PEM`, with `PROXY_TLS_CERT` / `PROXY_TLS_KEY` /
`PROXY_TLS_CA` and `PROXY_KAFKA_SSL_CERT` / `_KEY` / `_CA`). PEM private keys must be PKCS#8
(`-----BEGIN PRIVATE KEY-----`); convert with `openssl pkcs8 -topk8 -nocrypt`.

## Apply and verify

```bash
kubectl apply -f kafkauser.yaml
kubectl apply -f apicurio-with-proxy-deployment.yaml
kubectl -n kafka logs deploy/apicurio-registry -c rbac-proxy | grep -E 'PolicyEngine|KafkaAclPolicySource|Listening'
```

Expected on start: `[PolicyEngine] Loaded N rules`, `[KafkaAclPolicySource] Loaded N ACL bindings`
and `Listening on: https://0.0.0.0:8443`. Readiness is down until both have happened.

A service with a Strimzi `KafkaUser` certificate (Secret `orders-service`):

```bash
kubectl -n kafka get secret orders-service -o jsonpath='{.data.user\.p12}' | base64 -d > user.p12
PW=$(kubectl -n kafka get secret orders-service -o jsonpath='{.data.user\.password}' | base64 -d)
curl --cert-type P12 --cert user.p12:"$PW" --cacert proxy-ca.crt \
     https://apicurio-registry.kafka.svc:8443/apis/registry/v2/groups/default/artifacts/orders-value
```

A human with a token:

```bash
curl -H "Authorization: Bearer $TOKEN" --cacert proxy-ca.crt \
     https://apicurio-registry.kafka.svc:8443/apis/registry/v2/search/artifacts
```

Every decision is one JSON audit line on stdout; `principal` is `user:CN=…` for mTLS and
`user:<oidc name>` for OIDC. Set `KAFKA_AUDIT_BOOTSTRAP` (plus `KAFKA_AUDIT_TLS_*`) to also
ship the audit stream to a Kafka topic.

## Notes

- Registry-wide operations (listing, free-text search, create without an artifact id in the
  path) map to artifact `*` and require `ALL` on topic `*` for a certificate identity. Lookups
  by `globalId` / `contentId` are resolved to the real artifact first, so they work with
  topic-scoped ACLs.
- The Kafka principal derived from a certificate must equal the principal in the ACLs. Strimzi
  `KafkaUser` certificates have subject `CN=<name>` and ACLs `User:CN=<name>`, which `DN` mode
  matches. Use `PROXY_MTLS_PRINCIPAL=CN` only if your brokers map principals to the bare CN.
- ACLs are cached and refreshed every `PROXY_KAFKA_ACL_REFRESH_SECONDS`; a change in Kafka
  takes up to that long to apply. If Kafka is unreachable, the last snapshot stays in use.

## Environment reference

| Variable | Default | Meaning |
|---|---|---|
| `PROXY_TLS_ENABLED` | `false` | Enable the HTTPS listener with optional client certificates. |
| `PROXY_TLS_PORT` | `8443` | HTTPS port. Plain HTTP is disabled when TLS is enabled. |
| `PROXY_TLS_KEYSTORE`, `_PASSWORD`, `_TYPE` | — / `PKCS12` | Server identity. PEM: `PROXY_TLS_CERT` + `PROXY_TLS_KEY`. |
| `PROXY_TLS_TRUSTSTORE`, `_PASSWORD`, `_TYPE` | — / `PKCS12` | CAs client certificates must chain to. PEM: `PROXY_TLS_CA`. |
| `PROXY_MTLS_PRINCIPAL` | `DN` | `DN` (RFC 2253 subject, Strimzi default) or `CN`. |
| `PROXY_KAFKA_BOOTSTRAP` | — | Enables the Kafka ACL source. Without it certificate identities are denied. |
| `PROXY_KAFKA_SECURITY_PROTOCOL` | `SSL` | `SSL` or `PLAINTEXT`. |
| `PROXY_KAFKA_SSL_KEYSTORE`, `_PASSWORD`, `_TYPE` | — / `PKCS12` | Proxy's Kafka client identity. PEM: `PROXY_KAFKA_SSL_CERT` + `_KEY`. |
| `PROXY_KAFKA_SSL_TRUSTSTORE`, `_PASSWORD`, `_TYPE` | — / `PKCS12` | Kafka cluster CA. PEM: `PROXY_KAFKA_SSL_CA`. |
| `PROXY_KAFKA_ACL_REFRESH_SECONDS` | `30` | ACL snapshot refresh interval. |
| `PROXY_ARTIFACT_SUFFIXES` | `-value,-key` | Suffixes stripped from an artifact id to find its topic. |
| `OIDC_ISSUER_URL`, `OIDC_ROLE_CLAIM`, `POLICY_FILE`, `APICURIO_URL`, `XML_SCHEMA_URL`, `KAFKA_AUDIT_*` | as before | Unchanged. `KAFKA_AUDIT_TLS_KEYSTORE*` / `_TRUSTSTORE*` are accepted in addition to the PEM variables. |
