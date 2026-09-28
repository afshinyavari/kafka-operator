# Apicurio RBAC proxy as a Strimzi sidecar

Humans authenticate with OIDC bearer tokens and are authorized by `policy.yaml` (roles →
artifacts). Services authenticate with mTLS client certificates and are authorized by their
**Kafka ACLs**: WRITE on topic `orders` ⇒ READ+WRITE on artifacts `orders-key` / `orders-value`;
READ/DESCRIBE ⇒ READ; DELETE ⇒ DELETE; ALL ⇒ everything. DENY wins, PREFIXED and `*` patterns
work as in Kafka. An OIDC identity is never checked against ACLs. A certificate identity can in
addition be given roles in `policy.yaml` (see [Rules that are not Kafka ACLs](#rules-that-are-not-kafka-acls)).

Both mechanisms are active on one HTTPS listener: a request with `Authorization: Bearer …`
goes the OIDC way, a request presenting a trusted client certificate goes the mTLS way, a
request with neither gets 401.

## Deployment models

Pick one; the proxy itself is stateless and identical in all three.

| File | Model | When |
|---|---|---|
| `apicurio-with-proxy-deployment.yaml` | Plain Deployment with Apicurio + proxy sidecar; Apicurio bound to `127.0.0.1` so only the proxy is reachable. | You manage the registry Deployment yourself. |
| `apicurio-operator-sidecar.yaml` | `ApicurioRegistry` CR with the proxy added through `spec.deployment.podTemplateSpecPreview`, plus a Service on 8443 and a NetworkPolicy closing 8080. | Apicurio Registry Operator (2.x, operator ≥ 1.1). Apicurio must keep listening on the pod IP for the operator's probes, hence the NetworkPolicy. |
| `proxy-standalone-deployment.yaml` | Proxy as its own Deployment (2 replicas) in front of the registry Service, plus a NetworkPolicy letting only proxy pods reach the registry. | Any registry, operator-managed or not. Simplest with the operator: the CR is untouched and the proxy scales and rolls independently. **Recommended with the operator.** |

`kafkauser.yaml` (the proxy's `KafkaUser` with `Describe` on `Cluster`, needed for `describeAcls`)
applies to all three.

NetworkPolicy notes: kubelet probes are not blocked by NetworkPolicy in the common CNIs
(Calico, Cilium, OVN); if yours differs, allow ingress from the node CIDR on 8080. Check the labels
the operator puts on registry pods (`kubectl get pods --show-labels`) and adjust `podSelector`.

## Build

```bash
cd apicurio-proxy
mvn -q package -DskipTests
docker build -t registry.example.com/apicurio-rbac-proxy:1.0 .
docker push registry.example.com/apicurio-rbac-proxy:1.0
```

The module builds standalone (no other part of this repository is needed).

## Policy file

`policy.yaml` is read at startup from `POLICY_FILE` (default `/opt/rbac/policy.yaml`). The
manifests mount it from the ConfigMap `apicurio-proxy-policy`:

```bash
kubectl -n kafka create configmap apicurio-proxy-policy --from-file=policy.yaml \
  --dry-run=client -o yaml | kubectl apply -f -
```

The Deployments carry `configmap.reloader.stakater.com/reload: apicurio-proxy-policy`, so
Stakater Reloader rolls the pods on a change. Without Reloader, run `kubectl rollout restart`;
the proxy's own file watcher does not see ConfigMap updates (Kubernetes swaps a symlink). The
image contains an example `policy.yaml` that is used only when nothing is mounted.

```yaml
principals:                         # certificate principal → roles (mTLS only)
  "CN=ci-pipeline": [schema-admin]
rules:                              # roles → artifacts and actions (OIDC and mapped certificates)
  - roles: [orders-team]
    resources:
      - artifact: orders-value      # exact artifact id, or "*"
        actions: [READ, WRITE]      # READ | WRITE | DELETE
  - roles: [schema-admin]
    resources:
      - artifact: "*"
        actions: [READ, WRITE, DELETE]
```

OIDC roles come from the token claim `OIDC_ROLE_CLAIM`. Rule artifacts are matched exactly:
the `-value`/`-key` → topic mapping applies to Kafka ACLs only.

### Rules that are not Kafka ACLs

`principals` gives a client certificate roles, so it gets the same `rules` as an OIDC user
with those roles. A certificate request is then allowed when **either** its Kafka ACLs **or**
its mapped roles allow it, with two limits:

- A Kafka `DENY` on the artifact's topic for the same operation (READ, WRITE, DELETE) or `ALL`
  blocks the role grant too, so a DENY in Kafka always wins.
- While the ACL source is enabled but has not loaded a snapshot yet, certificate requests are
  denied (a DENY cannot be ruled out). With `PROXY_KAFKA_BOOTSTRAP` unset, only the mapping applies.

Keys are the certificate principal as `PROXY_MTLS_PRINCIPAL` derives it: in `DN` mode the
subject DN (`CN=<name>` for Strimzi `KafkaUser`s; spacing after commas does not matter), in
`CN` mode the bare common name. A Kafka-style `User:` prefix is accepted. Roles carried by the
identity itself are never used for certificates, only the mapping.

## Secrets

| Secret | Keys | Source |
|---|---|---|
| `apicurio-proxy-server-tls` | `server.p12`, `password` | Your PKI / cert-manager: server certificate for the proxy's hostname |
| `my-cluster-clients-ca-cert` | `ca.p12`, `ca.password` | Created by Strimzi — trust anchor for client certificates |
| `apicurio-proxy-kafka` | `user.p12`, `user.password` | Created by Strimzi from `kafkauser.yaml` |
| `my-cluster-cluster-ca-cert` | `ca.p12`, `ca.password` | Created by Strimzi — trust anchor for Kafka |

All stores can be JKS (`*_TYPE=JKS`) or PEM (`*_TYPE=PEM`, with `PROXY_TLS_CERT` / `PROXY_TLS_KEY` /
`PROXY_TLS_CA` and `PROXY_KAFKA_SSL_CERT` / `_KEY` / `_CA`). PEM private keys must be PKCS#8
(`-----BEGIN PRIVATE KEY-----`); convert with `openssl pkcs8 -topk8 -nocrypt`.

## Apply and verify

```bash
kubectl apply -f kafkauser.yaml
kubectl apply -f apicurio-with-proxy-deployment.yaml     # or apicurio-operator-sidecar.yaml / proxy-standalone-deployment.yaml
kubectl -n kafka logs deploy/apicurio-registry -c rbac-proxy | grep -E 'PolicyEngine|KafkaAclPolicySource|Listening'
```

Expected on start: `[PolicyEngine] Loaded N rules and M principal mappings`, `[KafkaAclPolicySource] Loaded N ACL bindings`
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
| `PROXY_KAFKA_BOOTSTRAP` | — | Enables the Kafka ACL source. Without it certificate identities get only what `principals` in `policy.yaml` grants. |
| `PROXY_KAFKA_SECURITY_PROTOCOL` | `SSL` | `SSL` or `PLAINTEXT`. |
| `PROXY_KAFKA_SSL_KEYSTORE`, `_PASSWORD`, `_TYPE` | — / `PKCS12` | Proxy's Kafka client identity. PEM: `PROXY_KAFKA_SSL_CERT` + `_KEY`. |
| `PROXY_KAFKA_SSL_TRUSTSTORE`, `_PASSWORD`, `_TYPE` | — / `PKCS12` | Kafka cluster CA. PEM: `PROXY_KAFKA_SSL_CA`. |
| `PROXY_KAFKA_ACL_REFRESH_SECONDS` | `30` | ACL snapshot refresh interval. |
| `PROXY_ARTIFACT_SUFFIXES` | `-value,-key` | Suffixes stripped from an artifact id to find its topic. |
| `OIDC_ISSUER_URL`, `OIDC_ROLE_CLAIM`, `POLICY_FILE`, `APICURIO_URL`, `XML_SCHEMA_URL`, `KAFKA_AUDIT_*` | as before | Unchanged. `KAFKA_AUDIT_TLS_KEYSTORE*` / `_TRUSTSTORE*` are accepted in addition to the PEM variables. |
