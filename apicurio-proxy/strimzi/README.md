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

The proxy is written for **Apicurio Registry 3.x** and its core API v3 (`/apis/registry/v3`),
verified against registry 3.3.3 with the Apicurio 3.3.3 and Confluent 8.3 Avro serdes. The
v2 core API and the Confluent-compatible API (`/apis/ccompat/v7`) are understood as well —
see [Registry APIs](#registry-apis).

## Deployment models

Pick one; the proxy itself is stateless and identical in all three. All three manifests are
written for Apicurio Registry 3.x and for mTLS only (see [mTLS only](#mtls-only-no-oidc)).

| File | Model | When |
|---|---|---|
| `apicurio-with-proxy-deployment.yaml` | Plain Deployment with Apicurio 3.x (kafkasql storage) + proxy sidecar; the registry API bound to `127.0.0.1` so only the proxy is reachable. | You manage the registry Deployment yourself. |
| `apicurio-operator-sidecar.yaml` | `ApicurioRegistry3` CR with the proxy added through `spec.app.podTemplateSpec`, the registry API bound to loopback, plus a Service on 8443 and a NetworkPolicy. Configured for mTLS only. | Apicurio Registry 3 operator (checked with 3.3.3). The operator's probes use the management port 9000, so port 8080 can be closed to everything outside the pod. |
| `proxy-standalone-deployment.yaml` | Proxy as its own Deployment (2 replicas) in front of the registry Service, plus a NetworkPolicy letting only proxy pods reach the registry. | The proxy should scale and roll independently of the registry. With the 3.x operator the CR needs `spec.app.networkPolicy.enabled: false` and `spec.ui.enabled: false`, and the CNI must enforce NetworkPolicy; otherwise the registry is reachable past the proxy. |

`kafkauser.yaml` (the proxy's `KafkaUser` with `Describe` on `Cluster`, needed for `describeAcls`)
applies to all three.

NetworkPolicy notes: kubelet probes are not blocked by NetworkPolicy in the common CNIs
(Calico, Cilium, OVN); if yours differs, allow ingress from the node CIDR on the probe ports
(8443, and 9000 for an operator-managed registry). Check the labels the operator puts on
registry pods (`kubectl get pods --show-labels`) and adjust `podSelector`.

Operator sidecar specifics (3.x): the operator's own NetworkPolicy allows 8080 and 9000 only,
so it is switched off (`spec.app.networkPolicy.enabled: false`) in favour of one that allows
8443. The registry UI is switched off as well: it calls the API from the browser, which has
no client certificate. Exposing the proxy outside the cluster needs TLS **passthrough** (an
edge or re-encrypt route terminates TLS and drops the client certificate).

## mTLS only (no OIDC)

Set `QUARKUS_OIDC_TENANT_ENABLED=false` on the proxy and leave `OIDC_ISSUER_URL` /
`OIDC_ROLE_CLAIM` out, as the three manifests do. Leaving `OIDC_ISSUER_URL` out without the
flag fails startup. A request with a trusted client certificate is authorized as usual; a request
without one gets 401, also when it carries a bearer token. To add OIDC later, remove the flag
and set `OIDC_ISSUER_URL`; no rebuild is needed.

With no OIDC users, `rules` in `policy.yaml` apply only through `principals`. An example for
a setup where services are authorized by their Kafka ACLs and two certificates need more:

```yaml
principals:
  "CN=ci-pipeline": [schema-admin]        # registers and deletes schemas for every topic
  "CN=orders-service": [shared-types]     # its schemas reference a shared artifact
rules:
  - roles: [schema-admin]
    resources:
      - artifact: "*"
        actions: [READ, WRITE, DELETE]
  - roles: [shared-types]
    resources:
      - artifact: common-address
        actions: [READ]
```

`policy.yaml` must exist and parse even when every service is covered by its ACLs;
`rules: []` is enough.

## Build

```bash
cd apicurio-proxy
mvn -q package -DskipTests
docker build -t registry.example.com/apicurio-rbac-proxy:1.0 .
docker push registry.example.com/apicurio-rbac-proxy:1.0
```

The module builds standalone (no other part of this repository is needed).

## Policy file

`policy.yaml` is read from `POLICY_FILE` (default `/opt/rbac/policy.yaml`) at startup and
re-read whenever its content changes. The manifests mount it from the ConfigMap
`apicurio-proxy-policy`:

```bash
kubectl -n kafka create configmap apicurio-proxy-policy --from-file=policy.yaml \
  --dry-run=client -o yaml | kubectl apply -f -
```

Re-applying the ConfigMap is enough; the pods are not restarted. The proxy polls the file
every `POLICY_RELOAD_SECONDS` (default 5) and loads it when the content differs:

- **Delay.** Kubelet syncs a ConfigMap volume periodically, so a change usually reaches the
  pod within about a minute. The poll interval adds at most a few seconds on top.
- **Replicas** reload independently and can briefly enforce different policies.
- **A broken file** (invalid YAML, unknown action, empty) is rejected: the proxy keeps the
  policy it has and logs `[PolicyFileWatcher] Reload of … failed, keeping current policy`.
  At startup a broken file fails the pod instead.
- **`subPath` mounts are never updated** by Kubernetes. Mount the ConfigMap as a directory, as
  the manifests do.

Confirm a reload in the log: `[PolicyEngine] Loaded N rules and M principal mappings`. The
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

## Registry APIs

Every request is reduced to one artifact and one action (READ, WRITE, DELETE) before the
policy is asked. GET is READ, DELETE is DELETE, everything else is WRITE unless stated.

| Request | Artifact | Action |
|---|---|---|
| `…/v3/groups/{g}/artifacts/{a}[/…]` (also v2) | `{a}` | by method |
| `POST …/v3/groups/{g}/artifacts` (create, serde auto-register) | `artifactId` in the JSON body | WRITE |
| `POST …/v2/groups/{g}/artifacts` | `X-Registry-ArtifactId` header | WRITE |
| `…/v3/search/versions?artifactId={a}`, `…/v3/search/artifacts?artifactId={a}` (serde lookup by content) | `{a}` | READ, also for POST |
| `…/ids/globalIds/{id}`, `…/ids/contentIds/{id}`, `…/search/…?globalId=` / `?contentId=` (v2 and v3) | resolved from the registry | READ |
| `/apis/ccompat/v7/subjects/{s}/versions` (register) | `{s}` | WRITE |
| `POST /apis/ccompat/v7/subjects/{s}` (lookup), `POST …/compatibility/subjects/{s}/…` | `{s}` | READ |
| `/apis/ccompat/v7/subjects/{s}/…`, `/config/{s}`, `/mode/{s}` | `{s}` | by method |
| `/apis/ccompat/v7/schemas/ids/{id}[/…]` | resolved from the registry | READ |
| `/apis/ccompat/v7/associations/resources/{ns}/{topic}?associationType=value\|key` | `{topic}-value` / `{topic}-key` | READ |
| anything else: listings, free-text search, `/admin`, `/system`, `/subjects`, `/config` | `*` | by method |

- **Ids are resolved, not trusted.** An id names content, not an artifact, so the proxy asks the
  registry which artifacts hold it (on the API the request came in on) and allows the request
  when the caller may READ **any** of them. Identical content shares one content id across
  artifacts — a `string` key schema used by many topics is one content id — and v3 serdes use
  the content id by default. The `?subject=` hint of ccompat `schemas/ids/{id}` is ignored:
  the registry does not enforce it. An id the registry does not know needs `*`.
- **Ambiguous requests are registry-wide.** A create body with a duplicate or missing
  `artifactId`, a non-JSON create, or a repeated `artifactId` query parameter maps to `*`.
- **Groups are not part of the policy.** `team-a/orders-value` and `default/orders-value` are
  both artifact `orders-value`.
- **Schema references** are fetched by the serde as ordinary artifact reads, so a client needs
  READ on every artifact its schemas reference.
- **Denials are answered in the API's own error format** (v3 problem details, v2
  `error_code`, ccompat `error_code` 40301) with the denied action and artifact in the
  message, so serdes report them as an authorization error.
- Everything under `/apis/` goes to `APICURIO_URL`; any other path goes to `XML_SCHEMA_URL`.

## Secrets

| Secret | Keys | Source |
|---|---|---|
| `apicurio-proxy-server-tls` | `server.p12`, `password` | Your PKI / cert-manager: server certificate for the proxy's hostname |
| `my-cluster-clients-ca-cert` | `ca.p12`, `ca.password` | Created by Strimzi — trust anchor for client certificates |
| `apicurio-proxy-kafka` | `user.p12`, `user.password` | Created by Strimzi from `kafkauser.yaml` |
| `my-cluster-cluster-ca-cert` | `ca.p12`, `ca.password` | Created by Strimzi — trust anchor for Kafka |
| `apicurio-registry-kafka` | `user.p12`, `user.password` | The registry's own `KafkaUser` for kafkasql storage (your existing one); not used by the standalone proxy |

All stores can be JKS (`*_TYPE=JKS`) or PEM (`*_TYPE=PEM`, with `PROXY_TLS_CERT` / `PROXY_TLS_KEY` /
`PROXY_TLS_CA` and `PROXY_KAFKA_SSL_CERT` / `_KEY` / `_CA`). PEM private keys must be PKCS#8
(`-----BEGIN PRIVATE KEY-----`); convert with `openssl pkcs8 -topk8 -nocrypt`.

## Apply and verify

```bash
kubectl apply -f kafkauser.yaml
kubectl apply -f apicurio-with-proxy-deployment.yaml     # or apicurio-operator-sidecar.yaml / proxy-standalone-deployment.yaml
kubectl -n kafka logs deploy/apicurio-registry -c rbac-proxy | grep -E 'PolicyEngine|KafkaAclPolicySource|Listening'
```

The Deployment and Service names depend on the model: `apicurio-registry` in the plain
Deployment, `registry-app-deployment` and Service `registry-rbac` with the operator sidecar,
`apicurio-rbac-proxy` for the standalone proxy.

Expected on start: `[PolicyEngine] Loaded N rules and M principal mappings`, `[KafkaAclPolicySource] Loaded N ACL bindings`
and `Listening on: https://0.0.0.0:8443`. Readiness is down until both have happened.

A service with a Strimzi `KafkaUser` certificate (Secret `orders-service`):

```bash
kubectl -n kafka get secret orders-service -o jsonpath='{.data.user\.p12}' | base64 -d > user.p12
PW=$(kubectl -n kafka get secret orders-service -o jsonpath='{.data.user\.password}' | base64 -d)
curl --cert-type P12 --cert user.p12:"$PW" --cacert proxy-ca.crt \
     https://apicurio-registry.kafka.svc:8443/apis/registry/v3/groups/default/artifacts/orders-value
```

A human with a token:

```bash
curl -H "Authorization: Bearer $TOKEN" --cacert proxy-ca.crt \
     https://apicurio-registry.kafka.svc:8443/apis/registry/v3/search/artifacts
```

Every decision is one JSON audit line on stdout; `principal` is `user:CN=…` for mTLS and
`user:<oidc name>` for OIDC. Set `KAFKA_AUDIT_BOOTSTRAP` (plus `KAFKA_AUDIT_TLS_*`) to also
ship the audit stream to a Kafka topic.

## Notes

- Registry-wide operations (listing, free-text search, create without an artifact id) map to
  artifact `*` and require `ALL` on topic `*` for a certificate identity. Serde traffic —
  auto-register, lookup by content, find-latest and lookups by `globalId` / `contentId` —
  names or resolves to a real artifact, so it works with topic-scoped ACLs
  (see [Registry APIs](#registry-apis)).
- Confluent clients 8.x ask `associations/resources/-/{topic}` before every call. Apicurio
  answers 404, which the client expects, and the audit stream records that 404 as `deny`
  like any other 4xx from the registry.
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
| `POLICY_RELOAD_SECONDS` | `5` | How often `policy.yaml` is checked for changed content (minimum 1). |
| `QUARKUS_OIDC_TENANT_ENABLED` | `true` | `false` runs the proxy without an OIDC provider (mTLS only). |
| `OIDC_ISSUER_URL`, `OIDC_ROLE_CLAIM`, `POLICY_FILE`, `APICURIO_URL`, `XML_SCHEMA_URL`, `KAFKA_AUDIT_*` | as before | Unchanged. `KAFKA_AUDIT_TLS_KEYSTORE*` / `_TRUSTSTORE*` are accepted in addition to the PEM variables. |
