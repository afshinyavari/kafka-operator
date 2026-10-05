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
and `certificates.yaml` (the cert-manager `Certificate`s the manifests mount) apply to all three.

The manifests take every certificate from cert-manager and every trust anchor from one CA
bundle; no Strimzi CA Secret is involved (see [Certificates and trust](#certificates-and-trust)).

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

Keys are the certificate principal as the proxy derives it: in `DN` mode the subject DN
(`CN=<name>` for a certificate with only a common name; spacing after commas does not matter),
in `CN` mode the bare common name, and with `PROXY_MTLS_PRINCIPAL_MAPPING_RULES` whatever the
rules produce. A Kafka-style `User:` prefix is accepted. Roles carried by the
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

## Certificates and trust

| Object | Keys | Source |
|---|---|---|
| Secret `apicurio-proxy-server-tls` | `tls.crt`, `tls.key` | cert-manager: server certificate for the proxy's Service names |
| Secret `apicurio-proxy-kafka` | `tls.crt`, `tls.key` | cert-manager: the proxy's Kafka client certificate, subject `CN=apicurio-proxy-kafka` |
| Secret `apicurio-registry-kafka` | `tls.crt`, `tls.key` | cert-manager: the registry's Kafka client certificate for kafkasql storage; not used by the standalone proxy |
| Secret `tls-trust-bundle` | `ca-bundle.crt` | Your CA bundle: trust anchor for client certificates **and** for Kafka |

`certificates.yaml` has the three `Certificate`s. The Kafka listener the proxy and the registry
connect to must present a certificate that chains to a CA in the bundle and must trust the CA
that issues the client certificates; the Strimzi cluster CA and clients CA are not used.

- **Private keys must be PKCS#8** (`-----BEGIN PRIVATE KEY-----`) wherever a Kafka client reads
  them: `apicurio-proxy-kafka` and `apicurio-registry-kafka`. cert-manager writes PKCS#1 unless
  the `Certificate` sets `privateKey.encoding: PKCS8`; with a PKCS#1 key the proxy fails at
  startup with `Invalid PEM keystore configs`. The proxy's server key may be either.
- **The bundle decides who is authenticated.** A client certificate from *any* CA in
  `ca-bundle.crt` passes the TLS handshake; what it may do is then decided by the Kafka ACLs of
  its subject and by `principals` in `policy.yaml`. A certificate with neither gets 403.
- **The certificate subject is the Kafka principal.** The proxy looks up ACLs for the full
  subject DN (`PROXY_MTLS_PRINCIPAL=DN`), so `CN=orders-service,O=Org` needs ACLs on exactly
  `User:CN=orders-service,O=Org`. A Strimzi `KafkaUser` (`tls` or `tls-external`) manages ACLs
  for `User:CN=<name>`, which matches a certificate whose subject is only that common name.
- **Subjects with more than a CN need the brokers' mapping rules.** When your PKI adds fields
  such as `O` or `C`, the brokers reduce the subject with `ssl.principal.mapping.rules`, e.g.
  `RULE:^CN=([^,]+),.*$/CN=$1/,DEFAULT` to keep `CN=<name>`. Give the proxy the same string in
  `PROXY_MTLS_PRINCIPAL_MAPPING_RULES`: it evaluates it with Kafka's own mapper, so both derive
  the same principal, and `PROXY_MTLS_PRINCIPAL` is then ignored. A certificate no rule matches
  is denied, as Kafka rejects it. The startup log shows the rules in use
  (`[PolicyEngine] Certificate principals by rules …`).
- **Renewed certificates need a restart.** The proxy and the registry read key material at
  startup, and the registry gets it through env. Restart the pods when cert-manager renews a
  Secret or the bundle changes (e.g. with Stakater Reloader on the four Secrets);
  otherwise connections fail once the old certificate expires.
- `tls-trust-bundle` is mounted as a Secret. If your bundle is a ConfigMap (the trust-manager
  default), change the `trust-bundle` volume to `configMap:` and the registry's `secretKeyRef`
  to `configMapKeyRef`.
- The registry's `spec.app.storage.kafkasql.tls` block of the 3.x operator is not used: it takes
  PKCS12 files with a password. The PEM material is passed as `APICURIO_KAFKA_COMMON_SSL_*` env.

Stores can instead be PKCS12 (`PROXY_TLS_KEYSTORE` / `_PASSWORD`, `PROXY_TLS_TRUSTSTORE` /
`_PASSWORD`, same for `PROXY_KAFKA_SSL_*`) or JKS (`*_TYPE=JKS`), e.g. for Strimzi-issued
`KafkaUser` Secrets (`user.p12` / `user.password`) and CA Secrets (`ca.p12` / `ca.password`).

## Apply and verify

```bash
kubectl apply -f certificates.yaml -f kafkauser.yaml
kubectl apply -f apicurio-with-proxy-deployment.yaml     # or apicurio-operator-sidecar.yaml / proxy-standalone-deployment.yaml
kubectl -n kafka logs deploy/apicurio-registry -c rbac-proxy | grep -E 'PolicyEngine|KafkaAclPolicySource|Listening'
```

The Deployment and Service names depend on the model: `apicurio-registry` in the plain
Deployment, `registry-app-deployment` and Service `registry-rbac` with the operator sidecar,
`apicurio-rbac-proxy` for the standalone proxy.

Expected on start: `[PolicyEngine] Loaded N rules and M principal mappings`, `[KafkaAclPolicySource] Loaded N ACL bindings`
and `Listening on: https://0.0.0.0:8443`. Readiness is down until both have happened.

A service with a cert-manager client certificate (Secret `orders-service`):

```bash
kubectl -n kafka get secret orders-service -o jsonpath='{.data.tls\.crt}' | base64 -d > tls.crt
kubectl -n kafka get secret orders-service -o jsonpath='{.data.tls\.key}' | base64 -d > tls.key
kubectl -n kafka get secret tls-trust-bundle -o jsonpath='{.data.ca-bundle\.crt}' | base64 -d > ca-bundle.crt
curl --cert tls.crt --key tls.key --cacert ca-bundle.crt \
     https://apicurio-registry.kafka.svc:8443/apis/registry/v3/groups/default/artifacts/orders-value
```

A human with a token:

```bash
curl -H "Authorization: Bearer $TOKEN" --cacert ca-bundle.crt \
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
- The Kafka principal derived from a certificate must equal the principal in the ACLs. A
  certificate with subject `CN=<name>` and ACLs on `User:CN=<name>` (what a Strimzi `KafkaUser`
  manages) match in `DN` mode. If your brokers rewrite subjects with
  `ssl.principal.mapping.rules`, set `PROXY_MTLS_PRINCIPAL_MAPPING_RULES` to the same rules.
- ACLs are cached and refreshed every `PROXY_KAFKA_ACL_REFRESH_SECONDS`; a change in Kafka
  takes up to that long to apply. If Kafka is unreachable, the last snapshot stays in use.

## Environment reference

| Variable | Default | Meaning |
|---|---|---|
| `PROXY_TLS_ENABLED` | `false` | Enable the HTTPS listener with optional client certificates. |
| `PROXY_TLS_PORT` | `8443` | HTTPS port. Plain HTTP is disabled when TLS is enabled. |
| `PROXY_TLS_CERT`, `PROXY_TLS_KEY` | — | Server identity as PEM files. |
| `PROXY_TLS_CA` | — | PEM file with the CAs client certificates must chain to; may hold several. |
| `PROXY_TLS_KEYSTORE`, `_PASSWORD`, `_TYPE` | — / `PKCS12` | Server identity as a PKCS12 or JKS store, instead of the PEM pair. |
| `PROXY_TLS_TRUSTSTORE`, `_PASSWORD`, `_TYPE` | — / `PKCS12` | Client CAs as a PKCS12 or JKS store, instead of `PROXY_TLS_CA`. |
| `PROXY_MTLS_PRINCIPAL` | `DN` | `DN` (RFC 2253 subject, Kafka's default principal) or `CN` (bare common name). |
| `PROXY_MTLS_PRINCIPAL_MAPPING_RULES` | — | The brokers' `ssl.principal.mapping.rules`, verbatim. Replaces `PROXY_MTLS_PRINCIPAL` when set; an invalid rule list fails startup. |
| `PROXY_KAFKA_BOOTSTRAP` | — | Enables the Kafka ACL source. Without it certificate identities get only what `principals` in `policy.yaml` grants. |
| `PROXY_KAFKA_SECURITY_PROTOCOL` | `SSL` | `SSL` or `PLAINTEXT`. |
| `PROXY_KAFKA_SSL_CERT`, `PROXY_KAFKA_SSL_KEY` | — | Proxy's Kafka client identity as PEM files (PKCS#8 key). |
| `PROXY_KAFKA_SSL_CA` | — | PEM file with the CAs the Kafka listener's certificate chains to; may hold several. |
| `PROXY_KAFKA_SSL_KEYSTORE`, `_PASSWORD`, `_TYPE` | — / `PKCS12` | Kafka client identity as a PKCS12 or JKS store, instead of the PEM pair. |
| `PROXY_KAFKA_SSL_TRUSTSTORE`, `_PASSWORD`, `_TYPE` | — / `PKCS12` | Kafka CAs as a PKCS12 or JKS store, instead of `PROXY_KAFKA_SSL_CA`. |
| `PROXY_KAFKA_ACL_REFRESH_SECONDS` | `30` | ACL snapshot refresh interval. |
| `PROXY_ARTIFACT_SUFFIXES` | `-value,-key` | Suffixes stripped from an artifact id to find its topic. |
| `POLICY_RELOAD_SECONDS` | `5` | How often `policy.yaml` is checked for changed content (minimum 1). |
| `QUARKUS_OIDC_TENANT_ENABLED` | `true` | `false` runs the proxy without an OIDC provider (mTLS only). |
| `OIDC_ISSUER_URL`, `OIDC_ROLE_CLAIM`, `POLICY_FILE`, `APICURIO_URL`, `XML_SCHEMA_URL`, `KAFKA_AUDIT_*` | as before | Unchanged. `KAFKA_AUDIT_TLS_KEYSTORE*` / `_TRUSTSTORE*` are accepted in addition to the PEM variables. |
