# Security

Threat model, hardening defaults, and the explicit trade-offs the operator makes.

## Trust boundary

The operator is the trust root for Kafka access in its namespace. Anyone with
`create` on `KafkaRbac`/`KafkaCluster`/`KafkaTopic` in the operator's namespace
effectively has cluster-admin on Kafka. Treat that namespace's RBAC like a
production secrets vault — `create`/`update` on these CRs is a privileged action
even when the K8s `Role` looks innocuous.

## Defaults (Wave 2 of the 2026-05-21 audit)

- **NetworkPolicies**: a deny-all baseline + named allowed flows is shipped under
  `kind/manifests/networkpolicies.yaml`. It is **opt-in** because Submariner's
  Lighthouse DNS / cross-cluster traffic was breaking under the strict variant.
  Apply once you've confirmed the cross-cluster paths your topology actually
  needs. For single-cluster topologies apply directly.
- **Pod `securityContext`**: every workload the operator generates (broker
  pods, controller pods, proxy, Apicurio, UI) has `runAsNonRoot=true`,
  `seccompProfile.type=RuntimeDefault`, `allowPrivilegeEscalation=false`,
  `capabilities.drop=[ALL]`. Matches the K8s `restricted` Pod Security Standard.
- **Operator RBAC** is namespace-scoped (a `Role` + `RoleBinding` in `kafka`),
  not a cluster-wide `ClusterRole`. `quarkus.operator-sdk.namespaces=kafka` in
  `application.properties` keeps informers in scope. Override both together to
  manage additional namespaces.
- **CRD CEL validation** bars wildcard topics on listener external-access,
  enforces TLS on externally-reachable listeners (Wave 5 removed those entirely),
  and requires `spec.proxy` on every KafkaCluster.

## Kafka-side hardening (Wave 4a)

- The Kroxylicious proxy is the **only** external entry point for Kafka client
  traffic. Direct per-broker external-access (NodePort) was removed in Wave 5.
- Proxy ↔ broker is **always mTLS** — no toggle, no plaintext path. Broker
  `super.users` lists the proxy principal + the per-pool broker CN.
  `StandardAuthorizer` is on with `allow.everyone.if.no.acl.found=false`.
- Cross-cluster controller (KRaft) traffic is mTLS when
  `KafkaCluster.spec.controllerTls.mutualTls=true` (set in the kind fixture).
- The INTERNAL broker listener uses CN-only certs without hostname verification
  on the client side — `ssl.endpoint.identification.algorithm=` (empty) in
  `ServerPropertiesBuilder`. This is intentional: the cert's CN is
  `kafka-proxy` regardless of pod hostname, so hostname verification would
  permanently fail. The SSL handshake still validates the cert chain against
  the operator-provisioned CA, so a stolen private key is required to forge.

## Cert rotation (Wave 3)

`SecretRevisionTracker` folds the `resourceVersion` of every mounted Secret
into each Pod's `configHash`. When cert-manager rotates a cert in place, the
Secret's revision changes, the configHash changes, and the operator rolls the
Pod. A `Secret` `InformerEventSource` wakes the reconciler immediately rather
than waiting for the next periodic tick. This works the same for brokers, the
proxy, and Apicurio.

### Synchronized rotations across MCS clusters

cert-manager typically rotates Certificates on the same schedule across all
three K8s clusters, so the operator must serialize the resulting rolls or it
risks taking out every replica of a cross-cluster partition at once. Set
`KafkaCluster.spec.clusterRollOrder` and the cross-cluster gate applies
uniformly to controllers, brokers, the Kroxylicious proxy Deployment, and the
Apicurio Registry Deployment — each successor cluster waits for predecessors
to report `upgradePhase=IDLE` via `GET /operator/upgrade-phase` before
starting its own roll. The `RollTracker` in-memory marker keeps the ROLLING
signal visible for the duration of a `KafkaPodSet` roll, which would
otherwise be invisible in etcd until after `rollPod()` returns.

## BYO IDP

The operator does not deploy or manage an IDP. Production users supply their
own (Keycloak, Okta, Auth0, ...). What the operator consumes:

- `spec.proxy.oidc.jwksEndpointUrl` — JWKS endpoint for token signature
  verification. **Must be HTTPS in production.**
- `spec.proxy.oidc.expectedIssuer` — must match the IDP's `iss` claim.
- `spec.proxy.oidc.expectedAudience` — must match the `aud` claim the proxy
  expects (typically the proxy's client ID).
- `spec.proxy.oidc.groupsClaim` — JWT claim to read group membership from.
  Default `realm_access.roles` (Keycloak shape).

The `kind/manifests/keycloak*.yaml` fixtures are **test-rig only**, used to
make `make e2e` self-contained. They use HTTP and a static realm — never
copy them to production.

## BYO cert-manager

The operator does not sign certs. It expects Secrets to already exist by the
names listed in `KafkaCluster.spec.proxyMtls.adminClientCertSecretRef`,
`spec.proxy.tls.{clientCertSecretRef, serverCertSecretRef}`, and
`spec.apicurio.storage.tlsSecretRef`.

In production, run cert-manager and emit Certificates that produce Secrets
with the standard `tls.crt` / `tls.key` / `ca.crt` keys. Rotation policy is
the user's choice; the operator reacts (see Cert Rotation above).

In tests, `kind/mcs-setup.sh` provisions self-signed certs into the same
Secret names.

## Threats considered but not (yet) defended

- **Compromised operator pod**: today the operator has full Kafka admin via
  its AdminClient mTLS cert and ConfigMap-write access in its namespace. A
  pod compromise gives both. Mitigations to consider: a separate AdminClient
  identity per CR, runtime-detection via the audit log, an admission webhook
  enforcing CR mutation policy.
- **CR mutation by a namespace member with `update` on KafkaRbac**: trusted
  by design (see Trust Boundary). Mitigate with K8s `Role` design — don't
  grant CR update to humans.
- **JWT replay**: tokens carry expiry; the operator doesn't enforce nonce or
  short lifetimes. Use short-lived tokens at the IDP.
- **Quota bypass via direct broker connection**: the proxy is the enforcement
  point, and direct broker access was removed in Wave 5. NetworkPolicies
  (opt-in) further constrain east-west.

## Reporting

Security issues — open a private security advisory on the GitHub repo. Do not
file public issues for unpatched vulnerabilities.
