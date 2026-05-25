# Audit logging

Every authorisation decision made by Kroxylicious (Kafka requests) and the Apicurio rbac-proxy (HTTP requests) is captured as a single JSON line on the dedicated SLF4J channel `kafka-audit`. The stream is always on — there is no toggle. Operators can ship it via Fluent Bit, Vector, or Loki Promtail to any log backend.

When the cluster opts in to the Kafka-topic sink (`spec.audit.kafkaTopic.enabled=true`), the same records are also produced to a Kafka topic on the parent cluster (`__audit` by default) for in-cluster analytics.

## Event schema

```json
{
  "ts": "2026-05-25T12:00:00.123Z",
  "principal": "user:alice",
  "op": "PRODUCE",
  "resource": "orders",
  "decision": "allow",
  "latencyMs": 12,
  "correlationId": "42"
}
```

| Field | Type | Notes |
|-------|------|-------|
| `ts` | ISO-8601 UTC | Emit time on the proxy/Apicurio pod. |
| `principal` | string | `user:<CN-or-sub>` for an authenticated caller, `group:<g>` when only a group claim is present, `anonymous` otherwise. |
| `op` | string | Verb the component understands. Kafka: `PRODUCE`, `FETCH`. Apicurio rbac-proxy: `READ`, `WRITE`, `DELETE`. |
| `resource` | string | Topic name for Kafka, artifact id (or `*`) for the rbac-proxy. One event per topic on multi-topic Produce/Fetch requests. |
| `decision` | enum | `allow`, `deny`, `error`. `error` is reserved for upstream 5xx responses on the rbac-proxy. Kafka requests always set `allow` or `deny`. |
| `latencyMs` | number | End-to-end request time as observed inside the proxy. |
| `correlationId` | string \| absent | Kafka correlation id (Kroxylicious) or MDC `correlationId` (rbac-proxy). Omitted when not set. |

## Sinks

### Stdout (always on)

The `kafka-audit` SLF4J logger is wired in every Kroxylicious pod and every Apicurio rbac-proxy pod at INFO level. Records appear in the container's stdout next to ordinary application logs. With `quarkus.log.console.json=true` (production profile) every line is itself JSON, so a log shipper can scoop both kinds of records into the same downstream system without an extra parser.

### Kafka topic (opt-in)

Set on the parent `KafkaCluster`:

```yaml
spec:
  audit:
    kafkaTopic:
      enabled: true
      name: __audit            # default
      retentionDays: 30        # default
      partitions: 3            # default
      replicationFactor: 3     # default
    includeOps:
      - PRODUCE
      - CREATE_TOPICS
      - WRITE
      - DELETE                 # drop high-volume FETCH/READ noise
```

When this turns on, the operator:

1. Upserts a `KafkaTopic` named `<cluster>-audit` (with the configured retention / partitions / RF, `cleanup.policy=delete`, `compression.type=zstd`).
2. Injects `KAFKA_AUDIT_BOOTSTRAP`, `KAFKA_AUDIT_TOPIC`, and `KAFKA_AUDIT_TLS_{CERT,KEY,CA}` env on the proxy + Apicurio rbac-proxy Deployments.
3. Mounts the proxy's existing mTLS client cert Secret at `/etc/audit-tls`.

The audit producer connects **direct to the broker `INTERNAL` listener** — it never re-enters Kroxylicious, so there is no audit-the-audit loop. The producer is configured non-blocking (`max.block.ms=0`, `acks=1`, `linger.ms=20`, `compression.type=zstd`); a full buffer or broker timeout drops back to the stdout sink and logs a rate-limited WARN once per minute.

Consume the topic with any Kafka client. For a quick check:

```sh
kubectl exec -n kafka deploy/cruise-control -- \
  kafka-console-consumer.sh \
  --bootstrap-server my-cluster-bootstrap:9092 \
  --topic __audit \
  --from-beginning \
  --max-messages 10
```

## Operational concerns

- **Cardinality**: `FETCH` is the hot verb. If you only need to audit writes, set `spec.audit.includeOps` to `[PRODUCE, CREATE_TOPICS, ALTER_TOPICS, WRITE, DELETE]`.
- **Schema evolution**: v1 fields are stable. Future versions may add fields without removing existing ones; consumers should ignore unknown keys.
- **Failure mode**: a misconfigured Kafka sink (wrong bootstrap, ACL not yet propagated, topic not yet created) never breaks request handling — events fall back to stdout and the proxy logs a single WARN per minute identifying the cause.
- **Correlation**: the Kroxylicious correlationId is the Kafka request correlation id, which the same client sees in its own logs. The Apicurio rbac-proxy carries an MDC `correlationId` only when an upstream component sets it.

## v2 (deferred)

- Loki / OpenSearch HTTP sinks.
- CloudEvents 1.0 envelope.
- Richer schema: JWT claims (`sub`, `iss`, `aud`), source IP, user-agent, request bytes, response code.
- Per-tenant topic routing.
- Audit-log signing / tamper evidence.
- Operator-side audit (control-plane reconcile decisions).
