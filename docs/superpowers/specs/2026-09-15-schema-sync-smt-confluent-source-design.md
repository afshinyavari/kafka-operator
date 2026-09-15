# Schema-sync SMT: configurable registry formats + mTLS

**Date:** 2026-09-15
**Module:** `schema-sync-smt/`
**Status:** approved (chat), implementing

## Goal

Make `ApicurioSchemaTransferSmt` usable outside this operator — specifically in a
Strimzi `KafkaMirrorMaker2` where the **source** cluster uses **Confluent Schema
Registry** and the **target** uses **Apicurio Registry (native API)**, with **mTLS**
towards both registries. The existing Apicurio→Apicurio mode must keep working with
the same configuration keys as today.

## Configuration

| Key | Values | Default | Meaning |
|---|---|---|---|
| `source.format`, `target.format` | `APICURIO`, `CONFLUENT` | `APICURIO` | Selects **both** the wire envelope and the REST API for that side. |
| `<side>.ssl.keystore.location` | path | — | Client keystore for mTLS. |
| `<side>.ssl.keystore.password` | password | — | Keystore password (PKCS12/JKS only). |
| `<side>.ssl.keystore.type` | `PKCS12`, `JKS`, `PEM` | `PKCS12` | With `PEM`, `keystore.location` is the certificate chain file. |
| `<side>.ssl.key.location` | path | — | PKCS#8 private key file (PEM mode only). |
| `<side>.ssl.truststore.location` | path | — | Optional CA bundle; JDK default trust when unset. |
| `<side>.ssl.truststore.password` | password | — | Truststore password (PKCS12/JKS only). |
| `<side>.ssl.truststore.type` | `PKCS12`, `JKS`, `PEM` | `PKCS12` | With `PEM`, `truststore.location` is a CA certificate bundle. |
| `target.subject.prefix` | string | _(empty)_ | Prepended to the top-level subject/artifactId on the target so it mirrors MirrorMaker's topic prefix (e.g. `prod.`). References keep their source subject. Added after review, 2026-09-15. |
| `cache.negative.ttl.ms` | int | `60000` | How long a failed id lookup is remembered before the registry is asked again. `0` disables. |

`<side>` is `source` or `target`. Existing keys (`*.url`, `*.auth.header`,
`*.auth.oauth.dir`, `cache.size`, `behavior.on.error`, `apply.to`, `apply.to.topics`,
`max.ref.depth`) are unchanged.

`APICURIO`: 9-byte envelope `[0x00][globalId:int64 BE][payload]`, Apicurio REST v2.
`CONFLUENT`: 5-byte envelope `[0x00][schemaId:int32 BE][payload]`, Confluent Schema
Registry REST (also what Apicurio serves under `/apis/ccompat/v7`).

Source and target formats are independent, so all four combinations work. The
target-side use case for this change is `source.format=CONFLUENT`,
`target.format=APICURIO`.

## Components

- **`EnvelopeCodec`** — `parse(byte[]) → Parsed(id, payloadOffset) | null` and
  `encode(long id, byte[] src, int payloadOffset) → byte[]`. Implementations
  `ApicurioEnvelopeCodec` and `ConfluentEnvelopeCodec`. Cross-format rewrites re-frame
  the payload with the target codec (payload length changes between 5 and 9 bytes).
- **`SchemaRegistryClient`** (interface) — `fetchById(long) → RegistrySchema`,
  `upsert(RegistrySchema) → Registered(id, version)`, `lookupId(SchemaRef) → Long|null`.
  `ApicurioClient` (existing, v2) and new `ConfluentClient` implement it.
- **Shared model** — `RegistrySchema(groupId, artifactId, type, content, references)`
  and `SchemaRef(name, groupId, artifactId, version)`. Confluent `subject` ↔ Apicurio
  `artifactId` in group `default`; Confluent `schemaType` ↔ Apicurio artifact type.
- **`RegistryTls`** — builds an `SSLContext` from the `ssl.*` keys (PKCS12/JKS via
  `KeyStore`; PEM via `CertificateFactory` + `PKCS8EncodedKeySpec` for RSA and EC).
  No new dependencies.
- **SMT** — keeps passthrough rules, LRU cache, reference DFS and
  `behavior.on.error`, but talks to `SchemaRegistryClient` + `EnvelopeCodec`. Adds a
  negative cache keyed on source id with a TTL.

## Reference handling

Registries assign their own version numbers. The DFS therefore: (1) resolves each
reference's source id via `lookupId`, (2) ensures it on the target, (3) rewrites the
reference's `version` to the version the target assigned, (4) upserts the parent with
the rewritten references. Apicurio→Apicurio keeps today's behaviour (references passed
by coordinates, server-side resolution) but now also benefits from step 3.
`upsert` on both registries is idempotent, so retries are safe.

Target subject/artifactId equals the source subject unless `target.subject.prefix` is
set, in which case the record's top-level schema is registered under
`<prefix><source subject>`. References are never prefixed. When a prefix is set, the
cache is not seeded from reference registrations, so the same schema seen top-level is
registered again under the prefixed subject.

## Out of scope

- Apicurio REST v3 native client (Apicurio 3.x serves v2 as a compatibility API).
- Operator CRD changes — `Mm2ConfigBuilder` keeps emitting the Apicurio defaults.

## Testing

- Codec unit tests (parse/encode, boundary lengths, non-envelope bytes).
- `ConfluentClient` against an in-process fake (`FakeConfluent`) mirroring the style of
  the existing `FakeApicurio`.
- `RegistryTls` against an in-process HTTPS server requiring a client certificate;
  keystores generated with `keytool` at test time, PEM derived from them.
- SMT end-to-end unit tests: Apicurio→Apicurio (existing), Confluent→Apicurio with
  references, negative cache, `ssl.*` wiring.
- `maven.compiler.release` lowered to 17 for Strimzi's Java 17 images.
- Live MirrorMaker validation is done by the user; `make e2e` not run for this change.
