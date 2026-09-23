# Server-provided Fast Client multi-key retry policy

Servers can advertise one cluster-scoped long-tail retry range table for **compute and multi-get (batch-get)**. This
does not change Thin Client/router retry tuning, retry enablement, retry budgets, error/429 handling, or request key
limits.

## Configuration

The optional server property is:

```properties
server.fast.client.multi.key.long.tail.retry.thresholds.ms=
```

Empty (the default) advertises no server policy. Configure it consistently on all servers in a cluster through your
server configuration deployment. Server config is validated at startup; this is not a restartless server-config
mechanism. Cluster selection belongs in deployment configuration, not OSS Java.

The format is `lower-upper:milliseconds,...,lower-:milliseconds`. Ranges may be unordered but must cover every positive
key count contiguously, without gaps or overlaps, starting at 1 and ending in an open-ended range. Bounds must fit a
positive Java integer. Delays must be positive and at most 2,147,483 ms so conversion to the existing
integer-microsecond timer API cannot overflow. Only the exact empty string withdraws a policy; whitespace-only and
malformed values are invalid.

For reference, the **unchanged existing client defaults** are:

```properties
server.fast.client.multi.key.long.tail.retry.thresholds.ms=1-12:8,13-20:30,21-150:50,151-500:100,501-:500
```

This example transports the existing defaults, not new latency tuning. In particular, the 501+ bucket remains 500 ms.
Choose new values only after measuring end-to-end latency, caller deadlines, retry amplification and server capacity. A
retry policy alone does not guarantee a latency SLO.

## Precedence and refresh

- Compute: explicit local compute range > server policy > existing compute default.
- Batch-get: positive fixed local microsecond threshold > explicit local batch-get range > server policy > existing
  batch-get default.

Calling a range setter explicitly with the default string is still an override. Cloning the client builder preserves
whether each range was explicitly set. Wrappers that skip the setter for an empty wrapper default continue to accept the
server policy. Existing custom `StoreMetadata` implementations can leave the default policy accessor unchanged and
continue using local behavior.

`MetadataResponseRecord.multiKeyLongTailRetryThresholdsInMs` carries the table. Clients parse each metadata response
once and publish one immutable policy only after the refresh succeeds. A request captures its effective delay once using
the original whole distinct-key count, before fanout; refresh does not retime pending retries. Empty requests delegate
without looking up a range or scheduling a retry timer.

Successful empty or old-schema responses clear the policy. Invalid nonempty responses retain the last valid policy
**only for the same store/cluster**, while other metadata can refresh. They increment the per-store
`invalid_multi_key_retry_policy.Count` metric and emit a rate-limited warning. Transport, schema lookup, or other
refresh failures retain the policy for the same origin. Once discovery changes cluster, the previous cluster's policy is
not exposed, even if the new cluster's fetch fails or policy is absent/invalid. There is no expiration TTL. Normal
metadata refresh timing applies; the default 60-second interval is not a propagation SLA. Policy publication is atomic;
the rest of the existing metadata cache is **not** a transaction.

The server fallback key cap remains 500 when no positive store `batchGetLimit` is configured. This cap applies to
compute and batch-get before transport fanout. Supporting 5,000-key requests requires a separately approved store cap
change; the open-ended retry range does not grant a larger cap.

## Schema rollout and rollback

This change activates metadata response schema **v5**; generated writer/reader schema and `SERVER_METADATA_RESPONSE`
version must advance together. Historical schemas v1-v4 must remain unchanged. Split delivery into a schema-only
merge/release followed by the retry-policy feature merge/release.

The schema-only release contains the v5 schema, protocol-version bump, empty-field initialization and generated-record
fixture compatibility changes. It does not read server policy configuration or change client retry selection. However,
schema generation already selects v5, `MetadataResponse` serializes against that generated schema, and its writer-schema
header follows `SERVER_METADATA_RESPONSE`. **A server using the schema-only release already emits v5 with an empty
policy.** Empty policy is not a wire-version switch, and this endpoint has no request-version negotiation.

1. Merge and release the schema-only changes. Deploy the matching library to **controllers/schema-registration
   components only**, and verify schema ID 5 and its exact schema in every relevant metadata response system-schema
   store **before deploying any server that contains either release**. The initialization routine registers versions
   through the active protocol enum; merely adding the schema resource does not register it. Keep older schemas
   available. Do not couple the initial controller rollout to a server rollout.
2. After registration is verified, merge/release the feature changes and update the matching OSS library dependencies
   for servers and clients. Deploy server capability initially with empty policy. Schema-only servers may also be
   deployed after the registration gate, but do not provide the dynamic retry feature.
3. Roll compatible clients and then configure server policies consistently in the intended clusters. The current-default
   table preserves delays; any new tuning or store-cap increase is separate work. Validate representative supported old
   clients: current clients can fetch the writer schema and resolve old/new records, but not every historical client is
   necessarily supported.

Avro v4 readers ignore the added v5 field; v5 readers default it to empty when reading v4 writers. Both require the
advertised writer schema to be available. An unavailable schema 5 can prevent metadata refresh, so wire compatibility
does not remove the registration prerequisite.

Rollback by deploying an empty policy to every server (or using an explicit local override). Clients fall back on their
next successful refresh. A successful old-schema response also withdraws the policy; a transport failure deliberately
does not. Mixed old/new servers or inconsistent configuration can alternate the effective policy, so converge the server
cohort and monitor refresh failures during rollout.
