# Storage

How bigname persists raw facts, identity, normalized events, projections, and execution. Architecture model in [`architecture.md`](architecture.md); intake in [`chain-intake.md`](chain-intake.md); manifest schema in [`manifests.md`](manifests.md); read model in [`projections.md`](projections.md); execution layout in [`execution.md`](execution.md).

## Layers

The system of record splits into six layers, each owned by exactly one writer.

```
┌────────────────────────────────────────────────────────────────────┐
│ 1  chain_lineage      block ancestry, head promotion               │
├────────────────────────────────────────────────────────────────────┤
│ 2  raw_facts          selected logs + minimal tx/receipt fields    │
│                       call snapshots, code-hash observations,      │
│                       compact payload-cache metadata               │
├────────────────────────────────────────────────────────────────────┤
│ 3  manifests +        source manifests, discovery edges,           │
│    discovery          watch plan                                   │
├────────────────────────────────────────────────────────────────────┤
│ 4  identity +         NameSurface, SurfaceBinding, resources,      │
│    events             token_lineages, normalized_events            │
├────────────────────────────────────────────────────────────────────┤
│ 5  projections        current-state read models                    │
├────────────────────────────────────────────────────────────────────┤
│ 6  execution          traces, steps, cache outcomes                │
└────────────────────────────────────────────────────────────────────┘
```

Layers 1–5 rebuild current declared state. Layer 6 replays verified answers and explains them.

A worker-owned audit family lives alongside as `manifest_alert_*` for drift observations. It records findings; it isn't manifest truth, discovery admission, or projection state.

## Substrates

**Postgres** is the hot indexed and replay-focused store. It retains:

- lineage and header anchors
- selected target logs and the minimal tx/receipt fields needed to decode them
- block-scoped call snapshots required by replay
- code-hash observations
- compact metadata and optional digests for full payloads fetched as cache

**Hash-addressed object storage** (S3-compatible) holds large execution payloads — CCIP bodies, big metadata responses, trace attachments. Postgres records the digest, size, content type, and object key; the bytes live in the bucket. Object storage is a durability boundary only for raw payload classes a doc-first policy explicitly declares durable. Otherwise it's evictable cache.

## Write ownership

| Family | Writer | Purpose |
| --- | --- | --- |
| `chain_*` | indexer | lineage, canonical block graph |
| `raw_*` | indexer | immutable hot replay facts, payload-cache metadata |
| `backfill_*` | worker / backfill | persisted backfill jobs, range checkpoints |
| `normalized_replay_*` | indexer | replay cursors |
| `manifest_*` | manifests / discovery | source manifests, declared admission, capabilities |
| `discovery_*` | manifests / discovery | reachable contract graph, watch plan |
| `manifest_alert_*` | worker / audit | drift observations |
| `name_surfaces`, `surface_bindings`, `resources`, `token_lineages` | adapters | identity anchors |
| `normalized_events` | adapters | append-only normalized events |
| `projection_*` | projection workers | disposable read models |
| `current_projection_replay_status` | projection workers | replay completion markers |
| `execution_*` | execution workers (+ reorg-only delete path) | traces, steps, cache outcomes |

The API is read-only against storage.

The only non-execution-worker write to `execution_*` is the synchronous reorg path: it may delete or invalidate `execution_cache_outcomes` rows whose dependencies hit an orphaned block. Traces, steps, attachments, and normal outcomes stay execution-owned.

## Identity

### Deterministic text IDs

`logical_name_id = "<namespace>:<normalized_name>"` — stable, human-auditable, derivable without DB lookup.

### Opaque UUIDs

- `resource_id`
- `token_lineage_id`
- `contract_instance_id`
- `surface_binding_id`
- `execution_trace_id`

These survive projection rebuilds. Token IDs, node hashes, and resolver addresses are attributes — never identity anchors.

### Append-only event IDs

`bigint generated always as identity` for raw fact rows, normalized event rows, and projection job rows.

### Continuity rules

The cross-layer rules (ENSv1 wrap/unwrap/re-registration, ENSv2 token regeneration, proxy churn) live in [`architecture.md`](architecture.md) under § Identity. Storage's job is to enforce these guarantees:

- One admitted contract address on one chain → one stable `contract_instance_id` across all admission epochs. Re-admission appends a non-overlapping active range.
- Proxies and implementations are separate `contract_instance_id`s linked by a time-ranged proxy/implementation edge.
- Contract addresses are time-ranged attributes, never primary keys.
- For interval rows like `surface_bindings`, `active_from` and the stable identity anchors are immutable. `active_to` is replay-derived. Canonical replay can tighten an existing non-null `active_to` to an earlier close point; it never reopens or extends a closed interval.

For ENSv2, `resource_id` keys by `(chain_id, registry_contract_instance_id, upstream_eac_resource)` after observing the upstream EAC resource — not by the current ERC-1155 token id. `TokenResource(tokenId, resource)` and `TokenRegenerated(oldTokenId, newTokenId)` link and refresh tokens; the resource stays put. Unregister/re-register increments both `eacVersionId` and `tokenVersionId`, minting fresh ids.[^v2-pr-token]

## Canonicality

`chain_lineage` keys by `(chain_id, block_hash)` and persists the recent reconciled block window:

- `parent_hash`
- `block_number`
- `timestamp`
- checkpoint-promotion state

Header audit fields (`logs_bloom`, `transactions_root`, `receipts_root`, `state_root`) are optional retention via `chain_header_audit`. Reorg repair walks `(block_hash, parent_hash)` until it reaches a stored ancestor, then marks the losing branch and dependent facts non-canonical from that point forward.

`raw_blocks` is not durable. Intake, replay, workers, adapters, audit helpers, and tests read block timestamps and canonicality from `chain_lineage` and read optional audit roots/bloom from `chain_header_audit` when enabled.

Every reorg-invalidatable row carries `chain_id`, `block_number`, `block_hash`, `canonicality_state`, `observed_at`. States: `observed`, `canonical`, `safe`, `finalized`, `orphaned`.

Rules:

- Block hash is the identity anchor; block number is position only.
- Fork detection marks rows `orphaned`; it doesn't delete them.
- Reorg repair preserves audit truth — orphaned rows remain so explain and history reads can reconstruct what was observed.
- Optional header audit fields are verified when both stored and incoming audit rows carry them. A minimal replay does not conflict with an existing auditable row solely because it omitted those fields.
- Projection rebuilds read `canonical`, `safe`, or `finalized` rows by default. History and audit tools can opt into `observed` and `orphaned`.
- Safe and finalized promotion is monotonic per chain.

## Raw-log retention

`raw_logs`, selected `raw_transactions`, and selected `raw_receipts` have two retention modes selected operationally:

| Mode | Behavior |
| --- | --- |
| `minimal` | Adapter-replay staging. May be compacted after the normalized replay cursor advances past the range and the corresponding `normalized_events`, identity rows, lineage rows, and projection inputs are durable. |
| `log-audit` | Same rows remain durable audit facts with heavier indexes for historical raw-fact replay. |

`bigname-worker raw-facts compact-log-staging` is the manual compaction boundary for `minimal` mode. It refuses to compact unless the replay cursor is caught up and failure-free, and only operates on raw-log staging families.

After compaction, `chain_lineage` and compact `raw_blocks` remain the block-hash path for losing-branch repair, and `normalized_events` carry the block identity, source identity, event identity, and provenance needed by projection rebuilds and history reads. Historical adapter replay from compacted ranges is an explicit backfill / refetch operation against the configured provider/cache substrate, or requires log-audit retention. It's not an implicit API fallback.

## Evictable payload cache

Large or full block payloads, non-indexed tx/receipt/block bodies, and non-audit raw-log staging rows are evictable cache by default once the selected replay contract has been satisfied. They may live inline during a hot window, in local/provider cache, in object storage, or not be retained at all.

Retained cache metadata describes what was fetched: payload kind, chain id, block hash/number where block-scoped, optional digest, size, content type/encoding, source observation metadata, observed time, canonicality state. **A retained digest authorizes later byte use; metadata without one cannot.**

Provider re-fetch is an explicit, fallible cache-fill path. For block-scoped payloads:

- block-hash-scoped
- verifies the retained digest before any bytes are used
- fails closed when the digest is absent, mismatches, or the provider can't serve the exact historical payload

It is not a substitute for retained lineage, normalized events, execution artifacts, or orphaned-branch audit truth.

Local execution-client storage (e.g. a same-host Reth database) is provider/cache substrate, not a new storage family. Client table keys, row cursors, static-file offsets, and data-directory paths appear only in operational source metadata or evictable cache metadata — never as durable `raw_fact_ref` identities, normalized-event provenance, or projection inputs.

## Manifests and discovery

- `contract_instances` — one row per stable `contract_instance_id` with chain, contract kind, and provenance. Roots use the same identity family as ordinary instances.
- `contract_instance_addresses` — time-ranged address attributes keyed by `contract_instance_id`. One id may carry multiple non-overlapping active ranges. Manifest-declared address ranges may carry nullable inclusive `start_block`.
- `discovery_edges` — keyed by `edge_id` with `from_contract_instance_id`, `to_contract_instance_id`, `edge_kind`, active range, provenance, canonicality.
- Materialized watch-plan rows keyed by `contract_instance_id`. Address is a derived watch target, not durable identity. An omitted `start_block` is null, not zero.

Resolver-profile admission state (PublicResolver-generation profiles for ENSv1, `L2Resolver` compatibility for Basenames) is gated separately from contract-instance admission. It can be derived from existing discovery provenance, normalized resolver-discovery events, manifest contract roles, code-hash facts, and proxy/implementation edges; a dedicated profile-fact table isn't required. Profile admission gates complete-family, resolver-overview, latest-only, authorization, and onchain-call parity claims for the affected resolver instance — not baseline generic resolver-event observation.[^v1-pres][^bn-l2resolver]

`manifest_alert_*` rows carry observation kind (`manifest_drift` or `proxy_implementation_drift`), lifecycle status, manifest version, source family, chain, contract-instance references, nullable proxy/implementation edge references, expected and observed code-hash material, derived watch-plan metadata, first/last observed timestamps, and nullable remediation metadata. Writing one doesn't write `normalized_events`, mutate manifest truth, mutate discovery admission, change capability flags, or expose API state. A proxy implementation observation preserves the proxy `contract_instance_id`; implementation churn is an edge.

## Backfill

- `backfill_jobs` — one row per bounded job: profile, chain, selector kind, resolved source identity, scan mode, declared range, idempotency key, lifecycle status, failure metadata, timestamps.
- `backfill_ranges` — child range records: bounds, next checkpoint, lease owner/token/expiry, attempt counters, lifecycle, failure metadata.
- Helper-owned monotonic checkpoints let a worker resume after crash without widening the original range.

Selector identity:

| Field | Meaning |
| --- | --- |
| `selector_kind` | `whole_active_watched_chain`, `source_family`, or `watched_target_set` |
| `source_family` | requested family (when applicable) |
| `requested_watched_targets` | caller-supplied targets (when applicable) |
| `selected_targets` | resolved sorted target set |
| `source_identity_hash` | digest of the above |

Very large source-family jobs may persist a compact selector identity instead of a full target array — `source_identity_payload_format=selected_targets_digest_v1` carries `selected_target_count`, `selected_targets_digest_algorithm`, `selected_targets_digest`, a first/last `selected_targets_sample`, and `source_identity_hash`. The digest input is the sorted canonical tuple.

`effective_to_block` is finite at creation. Bootstrap ranges go from each eligible target's manifest/discovery admitted start to the finite provider head observed at job creation. A target whose manifest-declared `start_block` is unknown is skipped; bootstrap creates no synthetic range for it.

### Range checkpoint vs chain checkpoint

Backfill range checkpoints are operational state. They never change `canonicality_state` and never advance `canonical_head`, `safe_head`, or `finalized_head`.

Backfill raw admission still writes canonicality for the facts it admits. When the admitted range is already proven canonical/safe/finalized by retained lineage or provider checkpoint evidence, new lineage, raw-fact, and normalized-event rows use that state rather than staying `observed`.

## Partitioning baseline

Partitioned tables:

- `chain_lineage`
- `chain_header_audit` (when retention produces enough rows to justify it)
- `raw_transactions`
- `raw_receipts`
- `raw_logs`
- `normalized_events`
- `execution_steps`

Partition keys: `chain_id` and block-number range. Current-state projection tables start unpartitioned unless measurements prove otherwise.

## Reorg repair

Reorg repair preserves audit truth: orphaned rows persist for explanation and rebuild, not deletion. Lineage, identity rows, and normalized events for the losing branch stay `orphaned` so explain and history routes can still reconstruct what was observed.

Execution cache rows follow the same hash-first canonicality rule. When repair marks a block identity `orphaned`, the synchronous indexer/reorg path invalidates or deletes any reusable `execution_cache_outcomes` whose dependency set includes that `(chain_id, block_hash)` or a boundary resolved through one. Traces, steps, and attachments stay durable.

Reusable `execution_cache_outcomes` rows must carry dependencies tied to explicit block-hash-bearing chain positions or boundaries. Rows that lack those dependencies fail closed.

## Replay

Raw-fact normalized-event replay is indexer-orchestrated over the adapter-owned `normalized_events` boundary. It selects bounded canonical raw facts and asks adapters to perform an upsert-only resync. It advances only its own `normalized_replay_*` cursor.

Whole-range replay is the default. Automatic bootstrap and automatic catch-up share one all-source chain cursor over persisted canonical raw facts in block order — adapter-owned identity histories combine registry, registrar, wrapper, resolver, and reverse-claim signals into one storage write boundary, so independent per-source-family cursors would tear those histories.

Source-scoped or per-target replay is an operational repair mode. It narrows the raw-log selection and adapter source scope. It doesn't narrow canonicality, change persisted backfill job identity, delete raw facts from other sources, mutate discovery or manifests, or graduate coverage.

Replay reads canonical durable hot facts first. It may use a retained durable cold payload only when an explicit replay contract requires it. For block-scoped payloads, provider re-fetch is allowed only through the digest-checked, fail-closed cache-fill path.

Replay does not delete stale `normalized_events` or replace existing payloads for an already persisted normalized-event identity. Inserts populate absent rows and refresh canonicality for matching identities; conflicting payloads remain mismatches. Adapter-owned identity rows may be marked `orphaned` only when those rows have no backing normalized event, were produced by the same adapter boundary, and would otherwise overlap the incoming identity interval.

Replay does not mutate `chain_*`, `raw_*`, `backfill_*`, `projection_*`, `execution_*`, manifests, discovery rows, public API state, or checkpoint promotion state.

### Adapter repair

Explicit adapter repair is narrower than replay. It exists for deterministic adapter bugs where the persisted normalized-event identity is correct but a small payload field was encoded lossily. It is bounded by existing `normalized_events` rows, matches the retained `(chain_id, block_hash, transaction_hash, log_index)` identity, decodes through adapter-owned logic, and updates only documented lossy fields. In minimal raw-log deployments, repair may fetch exact historical logs directly from the configured provider or same-host Reth substrate without re-materializing `raw_logs`.

The currently admitted repair: ENSv1 PublicResolver-compatible `TextChanged` payload repair. Legacy generic `RecordChanged` rows with `record_family=text`, `record_key=text`, `selector_key=null` are rewritten to selector-specific `text:<key>` rows. Selector-specific text rows missing a retained value have it filled when the source log verifies against the indexed key hash.[^v1-text]

### Bulk-load index deferral

During fresh normalized replay (current projection tables empty, replay cursor not at target), the indexer may defer normalized-event indexes that exist only for projection/API readback while keeping replay-required indexes for event identity, reverse-claim lookup, and latest resolver/version preloads. Deferred indexes are recreated before projection rebuilds or API-ready declared reads complete.

`current_projection_replay_status` rows let worker restarts resume from the first unfinished projection family instead of restarting bootstrap from the start. They are operational worker state — not API truth, not projection data, and ignored unless the recorded replay version is current.

## Projection storage rules

Every current-state projection row carries provenance pointers, manifest version, relevant chain positions, canonicality summary, and `last_recomputed`.

Projection tables may be truncated and rebuilt from canonical facts plus normalized events.

Exact-name snapshot selection is a storage read boundary, not a new family. The API resolves `at`, explicit `chain_positions`, and `consistency` to one concrete `ChainPositions`, then reads only projection rows and execution outputs eligible for that exact object. `name_current`, `coverage_current`, `surface_bindings_current`, `permissions_current`, and `record_inventory_current` retain enough chain-position context for the API to reject mismatched joins rather than combine rows from different snapshots.

If the selected positions are valid but no eligible projection or persisted execution output exists, the serving path returns the documented `stale`, `unsupported`, or `not_found` API state. It doesn't read raw facts, adapter-owned identity/event rows, or provider data directly to fill the public response.

## Execution storage

Inline in Postgres for small payloads:

- request metadata
- response digests
- decoded final values
- failure reasons

In hash-addressed object storage, addressed by SHA-256 digest:

- CCIP payload bodies
- large metadata responses
- trace attachments

Postgres records the digest, size, content type, and object key for each attachment.

`execution_traces` and `execution_steps` preserve what was executed and why. Normal `execution_cache_outcomes` writes record whether a verified outcome can be reused under its request key, manifest versions, and block-hash-bearing dependency boundaries. The reorg-invalidation exception is the only non-execution-worker write path.

Exact block-anchored `raw_call_snapshots` used by verified resolution stay in the intake-owned `raw_*` family. Execution may hand off candidate snapshots only through the raw-fact boundary, only for the exact requested chain position, and only for support classes that admit them.

Before a verified-resolution selector persists as a supported reusable outcome, execution reloads the exact manifest versions for the request, the same declared topology snapshot a mixed route would serve, and any resolver-profile admission state required by participating resolver-local fact families. The frozen support class derives from those stored inputs and matches the persisted trace and cache key. If those inputs can't re-establish one frozen class, the trace remains a durable audit artifact but the selector doesn't persist as a supported reusable outcome.

## Inspection

Read-only worker-owned tooling reads storage audit helpers and renders stable JSON. None of these creates a public `v1` route or mutates state.

| Command | What it shows |
| --- | --- |
| `bigname-worker inspect canonicality --chain-id <id> --block-hash <hash>` | One stored block: lineage, parent hash, block number, canonicality state, optional header audit, raw fact counts, payload-cache metadata counts, normalized-event counts. |
| `bigname-worker inspect stored-lineage-range --chain-id <id> --from <block> --to <block>` | Stored lineage rows in `(block_number, block_hash)` order. Doesn't infer missing heights or gaps. |
| `bigname-worker inspect backfill-job --backfill-job-id <id>` | One persisted job and its child ranges. Sorted by range bounds and id. |
| `bigname-worker inspect execution-trace --execution-trace-id <id>` | One stored trace plus its steps and attachment metadata. |
| `bigname-worker inspect manifest-drift --json` | Persisted alert observations from `manifest_alert_*`. Read-only — doesn't fetch fresh chain state, mutate alerts, or change manifests. |
| `bigname-worker inspect watch-plan --json` | Active watched contracts with source kind, source families, instance IDs, addresses, manifest IDs, active block ranges. |

## Migrations

- Schema changes land through checked-in migrations only.
- Append-only tables prefer additive changes over destructive rewrites.
- Backfill job and range checkpoint storage lands as additive `backfill_*` tables or columns. It does not overload `chain_lineage`, projection job state, or public API tables.
- Projection tables may be recreated when the rebuild path already exists.
- Migrations that change a shared interface require the companion doc update first.

## Ownership summary

- Storage owns migrations and query primitives.
- Storage owns backfill job/range helpers (idempotent create, reserve, advance, complete, fail).
- Worker/backfill code owns operational writes to `backfill_*` through those helpers, including finalized catch-up chunks and capacity pause/failure metadata.
- Adapters own inserts into identity and `normalized_events` tables.
- Projection workers own materialized read models.
- Execution workers own trace and step writes plus normal cache outcome writes.
- Synchronous indexer/reorg repair owns only `execution_cache_outcomes` invalidations tied to orphaned blocks.
- Raw-fact normalized-event replay is indexer orchestration over the adapter-owned `normalized_events` boundary.
- Intake owns durable hot raw-fact writes plus optional payload-cache metadata.
- API code does not query raw-fact tables directly except for explicit audit endpoints.
- Inspection tooling is worker-owned and read-only.
- Manifest-drift and proxy alerting writes only `manifest_alert_*`.

---

[^v2-pr-token]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L451 @ ens_v2@554c309)
[^v1-pres]: (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L20 @ ens_v1@91c966f)
[^bn-l2resolver]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L22 @ basenames@1809bbc)
[^v1-text]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/TextResolver.sol:L21 @ ens_v1@91c966f)
