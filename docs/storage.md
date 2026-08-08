# Storage

PostgreSQL is the durable indexing and serving store. Current runtime objects
live in `bigname_phase`; the append-only SQLx history in `migrations/` records
the retired `public` schema and ends with a schema-qualified deletion migration.

## Invariants

- [Raw facts](glossary.md#raw-fact) are immutable and block-hash anchored.
- [Canonicality](glossary.md#canonicality) is explicit; block number alone is
  never sufficient identity.
- Interpretation output and [projections](glossary.md#projection) are
  rebuildable.
- Execution-provider responses are request-scoped. They are not persisted as
  reusable outcomes or durable traces.
- Unsupported behavior is stored and returned explicitly.
- API serving reads use `bigname_phase`; they have no fallback to legacy
  `public` tables.

## Schema and layers

`phase-runner init-schema` installs the fresh baseline into an empty
`bigname_phase` namespace and refuses a nonempty target. The phase runner and
API use that namespace in one database. A reviewed replacement procedure is
required when the baseline cannot be upgraded in place.

The physical layers are:

1. lineage and head state — `chain_lineage`, `chain_header_audit`,
   `chain_heads`, and per-phase progress;
2. selected immutable raw facts — admitted blocks, transactions, receipts,
   and logs;
3. manifests and discovery — synchronized declarations, admitted contract
   instances, capability state, and discovered edges;
4. interpreted identity and events — name surfaces, bindings, resources, token
   lineages, label preimages, and normalized events; and
5. current projections — name, relation, child, permission, resolver, record,
   and primary-name read models.

The first four layers are inputs to Project. Current projections can be rebuilt
from canonical identity and normalized events. Canonical-head
[hydration](glossary.md#hydration) is execution-derived enrichment applied by
Project to the documented record and primary-name surfaces after event-derived
publication.

## Identity

Stable identity follows [ADR 0002](adrs/0002-surface-resource-identity.md) and
the continuity rules in [`architecture.md`](architecture.md#identity-strategy).

- deterministic namehash-based IDs identify chain-native name surfaces;
- opaque UUIDs identify backing resources, bindings, and token lineages where
  upstream has no stable global identifier; and
- monotonic database IDs are limited to append-only observation order where
  they are not public identity.

Permissions and control attach to `resource_id`, not display text. Historical
surface-to-resource changes remain reconstructible through `surface_bindings`.
Canonical display text is derived from verified preimages and normalization
state; it is never identity.

## Table ownership

| Family | Writer | Meaning |
| --- | --- | --- |
| `chain_lineage`, `chain_header_audit`, `chain_heads`, ingest cursors | Ingest and head publication | Block ancestry, readable heads, source progress, and explicit canonicality. |
| selected `raw_*` | Ingest | Immutable transaction, receipt, and log interpretation inputs. |
| `manifest_*` | manifest synchronization | Authored source declarations and admitted capability versions. |
| `discovery_*` | Interpret | Canonical discovered edges and admission evidence. |
| `name_surfaces`, `surface_bindings`, `resources`, `token_lineages` | Interpret | Stable identity anchors. |
| `label_preimages` | Interpret and `phase-runner label-preimages import-ens-rainbow` | Verified labelhash-to-label observations from chain events and the proof-checked rainbow import. |
| `ens_names` | operator rainbow load | Unverified rainbow-table candidates consumed by the import command. |
| `normalized_events` | Interpret | Protocol events normalized transactionally with identity output. |
| `*_current` projection families | Project | Current serving state, rebuildable from canonical interpreted input. |
| `chain_phase_state`, redo/invalidation state, `service_heartbeats` | phase runner | Phase progress, repair work, and runtime liveness. |
| `resolution_divergences` | guarded lookup functions | Active live/indexed resolver disagreements; diagnostic only. |

Adapters provide interpretation behavior. They do not write projections. API
code reads projections and lookup output only, except for the guarded
[resolution divergence ledger](glossary.md#resolution-divergence-ledger) write.

## Raw facts and payload retention

Ingest persists the minimum selected transaction, receipt, and log fields
needed to reproduce interpretation. These `bigname_phase` rows are the complete
current raw-fact family; there is no retained call-snapshot or generic payload-
cache table. Project hydration and request-scoped lookup call providers at an
explicit block position without turning those responses into raw facts.

`label_preimages` stores a verified labelhash-to-label observation. A preimage
may improve readability but cannot create a surface, ownership, resolver,
record, permission, or primary-name fact by itself.

## Rainbow-table preimage import

The ENS rainbow table maps a labelhash to a candidate human-readable label.
`ens_names` keeps the upstream table shape — one `(hash, name)` row per
candidate, where the generator records `hash =
keccak256(name)`.[^graph-ens-rainbow-table][^graph-ens-rainbow-hash]

Authority stays split between the two tables. `ens_names` is an unverified
staging store: every operator-loaded row is only a claim. `label_preimages`
remains the verified store: a candidate becomes a preimage row only when it is
exactly one DNS label and re-hashes — keccak256 of the raw label bytes — to the
row's recorded hash. A rejected candidate leaves no trace beyond the import's
logged counters. Collapsing the split would put unverified claims into the
verified store, so it stays.

`phase-runner label-preimages import-ens-rainbow` walks `ens_names` in
hash-keyset batches, proof-checks every row, and inserts the survivors with
`source_kind = 'ens_rainbow_import'` at priority 10 — below the interpreter's
chain-observed priority 100, so a later chain observation of the same label
takes provenance precedence. Conflicts on the `label_preimages` primary key
insert nothing: a re-run is a no-op and an existing verified row is never
rewritten. Rows carry the same normalization verdict the interpreter stores —
a proof-checked label whose bytes differ from their normalized form is kept
with `normalized_under_version = false` and the reason, not discarded. The
deleted worker importer instead hashed the normalized form, which admitted only
already-normalized labels; the current schema keys the labelhash on the raw
bytes and stores the verdict as a flag, so the port proofs the raw bytes.

The import writes `label_preimages` only — never projection rows. Projections
pick up the new preimages through Project:

- On a fresh deployment, load `ens_names` and run the import before the first
  Project walk; the first walk derives child names with the preimages present.
- On a populated database, run the import and then redo Project over each
  chain's full retained range, for example
  `phase-runner redo --chain ethereum-mainnet --phase project --from-block <first retained block> --to-block <head>`.
  A windowed or incremental Project run re-derives only the names touched by
  events in its window, so it does not pick up preimages for older child edges;
  the full-range redo is the required sequence.

[^graph-ens-rainbow-table]: (upstream: .refs/ens_rainbow/src/main.rs:L36 @ ens_rainbow@bc44492)
[^graph-ens-rainbow-hash]: (upstream: .refs/ens_rainbow/src/main.rs:L50 @ ens_rainbow@bc44492)

## Canonicality and reorgs

Every fact-derived row that can be invalidated by a reorg carries chain,
number, hash, and canonicality evidence. `chain_lineage` is the authority for
parentage and readable block identity. Serving paths never join the deleted
`public.chain_lineage` table.

Head publication walks by block hash, marks the orphaned suffix explicitly,
publishes the replacement readable head, and records downstream redo in one
transaction. The same transaction clears active resolution-divergence
observations whose recorded positions include an orphaned block. There is no
legacy execution-cache invalidation call.

Project output is admitted only when its publication target is at or before the
selected readable head. Equal-height admission requires the selected block hash
to match. Name, relation, inventory, primary-name, and GraphQL reads all apply
this rule against phase lineage.

## Interpretation replay

Interpretation is deterministic for a fixed manifest set, interpreter content
hash, canonical raw facts, and requested block range. A bounded Interpret redo
may replace only derived identity, discovery, and normalized-event output in
that range. Raw facts are never edited by replay.

The interpret engine loads the prior identity state required by the range,
folds physical batches without changing semantic order, and revalidates the
resume marker and current block anchors in the write transaction. A concurrent
reorg therefore cannot publish interpretation derived from an unreadable
branch.

Redo preparation restages only identities anchored inside the range, so an
identity derived before it keeps its anchor even when an in-range event
references it. An identity the replay re-observes is restored by the ordinary
upsert at its first derivation block; only one still orphaned afterwards is
re-anchored, and a name surface re-anchors from the earliest surviving
observation that carries the name itself, staying orphaned when none survives.
Outside that orphan replacement, a name surface's `deactivated_at` moves only
for a strictly lower incoming block, so the stored value does not depend on the
order emissions arrive in.

The interpreter content hash covers the current interpretation inputs: the
adapter, manifest-authority, and project sources, the manifest ABI event
declarations, and the named semantic dependencies those sources call to decide
a persisted row — ENS normalization, plus the resolver-call encode/decode,
record-selector vocabulary, batched record and reverse-name read helpers, and
the JSON-RPC envelope interpretation deciding which provider response those
helpers accept as an answer. Interpret's persistence stage is covered on the
same rule: which interpreted row wins a conflict, how a redo range reopens and
reanchors bindings, and which surfaces a normalizer-version recompute
activates all decide which identity, discovery, and label-preimage rows the
projections then read, so they are interpretation rather than plumbing.
Interpret's batch sizing stays outside, because folding the same events into
differently sized physical batches produces the same rows. Request-scoped
serving is outside because it writes no interpreted, discovery, or projection
row — the guarded divergence ledger is diagnostic output, not interpretation
input. The rest of RPC transport — client construction, timeouts, and endpoint
configuration — is outside because it can only abort a request, never reshape
an answer. So a serving-only change does not force a re-derivation.

Several semantic surfaces are outside the hash today and are guarded by review
rather than by a rotation:

- interpret's input loader — which earlier interpreted state an adapter sees,
  which manifest versions, discovery rules, admitted address ranges, and
  canonical blocks it reads, and the order raw logs arrive in;
- the interpret engine's redo and completion gates, which decide whether a run
  clears a redo range or reanchors stable identities, and its prior-session
  reuse rule, which decides whether a batch folds onto retained adapter state
  or reloads it;
- the phase runner, which owns the redo marker, decides the replay range each
  run receives, and publishes the
  [lineage orphaning epoch](glossary.md#lineage-orphaning-epoch) interpret's
  prior cache revalidates against. It is outside the hash on purpose — no
  semantic interpretation may live there — but that is a rule it must be held
  to, not a property the hash enforces;
- checked-in SQL, meaning migration trigger bodies and the schema-v2 baseline
  constraints;
- chain intake's event-signature allowlist, which decides which resolver and
  registry-announcement logs become raw facts at all on the all-emitter path —
  the logs matched by topic with no address filter, and so the only way an
  unwatched emitter's logs are retained. A change there is a re-ingest decision
  as well as a re-derivation one.

Treat a change to any of them as a re-derivation decision and follow the
[planned migration and fingerprint boundary](runbooks/production-docker.md#planned-migration-and-fingerprint-boundary).

A hash rotation requires a planned full-history interpretation and projection
walk; the system refuses to mix generations from different hashes. Moving a
covered semantic source without updating the covered set fails the build rather
than silently narrowing the fingerprint.

## Projection publication

Project is the only projection writer. It derives the affected scope from
canonical interpreted input, stages rows in connection-local tables, and
publishes the affected projection set transactionally. It has no legacy claim
queue, durable replay stage tables, apply cursors, dead-letter queue, database
session version stamp, or worker heartbeat.

Projection rows carry:

- stable identity keys;
- manifest and source-family evidence;
- support status and an explicit unsupported reason when applicable;
- canonical chain-position or target-publication evidence; and
- the last recomputation time.

An unchanged row may retain an earlier publication target when a later Project
run does not affect its scope. Serving admission therefore accepts targets at
or before the selected head rather than requiring every row to equal the latest
Project block.

The projection families used by the API include:

- `name_current` and identity companions;
- `address_names_current`;
- `children_current`;
- `permissions_current` and its per-resource summary;
- `resolver_current`;
- `record_inventory_current`; and
- `primary_names_current`.

Coverage wording is not an exhaustiveness claim. `support_status` and
`unsupported_reason` carry admission separately from projection completeness.
Readers fail closed on unknown or inconsistent vocabulary.

## Snapshot serving

Snapshot selection resolves `at`, explicit `chain_positions`, and consistency
to one concrete set of phase chain positions. Current head, safe, and finalized
positions come from `chain_heads`; timestamp and historical selection use
readable `chain_lineage` rows.

Every selection also requires the current Project generation to be complete at
the newest stored head with the API's compiled interpreter content hash. The
API reads only projections eligible for the selected positions and revalidates
the Project generation before returning. A concurrent head or generation
change returns `409 stale`.

GraphQL carries the selected ENS head from the root operation into nested record
inventory reads, scopes list and count queries to the same selected chains, and
excludes unsupported name rows. An unsupported inventory maps to the existing
empty compatibility shape.

## Verified lookup storage

Schema-v2 verified lookup has no execution cache, durable trace, reusable
outcome, or persisted request-validation state. Each admitted provider lookup
runs for the current request at the selected block identity. See
[`execution.md`](execution.md).

For guarded direct resolver comparisons, fixed-`search_path`,
security-definer functions revalidate the selected state and then create,
refresh, or clear one active divergence observation. The API role receives
`EXECUTE` on those functions but no direct write access to
`resolution_divergences`. Ledger rows are durable operational observations;
they are not projection input or a response cache.

## Inspection

`phase-runner inspect` provides bounded, read-only block-canonicality,
stored-lineage, and raw-event windows. These commands do not expose public API
routes and do not mutate raw facts, canonicality, manifests, or projections.
There is no worker inspection surface for backfill jobs, replay staging,
manifest drift, watch plans, or execution traces.

## Migration rules

- SQLx migrations are append-only and versioned. Applied files are never
  rewritten.
- Every destructive migration names the target schema explicitly. This is
  mandatory where retired `public` table names collide with `bigname_phase`
  names.
- Schema-v2 baseline changes require either a reviewed in-place upgrade or the
  documented offline namespace replacement and full pipeline walk.
- A migration that changes identity, manifest authority, canonicality,
  projection meaning, or replay behavior updates the corresponding contract
  docs in the same change.
- The generic worker migration entrypoint has been deleted. Deployment
  automation applies reviewed versioned migrations at a planned boundary.

The legacy deletion migration removes only schema-qualified `public` objects.
It preserves the SQLx migration ledger and extension-owned objects.

## Repository ownership

- `crates/ingest` and phase-runner intake own immutable chain facts and lineage.
- `crates/interpret` plus adapters own derived identity, discovery, and
  normalized events.
- `crates/project` and the phase runner own current projection publication.
- `crates/lookup` owns verified lookup behavior and guarded divergence writes.
- `crates/storage` provides the typed persistence and read boundaries above.
- `apps/api` reads phase projections and lookup output; it does not write raw
  facts, interpretation output, projection rows, or legacy execution artifacts.
