# Storage

PostgreSQL is the durable indexing and serving store. Current runtime objects
live in `bigname_phase`; the append-only SQLx history in `migrations/` records
the retired `public` schema, its schema-qualified deletion, and reviewed
in-place schema-migrations for initialized `bigname_phase` databases.

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
API use that namespace in one database. Reviewed versioned schema-migrations
normally upgrade an initialized namespace in place when the change can preserve
its durable state; the reviewed replacement procedure is required otherwise.
An additive baseline index may be an explicitly reviewed release exception when
its production build must use `CREATE INDEX CONCURRENTLY`: the release runbook
must carry the exact live DDL, validity checks, recovery procedure, and
release-record evidence instead of silently treating the baseline edit as an
initialized-namespace upgrade.

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

## Query ownership

`crates/storage` owns [canonicality](glossary.md#canonicality), snapshot selection,
reusable row reads, and database invariants. These rules are shared across callers and remain
below route code even when a route composes them into a larger query.

`apps/api` owns route-specific joins, pagination, wire shaping, and GraphQL compatibility.
GraphQL compatibility queries therefore live with the API surface, while their reusable
canonicality predicates come from storage. API helpers that confirm one request reads against an
unchanged selected chain position also remain in `apps/api`; they are not reusable database reads.

This documented boundary is authoritative. `scripts/check-query-ownership` is a tripwire for
known naming patterns, not a complete classification of SQL ownership. Review for every new
direct-SQL module in `apps/api` must state whether storage or the API owns its query behavior.

The first four layers are inputs to Project, but ENSv1→ENSv2
migration-correlated contributions marked `consumer_visibility=candidate` are
diagnostic input only until their contracted consumer activation. Current
projections can be rebuilt from canonical consumer-visible identity and
normalized events. Canonical-head
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
| `project_redo_resolver_evidence` | Interpret, then Project consumption | Pre-delete resolver and permission-resource references preserved across Interpret retries for one redo range; redo coordination only, never serving data. |
| `migration_event_associations`, `migration_discovery_associations`, `migration_candidate_identity_effects`, `migration_candidate_discovery_effects` | Interpret | Correlation-versioned diagnostic associations and effects that slice 1 must not use to alter independently admitted normalized events, identity rows, or discovery edges. The ordinary `registry_announcement` indexability edge remains a watch-plan input. |
| `*_current` projection families | Project | Current serving state, rebuildable from canonical interpreted input. |
| `chain_phase_state`, redo/invalidation state, `service_heartbeats` | phase runner | Phase progress, repair work, and runtime liveness. |
| `project_generation_failures` (planned) | phase runner after Project rollback | Append-only audit evidence for a projection-blocking invariant failure; never a product projection. |
| `resolution_divergences` | guarded lookup functions | Active live/indexed resolver disagreements; diagnostic only. |

Adapters provide interpretation behavior. They do not write projections. API
code reads projections and lookup output only, except for the guarded
[resolution divergence ledger](glossary.md#resolution-divergence-ledger) write.

A non-retryable validation failure on an already-completed Ingest or Verify
row changes its lifecycle status from `completed` to `failed` without clearing
the retained range markers, source provenance, verification level, or content
hash. A retained row may be restored without replay only from structural
evidence: Ingest requires matching current, target, and live-handoff markers;
Verify requires matching current and target markers plus a verification level.
The next accepted start repeats the checks for the retained completion and
moves that row through `failed` to `completed`. Error text alone never
authorizes that transition. This preserved evidence is diagnostic state, not
permission to publish: provider-trusted Sepolia readiness requires both Ingest
and Verify to remain completed.

At runner startup, a `running` or `paused` Interpret, Project, or Verify row
with no explicit redo is resolved only while its advisory lock remains held.
A saved Interpret or Project final checkpoint is recorded as `completed`; an
earlier checkpoint is recorded as `failed` so ordinary phase execution can
resume it. A saved Verify final checkpoint stays `failed` until current
configuration and retained verification evidence pass the completed-Verify
checks. A lock still held by another runner, or a lost lock connection during
the state update, stops the new runner. The update and lock use one database
connection. If the client cannot tell whether PostgreSQL committed the update
before that connection failed, the next start reads the durable phase state
again. An unlock or connection-close error after an acknowledged update is also
reported.

Startup settlement for a chain absent from runtime configuration records
`settled_while_unconfigured = true` on every active phase row that it changes to
`completed`. This nullable marker distinguishes a row deliberately settled
during chain removal from an ordinary completed row. Settlement requires the
row's `updated_at` revision to remain exactly the one observed by the startup
scan; if it changes before the locked update, startup reports a transient error
and its retry scans the durable state again. When that chain is configured
again, incomplete Ingest evidence resumes from its preserved source cursors,
and incomplete Verify evidence with the marker resumes normal verification.
The same incomplete Verify row with a NULL marker follows completed-evidence
validation and is recorded as failed with its diagnosis. Existing rows remain
NULL and therefore retain their ordinary phase-start behavior. Only
unconfigured-chain startup settlement writes the marker. It remains present
through a resumed attempt or retry. Genuine normal completion, accepted
completed-state revalidation, or a successful redo that leaves complete
retained phase evidence clears it, so the recovered row is indistinguishable
from an ordinary completion. These clearing writes use the phase advisory-lock
connection, so losing phase ownership aborts the write. While any phase marker
is present, that chain is not eligible for `ready` on the status endpoint.

### ENSv1→ENSv2 correlation visibility

The slice-1 ENSv1→ENSv2 intake persists the
[migration correlation group](glossary.md#migration-correlation-group) without
making it consumer-authoritative. A normalized event whose existence depends on
that correlation stores top-level `migration_correlation_ids` and
`consumer_visibility`. Ordinary events default to an empty ID set and
`activated`; a correlation-dependent event has a sorted, duplicate-free,
nonempty ID set and is `candidate` in slice 1 or `activated` in slice 2.
`MigrationApplied` has exactly one ID. A shared correlation-dependent event
keeps one event identity and lists every participating per-name ID; a
name-independent registrar controller event has one stable
`controller_configuration` derivation-group ID.

Independent admission takes precedence over correlation visibility. If an
existing manifest and discovery path already produces a normalized event without
the ENSv1→ENSv2 correlation, Interpret reproduces that ordinary event
byte-for-byte: its event identity, payload, provenance, and `activated`
visibility do not change. Interpret records the candidate relationship in a
separate `migration_event_associations` row keyed to the ordinary event identity,
with the sorted correlation ID set, `correlation_kind`, evidence references,
chain positions, canonicality, and `consumer_visibility=candidate`. Correlation
never duplicates, suppresses, or reclassifies the independently admitted event.
The event identity is a plain value rather than a foreign key. A redo deletes
normalized events in its range before replay, but retains association rows whose
lineage is already orphaned as fork evidence; such a row may therefore have no
normalized-event parent. Replay re-creates the canonical-path event under the
same identity. Project and product history readers ignore association rows;
diagnostic readers treat the normalized-event join as optional and can read a
retained association from its own position and `chain_lineage` anchor.

Slice 1 applies the same precedence to identity and discovery, with one explicit
intake carveout. A migration-created registry's independently admitted
`registry_announcement` edge remains an ordinary discovery row, active from the
announcement position, because it records indexability only and the watch plan
traverses it. Interpret attaches the `migration_registry_creation` candidate
relationship in `migration_discovery_associations`, keyed to that ordinary edge;
the association does not change the edge's columns or active range and is never
Project input. Correlation-dependent parent, topology, identity, role,
registration, renewal, and normalized-event effects from the watched registry
remain candidate until activation. Association with the migration group is not
sufficient to reclassify an effect that the ordinary edge and raw event produce
without that association; independently derivable existing-family output remains
ordinary.

Each `migration_discovery_associations` row keys identity by the tuple
([`logical_edge_identity`](glossary.md#logical-discovery-edge-identity),
`migration_correlation_id`), never by the sequence-
assigned `discovery_edge_id`. `logical_edge_identity` uses the exact canonical
tuple, length-prefix encoding, domain separator, and Keccak-256 representation
in [ADR 0002](adrs/0002-surface-resource-identity.md#discovery-edge-observation-identity). The row may
retain the current numeric edge ID as a foreign-key join accelerator, but a full
schema rebuild rebinds that value without changing association identity. The row stores
`correlation_kind=migration_registry_creation`, the announcement position,
complete evidence references, canonicality anchors, `consumer_visibility`, and
the interpreter content hash. A reorg retains the association as diagnostic
evidence under its original lineage but excludes it from current correlation
state when either the ordinary edge or cited evidence is unreadable. On an
Interpret restart, the input loader restores readable associations for active
ordinary announcement edges before folding later facts from those registries.
Full replay restages the association before its downstream effects; replaying
the same evidence produces the same key and payload, with candidate or activated
visibility derived under the current interpreter content hash.

Other candidate identity or discovery values do not merge into ordinary
materialized rows. Interpret writes correlation-versioned
`migration_candidate_identity_effects` and
`migration_candidate_discovery_effects` rows containing the proposed stable
identity or edge key, complete proposed value/range delta, sorted correlation ID
set, `correlation_kind`, evidence references, chain positions, canonicality, and
`consumer_visibility=candidate`. Those diagnostic rows are not Project input
and cannot update an ordinary row's columns, provenance, `active_from`, or
`active_to`. An independently activated ordinary identity or discovery row is
therefore byte-for-byte unchanged by candidate evidence. Slice 1 also writes no
ENSv1→ENSv2 migration-driven predecessor close or successor open to
`surface_bindings`.

Consumer activation is a re-derivation semantic, not an in-place serving flag.
Slice 2 rotates the interpreter content hash and performs the planned full
Interpret walk, reproducing stable correlation IDs and event identities while
replacing candidate normalized events and diagnostic effect rows with activated
normalized events and ordinary materialized identity/discovery output. It
re-derives `migration_event_associations` and
`migration_discovery_associations` as activated diagnostics without rewriting
their independently admitted normalized events or ordinary registry-announcement
edges. Only an
`authority_transition` group derives the deferred `SurfaceBinding` transition;
other group kinds cannot change a binding or authority epoch. The downstream
full Project walk adopts only that one hash and publishes one coherent Project
result. A partial candidate and activated mixture is invalid.

The separately reviewed and separately merged slice-1 and slice-2 implementation
PRs deploy together at one planned [re-derivation
boundary](glossary.md#re-derivation-boundary), alongside
[PR #391](https://github.com/ensdomains/bigname/pull/391). The deployment adopts
one interpreter content hash, performs one full source re-walk, and makes one
Project publication decision for `ethereum-sepolia`.
Other chains retain independent publication decisions. Candidate
and activated forms remain distinct replay and acceptance-test inputs, but there
is no production interval that serves candidate-only data on the migration
target chain for ENSv1→ENSv2 migration. The ordinary announcement edge prevents
an intake gap in the restart, historical-replay, and live-follow boundary
fixtures.

For normalized-event-backed product collections, the slice-1 test re-walk must
also preserve outstanding cursor continuation at a fixed readable chain head.
A cursor issued before the re-walk must resume after publication at the same
normalized-event keyset anchor and preserve every remaining product row, page,
field, `has_more`, and summary result. The anchor may be an unmapped event, so an
interleaved non-product event at a page boundary must not skip or duplicate a
visible row. A diagnostic-events cursor must also remain valid and continue from
the same stable normalized-event anchor, although its remaining rows and fields
may reflect candidate admission. A pre-existing diagnostic row's numeric
`normalized_event_id` may change while its `event_identity` and pre-existing
semantic fields remain stable. Storage may preserve the numeric normalized-
event ID or resolve the old token through stable `event_identity` plus its stored
sort tuple; these are alternative strategies. Newly issued cursor bytes need
not match their earlier values.
The control and candidate test runs hold every other shared-boundary input
constant, including PR #391's topology serialization.

## Raw facts and payload retention

Ingest persists the minimum selected transaction, receipt, and log fields
needed to reproduce interpretation. These `bigname_phase` rows are the complete
current raw-fact family; there is no retained call-snapshot or generic payload-
cache table. Project hydration and request-scoped lookup call providers at an
explicit block position without turning those responses into raw facts.

`label_preimages` stores a verified labelhash-to-label observation: the proven
raw label bytes plus the normalization verdict for those bytes. A preimage
may improve readability but cannot create a surface, ownership, resolver,
record, permission, or primary-name fact by itself. The verdict gates how the
decoded text is consumed: Project reads it at build time and composes the text
into name-typed output only when the verdict is true. A later verdict change
does not reach projections by itself — a recompute-flags verdict flip stamps a
redo only for a surface visibility-class transition, so a flip on a label with
no name surface propagates through the full-range Project redo the
[recompute-flags runbook](deployment.md) requires after a normalizer-version
bump, not automatically. A false or error verdict keeps the proven bytes in the
store but withholds the text from names, so serving falls back to the
documented [non-name forms](glossary.md#non-name-form) — the escape-encoded raw
bytes when the label does not decode, the labelhash placeholder when it does.

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
chain-observed priority 100, so within one normalizer version a later chain
observation of the same label takes provenance precedence. A normalizer-version
bump suspends that precedence until repair: every `label_preimages` row written
under the old version — rainbow-imported rows included — must be refreshed by
`phase-runner redo --phase recompute-flags` before interpretation of the
label's next chain observation can proceed, and because rainbow rows carry no
chain coordinates the recompute selects them chain-independently, so one
chain's pass repairs every rainbow row. Conflicts on the `label_preimages`
primary key insert nothing: a re-run is a no-op and an existing verified row
is never rewritten. Rows carry the same normalization verdict the interpreter stores —
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
  A windowed or incremental Project run re-derives only its affected scope.
  Child-topology closure can add a connected component, but it does not cover
  older disconnected child edges; the full-range redo is the required
  sequence.

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

When a chain is removed from runtime configuration, recovery may change its
active phase row to the non-paging `completed` state without claiming that the
phase finished its work. If the chain is configured again, a completed Ingest
row resumes unless its current block and live handoff match its target. A
completed Verify row resumes unless it has a matching current/target block pair
and a [verification level](glossary.md#verification-level). Ingest persists its
summary and every source cursor in one transaction, so those completion markers
cannot survive without the matching source progress. Recovery also clears the
live handoff when it changes an active Ingest row to `completed`; this makes a
later re-add resume from the preserved source cursors even if an older runner
stopped between its formerly separate summary and cursor writes.

When a bounded Ingest redo replaces the hash at a source cursor's latest stored
height, and the per-source progress proves that height was inside the completed
redo range, redo completion updates that cursor hash only when matching block
lineage already records that height and hash. The cursor update and phase
summary share one transaction. The previous live handoff remains in place until
the next normal Ingest pass confirms the reconciled cursor and publishes the
replacement handoff.

## Interpretation replay

Interpretation is deterministic for a fixed manifest set, interpreter content
hash, canonical raw facts, and requested block range. A bounded Interpret redo
may replace only derived identity, discovery, and normalized-event output in
that range. Immediately before replacement, it may preserve the resolver
references Project needs to identify rows affected by disappearing events.
Those coordination rows are consumed by Project publication and are never
served. Raw facts are never edited by replay.

The ENSv1→ENSv2 `consumer_visibility` rule is included in the interpreter
content hash. Replaying one fixed hash reproduces the same correlation sets,
visibility, event identities, and payloads. Changing candidate groups to
activated groups therefore invalidates the full interpreted range and downstream
Project range; it is never a row-local patch or API-only configuration change.

Interpret redo proves raw-data presence without pretending that Live extended
each finite ingest source. Each `ingest_cursors` row proves that the source
reached from its configured start through its persisted target; Live does not
advance those source cursors. Before checking range coverage, the redo guard
requires the configured source-key set and every source's normalized kind,
seed basis, and start block to match the persisted cursor identities. A runtime
start above the redo range does not bypass that identity check. The guard also
requires exactly one readable
`chain_lineage` row at every height in the full execution range. Cursors and
lineage both prove only the facts selected by the [watch
plan](glossary.md#watch-plan--watched-tuple) active when each block was loaded;
neither proves facts added by a later watch plan. Manifest synchronization
records a [manifest-authority marker](glossary.md#manifest-authority-marker)
when that authority changes. Every Interpret redo that would discharge the
marker fails closed unless the operator passes
`--attest-watch-set-coverage <token>` with the invalidation token printed by the
fence error. Before passing it, the operator must run the
[mandatory historical fetch for any widened
range](manifests.md#mandatory-historical-fetch-after-watch-plan-widening), or
confirm that the change widened nothing. A multi-chain redo takes repeated
`--attest-watch-set-coverage <chain>=<token>` values. The locked redo begin
rejects a token that no longer matches the current marker.

Each attested discharge appends one immutable
`manifest_authority_attestations` row in the same transaction that begins the
redo and adopts the new [interpreter content
hash](glossary.md#interpreter-content-hash). It records the chain, Interpret
phase, redo range, authority fingerprint, invalidation token, runner instance
ID, and attestation time, with one row allowed per chain, phase, and generation.
The runner emits error-level structured telemetry from that row after commit;
if it stops before emission completes, a restart re-emits the row only after
the locked begin matches and commits the same interrupted redo. The same token
may resume that exact active, audited redo, but it is invalid after completion
or for any other redo. If the interpreter content hash changes while that redo
is interrupted, the same token and exact range preserve the audit
association while the redo cursor is cleared. Interpret walks the audited range
again from its beginning under the new hash; later interruptions under that
hash resume normally.

The system cannot verify the fetch or the no-widening review; the attestation is
the operator's responsibility. The guard cannot distinguish widening from
another manifest-authority change, so every such change is fenced regardless of
finite-cursor or Live-lineage coverage until issue #376 binds watch-plan
evidence to loaded facts. An interpreter content hash rotation with neither a
current manifest-authority marker nor an active audited redo remains flagless.
A missing lineage height, an ambiguous readable height, or an uncovered part of
a source's finite target remains a fatal presence failure.

The interpret engine loads the prior identity state required by the range,
folds physical batches without changing semantic order, and revalidates the
resume marker and current block anchors in the write transaction. A concurrent
reorg therefore cannot publish interpretation derived from an unreadable
branch.

### Interpret process memory

`normalized_events` is the working store for each [interpreter state
key](glossary.md#interpreter-state-key)'s `after_state`. The retained
[interpreter session](glossary.md#interpreter-session) may keep only a bounded
cache of those values. Cache capacity is an operator setting measured in entries;
changing it must not change normalized events, identity rows, discovery edges,
or the latest persisted state per key. A smaller capacity may cause more
database reads, but it has no interpretation meaning and is not part of the
[interpreter content hash](glossary.md#interpreter-content-hash).

Every cached value is the `after_state` of the latest readable normalized event
for the exact interpreter state key before the current batch. A cache miss uses
the existing interpreter-state history index: chain, presence of an opaque key,
SHA-256 of that key, and descending event position select the bounded index
range, then an exact comparison of the original key preserves correctness in
the event of a digest collision. The lookup applies the same canonical-lineage
and pre-batch boundary rules as a full restore. It does not scan an event range.
Every block-anchored normalized event produced by the schema-v2 adapter carries
an opaque state key; normalized bookkeeping rows without one are not adapter
state and are not eligible for cache reload. A key with no earlier readable row
has the empty object as its prior state, as it does during a fully resident
walk. Its [state facet](glossary.md#state-facet) groups event kinds that share
one value stream.

Values derived while a physical batch is being interpreted are a separate,
batch-bounded working set. They are not reloadable before the batch commits.
After reconciliation, before-state chaining starts from the cached or reloaded
pre-batch value and advances through the exact surviving normalized-event
sequence. Only those survivors update the retained cache after the database
transaction persists the batch. This keeps dropped or retargeted provisional
events out of both retained memory and future restore input.

A cold restore streams the latest readable event per retained state key in
chain order. It rebuilds the adapter's protocol state while admitting at most
the configured number of `after_state` values to the cache; it does not first
materialize every retained JSON value in one process allocation. If the chain
[lineage orphaning epoch](glossary.md#lineage-orphaning-epoch) changes, the
process discards the whole interpreter session and rebuilds it from readable
rows. It retains only the block anchors added since the last validation while
the epoch is unchanged, rather than one dependency entry per historical state
key.

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
a persisted row — ENS normalization, the typed projected-resolution topology
serializer and its closed wire vocabularies, plus the resolver-call
encode/decode, record-selector vocabulary, batched record and reverse-name read
helpers, and the JSON-RPC envelope interpretation deciding which provider
response those helpers accept as an answer. The lockfile fingerprints (version
and checksum) of the semantic dependencies are covered on the same rule:
alloy-sol-types, alloy-sol-macro and its expander and input crates,
alloy-sol-type-parser, alloy-dyn-abi, and alloy-primitives can change how a raw
log word decodes into a persisted event body; serde, serde-core, serde-derive,
and serde-json can change the final projected-topology serialization. The rest
of the lockfile stays
outside, so an unrelated dependency bump does not force a re-derivation.
Interpret's persistence stage is covered on the
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

An interpreter content hash rotation requires a planned full-history
interpretation and projection walk; the system refuses to mix generations from
different hashes. Interpret accepts the full finite-ingest range and extends
its execution through its recorded live-followed head. Its downstream Project
redo carries that effective range clipped to Project's own recorded head —
identical unless a crash between the two phases' live-cycle advances left
Project one block behind — and Project adopts the new hash only when the redo
covers its entire recorded head. An
interrupted redo retains that same effective range; recovery cannot narrow back
to the finite ingest handoff. If an interrupted attested Interpret redo spans
the hash rotation, its token remains valid only for that exact range. The new
binary clears the redo cursor written under the prior interpreter content hash
and walks the range from its beginning while retaining the durable audit
association. Moving a covered semantic source without updating the covered set
fails the build rather than silently narrowing the fingerprint.

## Projection publication

Project is the only projection writer. It derives the affected scope from
canonical interpreted input, stages rows in connection-local tables, and
publishes the affected projection set transactionally. It has no legacy claim
queue, general-purpose durable replay stage tables, apply cursors, dead-letter
queue, database session version stamp, or worker heartbeat. The sole replay
handoff, `project_redo_resolver_evidence`, contains pre-delete resolver
references rather than staged projection rows and is consumed by the matching
redo or later normal catch-up publication.

Consumer slice 2 adds one diagnostic exception to durable staging, not to
projection ownership. A post-reconciliation dual-current invariant makes the
Project transaction return a structured failure before `publish::swap`; that
transaction rolls back completely. The phase runner then appends one
`project_generation_failures` audit row in a separate transaction. The row is
keyed by chain, target block number/hash, interpreter content hash, and failure
kind, and stores both binding/resource identities, the activated boundary event
identity, every relevant block/transaction/log position, and the canonicality
observed at failure. It marks the target generation not ready. A later reorg or
successful generation never deletes the audit row: its recorded block hashes
remain resolvable through lineage as canonical or orphaned, and a later success
is a separate generation. Operator diagnostics may read this table; product
routes may not.

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

`name_current.declared_summary` carries the current ENSv1 NameWrapper lifecycle
label and [expiry-effective](glossary.md#expiry-effective-namewrapper-fuse-word)
fuse summary together. The underlying normalized
`PermissionScopeChanged` event keeps its expiry-unadjusted interpreted fuse word
unchanged; Project
clears only the rebuildable current summary when the served projection timestamp
passes wrapper expiry. Permission reads join this current summary by
`resource_id` rather than persisting a second copy in `permissions_current`.

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

## Schema-migration rules

- SQLx schema-migrations are append-only and versioned. Applied files are never
  rewritten.
- Every destructive schema-migration names the target schema explicitly. This is
  mandatory where retired `public` table names collide with `bigname_phase`
  names.
- Schema-v2 baseline changes require either a reviewed in-place upgrade or the
  documented offline namespace replacement and full pipeline walk.
- Explicitly reviewed additive baseline indexes may use the manual concurrent
  build procedure documented for that release. The step must be named in
  deployment preflight and recorded outside `_sqlx_migrations`; it does not
  establish a general manual schema-change path.
- A schema-migration that changes identity, manifest authority, canonicality,
  projection meaning, or replay behavior updates the corresponding contract
  docs in the same change.
- The generic worker schema-migration entrypoint has been deleted. Deployment
  automation applies reviewed versioned schema-migrations at a planned boundary.

The legacy deletion schema-migration removes only schema-qualified `public`
objects. It preserves the SQLx schema-migration ledger and extension-owned
objects.

## Repository ownership

- `crates/ingest` and phase-runner intake own immutable chain facts and lineage.
- `crates/interpret` plus adapters own derived identity, discovery, and
  normalized events.
- `crates/project` and the phase runner own current projection publication.
- `crates/lookup` owns verified lookup behavior and guarded divergence writes.
- `crates/storage` provides the typed persistence and read boundaries above.
- `apps/api` reads phase projections and lookup output; it does not write raw
  facts, interpretation output, projection rows, or legacy execution artifacts.
