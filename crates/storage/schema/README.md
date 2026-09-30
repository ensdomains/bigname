# Fresh product schema

This directory contains the fresh database baseline installed by
`phase-runner init-schema`, plus fixtures for schema regression checks.
The current storage contract is [`docs/storage.md`](../../../docs/storage.md).
Run `scripts/check-schema` from the repository root to check the baseline,
versioned upgrades, and operational SQL together.

The append-only SQLx history remains in [`migrations/`](../../../migrations/).
Its files retain their original bytes and checksums; moving this baseline
requires no database migration. The source-path changes do rotate the
[interpreter content hash](../../../docs/glossary.md#interpreter-content-hash):
deployments crossing this cleanup must complete the resulting rebuild before
ordinary serving resumes. The July rewrite decisions linked below are
preserved in the [historical archive](../../../docs/internal/archive/README.md).

## Installation boundary

`phase-runner init-schema` is the runtime installer for this baseline. It
installs into an empty `bigname_phase` PostgreSQL schema and refuses a nonempty
phase schema until a reviewed upgrade or rebuild mechanism exists. The phase
runner writes this schema and the API reads its projections, lookup state, and
operational status. The retired `public` indexing schema is removed by the
append-only SQLx migration history; reorg publication repairs only current
phase state and guarded resolution-divergence observations.

After installation, the supervised runner and `verify` redo require a second
database URL for a dedicated login with USAGE on `bigname_phase` and SELECT on
every relation in it. The verifier rejects a login with application write or
database/schema creation authority, elevated role attributes, or role
memberships. Those role grants are deployment configuration, not baseline DDL;
see [`docs/deployment.md`](../../../docs/deployment.md#phase-runner-configuration).

## Chain lineage and heads

`chain_lineage`, `chain_header_audit`, and `chain_heads` store block ancestry, explicit chain state, optional raw header fields, and the latest, safe, and finalized markers. A stored block's chain, hash, parent, height, and timestamp are immutable; only its explicit canonicality and observation metadata may change. Head validation locks its referenced lineage rows through commit, so a concurrent canonicality change cannot strand a marker on a noncanonical block. Intake writes these tables. The phase runner, the API status path, and the read-only `phase-runner inspect block-canonicality` and `stored-lineage` windows read them. The [storage census and head-marker finding](../../../docs/internal/archive/simplification-audit-20260730.md#cratesstorage-fable) authorize all three tables, and [audit entry 9](../../../docs/internal/archive/simplification-audit-20260730.md#inventory--verdicts) authorizes the stored-header inspection fields.

Canonicality promotion follows the stored transition graph one edge at a time. When a provider checkpoint advances several levels at once, the phase runner updates the affected rows in order — `observed` to `canonical`, `canonical` to `safe`, and `safe` to `finalized` — inside the same transaction that publishes the new heads. A re-canonicalized row moves from `orphaned` to `canonical` before any later promotion. The retained checkpoint helper's single target-state assignment is therefore not a portable write pattern for this schema.

## Raw facts and inspection

`raw_transactions`, `raw_receipts`, and `raw_logs` store immutable [raw facts](../../../docs/glossary.md) under a block hash. Intake writes these tables. Interpretation, projection hydration, and the read-only `phase-runner inspect block-canonicality` and `raw-events` windows read them. The [storage census](../../../docs/internal/archive/simplification-audit-20260730.md#cratesstorage-fable) and the [permanent raw-store decision](../../../docs/internal/archive/simplification-audit-20260730.md#maintainer-question-list-consolidated-for-decision) authorize these tables; [audit entry 9](../../../docs/internal/archive/simplification-audit-20260730.md#inventory--verdicts) authorizes their inspection reads.

## Identity and contract admission

`contract_instances`, `contract_instance_addresses`, `discovery_edges`, `token_lineages`, `resources`, `name_surfaces`, and `surface_bindings` store stable contract, token, authority-object, raw-name, and name-to-authority identities. Manifest sync writes declared contract rows. The interpreter writes event-derived identity rows. [Projection](../../../docs/glossary.md) and request-scoped lookup code read them. The [identity storage census](../../../docs/internal/archive/simplification-audit-20260730.md#cratesstorage-fable), the [raw-label normalization decision](../../../docs/internal/archive/simplification-audit-20260730.md#normalization-as-a-gate-not-stored-identity), and the [event-announcement discovery design](../../../docs/internal/archive/simplification-audit-20260730.md#discovery-design-decided-2026-07-30) authorize these tables.

A [`registry_announcement` discovery edge](../../../docs/glossary.md#registry-announcement-edge-registry_announcement) is the announcing registry's self-edge admitted forward-only by `RegistryCreated`; a `resolver` self-edge is also admitted by `ResolverCreated`; all other edges require distinct endpoints, as ruled by the audit's [discovery design](../../../docs/internal/archive/simplification-audit-20260730.md#discovery-design-decided-2026-07-30).

The logical identity of an on-chain name is `<namespace>:<namehash>`. On chain, a name is its namehash: ENSv1 registry records are keyed by `bytes32` node `(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L13 @ ens_v1@91c966f)`, ENSv2 resolver permissions and records use the namehash/node `(upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L68 @ ens_v2_sepolia_20260629@ccaeb58)` `(upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L133 @ ens_v2_sepolia_20260629@ccaeb58)`, and Basenames defines the resolver node as the namehash of the name `(upstream: .refs/basenames/src/L2/L2Resolver.sol:L88 @ basenames@1809bbc)`. Identity is therefore chain-native and independent of normalization rules. Normalization is only a per-label visibility flag; it never participates in identity. The current [surface and resource identity ADR](../../../docs/adrs/0002-surface-resource-identity.md) records this rule, following the audit's [Normalization as a gate, not stored identity](../../../docs/internal/archive/simplification-audit-20260730.md#normalization-as-a-gate-not-stored-identity) decision.

Every `name_surfaces` write sets `visibility_state` explicitly. The column has no default: omitting the normalization decision fails the write instead of making an incompletely interpreted name visible.
An attacker-controlled label that cannot decode as PostgreSQL-safe UTF-8 still has a deactivated identity row keyed by `<namespace>:<namehash>`; its unavailable text display fields are empty, and `label_preimages.raw_label` retains the authoritative bytes.

## Manifest declarations

`manifest_versions`, `manifest_contract_instances`, and `manifest_discovery_rules` store loaded declarations, declared contracts, start blocks, proxy links, ABI data, and admission rules. Manifest sync writes these tables. Intake, interpretation, projection, and request-scoped lookup read them. The [manifest census](../../../docs/internal/archive/simplification-audit-20260730.md#cratesmanifests--domain--metrics-fable) authorizes the declaration tables. The [declared-means-supported decision](../../../docs/internal/archive/simplification-audit-20260730.md#maintainer-question-list-consolidated-for-decision) excludes a separate capability-flag table. Authored capability flags remain inside `manifest_payload`, and changes to them are part of `SourceManifestUpdated`. Manifest sync also adds the non-authorable [compiled watch plan](../../../docs/glossary.md#compiled-watch-plan) as the `_bigname_compiled_watch` member of that payload. It records the exact emitter, event, and start entries compiled by the admitting binary so a later sync can compare against the prior binary-defined policy.

The authored manifest field remains `deployment_epoch` under the public [manifest contract](../../../docs/manifests.md#required-fields). Manifest sync stores that value unchanged in `manifest_versions.deployment_label`; it does not reinterpret or mint a second identifier. The schema-v2 writer applies this one-to-one field mapping in inserts, uniqueness checks, and prior-declaration queries. `manifest_payload` retains the authored field name.

## Normalized events

`normalized_events` stores plain [normalized events](../../../docs/glossary.md) with
source positions and before-and-after state. It has exactly two logical write
owners: chain interpreters write chain-derived rows, and manifest sync writes
`SourceManifestUpdated` through its `manifest_sync` manifest-change interpreter.
Project's family reducers and read-only history or raw-event inspection read
the table. The [adapter census](../../../docs/internal/archive/simplification-audit-20260730.md#cratesadapters-fable)
and the [storage census](../../../docs/internal/archive/simplification-audit-20260730.md#cratesstorage-fable)
authorize this table.

A bounded redo deletes and re-derives the range's normalized rows. Project
needs no copy of what was deleted: it undoes its journalled family publications
to a trusted base and replays activated canonical input from there, so there is
no Interpret-to-Project handoff table. Raw facts and re-derived
`normalized_events` remain the replay authority.

For chain-derived rows, `raw_fact_ref.interpreter_state_key` is an opaque,
adapter-owned key used to compact prior interpreter state between batches. The
phase loader may group by that key but does not derive it from event kinds, so
changes to state-facet semantics remain inside the interpreter content hash.

The event kind is a closed vocabulary. It admits candidate-only
`MigrationApplied` and `ContractDiscovered` ENSv1→ENSv2 migration facts, reserves
`RegistryCreated` for ENSv2 event-announcement discovery, and uses `Upgraded`
for admitted proxy history. The
checked-in fresh-schema manifests and adapter intake admit both signatures.
Their mandatory one-time historical-signature fetch must finish before the
replacement rebuild. ENSv2 declares `RegistryCreated` and emits it first in the
registry constructor.
(upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L9 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L113 @ ens_v2@a971bd64)
Its upgradeable resolver proxy declares and emits `Upgraded` with the new
implementation.
(upstream: .refs/ens_v2/contracts/src/universalResolver/UpgradableUniversalResolverProxy.sol:L30 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/src/universalResolver/UpgradableUniversalResolverProxy.sol:L114 @ ens_v2@a971bd64)
Manifest declaration changes use `SourceManifestUpdated`.
The deleted `ProxyImplementationChanged` and `CapabilityChanged` kinds are not
admitted.

The derivation kind is also closed and identifies the writer path, not the
upstream event. The admitted values are `ens_v1_reverse_claim`,
`ens_v1_unwrapped_authority`, `ens_v2_migration`, `ens_v2_permissions`, `ens_v2_registrar`,
`ens_v2_registry_resource_surface`, `ens_v2_resolver`, `manifest_sync`,
`proxy_upgrade`, `raw_log_preimage_observation`, and
`raw_block_preimage_observation`, and `standard_approval`. Their meanings and
write owners are defined
by the canonical
[derivation-kind definitions](../../../docs/glossary.md#derivation-kind).

## Current projections

The Project phase is the single writer of the
[owned key families](../../../docs/glossary.md#owned-key-family): the `project_*`
tables in `baseline/06_projections.sql`, with the
[family marker](../../../docs/glossary.md#family-marker) (`project_family_marker`),
the undo journal (`project_family_undo`) and the repair record
(`project_repair_record`). Each family keeps per-key current state: name identity
and binding candidates, registration and lease state, wrapper state, registry
ownership, resolver classification and pointers, records, grants and account
approvals, aliases, child edges, reverse claims, address indexes, name history
and the name summary. `child_registration_events` keeps historical child
membership. The API reads these tables through storage's family readers
(`crates/storage/src/families`), which compose names, records, permissions,
resolver collections and primary claims at read time; no composed row is
stored. [`docs/projections.md`](../../../docs/projections.md) is the contract.
Schema-migration `20260929160000_remove_served_projections.sql` dropped the
earlier per-route serving tables (`name_current`, `children_current`,
`permissions_current`, `permissions_current_resource_summary`,
`account_permission_state_current`, `record_inventory_current`,
`resolver_current`, `address_names_current`, `address_records_current` and
`primary_names_current`). The [support-status
decision](../../../docs/internal/archive/simplification-audit-20260730.md#kimi-k3-second-opinion-lenses--adjudicated)
keeps explicit support fields and removes exhaustiveness accounting.

Each Project block commits its changed family keys, their before-images and the
advanced marker in one transaction; a rebuild range commits several blocks
together. Readers see the prior publication or the complete successor, never a
half-published mixture. The phase runner's advisory lock, state row, interpreter
content hash and redo marker remain the operating control plane, and the family
marker and repair record are Project's restart boundary. On configured Ethereum
Mainnet follow blocks, Project may overlay hash-pinned Multicall3 results for
legacy reverse names and ENSv1 text values onto the event-derived baseline in
that same transaction. These values are execution-derived enrichment, not
project inputs: raw facts, identity, and normalized events remain unchanged.
Replay and rebuild make no calls, and a failed call leaves the baseline for a
later follow block to retry
([Follow-only hydration](../../../docs/projections.md#follow-only-hydration)).

Projection JSON coverage fields say only that the row was derived from stored
canonical inputs: `status = "projected"` and
`exhaustiveness = "not_asserted"`. Support remains separate in
`support_status` and `unsupported_reason`. Account-level approvals carry no
coverage object at all, so they emit neither field, and a consumer must not
probe them for one.

Child and name reads take exact label bytes and their decoded text from
`label_preimages` at read time; the family tables store labelhashes, not label
bytes. A topology-only child known by hashes keeps its labelhash and namehash
with null byte and text fields; synthesized placeholder bytes are never stored.
A later preimage changes what the next read composes without rewriting a
family row.

## Label data

`label_preimages` stores verbatim chain label bytes as identity truth, an optional exact PostgreSQL-safe UTF-8 decoding as display-name input, and the `normalized_under_version` flag computed from that decoding. Valid UTF-8 containing an embedded NUL is not representable as PostgreSQL `text`, so `decoded_label` is NULL while `raw_label` retains the exact bytes. The decoded text is derived input, never stored normalized identity. `ens_names` stores the operator-loaded rainbow rows. The interpreter and `phase-runner label-preimages import-ens-rainbow` write verified preimages; the import proof-checks each rainbow row before admitting it (see [Rainbow-table preimage import](../../../docs/storage.md#rainbow-table-preimage-import)). Identity and child projection code read `label_preimages`; the import command is the only `ens_names` reader. The audit's [Normalization as a gate, not stored identity](../../../docs/internal/archive/simplification-audit-20260730.md#normalization-as-a-gate-not-stored-identity) decision (§ 85) and the [label-preimage storage census](../../../docs/internal/archive/simplification-audit-20260730.md#cratesstorage-fable) authorize both tables.

## Service heartbeats

`service_heartbeats` stores one liveness row for each service instance, chain, and phase. The new phase-runner services write it. Health checks and `/v1/status` read it. The schema leaves `service_name` open because the [phase-runner design](../../../docs/internal/archive/a2-phase-runner-design-20260731.md) assigns the new names; it does not admit the retired indexer or worker names by default. The [indexer heartbeat absorption](../../../docs/internal/archive/simplification-audit-20260730.md#appsindexer-fable) and the [service-heartbeat storage census](../../../docs/internal/archive/simplification-audit-20260730.md#cratesstorage-fable) authorize this table; build-plan amendment F defines its per-chain and per-phase shape.

## Live/indexed resolution differences

`resolution_divergences` stores a row only when a live resolver answer differs
from the indexed exact entry or manifest-authorized derived read, evaluated from
the record inventory the family readers compose for the projected record
boundary's `resource_id`. It keeps at most one unresolved
row for each exact name, resolver, and record key. A wildcard lookup with no
exact inventory comparison executes without ledger persistence and never
compares the request with its wildcard ancestor's inventory. Lookup execution
pins the authoritative name chain to its newest processed block. For
Basenames, it also uses the timestamp-aligned Ethereum auxiliary position
captured with the composed name. Every recorded divergence position
therefore identifies an ingested block. Every active row must identify that
block in `chain_lineage` with the same chain, hash, height, and timestamp and
with readable canonicality; the strict position trigger remains required. The
guarded writer receives the captured family publication (marker sequence, block
identity and interpreter content hash) with the captured name and inventory,
and accepts a mutation only while the live
[family marker](../../../docs/glossary.md#family-marker) still matches that
publication. It evaluates the indexed answer from the captured exact entries
and projected read rules, then verifies the current requested name, selected
resolver, record selector, and record boundary before targeting a ledger row.
Callers supply only the live answer. When indexed comparison and live execution
use different blocks on one chain, `observed_positions` retains separate
`indexed` and `live` slots so either block's reorg clears the active row. Before
inserting a disagreement or clearing one after restored agreement, it also
locks every observed canonical lineage row and rejects a reorged observation.
Its writer refuses CCIP-participating results before the mutation-specific
guard or any mutation. A later chain canonicality change
clears every active row that observed the affected block. This reorg auto-clear
rule was maintainer-ratified on 2026-07-31. Position validation locks those
lineage rows through commit, so a concurrent canonicality change cannot miss
an uncommitted lookup insert. Projection support logic and operators read the
rows. After live execution, the serving transaction revalidates and locks the
authoritative head, the captured family publication, every observed canonical
position, and the Interpret and Project phase rows through
`revalidate_resolution_lookup_state`, refusing any redo that overlaps the
publication; it also verifies every selected manifest version and contract
declaration. A shared manifest-sync advisory lock is held through commit,
including for admitted shadow execution declarations. This guard runs when
wildcard or CCIP behavior precludes a ledger mutation. It then mutates the
ledger through `write_resolution_divergence`. This locking writer is retained
for non-API callers. Both are fixed-`search_path`, security-definer functions
with default `PUBLIC` execution revoked. The API uses
`revalidate_resolution_lookup_state_read_only`, a security-definer wrapper that
fixes locking to false, in a fresh repeatable-read, read-only transaction after
RPC, and never mutates the ledger. Deployment grants its role only that wrapper,
with no access to the private boolean core, writer function or ledger table.
Both wrappers call one security-invoker core with their owner's privileges.
ENS/60 primary-name verification uses the same
head, lineage, family-publication, and manifest-authority guard after its live
calls without passing a name or inventory comparison or mutating the ledger.
Project's only write to the ledger retires active direct observations for a name
in the family publication that changes its ENS Mainnet exact resolver to null. The
[B6 lookup-engine rule](../../../docs/internal/archive/simplification-build-plan-20260730.md#stage-b--port-the-keep-set)
and
[no-outcome-cache decision](../../../docs/internal/archive/simplification-audit-20260730.md#maintainer-question-list-consolidated-for-decision)
authorize this table as the only durable execution-adjacent store.

## Ingest cursors, phase state, authority attestations, and failure audits

Under the Issue #411 contract, `ingest_cursors` stores one cursor for each [intake-capable chain source](../../../docs/glossary.md#source-role), and verification-only sources never receive cursors. `chain_phase_state` stores one state row for each of the five phases, including the verify phase trust level and an explicit `paused` state for the capacity guard. Explicit redo fields retain the requested range, a cursor separate from normal progress, and a snapshot of the pre-redo lifecycle state; the marker remains until redo succeeds and blocks normal resume after an interruption. While an operator marker remains, `last_error` records the most recent failed redo attempt. A system-required downstream marker instead retains its ownership prefix and appends the most recent attempt failure so restart continues automatic repair. When a later redo completes, that attempt error is cleared and any pre-redo lifecycle error is restored. A normal verification mismatch also uses `last_error`; it records the block, field, stored value, and reference value without a new table or column.

`manifest_authority_attestations` is the append-only audit for an operator-authorized Interpret redo that discharges a [manifest-authority marker](../../../docs/glossary.md#manifest-authority-marker). The marker-discharge transaction inserts one row for the chain, phase, invalidation generation, authority fingerprint, effective redo range, runner instance, and attestation time. The `(chain_id, phase_name, generation_token)` key prevents a second audit row for the same discharge. The phase runner emits telemetry from this row after commit and reads it again when resuming the matching interrupted redo.

`interpret_decode_skips` is the append-only audit for malformed event logs from
undeclared emitters that Interpret skips under the manifest admission policy.
A decode failure from a manifest-declared emitter is fatal regardless of event
selection scope. Each row records the
raw-log position, emitting address, selected event and source, selection scope,
decoder context, and [interpreter content
hash](../../../docs/glossary.md#interpreter-content-hash). Interpret writes these rows from
adapter output with conflict ignore, so replaying one log under one interpreter
build records one diagnostic. Redo preparation and reorg repair do not delete
them. Operators may read this table; product routes may not.

The phase runner owns cursor and phase lifecycle and authority attestations. Manifest synchronization and a completed Interpret pass may atomically install required Ingest work through the shared `chain_phase_state` installer; neither owns an independent queue. Interpret also owns the [discovery-watch admission snapshot](../../../docs/glossary.md#discovery-watch-admission-snapshot) and the malformed-event diagnostic table. The runner, redo command, health checks, and status path read cursor and phase state; redo restart also reads the attestation audit. The [indexer absorption census](../../../docs/internal/archive/simplification-audit-20260730.md#appsindexer-fable) authorizes the cursor and phase-state tables. [Build-plan amendment B](../../../docs/internal/archive/simplification-build-plan-20260730.md#b-verify-carried-raw-before-deleting-its-coverage-record) lists the seed inputs as Base block `48,428,000`, the verified historical starts for the three newly watched signature groups, and the observed Ethereum head. The schema does not preload the dynamic starts or Ethereum head. [Build-plan amendment D](../../../docs/internal/archive/simplification-build-plan-20260730.md#d-status-label-honesty-razor-3) defines [provider-trusted](../../../docs/glossary.md#verification-level), independently cross-checked, and node-checked status; the Issue #411 source-role contract narrows it by denying independent evidence to any source that also serves intake. The production verifier records `cross_checked` for a distinct verification-only Base or Sepolia dRPC and `node_checked` for a distinct verification-only Ethereum Mainnet reth; without one, the target-covering intake cursor records `quick_synced`. The phase runner rejects a level stronger than the chain-specific verification path earned before persistence. Base's dRPC cross-check extent stops at the Coinbase-to-dRPC ingest seam, and a partial verify redo retains the weaker of retained and currently available evidence. [Build-plan amendment F](../../../docs/internal/archive/simplification-build-plan-20260730.md#f-specs-pinned) defines the five phase names and the ingest-to-live handoff fields. The [approved phase-runner design](../../../docs/internal/archive/a2-phase-runner-design-20260731.md#status-and-heartbeats) requires capacity pauses to remain distinguishable from failures.

Normal verification reads its start from these durable ingest cursors. A resumed
normal scan retains the weaker whole-extent verification level when its
reference level changes.

Head publication also uses those existing redo fields as a downstream guard.
If the newly orphaned suffix begins at or below an `interpret` or `project`
cursor, the publication transaction stamps that phase from the first orphaned
block through its recorded cursor. A successful interpret redo stamps project
for the same replayed suffix. The runner consumes these system-required stamps
in dependency order; they are ranges, not a second scheduler or queue. The
stamp's ownership marker distinguishes pending selection or live gap fill from
an active replay. Once active, the replay observes the same non-verify writer
exclusion as any other phase write. Project
redo undoes its journalled family publications to a trusted base below the range
and replays activated canonical input
([Reorg and redo](../../../docs/projections.md#reorg-and-redo)).
