# Fresh product schema

This directory is the fresh-schema baseline defined by the
[replacement build plan](../simplification-build-plan-20260730.md). It does not
modify `migrations/`. The SQL stores only the product data and phase state
authorized below.

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
see [`docs/deployment.md`](../docs/deployment.md#phase-runner-configuration).

## Chain lineage and heads

`chain_lineage`, `chain_header_audit`, and `chain_heads` store block ancestry, explicit chain state, optional raw header fields, and the latest, safe, and finalized markers. A stored block's chain, hash, parent, height, and timestamp are immutable; only its explicit canonicality and observation metadata may change. Head validation locks its referenced lineage rows through commit, so a concurrent canonicality change cannot strand a marker on a noncanonical block. Intake writes these tables. The phase runner, the API status path, and the read-only `phase-runner inspect block-canonicality` and `stored-lineage` windows read them. The [storage census and head-marker finding](../simplification-audit-20260730.md#cratesstorage-fable) authorize all three tables, and [audit entry 9](../simplification-audit-20260730.md#inventory--verdicts) authorizes the stored-header inspection fields.

Canonicality promotion follows the stored transition graph one edge at a time. When a provider checkpoint advances several levels at once, the phase runner updates the affected rows in order — `observed` to `canonical`, `canonical` to `safe`, and `safe` to `finalized` — inside the same transaction that publishes the new heads. A re-canonicalized row moves from `orphaned` to `canonical` before any later promotion. The retained checkpoint helper's single target-state assignment is therefore not a portable write pattern for this schema.

## Raw facts and inspection

`raw_transactions`, `raw_receipts`, and `raw_logs` store immutable [raw facts](../docs/glossary.md) under a block hash. Intake writes these tables. Interpretation, projection hydration, and the read-only `phase-runner inspect block-canonicality` and `raw-events` windows read them. The [storage census](../simplification-audit-20260730.md#cratesstorage-fable) and the [permanent raw-store decision](../simplification-audit-20260730.md#maintainer-question-list-consolidated-for-decision) authorize these tables; [audit entry 9](../simplification-audit-20260730.md#inventory--verdicts) authorizes their inspection reads.

## Identity and contract admission

`contract_instances`, `contract_instance_addresses`, `discovery_edges`, `token_lineages`, `resources`, `name_surfaces`, and `surface_bindings` store stable contract, token, authority-object, raw-name, and name-to-authority identities. Manifest sync writes declared contract rows. The interpreter writes event-derived identity rows. [Projection](../docs/glossary.md) and request-scoped lookup code read them. The [identity storage census](../simplification-audit-20260730.md#cratesstorage-fable), the [raw-label normalization decision](../simplification-audit-20260730.md#normalization-as-a-gate-not-stored-identity), and the [event-announcement discovery design](../simplification-audit-20260730.md#discovery-design-decided-2026-07-30) authorize these tables.

A [`registry_announcement` discovery edge](../docs/glossary.md#registry-announcement-edge-registry_announcement) is the announcing registry's self-edge admitted forward-only by `RegistryCreated`; every other discovery kind still requires distinct endpoints, as ruled by the audit's [discovery design](../simplification-audit-20260730.md#discovery-design-decided-2026-07-30).

The logical identity of an on-chain name is `<namespace>:<namehash>`. On chain, a name is its namehash: ENSv1 registry records are keyed by `bytes32` node `(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L13 @ ens_v1@91c966f)`, ENSv2 resolver permissions and records use the namehash/node `(upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L68 @ ens_v2@ccaeb58)` `(upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L133 @ ens_v2@ccaeb58)`, and Basenames defines the resolver node as the namehash of the name `(upstream: .refs/basenames/src/L2/L2Resolver.sol:L88 @ basenames@1809bbc)`. Identity is therefore chain-native and independent of normalization rules. Normalization is only a per-label visibility flag; it never participates in identity. The current [surface and resource identity ADR](../docs/adrs/0002-surface-resource-identity.md) records this rule, following the audit's [Normalization as a gate, not stored identity](../simplification-audit-20260730.md#normalization-as-a-gate-not-stored-identity) decision.

Every `name_surfaces` write sets `visibility_state` explicitly. The column has no default: omitting the normalization decision fails the write instead of making an incompletely interpreted name visible.
An attacker-controlled label that cannot decode as PostgreSQL-safe UTF-8 still has a deactivated identity row keyed by `<namespace>:<namehash>`; its unavailable text display fields are empty, and `label_preimages.raw_label` retains the authoritative bytes.

## Manifest declarations

`manifest_versions`, `manifest_contract_instances`, and `manifest_discovery_rules` store loaded declarations, declared contracts, start blocks, proxy links, ABI data, and admission rules. Manifest sync writes these tables. Intake, interpretation, projection, and request-scoped lookup read them. The [manifest census](../simplification-audit-20260730.md#cratesmanifests--domain--metrics-fable) authorizes the declaration tables. The [declared-means-supported decision](../simplification-audit-20260730.md#maintainer-question-list-consolidated-for-decision) excludes a separate capability-flag table. Authored capability flags remain inside `manifest_payload`, and changes to them are part of `SourceManifestUpdated`.

The authored manifest field remains `deployment_epoch` under the public [manifest contract](../docs/manifests.md#required-fields). Manifest sync stores that value unchanged in `manifest_versions.deployment_label`; it does not reinterpret or mint a second identifier. The schema-v2 writer applies this one-to-one field mapping in inserts, uniqueness checks, and prior-declaration queries. `manifest_payload` retains the authored field name.

## Normalized events

`normalized_events` stores plain [normalized events](../docs/glossary.md) with
source positions and before-and-after state. It has exactly two logical write
owners: chain interpreters write chain-derived rows, and manifest sync writes
`SourceManifestUpdated` through its `manifest_sync` manifest-change interpreter.
Projection builders and read-only history or raw-event inspection read the
table. The [adapter census](../simplification-audit-20260730.md#cratesadapters-fable)
and the [storage census](../simplification-audit-20260730.md#cratesstorage-fable)
authorize this table.

For chain-derived rows, `raw_fact_ref.interpreter_state_key` is an opaque,
adapter-owned key used to compact prior interpreter state between batches. The
phase loader may group by that key but does not derive it from event kinds, so
changes to state-facet semantics remain inside the interpreter content hash.

The event kind is a closed vocabulary. It reserves `RegistryCreated` for ENSv2
event-announcement discovery and `Upgraded` for admitted proxy history. The
checked-in fresh-schema manifests and adapter intake admit both signatures.
Their mandatory one-time historical-signature fetch must finish before the
replacement rebuild. ENSv2 declares `RegistryCreated` and emits it first in the
registry constructor.
(upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L9 @ ens_v2@ccaeb58)
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L113 @ ens_v2@ccaeb58)
Its upgradeable resolver proxy declares and emits `Upgraded` with the new
implementation.
(upstream: .refs/ens_v2/contracts/src/universalResolver/UpgradableUniversalResolverProxy.sol:L30 @ ens_v2@ccaeb58)
(upstream: .refs/ens_v2/contracts/src/universalResolver/UpgradableUniversalResolverProxy.sol:L114 @ ens_v2@ccaeb58)
Manifest declaration changes use `SourceManifestUpdated`.
The deleted `ProxyImplementationChanged` and `CapabilityChanged` kinds are not
admitted.

The derivation kind is also closed and identifies the writer path, not the
upstream event. The admitted values are `ens_v1_reverse_claim`,
`ens_v1_unwrapped_authority`, `ens_v2_permissions`, `ens_v2_registrar`,
`ens_v2_registry_resource_surface`, `ens_v2_resolver`, `manifest_sync`,
`proxy_upgrade`, and `raw_log_preimage_observation`. Their meanings and write
owners are defined by the canonical
[normalized-event contract](../docs/architecture.md#derivation-kinds).

## Current projections

`name_current`, `children_current`, `permissions_current`, `permissions_current_resource_summary`, `record_inventory_current`, `resolver_current`, `address_names_current`, and `primary_names_current` are the current-state tables written by the seven project-phase builders. The project phase is their single writer; the API and GraphQL read them now. The [historical simplification census](../simplification-audit-20260730.md#appsworker--cratesexecution-fable) and the [storage census](../simplification-audit-20260730.md#cratesstorage-fable) authorize this enumerated set. The [support-status decision](../simplification-audit-20260730.md#kimi-k3-second-opinion-lenses--adjudicated) keeps explicit support fields and removes exhaustiveness accounting.

Each run reads canonical-lineage identity rows and normalized events into
connection-local stages. It builds all retained families before one database
transaction replaces either the chain's complete projection set or the keys
affected by an incremental run or bounded redo. Readers therefore see either
the prior set or the complete successor set, never a half-published mixture.
Publication has no marker, claim, journal, dead-letter, or watermark table.
The phase runner's advisory lock, state row, content hash, and redo marker are
the operating control plane. After event-derived publication, a project run
with an Ethereum hydration RPC refreshes eligible legacy reverse-name and text
values through Multicall3 at the exact `chain_heads` number and hash. It writes
only `primary_names_current` and `record_inventory_current`, stores the pinned
head and replaced event-derived baseline in hydration provenance, and
revalidates that head in the publication
transaction. These values are execution-derived enrichment, not project
inputs: raw facts, identity, and normalized events remain unchanged, and a
project redo first reconstructs the event-derived rows before hydration is
layered on again. Hydration runs only when that publication target equals the
current `chain_heads` marker; a behind-head redo defers enrichment until project
catches up. A failed call restores the saved baseline and keeps the
project attempt retryable instead of retaining an earlier head's value. The
same rule applies to transport, RPC, decoding, or cardinality failure of an
entire Multicall batch: every call in that batch restores its baseline in the
head-revalidated publication transaction before the retryable failure returns.
A previously hydrated reverse tuple that no longer selects an eligible legacy
resolver also restores its baseline in that transaction without another call.

Projection JSON coverage fields say only that the row was derived from stored
canonical inputs: `status = "projected"` and
`exhaustiveness = "not_asserted"`. Support remains separate in
`support_status` and `unsupported_reason`.

`children_current.raw_label` and `children_current.raw_name` retain exact
observed bytes, including shadow identities that cannot be represented safely
as PostgreSQL text. For a topology-only child known by hashes, `labelhash` and
`namehash` remain non-null while all four byte/text fields are null; synthesized
placeholder bytes are never stored. The nullable `decoded_label` and
`decoded_name` companions are present only when raw bytes exist and UTF-8
decoding round-trips to them. A later preimage upgrades the same child row.

## Label data

`label_preimages` stores verbatim chain label bytes as identity truth, an optional exact PostgreSQL-safe UTF-8 decoding as display-name input, and the `normalized_under_version` flag computed from that decoding. Valid UTF-8 containing an embedded NUL is not representable as PostgreSQL `text`, so `decoded_label` is NULL while `raw_label` retains the exact bytes. The decoded text is derived input, never stored normalized identity. `ens_names` stores the imported rainbow rows. Storage and the interpreter write verified preimages. The import tool writes rainbow rows. Identity and child projection code read both tables. The audit's [Normalization as a gate, not stored identity](../simplification-audit-20260730.md#normalization-as-a-gate-not-stored-identity) decision (§ 85) and the [label-preimage storage census](../simplification-audit-20260730.md#cratesstorage-fable) authorize both tables.

## Service heartbeats

`service_heartbeats` stores one liveness row for each service instance, chain, and phase. The new phase-runner services write it. Health checks and `/v2/status` read it. The schema leaves `service_name` open because the [phase-runner design](../a2-phase-runner-design-20260731.md) assigns the new names; it does not admit the retired indexer or worker names by default. The [indexer heartbeat absorption](../simplification-audit-20260730.md#appsindexer-fable) and the [service-heartbeat storage census](../simplification-audit-20260730.md#cratesstorage-fable) authorize this table; build-plan amendment F defines its per-chain and per-phase shape.

## Live/indexed resolution differences

`resolution_divergences` stores a row only when a live resolver answer differs
from the exact indexed `record_inventory_current.entries` answer selected by
the projected record boundary's `resource_id`. It keeps at most one unresolved
row for each exact name, resolver, and record key. A wildcard lookup with no
exact inventory comparison executes without ledger persistence and never
compares the request with its wildcard ancestor's inventory. Lookup execution
pins the authoritative name chain to its newest processed block. For
Basenames, it also uses the timestamp-aligned Ethereum auxiliary position
already stored on that `name_current` row. Every recorded divergence position
therefore identifies an ingested block. Every active row must identify that
block in `chain_lineage` with the same chain, hash, height, and timestamp and
with readable canonicality; the strict position trigger remains required. The
guarded writer locks the compared inventory row and accepts a mutation only
while its `xmin` is unchanged from the read. It derives the indexed answer from
that row and verifies the current requested name, selected resolver, record
selector, and record boundary before targeting a ledger row. Callers supply
only the live answer. When indexed comparison and live execution use different
blocks on one chain, `observed_positions` retains separate `indexed` and `live`
slots so either block's reorg clears the active row. Before inserting a disagreement or
clearing one after restored agreement, it also locks every observed canonical
lineage row and rejects a reorged observation. Its writer refuses
CCIP-participating results before the mutation-specific guard or any mutation.
A later chain canonicality change
clears every active row that observed the affected block. This reorg auto-clear
rule was maintainer-ratified on 2026-07-31. Position validation locks those
lineage rows through commit, so a concurrent canonicality change cannot miss
an uncommitted lookup insert. Projection support logic and operators read the
rows. After live execution, the serving transaction revalidates and locks the
authoritative head, completed project generation, every observed canonical
position, the exact projected name when present, and the optional inventory row
through `revalidate_resolution_lookup_state`; it also verifies every selected
manifest version and contract declaration. The name lock precedes the inventory
lock to match projection publication. A shared manifest-sync advisory lock is
held through commit, including for admitted shadow execution declarations. This
guard runs when wildcard or CCIP behavior precludes a ledger mutation, and CCIP
still guards an inventory row it read. It then mutates the
ledger through `write_resolution_divergence`. Both are fixed-`search_path`,
security-definer functions with default `PUBLIC` execution revoked. Deployment
grants the API role explicit `EXECUTE` on them, but no direct write privilege on
the guarded relations or ledger. ENS/60 primary-name verification uses the same
head, lineage, project-generation, and manifest-authority guard after its live
calls without passing a name or inventory comparison or mutating the ledger. The
[B6 lookup-engine rule](../simplification-build-plan-20260730.md#stage-b--port-the-keep-set)
and
[no-outcome-cache decision](../simplification-audit-20260730.md#maintainer-question-list-consolidated-for-decision)
authorize this table as the only durable execution-adjacent store.

## Ingest cursors and phase state

`ingest_cursors` stores one cursor for each chain source, and `chain_phase_state` stores one state row for each of the five phases, including the verify phase trust level and an explicit `paused` state for the capacity guard. Explicit redo fields retain the requested range, a cursor separate from normal progress, and a snapshot of the pre-redo lifecycle state; the marker remains until redo succeeds and blocks normal resume after an interruption. While an operator marker remains, `last_error` records the most recent failed redo attempt. A system-required downstream marker instead retains its ownership prefix and appends the most recent attempt failure so restart continues automatic repair. When a later redo completes, that attempt error is cleared and any pre-redo lifecycle error is restored. A normal verification mismatch also uses `last_error`; it records the block, field, stored value, and reference value without a new table or column. The phase runner writes both tables. The runner, redo command, health checks, and status path read them. The [indexer absorption census](../simplification-audit-20260730.md#appsindexer-fable) authorizes both tables. [Build-plan amendment B](../simplification-build-plan-20260730.md#b-verify-carried-raw-before-deleting-its-coverage-record) lists the seed inputs as Base block `48,428,000`, the verified historical starts for the three newly watched signature groups, and the observed Ethereum head. The schema does not preload the dynamic starts or Ethereum head. [Build-plan amendment D](../simplification-build-plan-20260730.md#d-status-label-honesty-razor-3) defines provider-trusted, independently cross-checked, and node-checked status. The production verifier maps dRPC only to `cross_checked` and local reth to `node_checked`; the phase runner rejects a dRPC-backed `node_checked` report before persistence. Base's dRPC cross-check extent stops at the Coinbase-to-dRPC ingest seam, and a partial verify redo retains the level for the whole recorded extent. [Build-plan amendment F](../simplification-build-plan-20260730.md#f-specs-pinned) defines the five phase names and the ingest-to-live handoff fields. The [approved phase-runner design](../a2-phase-runner-design-20260731.md#status-and-heartbeats) requires capacity pauses to remain distinguishable from failures.

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
redo supplements the surviving-event range with current projection keys whose
normalized-event provenance citations are no longer readable, including both
event IDs carried by a primary-name tuple.
