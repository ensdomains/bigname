# Storage

PostgreSQL is the durable indexing and serving store. Current runtime objects
live in `bigname_phase`. The fresh baseline and schema fixtures live in
[`crates/storage/schema/`](../crates/storage/schema/README.md), with the
combined regression check at `scripts/check-schema`. The append-only SQLx
history in `migrations/` records
the retired `public` schema, its schema-qualified deletion, and reviewed
in-place schema-migrations for initialized `bigname_phase` databases.
Deployments do not require the database itself to use C collation, but the
deployed collation must order fixed-width lowercase hexadecimal text
byte-lexically as C does; the API relies on that property to retain the existing
B-tree service for identity keys. Other comparisons that need C ordering apply
it locally. Numeric or otherwise hex-incompatible collations are unsupported
until a schema-migration index or startup locale gate explicitly admits them.
The repository's CI/test database and default Docker deployment use
`postgres:16-alpine`: musl-backed libc collations are bytewise, so those images
satisfy this contract by construction but cannot validate its glibc behavior.
An external glibc 2.39 deployment probe confirmed that 25,000 fixed-width lowercase
hexadecimal strings sort identically under `en_US.UTF-8` and C. This remains a
deployment property rather than a suite-enforced gate. The ignored integration
test runs the same probe on the available glibc PostgreSQL image; issue `#833`
tracks glibc 2.39 CI. Expression-local `COLLATE "C"` remains load-bearing on a
glibc server for noncanonical operands, where lowercase and uppercase
hexadecimal text can sort differently.

## Invariants

- [Raw facts](glossary.md#raw-fact) are immutable and block-hash anchored.
- A Verify row with a recorded cursor is stamped before intake replay can
  rewrite its readable raw-fact extent; stamping makes any retained level
  historical until Verify reruns.
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
One deployment's `bigname_phase` tables are one table set. Chains carrying the
`ens` namespace never share a table set: Ethereum Mainnet and Ethereum Sepolia
must not write to the same tables, and Sepolia always runs as its own deployment
with its own tables. Two chains may share one database only when their
chain-native name-system namespaces differ, as in the supported Ethereum-plus-Base
production deployment. The phase runner derives each configured chain's
namespace from the binary-approved [deployment
profiles](glossary.md#deployment-profile) and refuses this invalid topology
before starting any chain. A chain ID absent from those approved deployment
profiles is unsupported and refused explicitly. The check runs for supervised
startup and operator redo after its chain set is resolved, before manifest
synchronization or any indexing phase runs.
An additive baseline index may be an explicitly reviewed release exception when
its production build must use `CREATE INDEX CONCURRENTLY`: the release runbook
must carry the exact live DDL, validity checks, recovery procedure, and
release-record evidence instead of silently treating the baseline edit as an
initialized-namespace upgrade.

Interpret redo preparation looks up name bindings across canonicality states,
including rows it has just staged as orphaned. The
`surface_bindings_chain_name_history_idx` index covers `(chain_id, logical_name_id)`
without a canonicality predicate for that historical lookup. It complements the
canonical-only serving indexes and changes no replay or identity semantics.
Existing installations receive it through
`20260916120000_surface_bindings_name_history_idx.sql`; pause the phase runner
while applying this ordinary index build, then resume its existing redo.

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

`apps/api` owns route-specific joins, pagination, and wire shaping. Route-specific queries
therefore live with the API surface, while their reusable canonicality predicates come from
storage. API helpers that confirm one request reads against an
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
Project to the documented record and primary-name surfaces. The served hydrator
runs after event-derived publication; the family path applies prepared results
inside follow-block publication and journals them with the event-derived rows.
The [projection contract](projections.md) describes text and reverse hydration
admission, invalidation, and replay behavior.

## Identity

Stable identity follows [ADR 0002](adrs/0002-surface-resource-identity.md) and
the continuity rules in [`architecture.md`](architecture.md#identity-model).

- deterministic namehash-based IDs identify chain-native name surfaces;
- opaque UUIDs identify backing resources, bindings, and token lineages where
  upstream has no stable global identifier; and
- monotonic database IDs are limited to append-only observation order where
  they are not public identity.

Permissions and control attach to `resource_id`, not display text. Historical
surface-to-resource changes remain reconstructible through `surface_bindings`.
Canonical display text is derived from verified preimages and normalization
state; it is never identity.

### Binding intervals and authority arms

Every `surface_bindings` row stores a non-null `authority_arm` with one of the
closed values `ens_v1`, `ens_v2`, or `basenames`. The value is the persistence
form of the name's [authority epoch](glossary.md#authority-epoch), not a
replacement for `binding_kind`: both ENS eras can use
`declared_registry_path`. Adapters put the arm on each binding or closure draft,
and Interpret writes it directly. SQL must not infer it from strings or
provenance.

Ordinary binding interval operations use
`(chain_id, logical_name_id, authority_arm)` as their conflict domain. Their
predecessor and successor lookups, explicit closes, and implicit predecessor
caps cannot affect another chain or arm. The existing ordering and interval
rules are otherwise unchanged within that domain. This permits an ordinary
ENSv1 row and an ordinary ENSv2 row derived from an [independently admitted
event](glossary.md#independently-admitted-event) for the exact same logical name
to remain simultaneously open until an explicit activated
[migration boundary](glossary.md#migration-boundary) selects the successor.

Interpret restores the timestamp of each activated `MigrationApplied` transition
from its canonical normalized evidence. A later ENSv1 registrar-expiry boundary
retains the old lease release without opening a replacement ENSv1 registry
binding or granting its retained registry owner current control. No new
persisted marker or identity is introduced. If a physical batch contains both
the migration and that later expiry, complete-group correlation first proves
the migration and the adapter reinterprets that batch with the proof's timestamp;
earlier blocks cannot use a future boundary. The resulting activated
transitions must remain identical. A resumed session and a cold restore apply
the same retained proof. Redo and reorg rebuild it from surviving canonical
events; replay without the activated boundary restores ordinary ENSv1 expiry
behavior. Real later ENSv1 ownership observations are not suppressed. This
interpreter-content change
requires the normal Interpret and Project redos for previously derived rows.

When an ENSv2 registration release, a move away from a registry path, or a
block-boundary expiry closes this arm-wide conflict domain but the surface still
has a registered holder with a linked resource, Interpret reasserts the elected
holder at the same raw-log or block-boundary position.
The elected holder is the greatest lowercase `registry-address:token-id` key
among the retained registered holders with linked resources. The reassertion
writes the replacement binding and a closure that exempts it; it does not
synthesize registration, release, transfer, expiry, resolver, or subregistry
normalized events. The affected surfaces are tracked only while interpreting
the current batch, cleared at its boundary, and never persisted or restored.
The non-lifecycle `PreimageObserved` row written with a survivor reassertion
records the replacement binding and closed authority arm for redo. A raw-log
reassertion uses the existing `raw_log_preimage_observation`
[derivation kind](glossary.md#derivation-kind); a block-boundary reassertion
uses `raw_block_preimage_observation`. The latter derivation kind is admitted
by the fresh schema and by an in-place schema-migration for initialized
databases.

The append-numbered phase-schema upgrade adds
`surface_bindings.authority_arm text NOT NULL` with the closed-value check. It
ships before the planned production re-walk from block zero, so it does not
guess arms for historical rows or perform a historical backfill. Fresh replay
always supplies the value. The fresh phase baseline has the identical column,
constraint, and comment. That historical schema-migration required an offline replacement of bindings
and their then-existing serving dependents; the removal schema-migration
`20260929160000_remove_served_projections.sql` later dropped those serving
tables. Preserve the actual dependency order for an upgrade from that
older schema; raw facts, manifest identities,
normalized-event identities, and unrelated phase rows remain in place for the
mandatory full Interpret and Project redos.

Reverse address lookup uses `project_address_name_index` to admit candidate name surfaces, seeks those
keys in primary-first, role, and lexical order (role order ranks names whose
`owner` is the address before names it is only the `manager` of), and recomputes their current
relations in batches of at most 64 names. Project writes that index with the
`token_holder` and `effective_controller` relations only, every address under
both, because a name with no token is owned by its registry owner; it writes no
`registrant` rows. The primary claims, relation masks,
exact count, and page inventories share one read-only repeatable-read snapshot.
The count visits every candidate but retains only a page and its overflow row.
The candidate SQL can inspect or sort more index entries than it returns, and
masked candidates can require additional seeks; production query plans and
latency still require production-scale qualification before activation.

## Table ownership

| Family | Writer | Meaning |
| --- | --- | --- |
| `chain_lineage`, `chain_header_audit`, `chain_heads`, ingest cursors | Ingest and head publication; the phase runner's startup [RPC chain check](deployment.md#rpc-chain-check) fills `ingest_cursors.verified_chain_id` and `verified_genesis_hash` | Block ancestry, readable heads, source progress, explicit canonicality, and the chain each intake source's endpoint reported when it passed the check. |
| selected `raw_*` | Ingest | Immutable transaction, receipt, and log interpretation inputs. |
| `manifest_*` | manifest synchronization | Authored source declarations and admitted capability versions. |
| `discovery_*` | Interpret | Canonical discovered edges and admission evidence. |
| `name_surfaces`, `surface_bindings`, `resources`, `token_lineages` | Interpret | Stable identity anchors. |
| `label_preimages` | Interpret and `phase-runner label-preimages import-ens-rainbow` | Verified labelhash-to-label observations from chain events and the proof-checked rainbow import. |
| `ens_names` | operator rainbow load | Unverified rainbow-table candidates consumed by the import command. |
| `normalized_events` | Interpret; manifest synchronization for `SourceManifestUpdated` only | Protocol events normalized transactionally with identity output, plus retained manifest-authority history. Manifest synchronization's rows must not be deleted or rebuilt as Interpret output: [discovery-rule widening checks](glossary.md#discovery-rule-widening-and-narrowing) reconstruct historical declaration floors from them. |
| `discovery_watch_admissions` | Interpret | The last acknowledged [discovery-watch admission snapshot](glossary.md#discovery-watch-admission-snapshot) for each active manifest-authority fingerprint and lineage-orphaning epoch. This is replay coordination state, never fetched-fact evidence, redo authority, projection, or serving data. |
| `interpret_decode_skips` | Interpret | Append-only operator diagnostics for selected event logs from undeclared emitters skipped after malformed ABI decoding, and for logs that preceded their emitter's same-batch discovery admission; never identity, normalized-event, projection, or serving data. |
| `migration_event_associations`, `migration_discovery_associations`, `migration_candidate_identity_effects`, `migration_candidate_discovery_effects` | Interpret | Correlation-versioned diagnostic associations and effects that slice 1 must not use to alter independently admitted normalized events, identity rows, or [discovery edges](glossary.md#discovery-graph--discovery-edge). The ordinary `registry_announcement` indexability edge remains a watch-plan input. |
| `child_registration_events` | Project | Historical membership of each name's [direct child registration](glossary.md#direct-child-registration) events, rebuildable from canonical interpreted input; name history selects rows through it and reads the events themselves from `normalized_events`. |
| `project_family_marker`, `project_family_undo`, `project_repair_record` and [owned key family tables](glossary.md#per-block-publication) | Project | Permanent current serving state, publication generation, undo journal and repair progress. Readers compose names, records, control, permissions, resolver collections, reverse claims and address relations from one family snapshot. Child lists and counts use `project_child_edge_candidate`, `project_parent_subregistry` and `project_name_summary`; the `GET /v1/names` expiry walk reads `project_name_summary.authority_arm` to skip names an `authority` filter cannot list. An unavailable marker or overlapping redo refuses composed reads. Family data is rebuildable from canonical interpreted input; hash-pinned hydration overlays follow the documented replay policy. |
| `project_text_hydration_work`, `project_reverse_hydration_work` | Project | Derived indexes of pending text hydration and continuously refreshed reverse tuples. Keyed like their source rows, with indexed attempt order. Publication and undo refresh affected keys transactionally; reset clears them and rebuild repopulates them. No provider payloads or separate history. |
| `chain_phase_state`, redo/invalidation state, `service_heartbeats` | phase runner; manifest synchronization may stamp or widen required Ingest redo work recorded by the [manifest-authority marker](glossary.md#manifest-authority-marker), and Interpret may stamp discovery-owned required Ingest work in the transaction that finalizes a completed pass | Phase progress, repair work, and runtime liveness. Both coordination writers use the shared required-Ingest installer under the existing synchronization and runner phase-exclusion rules. They preserve lifecycle backup fields, clear resumable evidence for genuinely new demand, and never execute the redo. The phase runner remains the sole executor and redo authority. |
| `resolution_divergences` | guarded non-API lookup functions; Project publication may only clear outdated direct observations | Active live/indexed resolver disagreements and retained observations retired after the exact resolver becomes null; diagnostic only. |

The API owns `normalized_events_registry_token_idx`, a read-only partial index on
Interpret's `normalized_events`. It serves `storage:normalized_events.registry_tokens`
(`crates/storage/src/registry_token_ids.rs`): at most 200 deduplicated resources per query,
with one latest-event index probe for each chain/resource bounded by the selected publication.
Only activated, readable `TokenResourceLinked` and `TokenRegenerated` events from the ENSv2
root/registry source families can supply a token. The query also checks matching readable
chain/hash/number lineage. It selects the latest event by physical block/transaction/log position
before parsing its token word as U256; invalid latest evidence fails instead of selecting an
older value. Resource UUIDs remain stable permission handles, independent of token versions.

Name detail loads its selected composed row and token evidence in one short read-only
REPEATABLE READ snapshot, then closes it before verified RPC work. Resolver bound-name pages
reuse their collection snapshot; detail lookup enriches all forward and reverse records once
before its existing served-head generation revalidation. Feed and DTOs without `token_id` make
no token read. There is no historical token endpoint: exact name/resolver selectors below the
current publication still return `stale`. The reader changes no normalized or projected rows.
The [concurrent prebuild runbook](../ops/registry-token-index/README.md) gives the large-database
installation, adoption and recovery procedure for schema-migration
`20261005090000_normalized_events_registry_token_index.sql`. This index is rebuilt with the
[walk index set](#walk-index-set) before serving resumes.

`project:families.hydrate.text.select` reads changed selector keys and the ordered share from
`project_text_hydration_work_order_idx`. The reverse selector uses
`project_reverse_hydration_work_active_idx` and `_stale_idx` before joining tuple state.
`project:families.hydrate.reverse.keys` finds dependents through
`project_reverse_tuple_node_idx`, `project_reverse_tuple_claim_idx`, and
`project_reverse_node_claim_event_idx`; the reverse selector's resource-pointer lookup uses
`project_resource_pointer_hydration_node_idx`. All are indexes on Project-owned tables.
Text dependency selection uses the existing value primary-key prefixes for resolver and
partition. Work-table deletion and insertion use the same source primary keys.

Schema-migration `20260930220000_project_hydration_work.sql` takes the family marker lock
and resets family state on first installation, atomically with creating both derived work
tables. Fresh baselines have the same tables and indexes. Reapplying the migration when both
tables exist preserves publication. The changed Project source rotates the interpreter content
hash; adoption follows the existing Interpret redo and installed Project rebuild policy.

Child pages and counts calculate authority-arm agreement once per child across the candidate
relation. Exact filtered totals still require evaluating every eligible child, even for a small
`page_size`. Display-name ordering uses label preimages with the documented placeholder
fallback; keyset pagination bounds the returned rows, while the count and ordering evaluate
the filtered child relation. A registry's labels page and count build only the ENSv2
candidates under a parent whose current subregistry is that registry, and evaluate the
ENSv1 and Basenames edges only for those children, since another arm can only refuse a child
that also has an ENSv2 candidate.

Family indexes serve these concrete readers:

- Expiring names use `project_lifecycle_event_expiry_idx`,
  `project_lifecycle_event_inexact_expiry_idx` and `project_wrapper_state_expiry_idx`;
  resolver-bound names use `project_named_resource_pointer_resolver_idx` and
  `project_registry_pointer_resolver_idx`.
- Name-summary recomposition uses `project_name_summary_recompose_idx`,
  `project_binding_candidate_predecessor_idx`, `project_binding_candidate_lease_idx`,
  `project_lifecycle_association_target_idx` and `project_registry_owner_event_resource_idx`;
  zero-owner attribution uses `project_registry_owner_event_name_idx`. The reserved names a
  Universal Resolver proxy change recomposes (`project:families.derived.cutover_names`) are
  read without an index: the statement first checks the block's journal by its primary key
  and scans `project_lifecycle_event` only on a block that changed a proxy row, a handful of
  blocks per chain. Project also reclassifies retained proxy rows when its captured
  manifest set changes or a current proxy declaration starts. These changes use the
  same undo journal and summary recomposition; retired addresses keep their latest
  upgrade without retaining the current client-facing role. The same statement also
  checks the release journal by primary key, then matches only the chain's active
  divergence names to affected second-level parents from the normal summary work
  list. It uses the existing active-ledger and surface identity indexes; ordinary
  blocks with neither change do not scan reservation or active-evidence candidates.
- Child pages and counts check that an ENSv1 or Basenames edge is its child's latest across
  parents through `project_child_edge_candidate_child_idx`, by chain, namespace and child
  node, and find a registry instance's ENSv2 registrations through
  `project_child_registration_state_registry_idx`; both primary keys lead with a column
  those lookups do not bind. The registry children an address owns read their edges through
  the same index by their candidate nodes rather than by parent.
- The same child reads, and the registry children an address owns, find the registrar lease
  events of a child with no name surface through `project_lifecycle_event_namehash_idx`, by
  chain and the child node, to tell whether its lease has been released and who holds it; the
  primary key leads with the lease's resource, which the child does not know. Children with an
  active named surface at the clock never probe, and the child counts read neither probe.
- The name-ordered walks (`storage:families.name.search_candidates` and
  `storage:families.name.bound_candidates`) can read `name_surfaces_name_order_idx` in page
  order, `(raw_name, namespace, namehash, logical_name_id)` over active, readable surfaces, with
  the keyset cursor as the index condition and no sort; the planner chooses it on cost. Its
  predicate leaves out names longer than 2000 bytes, because a btree entry larger than about
  2.7 KB fails the insert, and both walks carry the same bound, so those names are never listed
  there. A `LIKE` prefix becomes an index range only on a database whose collation PostgreSQL
  recognises as C (`C` or `POSIX`); under any other collation it filters the ordered scan. The
  reverse lookup candidates (`storage:families.records.reverse_candidates`) keep long names and
  start from `project_address_name_index`.
- Permission pages use `project_grant_subject_idx`, `project_grant_scope_idx`,
  `project_account_approval_subject_idx`, `project_registry_binding_observation_resource_idx`
  and `project_registry_binding_observation_owner_idx`.
- The composed name reader's resource pointer lookup
  (`storage:families.name.resource_pointers`) can probe `project_resource_pointer_pkey` by
  resource and the partial `project_resource_pointer_root_node_idx` (ENSv2 root registry
  pointers only) by namespace and namehash, joined by a BitmapOr; the planner chooses that
  path on cost, and the plan test pins it for a selective request.
- A follow block's name-summary work list (`project:families.derived.summary_names`) reads
  the registry events and name surfaces of the blocks after the family marker's through
  `normalized_events_chain_block_number_idx` (or its descending twin) and
  `name_surfaces_chain_block_number_idx`, with the marker's block bound as a parameter, and
  each such event's resource through `normalized_events_resource_history_idx`. Name-summary
  composition looks names up through `project_lifecycle_key_state_name_idx` (beside the key
  state primary key, joined by a BitmapOr) and `project_name_state_name_idx`, since
  `project_name_state`'s primary key leads with the namespace.


Interpret writes `discovery_edges` and `contract_instance_addresses`. A phase
that reads them may add read-only indexes through its own schema-migration, never
changing a row, when it records each index here with the statement it serves.
The owned key families add six, named for their reader:

| Index | Serves |
| --- | --- |
| `project_families_discovery_edges_resolver_from_block_idx` | `project:families.classification.activated`: resolver edges that start at the block |
| `project_families_discovery_edges_resolver_to_block_idx` | `project:families.classification.activated`: resolver edges that stop at the block |
| `project_families_contract_instance_addresses_from_block_idx` | `project:families.classification.activated`: contract addresses that start at the block |
| `project_families_contract_instance_addresses_to_block_idx` | `project:families.classification.activated`: contract addresses that stop at the block |
| `project_families_discovery_edges_resolver_destination_idx` | `project:families.classification.activated`: whether an address that starts or stops at the block is any resolver edge's destination, deactivated edges included |
| `project_families_discovery_edges_resolver_admission_idx` | `project:families.classification.classify`: one active resolver edge per active manifest to each touched resolver's contract instance |

Interpret also writes `name_surfaces`. The API's name-ordered readers add one read-only
index on it, installed by the identity baseline and its own schema-migration and changing no
row:

| Index | Serves |
| --- | --- |
| `name_surfaces_name_order_idx` | `storage:families.name.search_candidates` and `storage:families.name.bound_candidates`: readable surfaces in name order after the keyset cursor |

History's record attribution (`crates/storage/src/history/attribution`) adds two read-only
indexes on `normalized_events`, installed by the normalized-events baseline and
`20261003120000_normalized_events_record_id_attribution_indexes.sql` and changing no row:

| Index | Serves |
| --- | --- |
| `normalized_events_record_id_write_idx` | `push_record_link_arm` in `history/attribution/sql.rs`: a selected record's `RecordChanged` writes by chain, resolver and record id |
| `normalized_events_record_id_link_idx` | the `links` CTE of `push_record_link_ctes` in `history/attribution/sql.rs`: the `ResolverRecordLinked` rows on a pointer's chain and resolver at its node or the zero node |

Address history (`crates/storage/src/history/filters.rs`) adds one read-only index on
`normalized_events` for its registry root role branch, installed by the normalized-events
baseline and `20261005120000_normalized_events_address_root_permission_idx.sql` and changing no
row:

| Index | Serves |
| --- | --- |
| `normalized_events_address_root_permission_idx` | the `OrRootPermissionSubject` branch of `push_selector_filter` in `history/filters.rs`: activated, readable `RootPermissionChanged` rows by lowercased subject |

When an ENSv1 BaseRegistrar manifest admits ordinary numeric registration and renewal,
Interpret retains the registrar resource, token lineage, owner and expiry independently of
registrar-controller logs. Before an admitted plaintext label is known, these lifecycle rows
have no `logical_name_id`, and the numeric event creates no name surface. A previously admitted,
non-shadow preimage in the same namespace can make the name known before numeric registration;
with matching current-registry ownership setup, that registration binds the registrar resource.
A later admitted controller preimage binds the current retained ENSv1 authority, while a preimage
from another ENS source alone does not choose registrar authority. A subsequent numeric event can
bind a now-known name when the registrar remains current. Earlier resource-only rows are not rewritten.
(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L130-L168 @ ens_v1@91c966f)

When an admitted controller event names a registrar lease whose label was unknown, Interpret
writes the binding and nothing else about the lease: the earlier grant, renewal, expiry and
release rows keep their null `logical_name_id` and stay keyed by the registrar `resource_id`.
A resolver set on the node before the label was known was linked to that resource alone, so the
naming event also emits one named [state-derived normalized event](glossary.md#state-derived-normalized-event)
of kind `ResolverChanged` for the current non-zero resolver. It is sourced to the registrar's
manifest at the naming event's raw position, marked `state_derived=true` and
`surface_materialization=true` with `pointer_reason=surface_materialization_current_resolver`,
and copies the link's `resolver_source_role`. Restoration reads that key only for the
`registry_old` role, so it does not rebuild the named resolver link from this row. Later
resolver writes carry the name either way, whether the interpreter kept its state or restored it
from stored events. The replay is sourced to the manifest recorded on the authority being named;
a registry-only authority opened by a registrar transfer without `reclaim` records none, and its
resolver is not replayed.
Same-transaction reconciliation does not treat this replay as a successor authority epoch of the
resource, so the binding made at that position survives.

The `NameWrapped`-derived rows that inherit the wrap's shared observation object record
`after_state.wrapped_registrar_resource_id`. Those rows are `TokenControlTransferred`,
`ExpiryChanged` and `PermissionScopeChanged`, and the authority-transition rows the wrap emits
from the same object: `AuthorityEpochChanged`, and `SurfaceBound`, `SurfaceUnbound` and the
authority `ResolverChanged` whenever the wrap emits them. The value is the
`resource_id` of the BaseRegistrar lease whose token the wrap moved into the NameWrapper, when a
registrar lease with a token lineage is the node's current authority as the log is interpreted.
On those rows the key is always present and is `null` otherwise: for a wrapped subname, which has
no registrar lease, and for a registration where a controller event creates the lease only after
`NameWrapped` in the same transaction. The wrap's holder `PermissionChanged` rows and the
`PreimageObserved` row generated for its name are built separately and do not carry the key.
Project follows a wrap to its registrar lease through this recorded
identity rather than by matching names or timestamps.
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L264-L268 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L289-L305 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/deployments/mainnet/WrappedETHRegistrarController.json:L656 @ ens_v1@91c966f)

History reads follow the same recorded identity. A product read by `registration_id` reports
the lease for NameWrapper rows whose wrapper `SurfaceBound` row names it, and the name and
resource anchors of a history read include the leases a name's wrapper bindings recorded and the
names whose wrapper bindings recorded a lease. Like every other anchor loader, the wrapper-link
loader takes the read's publication block bounds, so a link recorded above the published block
of its chain is not followed. A registration-scoped product read gathers its candidates from
three index-keyed arms (the lease's names, the resources of those names, and resolver record
writes attributed to them) and then keeps a row only when its product registration identity is
the requested lease, or when the row has no resource and falls inside a binding of that lease
that was open at the row's position. The rows that prove the requested resource is a registration
(its grant, a wrapper `SurfaceBound` row that names it, the binding a resource-less row falls
inside) also lie at or below the read's published block, so a grant Interpret has written above
the publication a read is bound to does not turn that publication's older rows, count, or cursor
anchors into registration history.

Address history (`GET /v1/addresses/{address}/history`) runs three statements in
`crates/storage/src/history/`. The anchor lookup (`address_matches.rs`) finds the names and
resources the address holds now, through the family address-name reader, and held in the past from three kinds
of activated, canonical events. A current relation row counts only when the event Project cites
for it (`provenance.chain_id` and `chain_positions.block_number`) lies at or below the read's
published block of that chain, so a relation acquired after that block cannot admit the
resource's older events; a row without a cited block does not count under a bound. The name's
attachment to the row's resource is judged at or below the published block too: some
`surface_bindings` row with the row's `logical_name_id` and `resource_id` on that chain must have a
`block_number` at or below the bound, whatever block the row cites. It need not be the row's
current [surface binding](glossary.md#surface-binding): a registry owner moved away and restored
rebinds the name to the same resource above the bound while the holder held it throughout. A
resource first attached above the bound has no such row, so it does not count. The canonical read
counts canonical bindings only; the probe reads `surface_bindings` by name or resource. A row
cited above that block still counts when every registration event between the bound and the cited
one is a same-holder token transfer: the cited event (`provenance.normalized_event_id`, read by
primary key) is a `TokenControlTransferred` whose `before_state.from` and `after_state.to` both
equal the address, and every activated, canonical `RegistrationGranted`, `RegistrationReleased`
and `TokenControlTransferred` row on the cited event's resource with a block above the bound and
at or below the cited block is such a transfer too, with no ENSv2 `RegistrationReserved` row in
that range; an effective-controller row also needs no
`AuthorityTransferred`, `SurfaceBound` or `PermissionChanged` row in that range. The range is read
from `normalized_events_resource_history_idx`. Project cites the latest registration event for the
token holder and fallback controller rows, so a transfer from the holder to itself
moves the citation without changing the holder; the earliest transfer in the range names the
address as its sender, which proves the address held the name at the bound. The three
kinds of historical events are: a `RegistrationGranted` whose `registrant` is the address, a
`TokenControlTransferred` whose `to` is the address, and an `AuthorityTransferred` whose `owner`
is the address, each compared lowercased. One partial expression index per kind keys those rows
by the lowercased value: `normalized_events_address_registrant_match_idx`,
`normalized_events_address_token_holder_match_idx`, and
`normalized_events_address_registry_owner_match_idx`. Their expressions and predicates must stay
identical to the query text. The capped count and the page then read the rows of those names and
resources plus the resolver record writes attributed to the resources (`attribution.rs`,
described below). That filter is an OR of `logical_name_id`, `resource_id`, and
`normalized_event_id` conditions. The attributed event ids do not depend on the row, so the read
loads them once, inside the page's repeatable-read transaction, and binds them as an array
(`= ANY($ids)`); PostgreSQL then answers each branch from
`normalized_events_name_history_idx`, `normalized_events_resource_history_idx`, and the primary
key and combines the results. Written as `IN (SELECT ...)`, the branch cannot be an index
condition inside the OR, and the planner reads every canonical row to keep the few that match.
The three indexes cover only activated rows in readable canonicality states, so an anchor read
that drops either condition cannot use them and falls back to a broad scan, such as
`normalized_events_projection_idx` without the address as a key: a read with `canonical_only=false` (possible only through the
storage functions `load_address_history_for_relations` and
`load_address_history_page_for_relations`, which the public route always calls with `true`), and
`GET /v1/diagnostics/events` with an address filter, which also reads candidate rows.
That unbounded raw diagnostic path derives its anchors entirely from retained
normalized events, so clearing family rows during Project rebuild does not remove
the audit. It additionally accepts resource-scoped `PermissionChanged` evidence
whose before or after state assigns `resource_control` to the address and
state-derived registry-only `SurfaceBound` owner evidence. This intentionally
includes former-controller audit history after revocation or replacement; it does
not assert current ownership. The bounded product path keeps the current-relation
and publication checks above. No parallel current-state cache is introduced.
`GET /v1/names/{name}/history` with `scope=both` uses the same filter. The registration-scoped
read keeps a correlated `IN` because its attribution check refers to the row. These are access
paths only: no stored row, response, or [interpreter content
hash](glossary.md#interpreter-content-hash) input changes. Existing installations receive the
three indexes through `20260923120000_normalized_events_address_match_indexes.sql`; prebuild
them concurrently on a large database with
[`ops/address-history-indexes/install.sql`](../ops/address-history-indexes/README.md) first.

A registry root role change (`RootPermissionChanged`) belongs to no name, and its resource is
the registry's root resource, which every holder of that registry shares, so neither anchor
reaches it without also reaching every other holder's changes. The page and count of a product
address read in `both` or `registration` scope whose relations admit `role_holder` therefore
add one more branch to the same OR: `RootPermissionChanged` rows whose lowercased
`after_state ->> 'subject'` is the address. `normalized_events_address_root_permission_idx`
keys that branch by the lowercased subject, then the block and log position, over activated
rows in readable canonicality states, so a page whose address has no other anchor reads it in
history order. That branch also carries the read's namespace. Every name, resource and
registration branch of a product read excludes `RootPermissionChanged` rows, so a root role change reaches an
anchored read only through its subject, even when a nonconforming registry tied a name or
registration to its root resource. Diagnostics reads neither add the branch nor exclude those
rows. Existing installations receive the
index through `20261005120000_normalized_events_address_root_permission_idx.sql`; the same
`ops/address-history-indexes/install.sql` prebuilds it concurrently.

Node-keyed resolver record writes carry neither a name nor a resource, so history reaches them
through the registration's resolver pointers (`attribution.rs`). The reader evaluates the
evidence that ties a node-keyed write to a resource, but only the evidence at or below the read's
published block of each chain: the resource's
`ResolverChanged` pointers (each paired with its name's `name_surfaces.namehash`), the
node-keyed `RecordChanged` and `RecordVersionChanged` writes on each pointer's resolver before
the next pointer (through `normalized_events_ens_v1_record_node_resolver_idx` and
`normalized_events_basenames_record_node_resolver_idx`), the `ResolverRecordLinked` rows that
split a pointer's window on a record-ID resolver, and, for a latest pointer at a declared ENSv1
mirror resolver, the ENSv1 resolver the mirror would call as the ENSv1 registry stood at that
block. The reader follows that resolver only when the walk selects it at the name's own node
(`ancestor_depth = 0`), and it reads each registry pointer by the node the event addresses
(`child_node`, then `namehash`, then `node`), matching the family mirror reader. A nearest
resolver on an ancestor attributes nothing, and the reader does not look past it for a farther
one. A pointer or link above the published block neither attributes an older write nor closes
an earlier pointer's window, so a resolver selected after the block cannot pull an older write
into a read bound to it. Superseded pointers keep their windows and a clear closes the previous
window without opening one. A resource whose latest pointer is a mirror resolver that cannot be
followed to an ENSv1 resolver attributes nothing. One input is current state: whether a
resolver is a supported, manifest-declared ENSv1, `public_resolver_v2`, or mirror resolver comes
from its `project_resolver_classification` row, and the read refuses when a chain the pointer
walk reaches has no servable [family publication](glossary.md#family-marker). The record
inventory's `provenance.attributed_event_ids` is this same reader evaluated at the current
publication, so the two cannot drift. The family inventory reader takes the field from
its caller's mode: `FamilyAttribution::Load` computes it with this reader, as
`load_family_record_inventory` does; `FamilyAttribution::Given` supplies a caller's set, and the
topology, resolves-to and record-count reads pass an empty one; `FamilyAttribution::Omit` leaves
the field out. The inventory reads behind `GET /v1/names/{name}`, `GET /v1/names/{name}/records`,
`POST /v1/lookup`, verified lookup and `GET /v1/diagnostics/names/{name}/records` use `Omit`. An
unsupported mirror row attributes nothing, so under `Load` and `Given` it carries an empty list. No response, guard or comparison reads the field, and on a resolver with
many writes the reader costs seconds per resource. Reads without publication bounds (the unbounded storage loaders and diagnostics reads
that pass none) evaluate every readable pointer and write. The ENSv1 and Basenames node-keyed arms
use the node and resolver expression indexes on `normalized_events`. The ENSv2 declared-resolver
arm reads its writes through `normalized_events_project_node_history_idx`, keyed by chain and
node: its family comes from the resolver's classification at run time, so the arm also names the
two families it admits literally, which lets the planner prove that index's partial predicate.
The record-ID arm reads a selected record's writes through `normalized_events_record_id_write_idx`
and the record links on the pointer's resolver, at the pointer's node or the zero node (the
resolver's default link), through `normalized_events_record_id_link_idx`.
Without these, the declared-resolver arm read every `RecordChanged` and `RecordVersionChanged`
row of the chain, the record-ID arm every `RecordChanged` row and the `links` CTE every
`ResolverRecordLinked` row, through the broad
`normalized_events_projection_idx`. The mirror lookup of the ENSv1 registry pointer by addressed node uses
`normalized_events_project_v1_pointer_addressed_node_idx`
([`ops/mirror-pointer-index`](../ops/mirror-pointer-index/README.md)). The lookup of
the declaring manifest also reads through the projection index, on every deployment. Plan tests
in `history/address_plan_tests.rs` check that neither statement reads `normalized_events`
sequentially, that every record write and record link the attribution reads goes through one of
the five indexes above, that the plan reads `normalized_events_project_node_history_idx`,
`normalized_events_record_id_write_idx` and `normalized_events_record_id_link_idx`, each probe
keyed by the pointer (its node, its resolver and record id, or its resolver and node), and that
the mirror lookup, run over a non-empty walk, reads registry pointers
through `normalized_events_project_v1_pointer_addressed_node_idx`.

History loaders called with `canonical_only=false` also return rows of activated losing
branches. For those reads every binding, grant and wrapper-link witness must lie on the event's
own parent-hash path in `chain_lineage`; matching block numbers or canonicality states do not
connect two retained forks. The HTTP product routes always read canonical rows only, where the
check is skipped.

A canonical, admitted, normalization-valid readable observation may also disclose a retained unnamed ENSv1 registrar lease to its exact namehash and labelhash only when that same live resource and token lineage are already the selected authority. Current admitted ENS registry ownership evidence must match the registrar's nonzero current owner; missing ownership evidence, a different authority, expiry, release, or migration retirement prevents attachment. A readable observation does not select authority. The binding begins at the observation. Earlier resource-only events remain unchanged.

`registration_window` retains whether restoration reconciles preceding setup logs or the complete
registration transaction. `registration_registry_setup` retains proof of registry setup matching
the registrar owner. `registry_migrated` retains the current-registry ownership observation needed
to continue suppressing the retired registry, and `surface_known` retains whether an active
plaintext name was known at the authority observation. These are normalized-event restoration
fields, not new identity anchors or projection writes. Resource-only restoration derives an
internal name identity from namespace and namehash without publishing a readable-name binding.
Current-registry `Transfer` also restores terminal [registry fallback handoff](glossary.md#registry-fallback-handoff),
independently of the registration marker.
(upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L29-L34 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L63-L68 @ ens_v1@91c966f)

Numeric BaseRegistrar expiry above the signed timestamp range is retained as `i64::MAX`.
The `ens_v1_registrar_l1` controller events that repeat that expiry next to the label use the same
rule, so an out-of-range value does not fail interpretation and lose the label; Basenames expiry
decoding stays strict.
(upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L116-L124 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L133-L139 @ ens_v1@91c966f)
Settlement treats an expiry whose grace addition overflows that range as live.
Public rendering uses decimal Unix-second strings for finite retained expiry;
calendar range alone never makes it null. Null and an expiry reason describe
only a contract-specific absent expiry in a registration context (see
[the timestamp contract](api-v1.md#timestamp-format-and-absent-expiry)). Co-admitted
ENSv1→ENSv2 migration evidence retains over-`u64` expiry as decimal text and does not use it for
wrapper-expiry correlation. Exact Graveyard cleanup still requires its owner and expiry predicate.
The block-local unwrapped reconciliation and exact predecessor cleanup rules below are unchanged.
(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L130-L168 @ ens_v1@91c966f)

Adapters provide interpretation behavior. They do not write projections. API
code reads projections and request-scoped lookup output without writing database
state. The guarded [resolution divergence ledger](glossary.md#resolution-divergence-ledger)
writer remains available to non-API lookup callers.

Interpret finalizes the discovery-watch admission snapshot in the same database
transaction as the completed pass's discovery/address writes and any required
Ingest stamp. It compares the complete normalized union of concrete
address/topic intervals rather than a cursor-clipped view. An absent snapshot,
an active manifest-authority fingerprint change, or a lineage-orphaning epoch
change is a conservative empty baseline: existing discovery rows do not prove
that earlier intake fetched their address-scoped logs. The snapshot records
only that Interpret acknowledged the coverage demand; `chain_phase_state`
remains the sole work and redo authority.

Interpret redo may temporarily orphan and restage discovery rows without
creating repeated intake work because the acknowledged snapshot survives that
restaging. The row set is replaced only when a completed Interpret pass commits
under the same active authority and lineage epoch. Dropping and recreating a
chain starts a fresh comparison scope only when the wipe also clears that
chain's rows from `discovery_watch_admissions`; changing its active authority
fingerprint or advancing its lineage-orphaning epoch also starts a fresh scope.
Rollback leaves discovery writes, the snapshot, and the required Ingest stamp
unchanged together. Neither Project nor API code reads the snapshot.

Projection rows have foreign keys into `name_surfaces`, `surface_bindings`,
`resources`, and `token_lineages`. An offline rebuild must therefore remove
projection rows before removing any identity rows, rebuild or preserve the
identity rows first, and rebuild projections afterward. If an operator clears
projections before that rebuild completes, serving remains unavailable until
Project has published a coherent replacement; a failed partial rebuild is not
a serveable intermediate state.

Each `interpret_decode_skips` row records the chain, block and transaction
identity, log index, emitter, selected [source family](glossary.md#source-family)
and signature, selection scope, decoder context, and [interpreter content
hash](glossary.md#interpreter-content-hash). A malformed log from an emitter
declared in an active manifest is fatal regardless of selection scope, so this
table receives rows only for undeclared emitters. Its primary key combines
the raw-log position with that content hash, and Interpret inserts with conflict
ignore, so replaying or redoing the same log under one interpreter build does
not duplicate the diagnostic. The rows remain append-only across canonicality
changes and derived-state rebuilds; they are not replay input.

The same table also records a well-formed log that Interpret could not select
because its emitter was admitted by discovery only in a later block of the
same batch: discovery is forward-only from the admitting observation, so the
earlier log stays uninterpreted. Such a row carries the source family the
emitter interprets under, the log's `topic0` as its selection signature,
`match_all = false`, and a `decode_context` naming the admitting block. A log
from the admitting block itself is interpreted by the same-block second pass
([manifests.md](manifests.md#resolver-admission-by-implementation-announcement))
and produces no row.

For a manifest-declared address, an omitted `start_block` is initially stored
as `contract_instance_addresses.active_from_block_number = NULL`; interval
readers treat it as an effective block-zero lower bound. Refreshing that same
initial-epoch active row materializes zero instead of replacing it with a later
finite declaration start. Omitting a previously finite start also backdates
that active epoch to zero so the widened watch range is reproducible. A
readmission after retirement remains bounded after the preceding epoch, and a
fresh admission still stores its declared finite start. For compatibility,
retained omitted-start manifest history supplies the zero widening floor even
when an older binary left a finite first-observed block. Interpret's discovery
refresh now leaves the stored `NULL` untouched, fixing
[issue #547](https://github.com/ensdomains/bigname/issues/547), so this repair is
legacy-only for the laundering sequence between unchanged synchronizations of
an already-declared address, while it still intentionally fires when a finite
discovery-created address row is later declared for the first time with an
omitted start. When a desired active declaration omits its start,
synchronization restores zero on the
earliest address epoch even if retired; later re-admitted epochs keep their
bounded starts. It stamps the required Ingest redo from block zero (clamped to
the earliest configured source start) and invalidates the derived phases for the
restored interval. The repair is
one-shot because the stored row is then zero and its positive-floor predicate
cannot fire again; a current finite declaration keeps its finite watch bound.

Manifest synchronization does not reduce these address epochs to a minimum
when it validates a newly widened direct-address watch. It constructs the
continuous union of [persisted Ingest
coverage](glossary.md#persisted-ingest-coverage) for each chain, source family,
address, and event topic after applying the declaration start to every usable
epoch. A gap or finite tail refuses the synchronization transaction, preserving
the preceding manifest rows, address epochs, derived-phase markers, and Ingest
redo state. The refusal never stamps the missing range as repaired. Operators
must instead choose the first continuously covered start or rebuild from zero.
A retained database that still requires the earlier start needs a separately
planned repair which explicitly fetches the gap before the wider promise;
ordinary address-scoped redo follows the persisted epochs and cannot fill it.

Before re-deriving a range, Interpret preserves a finitely retired
manifest-declared contract-address row as coordination state. An event-derived
observation at or before that row's close block may reproduce its discovery
edge but cannot reopen its address range. An observation after the close block
may append a range or backdate an existing later active range to the greater of
the observation block and the greatest preceding address range's close plus
one. Retired ranges remain unchanged. This preservation does not change raw
facts, normalized-event identity, or projection ownership.

A non-retryable validation failure on an already-completed Ingest or Verify
row changes its lifecycle status from `completed` to `failed` without clearing
the retained range markers, source provenance, verification level, or content
hash. Verify can also retain its final completion evidence without ever becoming
`completed` when an ordinary, non-validation failure is recorded after its final
checkpoint but before phase completion. A retained row may be restored without
replay only from structural evidence: Ingest requires matching current, target,
and live-handoff markers; Verify requires matching current and target markers
plus a verification level. The next accepted start repeats the checks for the
retained completion and moves that row through `failed` to `completed`. Error
text alone never authorizes that transition. This preserved evidence is
diagnostic state, not permission to publish: policy-based Sepolia readiness
requires both Ingest and Verify to remain completed.

At runner startup, and before a `--phase ingest` redo begins while a required Ingest
redo is pending, a `running` or `paused` Interpret, Project, or Verify row with no
explicit redo is resolved only while its advisory lock remains held. A required Ingest
redo whose `last_error` begins with `required downstream redo active:` and outlived its
advisory-lock session is changed back to `required downstream redo:` while the next
runner holds that lock. Its `redo_from_block_number`, `redo_to_block_number`,
`redo_current_block_number`, `redo_current_block_hash`, `redo_target_block_number`,
`redo_target_block_hash`, `redo_source_boundary_markers`, and
`redo_manifest_authority_fingerprint` remain unchanged for the exact-range retry.
Pool-backed progress for a required Ingest redo also requires the active `last_error`
prefix, so a delayed write from the abandoned attempt cannot change those fields.
A saved Interpret or Project final checkpoint is recorded as `completed`; an
earlier checkpoint is recorded as `failed` so ordinary phase execution can
resume it. A saved Verify final checkpoint stays `failed` until current
configuration and retained verification evidence pass the completed-Verify
checks. A lock still held by another runner, or a lost lock connection during
the state update, stops the new runner or refuses the redo. The update and lock use one database
connection. If the client cannot tell whether PostgreSQL committed the update
before that connection failed, the next start reads the durable phase state
again. An unlock or connection-close error after an acknowledged update is also
reported.

Project redo retains its requested invalidation in
`redo_requested_from_block_number` and `redo_requested_to_block_number`.
Its existing `redo_from_block_number` and `redo_to_block_number` describe the
execution extent: undo can reach a journal predecessor below the request, a
rebuild starts from retained inputs, and replay can reach a standing marker
above the request. Before beginning the attempt, the runner asks Project to
plan that extent from its marker, repair record and undo journal. Every saved
marker remains inside the execution extent under the existing database check;
progress writes still require the exact attempt generation, mode and execution
bounds. Required stamps union requested invalidations while retaining automatic
ownership, clear stale progress, and advance the generation. Completion and
prior-hash supersession clear both pairs of bounds. Composed reads and lookup
revalidation use the requested lower bound to detect publication overlap, with
the existing lower bound as the fallback on legacy rows. A progress checkpoint
below the request does not itself invalidate that publication. Actual undo or
reset changes the family marker and sequence atomically with its rows, so an
earlier captured publication still fails revalidation.

The additive `20260930230000_project_redo_execution_extent.sql` migration leaves
existing rows unchanged. An active legacy Project row with NULL requested
bounds uses its existing redo bounds as the request at its next begin. A
same-hash interrupted rebuild can therefore resume after the runner atomically
plans its full execution extent; it does not need manual progress edits.
An accepted restart with a wider request retains the interrupted execution's
replay endpoint, capped by the current readable head. Retaining that pending
work does not authorize reuse of its partial prefix: reuse still requires the
same request, content hash, immediate attempt and unchanged input revision.
The request columns are otherwise limited to active Project redo and must be
contained in its execution extent. They are not a separate work authority.

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
through a resumed attempt or retry. A failed completed-state validation leaves
it present with the diagnosis. Recovery clears it only after the same current
configuration and retained phase evidence that ordinary completed-state
revalidation requires have been accepted. Genuine normal completion or a
successful redo that leaves complete retained phase evidence also clears it, so
the recovered row is indistinguishable from an ordinary completion. These
clearing writes use the phase advisory-lock connection, so losing phase
ownership aborts the write. While any phase marker is present, that chain is not
eligible for `ready` on the status endpoint and reports `degraded` unless a
stronger `stale` condition applies, such as a genuinely failed phase or an
expired heartbeat. Interpret and Project do not
run a separate completed-state revalidation pass. For those phases, the
phase-start check is the revalidation: their retained current block must match
the canonical head's height and hash before `AlreadyCompleted` authorizes the
marker-clearing write. If no canonical head is stored, startup reports a data
integrity error and leaves both the retained position and marker unchanged.

### ENSv1→ENSv2 correlation visibility

The slice-1 ENSv1→ENSv2 intake persists the
[migration correlation group](glossary.md#migration-correlation-group) without
making it consumer-authoritative. A normalized event whose existence depends on
that correlation stores top-level `migration_correlation_ids` and
`consumer_visibility`. Ordinary events default to an empty ID set and
`activated`; a correlation-dependent event has a sorted, duplicate-free,
nonempty ID set and is `candidate` before consumer activation or `activated`
after it.
`MigrationApplied` has exactly one ID. A shared correlation-dependent event
keeps one event identity and lists every participating per-name ID; a
name-independent registrar controller event has one stable
`controller_configuration` derivation-group ID.

Independent admission takes precedence over correlation visibility. If an
existing manifest and discovery path already produces a normalized event without
the ENSv1→ENSv2 correlation, Interpret reproduces that ordinary event
byte-for-byte: its event identity, payload, provenance, and `activated`
visibility do not change. Interpret records the correlation relationship in a
separate `migration_event_associations` row keyed to the ordinary event identity,
with the sorted correlation ID set, `correlation_kind`, evidence references,
chain positions, canonicality, and `consumer_visibility`: candidate while its
group is incomplete or refused and activated when its [complete
group](glossary.md#complete-group) is admitted. Correlation
never duplicates, suppresses, or reclassifies the independently admitted event.
The event identity is a plain value rather than a foreign key. A redo deletes
normalized events in its range before replay, but retains association rows whose
lineage is already orphaned as fork evidence; such a row may therefore have no
normalized-event parent. Replay of the same block re-creates that event under
the same identity; a block replaced by a competing fork does not, because every
event identity carries the block hash, so a row retained from the replaced fork
stays parentless. Project and product history readers ignore event-association rows;
diagnostic readers treat the normalized-event join as optional and can read a
retained association from its own position and `chain_lineage` anchor.

**The correlation tables' own `canonicality_state` is not maintained by
canonicality changes.** All four
ENSv1→ENSv2 correlation tables — `migration_event_associations`,
`migration_discovery_associations`, `migration_candidate_identity_effects`, and
`migration_candidate_discovery_effects` — copy `canonicality_state` from the
parent event when Interpret writes them, and no canonicality change touches it
afterwards: head publication orphans and re-promotes `chain_lineage` only,
Interpret redo orphaning covers identity, discovery, and binding rows but never
names these tables, and `clear_redo_range` deletes only rows whose anchor is
still readable so that losing-fork rows survive as evidence. The only later
write to a retained row is Interpret's own idempotent upsert
(`crates/interpret/src/write/migration.rs`): when Interpret derives the same
key again, in a repeated batch or a redo replay, with every stored column but
the stamps unchanged (position, kind, and evidence set, plus the registry,
manifest, and proposed effect on the discovery and candidate-effect tables),
`ON CONFLICT ... DO UPDATE` re-stamps `canonicality_state` from the re-derived
parent event, `consumer_visibility`, and the writing session's content hash.
Only three of the four tables can see that visibility change: completed-group
activation raises it on normalized events and the two association tables
(`crates/adapters/src/schema_v2/migration/activation.rs`), while both
candidate-effect tables stay `candidate` and a schema `CHECK` rejects any other
value, so their re-stamp never moves that column. A same-key row that differs in
any of those columns is refused: the Interpret write fails with a
data-integrity error and the batch rolls back. The re-stamp is a re-derivation
on the same block, never lineage maintenance. A retained losing-fork row
therefore reads `canonical` on an `orphaned` anchor: the column records what
was true when the row was last derived, not a current fact. Every event
identity these rows carry is fork-distinct: ordinary event identities embed
the block hash
(`crates/adapters/src/schema_v2/normalized.rs`), and a `MigrationApplied`
identity, `ens_v2_migration:{manifest}:{chain}:{correlation id}:MigrationApplied`,
is fork-distinct through its correlation id — a keccak over the serialized
evidence set, every entry of which carries the block hash, transaction hash,
and log index of the log it came from (`crates/adapters/src/schema_v2/migration/support.rs`,
`observation_evidence`, `event_evidence`, `correlation_id`). A losing-fork
association and the canonical event replayed after a reorg therefore never
share an `event_identity`. Each row also copies its position and stamp from the
very event whose identity it carries (`associate_event`), and the writer refuses
to move a row to a different position or evidence set, so a row's own anchor is
always the block of its named event. Two consequences follow. A lineage-checked
join to `normalized_events` on `event_identity` does establish the row's
readability whenever it finds a row, because a same-identity event can only
exist on the same block. What the join cannot do is find retained rows at all:
a redo deletes every normalized event in its range but keeps association rows
whose block is no longer readable, so a losing-fork row has no normalized-event
parent and still reads `canonical`. Any reader that reaches these rows without
that join, or treats one as current, must anchor the row's own
`(chain_id, block_number, block_hash)` on `chain_lineage` with a readable-state
predicate. The readers that treat these rows as current — the family child
reader (`crates/storage/src/families/topology/children.rs`) and the Interpret
admission loader (`crates/interpret/src/load/migration.rs`) — both anchor on
`chain_lineage`.

In today's one publishing reader the association-lineage predicate cannot be
the reason a row is withheld, so no test isolates it. The family child reader
reaches a correlation row only
through rows that sit at or after its block, and that ordering is bigname's own
invariant rather than a claim about ENSv2. A migration boundary's `evidence`
array is built from the raw-log observations the interpreter had already decoded
when it derived the boundary
(`crates/adapters/src/schema_v2/migration/support.rs`, `observation_evidence`),
and the child reader matches a correlation row only when the parent's retained
migration evidence (`project_name_state.migration_evidence`) contains the row's
`evidence_refs`. A matched
row therefore sits at or before the boundary that reads it, and the child
registration follows the boundary. A reorg that orphans the association's block
orphans every later block with it, so those rows fail their own lineage checks
in the same pass, and no reorg can orphan the row while leaving the rows that
read it readable. Both readers also require the `registry_announcement` edge,
joined on the association's own
`(block_number, block_hash)`, and an edge pinned to that block is orphaned by
the same Interpret redo. Deleting the lineage anchor from the child reader
therefore changes no outcome, both after the redo cascade and in the window
head publication opens before Interpret runs. Nor can a runner-driven run see
the checks disagree: head publication orphans the lineage and stamps the
required Interpret redo in one transaction, and Project cannot start until
Interpret has completed. After a real head publication and redo cascade the
retained row still reads `canonical` on an orphaned anchor, the edge is
orphaned, and no child is served from it. The anchor rule above still governs
any reader that reaches these rows without an edge join, which is where it is
the only guard.

The one place the identity-only attach is used is raw diagnostics, and it
never surfaces an orphaned row. `GET /v2/diagnostics/events` reads with the
canonical-only predicate (`apps/api/src/v2/diag_events.rs`,
`crates/storage/src/history/source.rs`): a normalized event is returned only
while its own `canonicality_state` and the `chain_lineage` row for its block
hash are both readable, so an event on a block that head publication has
orphaned leaves the route at once, before Interpret's redo deletes it. The
attach then adds every `migration_event_associations` row sharing the returned
event's `event_identity` with no lineage predicate of its own
(`crates/storage/src/history/paging.rs`). Because the identity is
fork-distinct, those rows sit on the returned event's readable block, and a
retained losing-fork row, whose event no longer exists, attaches to nothing.
What the response does not assert is current correlation: the only
per-association field it carries is `consumer_visibility`, which, like the
row's stored `canonicality_state`, is what Interpret recorded when it last
derived the row, and the route contract in `api-v1-routes.md` says so. The route behavior is pinned by
`diagnostics_hide_an_event_on_an_orphaned_lineage_with_its_still_canonical_association`
in `apps/api/src/tests/v2_history.rs`.
The position indexes on these
tables (`migration_event_associations_position_idx` and the two
`*_candidate_*_effects_position_idx` in `crates/storage/schema/baseline/05_normalized_events.sql`)
exist for `clear_redo_range` and range-scoped selection, both of which resolve
readability through `chain_lineage`; they are not an alternative to that anchor.

Slice 1 applies the same precedence to identity and discovery, with one explicit
intake carveout. A migration-created registry's independently admitted
`registry_announcement` edge remains an ordinary discovery row, active from the
announcement position, because it records indexability only and the watch plan
traverses it. Interpret attaches the `migration_registry_creation` relationship
in `migration_discovery_associations`, keyed to that ordinary edge;
the association does not change the edge's columns or active range. After an
activated parent transition, Project may use the readable canonical association
and active ordinary announcement to prove the current parent subregistry is the
migration-created `WrapperRegistry`; authority selection does not read it. Candidate or activated, the association establishes neither
result by itself and activates no correlation-dependent effect. Parent
reachability additionally requires the association's evidence-reference array
to be non-empty, every reference to be a non-empty object, and the whole array
to be contained in the activated boundary; an empty array, non-object reference,
or empty-object reference cannot authorize an ENSv1 child relation under a locked parent.
Correlation-dependent parent, topology, identity,
role, registration, renewal, and normalized-event rows from the watched registry
activate only when every group they reference is complete. Refused and incomplete
rows remain candidate. Association with the migration group is not
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
therefore byte-for-byte unchanged by candidate evidence. Candidate interpretation writes no
ENSv1→ENSv2 migration-driven predecessor close or successor open to
`surface_bindings`.

Consumer activation is a re-derivation semantic, not an in-place serving flag.
Slice 2A rotated the interpreter content hash for arm-scoped ordinary writes and
added an explicit transition value. The final activation slice now runs the
shared production/test activation function after all batch correlation paths finish;
there is no second test-only transition implementation. Its transition carries the exact
logical name, full chain position, expected `ens_v1` arm, predecessor selector,
expected `ens_v2` arm, and concrete successor binding/resource. The writer
locks the exact ENSv2 successor binding `FOR UPDATE`, resolves the predecessor
(the wrapper and child paths lock the one NameWrapper binding they close
`FOR UPDATE`; the registrar path reads the lease's evidence rows unlocked and
takes row locks only through the `UPDATE` that closes the open ENSv1
binding), and performs the cross-arm close and successor retain/open in that
same transaction. It never
ranks multiple predecessors and never applies the transition to descendants.
There is no runtime or manifest activation flag.

The `.eth` second-level selector is path-specific. The registrar-token
`unwrapped` and `unlocked_wrapped` paths record their exact BaseRegistrar
transfer to the Graveyard and select the lease: the one resource of the name
that carries an activated registrar lifecycle event (`RegistrationGranted`,
`RegistrationRenewed`, `ExpiryChanged`, `TokenControlTransferred`) with the
recorded token id, emitted by the recorded BaseRegistrar instance before that
cleanup, and whose registration was not released before the cleanup. Whether
the lease ever had an `ens_v1` binding is not consulted. The token is the
predecessor because the controller takes it from whoever holds it and reclaims
the registry record for itself before parking both in the Graveyard; the
registry-owner record never holds the token. A lease whose binding a
[registry-only handoff](glossary.md#registry-only-handoff) closed earlier
therefore still qualifies, and so does a lease granted with `registerOnly`
under such a binding, which never gets a binding of its own. Resources share a
token id only as successive leases of the same label, and a successor grant
requires the earlier lease to be past its grace period, which the adapters
settle as a `RegistrationReleased` no later than the grant's block, so the
release guard leaves exactly one live lease. Token evidence positioned at the
cleanup itself must also satisfy one of the two evidence conditions below.
Which of the four kinds carries the token id depends on the deployment profile:
the Sepolia profile indexes the BaseRegistrar's numeric `NameRegistered` and
`NameRenewed` as lifecycle events with the token id, while the Mainnet profile
declares `NameRegistered` for `RegistrationReleased` and `NameRenewed` for
`RegistrationRenewed` and `ExpiryChanged` (`manifests/mainnet/ethereum/ens/ens_v1_registrar_l1/v1.toml`);
neither declaration names `RegistrationGranted`, which is what the registrar
adapter requires before it interprets the numeric events as lifecycle events
(`crates/adapters/src/schema_v2/protocol/v1/registrar.rs`), and the
controller-derived `RegistrationGranted`/`RegistrationRenewed` after-state
carries no token id, so on Mainnet only `TokenControlTransferred` is lease
evidence, and which transfer that is depends on the path. A retained lease
never transferred before its direct unwrapped migration is handed to the
controller first, so the migration transaction's own holder-to-controller
transfer precedes the cleanup. On the unlocked-wrapped path `unwrapETH2LD`
moves the token from the NameWrapper straight to the Graveyard, so for a name
registered straight into the NameWrapper that cleanup transfer is the lease's
first and only token-bearing event. The writer admits token evidence positioned
at the cleanup in two cases: when a canonical ENSv1 binding for the same name
and resource is positioned at the cleanup log, or when that resource carries an
activated canonical registrar lifecycle event of an admitted kind for the same
name, on a canonical lineage block, positioned strictly before the cleanup.
The latter event need not carry a token id; on Mainnet it can be the controller
grant. An earlier event in the same transaction qualifies. Either way the token
id, the BaseRegistrar instance, the release guard and the exactly-one rule
still apply: the cleanup transfer alone cannot establish a predecessor.
Predecessor resolution therefore relies on those
transfers being activated with the name's `logical_name_id`, which the
ordinary registrar adapter gives them whenever the surface is known.
(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L118-L152 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L382-L395 @ ens_v1@91c966f)
(upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L128-L150 @ ens_v2@a971bd6) The writer closes whatever `ens_v1` binding of the
name is still open at the cleanup position, zero or one, and refuses an
`ens_v1` binding opened at the cleanup instant itself. Authority-boundary
events are not lease evidence: they carry the registrar observation but land
on the resource that gained or lost the name. The `locked_wrapped` path
selects the live NameWrapper resource immediately before the ENSv2 registration
boundary and closes it there.
(upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L92-L121 @ ens_v2@a971bd6)
(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L172-L175 @ ens_v1@91c966f) The unlocked wrapped controller unwraps before
injecting the ENSv2 registration, so ordinary ENSv1 interpretation has already
closed its wrapper binding and reactivated its registrar position before that
recorded transfer. If no prior registrar identity was materialized, that exact
transfer confirms the fallback identity with its binding effective from the
preceding `NameUnwrapped`; the cleanup-relative time predicate remains strict.
For an existing registrar-token `unwrapped` migration, adapters reconcile the
complete transaction before folding that block's retained ENSv1 state. The
proof requires the admitted BaseRegistrar holder-to-controller transfer,
registry reclaim to that controller, registry transfer to Graveyard, any
emitted resolver/TTL clears, the matching registrar transfer to Graveyard,
and exactly one complete ENSv2 successor for the same name and transaction.
The name may enter the transaction bound to its lease or, after a registrar
transfer without `reclaim`, to the registry-only resource the lease goes on
under; the registrar state is the lease either way.
The successor proof ends at its initial mint/resource-link/role-grant sequence;
subsequent same-transaction token transfers and role changes remain ordinary.
(upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/ETHRegistry.json:L2347 @ ens_v2@a971bd64)
Reconciliation retains raw facts and normalized ownership/cleanup observations,
but removes intervening ENSv1 authority bindings and the permission grants
those temporary authorities derive. Permission revocations stay on the resource
whose grant they close. On the lease every revocation is kept for audit. On the
registry-only resource a transfer without `reclaim` left the name bound to, a
revocation is kept when it closes a grant made before the transaction: the
controller's reclaim revokes the registry owner's handoff grants there, and
dropping those revocations would leave the grants as the latest permission rows
Project folds. A revocation that closes a grant the reconciliation itself
removed is removed with it; the removed grant must precede the revocation in the
transaction for the same subject and scope, since the registry owner the reclaim
revokes may already be the Graveyard, which the transaction's own registry
transfer grants again one log later.
(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L63-L68 @ ens_v1@91c966f)
Registry metadata observations remain attached
to the existing registrar resource without fields that would restore temporary
registry-only authority. Thus the actual registrar lease stays the predecessor
at cleanup, and no replacement ENSv1 binding survives the strict cross-arm
transition. Missing,
ambiguous, or mismatched proof leaves ordinary interpretation unchanged; zero
or multiple eligible predecessors remain integrity errors in the writer.
(upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L111-L119 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L146-L148 @ ens_v2@a971bd64)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L382-L395 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1022-L1031 @ ens_v1@91c966f)

For the `.eth` second-level names covered by slice 2A, zero matching ENSv1
predecessors and multiple matching ENSv1 predecessors are both integrity
errors. The unlocked ERC-721 entry accepts transfers only from BaseRegistrar,
whose `ownerOf` rejects a token after its expiry, and both wrapper entry points
accept transfers only from NameWrapper. NameWrapper treats a `.eth` second-level
name as expired for transfer at the start of registrar grace.
(upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L92-L103 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/src/migration/AbstractWrapperReceiver.sol:L48-L55 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/src/migration/AbstractWrapperReceiver.sol:L101-L124 @ ens_v2@a971bd64)
(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L35-L50 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L71-L76 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L815-L835 @ ens_v1@91c966f)
Therefore a completed supported second-level migration with no active ENSv1
predecessor means an ENSv1-from-genesis interpretation is corrupt; it is not a
valid chain state to tolerate. This rule is deliberately limited to `.eth`
second-level transitions. An emancipated child is gated by wrapper expiry
rather than registrar expiry and can migrate while its parent sits in registrar
grace, so slice 3A must state and prove its own predecessor rule instead of
inheriting this one.
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L820-L823 @ ens_v1@91c966f)

The separately reviewed and separately merged slice-1, slice-2A, slice-2B, and
slice-2C implementation PRs deploy together at one planned [re-derivation
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

Product history cursors hold a position in the history order rather than a
normalized-event row ID, so the slice-1 test re-walk leaves them to the
[history walk](glossary.md#history-walk) rule in
[api-v1.md](api-v1.md#cursors-and-pagination). A diagnostic-events
cursor issued before the re-walk at a fixed readable chain head must remain
valid and continue from the same stable normalized-event anchor, although its remaining rows and fields
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

**Load the dump into `bigname_phase`, not `public`.** The upstream generator
emits SQL that clears `search_path` and then creates and fills
`public.ens_names`
(upstream: .refs/ens_rainbow/src/main.rs:L26 @ ens_rainbow@bc44492)
(upstream: .refs/ens_rainbow/src/main.rs:L36 @ ens_rainbow@bc44492)
(upstream: .refs/ens_rainbow/src/main.rs:L46 @ ens_rainbow@bc44492).
bigname declares its own `ens_names` inside the phase schema
(`crates/storage/schema/baseline/07_labels.sql`), and the runner connects with
`search_path = bigname_phase` and no `public` fallback, so the import reads
`bigname_phase.ens_names` only. Applying the upstream dump unmodified therefore
fills a table the importer never reads, and the run reports zero scanned rows
rather than failing — the phase table exists, it is just empty. Load the dump's
rows into `bigname_phase.ens_names` without letting the rest of the dump run
against that table: `\copy` the data section, or extract the dump's `COPY`
statement and retarget only that. Do not rewrite the dump's schema
qualification globally, because the same rewrite also redirects the dump's
`DROP TABLE`
(upstream: .refs/ens_rainbow/src/main.rs:L33 @ ens_rainbow@bc44492)
and `CREATE TABLE`
(upstream: .refs/ens_rainbow/src/main.rs:L36 @ ens_rainbow@bc44492)
at the phase table. That drops it along with any rows already imported and
rebuilds it from upstream's two-column `character varying` definition, losing
the baseline's `text` types, both `CHECK` constraints, its comments, and the
`bigname_verify` SELECT grant, which is not restored by default privileges.
Then run the import and check the logged scanned-row counter against the dump's
row count.

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
  `phase-runner redo --chain ethereum-mainnet --source 'ethereum-mainnet:<key>:<kind>:<seed-basis>:<start>[:<role>]=<endpoint-env>' --phase project --from-block <first retained block> --to-block <head>`.
  Repeat `--source` with the complete intake-capable descriptor set recorded by
  that chain's Ingest cursors.
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

**At most one readable block per height, enforced by the schema.** A partial
unique index makes a second readable row at the same height impossible to
insert:

```sql
CREATE UNIQUE INDEX chain_lineage_readable_height_idx
    ON chain_lineage (chain_id, block_number)
    WHERE canonicality_state IN ('canonical', 'safe', 'finalized');
```

`chain_lineage` holds every competing branch it has observed, but only one row
per `(chain_id, block_number)` may be readable at a time, so "the block at
height N" is a total function on the readable set rather than a choice among
candidates. Head publication must therefore orphan a displaced branch in the
same transaction that promotes its replacement — that ordering is not a
convention, it is what keeps the index satisfiable.

Two consequences for readers. A height lookup on readable rows needs no
tie-break, ordering, or `LIMIT 1` to be deterministic; adding one hides a
constraint violation rather than resolving an ambiguity. And a presence check
that treats an ambiguous readable height as a fatal error — see the interpret
range checks below — is asserting an invariant the database already guarantees,
not handling a reachable state.

Head publication walks by block hash, marks the orphaned suffix explicitly,
publishes the replacement readable head, and records downstream redo in one
transaction. The same transaction clears active resolution-divergence
observations whose recorded positions include an orphaned block. There is no
legacy execution-cache invalidation call.

Project output is admitted only when its publication target is at or before the
selected readable head. Equal-height admission requires the selected block hash
to match. Name, relation, inventory, and primary-name reads all apply
this rule against phase lineage.

When a chain is removed from runtime configuration, recovery may change its
active phase row to the non-paging `completed` state without claiming that the
phase finished its work. If the chain is configured again, a completed Ingest
row resumes unless its current block and live handoff match its target. A
completed Verify row resumes unless it has a matching current/target block pair
and a [verification level](glossary.md#verification-level). Ingest already
persists its summary and every configured source cursor in one transaction.
That atomic set contains only
[intake-capable sources](glossary.md#source-role), so those completion markers
cannot survive without the matching source progress. Recovery also clears the
live handoff when it changes an active Ingest row to `completed`; this makes a
later re-add resume from the preserved source cursors even if an older runner
stopped between its formerly separate summary and cursor writes.

When a bounded Ingest redo loads a source boundary in its completing batch, the
source progress adopts the boundary marker returned by that load. The marker
must match the source target resolved before the load; a different hash at the
same boundary height fails the redo instead of substituting the pre-load target
for the loaded marker. The completing phase summary comes from the source whose
target is the redo range end after its boundary marker passes these checks. If
multiple sources meet at that height, all of their checked markers must agree.
In a multi-source redo, the final batch reloads an in-range source boundary below
the overall redo end when durable phase progress has passed it, so its completion
evidence also comes from the current
[watch plan](glossary.md#watch-plan--watched-tuple). An equal-height
durable phase marker is not intercepted by that reload. Each non-completing
batch also stores a map from source key to the boundary marker returned by an
actual source load during the active redo. At any source boundary where the
durable phase marker is exactly that height, including the overall redo range
end, a later batch resuming exactly there accepts only the stored load-derived
marker, which must
exist at that height and equal the fresh source target. A missing marker, such
as an active checkpoint written before this map existed, or a different hash
fails closed and requires a fresh redo of the full range. When this per-source
evidence proves that the boundary was inside the completed redo range, redo
completion updates the source cursor only when matching block lineage already
records that height and hash. The map is cleared with the other resumable redo
progress on completion or boundary divergence. The cursor update and phase
summary share one transaction. The previous live handoff remains in place until
the next normal Ingest pass confirms the reconciled cursor and publishes the
replacement handoff.

`chain_phase_state.redo_manifest_authority_fingerprint` binds the numeric
Ingest redo checkpoint and its per-source marker map to the chain's active
manifest payloads, excluding `normalizer_version`. Those payloads include the
roots, contracts, addresses, and watched block ranges contributed directly by
the manifests. Watch-relevant discovery-edge admissions are not fingerprinted.
An exact-range resume preserves evidence only when the stored fingerprint
matches the fingerprint of the current active payloads. Today no production
Interpret path can write a watch-relevant discovery edge while an interrupted
Ingest redo retains its checkpoint: phase-start compatibility checks gate those
writers. Manifest synchronization may run after the interrupted session's locks
are gone, but any widening stamp clears the redo cursor, fingerprint, and
per-source boundary markers before another attempt can resume. A missing or
different fingerprint likewise clears the resumable evidence and reports that
the active manifest/watch-plan inputs changed; rerunning the redo then loads the
full range under those inputs. Existing active redo rows receive no backfill,
so their first post-upgrade resume fails closed and requires that full-range
reload.

During one active Ingest redo attempt, the runner prepares persisted manifest and
discovery [watch intervals](glossary.md#watch-plan--watched-tuple) once, then clips
them to each fetch window. The existing phase exclusion and manifest-sync locks
keep those inputs stable. This process-local plan is keyed by chain, attempt
generation, and redo range; a new or resumed attempt reloads it. A completed
outcome or error from redo batch execution discards the matching entry.
Interruption or failure elsewhere in the runner may retain an entry until it is
replaced or the engine is dropped. A subsequent attempt cannot reuse that entry
because its generation changes. Canonical creation announcements are still read
for each window, and same-window announcements still expand the fetch before it commits.
Normal Ingest, Live, and uncoordinated library calls retain their per-window
planning. Redo batch size, provider reads, and fork checks are unchanged.

`chain_phase_state.redo_attempt_generation` has this contract: This nonnegative, row-local counter increments when an explicit redo begins, when the phase runner installs or extends a required redo stamp for a downstream phase (Interpret/Project), and when the shared required-Ingest installer records genuinely new manifest or discovery demand. New same-range demand advances the generation because an older attempt may already have passed those blocks under a narrower filter. Repeated observation of an unchanged discovery-watch admission never calls the installer and therefore does not advance the generation.
A batch carries that generation together with the persisted redo mode and the actual execution
range chosen at begin time. Its pool-backed progress update, including the
per-source boundary-marker map, succeeds only while all three values still
match the active row. No match means another attempt has superseded the batch;
the update records nothing and returns `redo attempt superseded; progress not
recorded`. Completion, failure recording, and downstream redo finalization use
the connection that owns the phase advisory lock, so losing that connection
also prevents their writes. This generation fence closes the redo-progress
instance of [#452](https://github.com/ensdomains/bigname/issues/452); that issue
continues to track whether every pool-backed phase write should move to its
lock-owning connection.

Retained lineage alone does not authorize that reconciliation when an
interrupted redo has already advanced past its last boundary. If the provider
then reports a different hash at the same boundary height and that older fork
also has retained lineage, the resumed redo fails instead of treating the fresh
hash as newly loaded. The failure keeps the redo marked in progress, clears
only its resumable progress, and leaves the source cursor unchanged. Re-running
the redo therefore starts at the requested range beginning and loads the
boundary under the current [watch plan](glossary.md#watch-plan--watched-tuple)
before cursor reconciliation can proceed. If that fresh hash has no retained
lineage, the equal-height evidence requirement still applies: the phase summary
cannot adopt the fresh resolution without a matching per-source marker returned
by a load during that redo.
Together with the per-chain manifest/watch-plan fingerprint, this closes the
last-boundary case when active inputs change between attempts. At manifest
synchronization, a semantic comparison of previous and desired watched event,
emitter scope, and start block now covers interior retained heights: any
widening that intersects stored Ingest coverage stamps a required Ingest redo
through the latest published ingested head. That redo reloads the whole
affected range under one current manifest/watch-plan fingerprint. Every redo
checkpoint uses the boundary returned by the load itself. Loaded headers must
form one parent-linked window, and each resumed window must descend from the
durable prior-batch checkpoint; a fork switch restarts the redo from its range
beginning instead of combining coverage from sibling forks. Completion of a
manifest-required redo also requires its loaded range-end hash to equal the
readable hash at that height. Ordinary repair redos retain their existing
ability to reconcile a source cursor to another retained fork before normal
head publication.

The widening comparison first proves that persisted address epochs continuously
honor the desired direct-address promise. It refuses an existing gap before the
ordinary widening path can stamp a required Ingest redo; redo state is created
only for a promise that the persisted interval union can represent. If a
required Ingest redo is pending anywhere on the chain, manifest synchronization
deliberately and conservatively refuses to remove a previous all-emitter watch
when the desired address-scoped replacement would expose a persisted epoch gap.
The redo need not belong to that watch; let it complete and retry, or split a combined
[registry-announcement widening](glossary.md#discovery-rule-widening-and-narrowing) and
all-emitter removal so its redo completes first. The transaction leaves the
previous watch plan and redo state unchanged.

### Resolver record IDs

For the record-ID resolver generation described in [architecture](architecture.md),
Interpret retains link and value changes as immutable normalized events. Project
alone joins canonical links to record values and publishes per-name inventory.
Interpret never stores a current record-value map for later event fan-out.
`ResolverPermissionArgument` retains the permission resource's raw argument;
Interpret may restore this bounded selector metadata to interpret subsequent
role changes. It is not a name binding or a record value. The fresh schema admits
`ResolverRecordLinked` and `ResolverPermissionArgument` in the normalized-event
kind constraint; both retain the existing `ens_v2_resolver` derivation kind.
Initialized databases require the three `20260909120000`–`20260909120200`
schema-migrations before enabling this resolver generation. They add a wider
constraint without validating old rows, validate it in a separate transaction,
and then replace the old constraint. No rows or identity keys change.
The old constraint continues to reject the new kinds until the final swap.
A failed stage leaves the preceding constraint effective; fix the failure and
resume the ordered schema-migrations. After new facts exist, retain the wider
constraint when rolling back application code so those immutable facts remain
valid. An application rollback must also stop interpreting this generation.

## Interpretation replay

Normalized-event writes preserve immutable event identities and payloads,
compatible replay, input occurrence order, and successful first-insertion order.
Database constraints remain authoritative for invalid references and values.
An error rejects the entire physical Interpret write transaction, including
identity and discovery writes made earlier in that transaction; it does not
undo previously committed batches. Runner progress is recorded after success,
separately from that write transaction.

Rejected input does not promise exact sequence consumption or subsequent
numeric IDs, that no later submitted row was attempted, or which error wins
when several faults coexist. SQL failures identify the attempted INSERT slice
with original submitted indexes and at most 500 identities; immutable conflicts
identify rejected identities. The SQLSTATE-based classifier and explicit
classification of immutable identity conflicts are unchanged. The former
preflight message and whole-call summary are not part of this contract.
This writer is covered by the [interpreter content hash](glossary.md#interpreter-content-hash);
changing it requires the full-history adoption described below, including
restarting an interrupted attested range when its hash changes.

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
already requires the complete configured source-key set and each source's
normalized kind, seed basis, and start block to match the persisted cursor
identities. That same check applies to
the configured intake-capable source set. A runtime
start above the redo range does not bypass that identity check. The guard also
requires one readable `chain_lineage` row at every height in the full execution
range. The schema-v2 baseline's partial unique index on
`(chain_id, block_number)` for `canonical`, `safe`, and `finalized` rows makes
two readable hashes at one height
structurally impossible in the supported schema; the redo check still fails if
the row is missing or if database integrity has been compromised. Cursors and
lineage both prove only the facts selected by the [watch
plan](glossary.md#watch-plan--watched-tuple) active when each block was loaded;
neither proves facts added by a later watch plan. Manifest synchronization
records a [manifest-authority marker](glossary.md#manifest-authority-marker)
when that authority changes, a persisted admission-floor repair invalidates
derived results, or stored manifest event history is repaired. Every Interpret
redo that would discharge the marker fails closed unless the operator passes
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

Manifest synchronization distinguishes manifest-authored watch-plan widening
from narrowing and unrelated authority changes. A widening over retained
coverage stamps the required Ingest redo; successful completion supplies the
current-watch-plan fetch before Interpret can run. The attestation remains the
operator's durable acknowledgement of every manifest-authority change,
persisted admission-floor repair, or stored manifest event-history repair,
including invalidations that stamp no Ingest work. An interpreter content hash
rotation with neither a current manifest-authority marker nor an active audited
redo remains flagless.
A missing lineage height, more than one readable row after loss of the schema
constraint, or an uncovered part of a source's finite target remains a fatal
presence failure.

The interpret engine loads the prior identity state required by the range,
folds physical batches without changing semantic order, and revalidates the
resume marker and current block anchors in the write transaction. A concurrent
reorg therefore cannot publish interpretation derived from an unreadable
branch.

An ENSv1 surface-materializing renewal may emit an additive
[state-derived normalized event](glossary.md#state-derived-normalized-event).
Its `source_manifest_id` comes from the retained registry authority or registry
state used for serving, while its block, transaction, log, canonicality, and
`raw_fact_ref` come from the renewal that materializes the surface. It retains
the existing `ens_v1_unwrapped_authority` derivation kind and is distinguished
by `after_state.state_derived=true`. The earlier [pre-surface](glossary.md#pre-surface)
`ResolverChanged` keeps its null `logical_name_id` and remains immutable. Its
`resource_id` may already identify a known control authority before the surface
is learned; it remains null when no authority or registry read resource was known.
This behavior requires no
`normalized_events` check change or schema-migration.
Surface-materialization and per-log authority-transition resolver copies carry
`after_state.resolver_source_role`, preserving their old- or current-registry origin
so compacted restoration survives a later global resolver selection.

The additive named `RegistrationGranted` is a [state-derived normalized event](glossary.md#state-derived-normalized-event), marked `state_derived`, `surface_materialization`, and `registrar_surface_snapshot`. It reports the retained lease's original registration timestamp and current expiry, owner, resolver, and ownership permissions. Its raw position is the readable trigger; a bounded provenance object retains the original numeric grant and latest registrar-owner, registry-owner, and resolver evidence. Subsequent retained state carries these references without accumulating history. Restoration handles the marked snapshot separately from an on-chain registration. Project uses the verified original timestamp only for this marked case; compact product history omits rows with both markers `state_derived=true` and `registrar_surface_snapshot=true` before pagination, while diagnostics retains them. Missing, null, or false markers do not exclude any row; other state-derived events and the original resource-only grant keep their existing history behavior.

The marked snapshot (the [registrar surface snapshot](glossary.md#registrar-surface-snapshot)) and Project's join by resource identity coexist, and each covers reveals the
other does not. The snapshot gives an immediate adapter-side binding when a source that is
neither a registrar controller nor the NameWrapper discloses the label; the ENSv1→ENSv2 migration
on Sepolia depends on that binding existing before the migration boundary. It is deliberately not
emitted where a registrar controller event or a wrap names the lease, which is nearly every mainnet
name, because copying a grant, an expiry and permission rows per registration would duplicate
facts the original rows already hold. There Project attaches the original resource-keyed rows to
the name (see [projections](projections.md#exact-name-projection)). When both exist for one name,
they describe one registration: the original grant and the snapshot share a `resource_id`, and
`registered_at` is the original grant's block time either way.

For this disclosure rule, launch-bounded transfers to the manifest-declared Graveyard and admitted cleanup observations carry `registrar_surface_retired` in their existing event payload. The bounded retained evidence records that retirement separately from the ENSv1 current-registry fallback marker. It prevents a later preimage from reopening that lease and does not replace migration correlation or relax exact cleanup evidence. A subsequent independently proven new numeric grant has a new lease identity.

Only active manifests participate in raw-log selection and watch authority.
Interpret separately retains metadata for stored deprecated manifest versions
so a state-derived event can preserve the manifest identifier and source family
of the state it surfaces. A retained manifest identifier absent from all stored
versions is a data-integrity error in both live interpretation and restoration;
it is never attributed to the currently active triggering source.

A current-registry `NewOwner` or `Transfer` that ends old-registry fallback
resolution persists that handoff at the ownership log's raw position. When an
old-registry pointer was already linked, the current registry source emits
additive linked `ResolverChanged` rows with the zero address for every retained
registry, registrar, or wrapper resource that could carry the old pointer, and
`after_state.registry_fallback_handoff=true`; the earlier selection and surface
materialization rows remain immutable. Retained linkage includes resources that
inherited the pointer during an earlier authority epoch and are no longer the
current registry, registrar, or wrapper resource. A same-owner `Transfer` still
leaves a normalized handoff row when it would otherwise produce no state delta,
so compacted restoration cannot reopen old-registry input. Same-transaction
registration reconciliation leaves each resource-specific handoff row attached
to its original resource.
An old-registry zero selection clears active copies but retains an inactive resource carrying the prior pointer. Whenever that resource becomes active again through a registry, registrar, or wrapper authority transition, reactivation emits a zero `ResolverChanged` before handoff.
A current-registry resolver selection discards the retained old-registry resource set, so a later ownership event cannot clear the current-registry pointer. A current-registry zero selection retains one per-name marker—not a per-resource fan-out set—so a known registrar reactivated after the clear cannot expose its earlier pointer.
(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L60-L68 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L75-L82 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L18-L24 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L48-L54 @ ens_v1@91c966f)

A current-registry `NewResolver` cannot precede that node's current-record creation:
`setResolver` authorizes against the owner stored in the current registry, while an
absent record has the zero owner and no caller able to authorize the write. A parent
owner can create a current record with a getter-visible zero owner and a resolver in
one `setSubnodeRecord`; its `NewOwner` precedes its `NewResolver`. The fallback getter
serves the old registry only until that current record exists, including when the
current registry stores itself for a requested zero owner.
(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L16-L20 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L49-L57 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L75-L82 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L86-L95 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L123-L131 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L153-L156 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L174-L182 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L18-L34 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L48-L55 @ ens_v1@91c966f)

### Resolver creation replay

`ResolverCreated` is stored as `ContractDiscovered` and an Interpret-owned
`resolver` self-edge anchored to the raw creation log. This is the only permitted
resolver self-edge. ENSv2 registry-pointer edges remain binding history and are
excluded from emitter admission. Canonical raw creation logs drive Ingest's
same-window capture and its subsequent windows; orphaned creation logs cannot
expand a watch filter. Installing
[creation capture](glossary.md#resolver-creation-capture) requires the normal
manifest-driven Ingest redo and full Interpret replay, preserving raw facts.

The rule is one validated CHECK on `discovery_edges` named
`discovery_edges_self_edge_check`. The baseline creates it on a fresh install.
Schema-migration `20260917140000_resolver_creation_self_edge.sql` replaces the
older rule on an existing database; it is already applied on a live database,
so its content is fixed. Schema-migration
`20260917141000_discovery_self_edge_check_name.sql` then settles the name: it
renames a rule that has the right text under a generated name, and replaces the
rule only when its text differs. Both files find the existing rule by searching
the text `pg_get_constraintdef` prints; `20260917141000` turns
`quote_all_identifiers` off while it reads that text and restores the caller's
value, while the fixed `20260917140000` needs the migration session to run with
the setting at its default, `off`, as the
[production runbook](runbooks/production-docker.md#planned-migration-and-fingerprint-boundary)
states.

### Interpret process memory

`normalized_events` is the working store for each [interpreter state
key](glossary.md#interpreter-state-key)'s `after_state`. The retained
[interpreter session](glossary.md#interpreter-session) may keep only a bounded
cache of those values. Cache capacity is an operator setting measured in entries;
changing it must not change normalized events, identity rows, discovery edges,
or the latest persisted state per key. A smaller capacity may cause more
database reads, but it has no interpretation meaning and is not part of the
[interpreter content hash](glossary.md#interpreter-content-hash).

ENSv1 [registry fallback handoff](glossary.md#registry-fallback-handoff)
tracking is populated only by pointers selected through the old registry. Its
per-name fan-out contains at most one entry for each distinct registry, registrar, or wrapper resource given an old-registry pointer since the preceding handoff. Repeated authority epochs and wrap/unwrap cycles can therefore grow this set without a fixed per-name ceiling until handoff.
Replacing an old-registry pointer on an exact resource overwrites that resource's
fan-out entry with the new address, while clearing it removes the resource. A
current-registry selection discards the old-source set, and the current-registry
handoff drains any remaining name entry. The single selected-link slot may retain a
current-registry zero marker until a later selection replaces it; this does not add
entries to the per-resource fan-out map.

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
After each block's same-transaction reconciliation, ENSv1 protocol state
advances through only that block's surviving normalized events before the next
block's time-derived ENSv1 lifecycle checks run. Before-state chaining still
starts from the cached or reloaded pre-batch value and advances through the
exact surviving normalized-event sequence. Only those survivors update the
retained cache after the database transaction persists the batch. This keeps
dropped or retargeted provisional events out of later ENSv1 block-boundary
decisions, retained memory, and future restore input.

Ordinarily, a cold restore streams the latest readable event per [interpreter
state key](glossary.md#interpreter-state-key) in chain order. A zero-address
ENSv2 `SubregistryUpdated` carrying the
[`subregistry_invalidated_token_ids` marker](architecture.md#normalized-event-taxonomy)
is retained separately from the ordinary latest event for the same key. This
preserves one logical before/after stream and the clear needed to invalidate
older token-version pointers. Restore rebuilds the adapter's protocol state
while admitting at most
the configured number of `after_state` values to the cache; it does not first
materialize every retained JSON value in one process allocation. The restore query
ranks and orders event identifiers before retrieving their payloads in the same
read snapshot. Plan validation must keep full payloads out of history-wide sorts
and materialization. This reduces the data carried by those operations; it does
not bound temporary storage, individual row size, or the protocol state retained
by the adapter. If the chain
[lineage orphaning epoch](glossary.md#lineage-orphaning-epoch) changes, the
process discards the whole interpreter session and rebuilds it from readable
rows. It retains only the block anchors added since the last validation while
the epoch is unchanged, rather than one dependency entry per historical state
key. A redo's first batch always rebuilds the session, because adapter state
only moves forward; a completed Interpret redo ends at the block the normal
phase resumes from, so its session carries into the next normal batch, under
the same epoch check. Interpret also supplies the timestamp of the resume
position's readable predecessor block from `chain_lineage`; there is no predecessor at block zero
or before the first retained lineage block. After replaying retained events,
the adapter advances time-derived protocol state to that timestamp. Exact
cold-restore reconstruction therefore depends on the predecessor remaining
readable in the same input snapshot.

On a chain whose manifests all belong to ENSv1, ENSv2 or Basenames Base source
families (or to the families that interpret no logs), Interpret instead
restores state for each batch with the [lookahead loader](glossary.md#lookahead-loader).
Interpret's adapter handles the Basenames Base families with its ENSv1 protocol
code, so the loader applies the same name model and dependency rules to them. Before
interpreting, the adapter decodes the batch's logs without interpreting them
and lists every name (by namehash) and resource the logs can touch.
Interpret adds the names whose registrar expiry plus the 90-day grace period
falls inside the batch's time span (the Basenames Base registrar has the same
grace period (upstream: .refs/basenames/src/util/Constants.sol:L15 @ basenames@1809bbc)
(upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L296 @ basenames@1809bbc)), because time-derived releases touch names no
log mentions. A registration is released at the first block whose timestamp is
strictly greater than its expiry plus the grace period (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L17 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L100-L103 @ ens_v1@91c966f), so the span runs from
the timestamp of the block before the batch, inclusive, to the timestamp of the
batch's last block, exclusive. One case lies below that span: a registrar event
in the block just before the batch that recorded an expiry already lapsed at its
own block. The adapter releases such a name at the next block boundary, which is
the batch's first block, so Interpret adds those names too; every earlier block
boundary has already settled. Interpret then reads, in the batch's input
snapshot, the latest readable event per interpreter state key among the events
of those names and resources, adds the names and resources those events
reference, and repeats until a round adds nothing. Each round reads only the
names, resources and ENSv2 state keys the previous round added, and a retried
attempt (below) only the names and keys it adds and what those link to, so
Interpret reads each name, resource and key once per batch: in one snapshot the
latest event of a key does not depend on which names, resources or keys asked
for it. To find the state keys of the
requested names and resources it reads every stored event of theirs marked
canonical, safe or finalized, whether or not the event's block is still on the
canonical lineage, and the restore then takes, for each key, the latest event on
that lineage. This returns the same events as checking every scanned event,
because every event of one interpreter state key is filed under the same name or
resource: the key embeds the event's logical name and resource, and its state
scope the node the event is filed under
(`crates/adapters/src/schema_v2/state_key.rs`). An event on an orphaned block can
therefore only name a key whose latest readable event, if there is one, is also
among the events read. Separately, every Interpret write rechecks that no block
was orphaned between the batch's input snapshot and the write
(`crates/interpret/src/write.rs`). The read by
[ENSv2 state key](glossary.md#ensv2-state-key) below still checks the lineage of
every event it scans. A registry may emit `LabelRegistered` for one token id under
a second label: the adapter keys the token's registration state by registry and
token and checks the supplied label only against its own hash, never against the
token (`crates/adapters/src/schema_v2/protocol/v2_registry.rs`,
`crates/adapters/src/schema_v2/state_v2.rs`), and the event's state scope carries
the token, not the label hash (`crates/adapters/src/schema_v2/protocol.rs`),
while its ENSv2 state keys include one derived from the label hash
(`crates/interpret/src/load/lookahead/v2_keys.sql`). So one interpreter state key
can be reached through several ENSv2 state keys. There is no round limit: each
continuing round adds a name, resource or ENSv2 state key from a finite set (the
names, resources and keys the batch's logs and the chain's stored history
reference or derive, and the registry-only resource of each of those names), so the
repetition ends. On a chain with an ENSv2 manifest the rounds also read the
readable ENSv2 events filed under each name and
[ENSv2 state key](glossary.md#ensv2-state-key) in the set, starting from the
keys the batch's logs name and the keys of registry tokens whose expiry falls
inside the batch's time span; the keys, names and resources those events
reference join the set. Interpret restores a fresh adapter state from exactly those events under the
same canonical-lineage and pre-batch boundary rules as a cold restore, and
interprets the batch against it in the same input snapshot. ENSv2
interpretation derives names from registry state, such as the ENSv1 predecessor
an [ENSv1→ENSv2 migration](glossary.md#ensv1ensv2-migration) retires, so the collector cannot list them all in advance: when
restore or interpretation reads a name or ENSv2 state key that was not loaded,
Interpret discards that attempt, adds it, repeats the rounds above and
interprets again. Every attempt that continues adds a name or key not loaded
before, and those a batch can read are derived from its logs and the snapshot's
finite stored history, so the attempts end. A read under another spelling than
the loaded one fails the batch only when that spelling is loaded too, so the
adapter's state keys must match the keys its events are filed under. Only an attempt that read nothing unloaded is
published. The
session is discarded after the batch.

A batch marks an ENSv2 registry for a name refresh when its parent claim
changes, when a subregistry pointer to it is set or cleared, or when the token
pointing at it is released, replaced, regenerated, or has its expiry changed or
crossed. Interpret then walks the registry's
[name suffix](glossary.md#ensv2-name-suffix-walk): its parent claim, the
parent token's subregistry pointer and expiry, and so on up to a manifest-declared
registry, whose suffix is fixed. It compares the result with the same walk over the
registry-level state as it stood at the previous refresh, which the session keeps
alongside its other state. That copy shares its unchanged parts with the current
state, so it costs memory only for what events have changed since; Interpret drops
it once the walks are compared, before refreshing names, so the refresh itself copies
nothing for it, and keeps a new one afterwards. When the two walks agree, no token changes name
because of the registry's suffix, so only the tokens the batch touches for their
own reasons (a registration, renewal, expiry, release or replacement) are
refreshed, and the lookahead loader reads only those and the rows the walk
reads. An ENSv1 event that makes its resource current for a name an ENSv2 token
also holds reads and refreshes that name's ENSv2 tokens as well. Of the ENSv2
tokens holding the name with a registration and a resource, the one whose
registry address and token id sort last stays current, immediately, as a full
refresh does; the ENSv1 resource is current only when no such token exists, and
becomes current again when the last one leaves the name. That holds for the session
a batch commits, which is rebuilt by replaying the batch's events; while the batch is
being interpreted, a token that a discovered registry registers again under another
label can leave its old name pointing at its resource until that replay. This is the adapter's
internal choice of which resource an ENSv2 resolver record attaches to, not the
authority the API serves. For a name moved from ENSv1 to ENSv2, the leftover
ENSv1 registration's resource can be attached again after the ENSv2 registration
ends, as a restore of the same history also does; whether it should stay detached
is open as TYR-206. When the two walks differ, both
loaders refresh every retained token in the registry at the triggering event:
each one whose registration is live takes the new name, or loses its name when
the suffix is gone, and a token that had already expired stays unnamed. The same
holds for every registry below it, whose suffixes move
too. The lookahead loader then reads the whole
history of each of those registries for that batch: the latest event per
interpreter state key filed under each registry's `<registry>:*` key. The batch
holds all of those events in memory at once, beside the rest of its input, so its
size is set by the moved registry and every registry below it. Once the input is
read, the runner logs one warning per registry with its token count, event
count and the bytes of the events' serialized state; it marks a completed read,
not a written batch, so a batch that fails later and is retried logs it again.
The first batch with ENSv2 events on a chain has no earlier refresh to compare
with, so it counts every dirty registry as moved and reads it whole, even a
manifest-declared one; its registries are new, so those reads return few or no
events. Apart from that batch, a manifest-declared registry, such as Sepolia's
`.eth` registry, is never read whole, because its walk ends at itself; only a
registry discovered through a subregistry pointer can move. No mainnet manifest
declares an ENSv2 registry yet; once one declares mainnet's `.eth` registry,
the same holds for it. Memory sizes here are estimates, not bounds: on a Sepolia
staging copy, one discovered registry's 37,832 retained events averaged about
1.4 KB of serialized state, and resident memory is higher than that. Assuming
about three retained events per token (registration, resource link, resolver),
a move of a registry tree holding 100,000 tokens needs at least 0.4 GB in that
batch, and one the size of a million-name `.eth` at least 4 GB.

Two partial expression indexes on
`normalized_events` serve these reads for the ENSv1 families:
`normalized_events_v1_direct_node_probe_idx` (events of one name) and
`normalized_events_v1_due_probe_idx` (registrar expiry ranges); two more with the
same expressions, `normalized_events_basenames_direct_node_probe_idx` and
`normalized_events_basenames_due_probe_idx`, serve them for the Basenames Base
families. For ENSv2, `normalized_events_v2_direct_node_probe_idx` has the
same name expression, `normalized_events_v2_key_probe_idx` is an inverted
(GIN) index over the [ENSv2 state keys](glossary.md#ensv2-state-key) each event
is filed under, `normalized_events_v2_due_probe_idx` finds registry tokens whose
expiry falls in a batch, and `normalized_events_v2_lookahead_probe_idx`, keyed
by chain and block, finds the latest ENSv2 registry event before it. An ENSv2
read of an unloaded state key is retried like an unloaded name. The loader is an access path, not a semantic: it must produce the same
normalized events, identity rows and discovery edges as the full-state loader,
and it is bound by the same interpreter content hash. The loader choice
therefore looks past the manifests the batch interprets: the full-state loader
restores every retained row regardless of family and lookahead reads only the
ENSv1, ENSv2 and Basenames Base families, so `normalized_events` history of a family lookahead does not cover,
written while that family's manifest was `active` and still retained after the
manifest moved to `draft` or `shadow`, would be restored by one loader and not
the other. Before choosing lookahead, Interpret lists the chain's manifests in
those two states and, for each uncovered family among them, asks whether a
readable event of that family is retained before the batch; one such event
chooses the full-state loader. The probe is bounded by the chain's manifests
because every event is written under one of them and manifest rows are only
ever moved between rollout states, never deleted. No index leads with
`source_family`, so each probed family costs one scan of the chain's retained
events, stopping at the first match; a chain with no uncovered manifest in those
states runs no probe.
It never publishes an attempt that read a name that was not loaded.

For ENSv2, a retained registry/root `PreimageObserved` event for a canonical
[name surface](glossary.md#surface-name-surface)
permanently establishes that the surface is known in restored protocol state.
A registration release or expiry can remove the current
binding and resource without removing that observation. Normalization-rejected
name observations are not admitted to this state. Later `RecordChanged` and
`RecordVersionChanged` resolver events
therefore retain the logical-name attribution but carry no `resource_id` when
no current resource exists, identically in a continuous walk and after a cold
restore. Project's record inventory attached to a resource
follows the resource's latest retained linked `ResolverChanged` event whose
name has a readable canonical surface staged at the target. If a later linked
event's name lacks such a surface, an earlier linked event with one is the
fallback. A selected zero-address resolver suppresses inventory rather than
reviving an older nonzero event; surface visibility does not participate in
this pointer choice. Record events that already carry a logical name are joined
without restricting either the pointer or record event's source family. An
`ens_v1_resolver_l1` event with no logical-name attribution may instead join
when the selected pointer's source family is `ens_v1_registry_l1`,
`ens_v1_registrar_l1`, or `ens_v1_wrapper_l1`. A selected
`ens_v2_registry_l1` or `ens_v2_root_l1` pointer may also join when its target
resolver has a final supported `ens_v1_resolver_l1` classification from an
applicable exact declaration, and that classifying manifest's namespace
matches the pointer's namespace. Incremental staging applies the same guarded
exception by requiring the pointer namespace and exact declared resolver
address to match.

For an exact direct `public_resolver_v2` declaration in `ens_v2_resolver_l1`,
node-keyed `RecordChanged` and `RecordVersionChanged` observations retain their
resolver instance, node, selector, value, and version provenance without an
ENSv1 `resource_id`, even if an old ENSv1 resource is materialized. Project uses
the existing guarded node-inventory join against a selected `ens_v2_registry_l1`
or `ens_v2_root_l1` pointer only when the namespace and exact declared resolver
address match. Record-only and version-only incremental changes select that
same resource; a stale pointer or different emitter cannot contribute records.
Canonical retraction and cold replay rebuild from the same retained source facts.
The active version is tracked per resolver and node, and inventory includes
only that version's observations. Explicit empty writes are retained; a version
change invalidates older values without deleting their historical observations.
The inherited reset increments `recordVersions[node]` and emits `VersionChanged`.
(upstream: .refs/ens_v1_publicresolver_5141a2a/contracts/resolvers/ResolverBase.sol:L8-L22 @ ens_v1_publicresolver_5141a2a@5141a2a)
This reuses existing normalized-event and inventory storage; no schema or
record-ID mapping is added. It does not supply PermissionedResolver
permission resources, or resolver binding enumeration.

A `basenames_base_resolver` event without logical-name
attribution may join only through a `basenames_base_registry` pointer on the
same chain, node, and resolver emitter. Basenames keeps the current resolver by
node, authorizes its registrar controller and reverse registrar independently
of the node owner, and stores text by record version, node, and key.
(upstream: .refs/basenames/src/L2/Registry.sol:L173-L180 @ basenames@1809bbc)
(upstream: .refs/basenames/src/L2/L2Resolver.sol:L193-L199 @ basenames@1809bbc)
(upstream: .refs/basenames/lib/ens-contracts/contracts/resolvers/ResolverBase.sol:L7-L24 @ basenames@1809bbc)
(upstream: .refs/basenames/lib/ens-contracts/contracts/resolvers/profiles/TextResolver.sol:L7-L36 @ basenames@1809bbc)
When an ended
resource still has a pointer to the emitting resolver, the newly attributed event can therefore
change that resource's rebuildable inventory row even though the event remains
resource-less. This does not restore a current binding. Registry-only ENSv1 and
Basenames names are different when a current nonzero resolver pointer remains
event-linked: the composed name's `resource_id` stays null while the
[`serving_resource_id`](glossary.md#serving-resource) joins resolver and
inventory reads without creating control. An ENSv2 TLD whose root-registry
token has a current [root-registry resolver
pointer](glossary.md#root-registry-resolver-pointer) but no observed
registration takes the same shape while its row stays
`current_authority_not_projected`. An explicitly released ENSv2 name
instead keeps a row for a [released v2
authority](glossary.md#released-v2-authority) whose `resource_id` still
references the released resource, but its `serving_resource_id` is null; the
tombstone's summary nulls resolver state, so inventory attributed to that
resource stays out of current serving. A [released v1
authority](glossary.md#released-v1-authority) keeps the same shape on its
lapsed lease binding. Family readers select the binding for composed names and address relations in
one publication snapshot. An overlapping Interpret or Project redo makes the
publication unavailable until the changed identity is reflected in the families.
Canonicality checks reject orphaned identity and family publications. Project omits current ownership
and address-record memberships when its selected name has
`control.status` set to `unregistered`; the retained name identity remains readable. Ownership uses the
same registrant and supporting event selected by name composition, rather than
ranking other tokens' registrations again. Released tombstones therefore
remain readable on their closed bindings without a separate binding-liveness
exception in the read filter. A state-derived ENSv2 expiry release
removes the composed name when ENSv2 is the selected authority, or when no
authority is selected and the row reports `current_authority_not_projected`;
for a resource-backed binding, the release's `resource_id` must also match the
binding's resource.
If a different ENSv2 reservation survives that expiry, the row's lifecycle
summary follows the reservation, but `surface_binding_id`, `resource_id`,
`serving_resource_id`, `token_lineage_id`, and `binding_kind` are all null: a
reservation does not write a surface binding, and the expired registration's
identity and record inventory are not current name data. This is an intentional serving narrowing
(a root-registry TLD reservation is the exception, served through its
[root-registry resolver pointer](glossary.md#root-registry-resolver-pointer)):
ENSv2 stores a nonzero resolver supplied for an ownerless reservation and
returns it until expiry. (upstream:
.refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L255-L258 @
ens_v2@a971bd64) (upstream:
.refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L461-L478 @
ens_v2@a971bd64)
A surviving row whose ENSv1 and ENSv2 evidence cannot select one authority
instead remains explicitly unsupported. For a removed row, retained inventory
is reachable only through history. ENSv2 stores resolver records by node and
version.
`setName` passes
part zero, selecting the node-specific, any-part permission resource; the cited
authorization path reads EnhancedAccessControl role mappings and contains no
current registry-registration lookup. (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L127-L133 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L77-L85 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L178-L186 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L467-L472 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L247-L254 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/libraries/PermissionedResolverLib.sol:L66-L78 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2/contracts/src/access-control/EnhancedAccessControl.sol:L185-L192 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/access-control/EnhancedAccessControl.sol:L374-L382 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/access-control/EnhancedAccessControl.sol:L443-L455 @ ens_v2@a971bd64)

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
Every `.sql` file under `crates/project/src` is a hash input, because the hash
reads those files whole without asking which code loads them. SQL that only
tests load, such as fixtures and reference-oracle queries, therefore lives in
`crates/project/testdata/sql/`, outside the hashed tree, and a content-hash
test fails when SQL under `crates/project/src` is loaded only by test code.
The part of the storage families code (`crates/storage/src/families`) that
Project's family step calls is covered as well: the step stores the [name
summaries](glossary.md#name-summary) that code composes and orders its rows by
the family positions defined there, so a change to it rotates the hash and
forces a rebuild like a project change does. That part is an explicit file list
(`COMPOSITION_FILES` in `crates/content-hash/src/storage_families.rs`): the
summary composition, the composed name-row loader it calls, the lifecycle
evaluation and control rows that loader reads, and the position ordinals. The
read-only queries beside it (search and bound-name listings, record, reverse,
permission, children and topology readers) are listed as readers and stay
outside, so an API-only change there needs no redo. Every production `.rs` file
under that directory must be in exactly one of the two lists: a listed
composition file that is missing, or an unlisted `.rs` file, fails the build, so
a file cannot move into or out of the hashed set unreviewed. A file the
composition embeds, such as SQL, must be listed as composition by hand. Also covered are
`crates/storage/src/address_names/query.rs` and its
`query/timestamps.rs` helper, whose expiry and registration timestamp reads the
summaries store, plus `crates/storage/src/unix_seconds.rs` and `expiry.rs`, which
decode exact expiry and classify the contract-specific absent-expiry values.
The rest of the
storage crate serves reads and stays outside.
Interpret's persistence stage is covered on the
same rule: which interpreted row wins a conflict, how a redo range reopens and
reanchors bindings, and which surfaces a normalizer-version recompute
activates all decide which identity, discovery, and label-preimage rows the
projections then read, so they are interpretation rather than plumbing.
Interpret's batch sizing stays outside, because completed walks that fold the same events into
differently sized physical batches produce the same rows. Request-scoped
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
redo covers what Project's hash adoption requires: from the first ingested
block to the Ingest handoff or Project's own recorded head, whichever is
higher. That matches the Interpret range unless a crash between the two
phases' live-cycle advances left Project one block behind, and it still
reaches the handoff when Project stood below it; while upstream discovery
repair holds required Ingest work, the stamp is clipped to Project's head until
the Interpret replay after that repair widens it. Project adopts the new hash
only when the redo covers that range. An
interrupted redo retains that same effective range; recovery cannot narrow back
to the finite ingest handoff. If an interrupted attested Interpret redo spans
the hash rotation, its token remains valid only for that exact range. The new
binary clears the redo cursor written under the prior interpreter content hash
and walks the range from its beginning while retaining the durable audit
association. A Project redo the prior hash started and left unfinished is superseded when
the new hash's Interpret redo starts, in the same transaction, and stamped again
when that redo completes. Moving a covered semantic source without updating the covered set
fails the build rather than silently narrowing the fingerprint.

### Walk index set

An operator may drop the `normalized_events` indexes Interpret does not read for the length
of a from-zero walk or a full-history Interpret redo, and rebuild them before Project runs,
with [`ops/walk-index-set`](../ops/walk-index-set/README.md). The indexes Interpret keeps are
the [walk index set](glossary.md#walk-index-set). Indexes are access paths, not hash inputs:
dropping or rebuilding one changes no stored row and leaves the primary key, the
`event_identity` unique key and every foreign key in place, so database constraints stay
authoritative. The rule that splits the indexes:

- An index is kept when a statement Interpret runs can read it: its loaders, its writer, the
  redo-range preparation, the flag recompute, or the manifest sync a runner start performs.
- Every other index is a read path for Project, the API or an operator's
  `phase-runner inspect` command. Project starts only after
  Interpret completes, and the API refuses the routes that read them while an Interpret redo
  is in progress (the public namespace snapshot and the composed name reads require each
  served chain's Interpret not to be in a redo). The one exception is the event audit,
  `GET /v1/diagnostics/events`, which stays available during a redo by design. Its record
  attribution reads six of the dropped indexes (the ENSv1 and Basenames record node indexes,
  the two record-ID indexes, `normalized_events_project_node_history_idx` and
  `normalized_events_project_v1_pointer_addressed_node_idx`), so while they are dropped it
  reads without them and, on a large database, may exceed the API's statement timeout
  (`BIGNAME_API_DB_STATEMENT_TIMEOUT_MS`, [production settings](production.md)) until
  `install.sql` has run. The `phase-runner inspect` block and raw-event windows, which an
  operator runs by hand, count and list normalized events by block hash through
  `normalized_events_block_idx` and read more of the table while it is dropped.

The 17 kept indexes and the statements that read them:

| Index | Read by |
| --- | --- |
| `normalized_events_pkey` | the lookahead loader's final join (`load/lookahead/events.sql`), the full-state restore's payload read |
| `normalized_events_event_identity_key` | the writer's `ON CONFLICT (event_identity)` and identity transitions |
| `normalized_events_interpreter_state_history_idx` | prior-state value reads and the full-state restore (`load/prior.rs`) |
| `normalized_events_resource_history_idx` | the lookahead loader's resource arm, registrar transition evidence |
| `normalized_events_name_history_idx` | migration transition evidence by name (`write/identity/transition/registrar.rs`) and the flag recompute's raw-label fallback (`recompute.rs`), which look a name's events up by name alone |
| `normalized_events_chain_block_number_idx`, `normalized_events_chain_block_number_desc_idx` | the redo-range clear and preparation, the full-state restore, the loader-choice family probe and the due-names block-before-batch read; either twin serves each |
| `normalized_events_projection_idx` | the manifest sync's retained admission history (`retained_admission_manifests` in `crates/manifests/src/schema_v2_persistence.rs`), which reads every `SourceManifestUpdated` row by kind |
| `normalized_events_manifest_idx` | the manifest sync's latest `SourceManifestUpdated` per manifest at runner start (`lock_phase_writers` in `crates/manifests/src/schema_v2_sync_state.rs`, `load_manifest_states` in `schema_v2_event_history.rs`), one index probe per manifest |
| `normalized_events_v1_direct_node_probe_idx`, `normalized_events_v1_due_probe_idx`, `normalized_events_basenames_direct_node_probe_idx`, `normalized_events_basenames_due_probe_idx`, `normalized_events_v2_direct_node_probe_idx`, `normalized_events_v2_key_probe_idx`, `normalized_events_v2_due_probe_idx`, `normalized_events_v2_lookahead_probe_idx` | the lookahead loader (`ops/v1-lookahead-indexes/README.md`); every lookahead chain runs every arm, so all eight stay even where some hold no rows |

The other 35 serve only Project, the API and `phase-runner inspect`, and `ops/walk-index-set/drop.sql` drops exactly
these: `normalized_events_registry_token_idx`,
`normalized_events_v1_subregistry_after_node_scope_idx`,
`normalized_events_v1_subregistry_after_child_scope_idx`,
`normalized_events_v1_subregistry_before_node_scope_idx`,
`normalized_events_v2_subregistry_pointer_scope_idx`,
`normalized_events_v1_subregistry_before_child_scope_idx`, `normalized_events_block_idx`,
`normalized_events_emitter_history_idx`, `normalized_events_v2_expiry_scope_idx`,
`normalized_events_ens_v1_record_node_resolver_idx`,
`normalized_events_basenames_record_node_resolver_idx`,
`normalized_events_record_id_write_idx`, `normalized_events_record_id_link_idx`,
`normalized_events_resolver_alias_history_idx`,
`normalized_events_resolver_upgrade_history_idx`,
`normalized_events_pointer_after_resolver_history_idx`,
`normalized_events_pointer_before_resolver_history_idx`,
`normalized_events_permission_after_resolver_history_idx`,
`normalized_events_permission_before_resolver_history_idx`,
`normalized_events_subregistry_registration_history_idx`,
`normalized_events_project_name_node_idx`, `normalized_events_project_name_child_idx`,
`normalized_events_project_name_after_target_idx`,
`normalized_events_project_name_before_target_idx`,
`normalized_events_project_primary_after_idx`, `normalized_events_project_primary_before_idx`,
`normalized_events_project_primary_after_source_idx`,
`normalized_events_project_primary_before_source_idx`,
`normalized_events_address_registrant_match_idx`,
`normalized_events_address_token_holder_match_idx`,
`normalized_events_address_registry_owner_match_idx`,
`normalized_events_address_root_permission_idx`,
`normalized_events_project_node_history_idx`, `normalized_events_project_v1_pointer_node_idx`
and `normalized_events_project_v1_pointer_addressed_node_idx`.

A new index on `normalized_events` joins one list in the change that adds it: the drop list
when Interpret does not read it, the kept set when Interpret does.
`crates/interpret/src/load/walk_index_set_tests.rs` fails until the two lists together are
every index the baseline defines on the table, and proves that Interpret, over ENSv1,
Basenames and ENSv2 histories through either loader, scans `normalized_events` sequentially
nowhere without the drop list and stores the same rows. That test shows each statement keeps
an index path, not that the path is keyed: a name-only read falls back to a range of
`normalized_events_chain_block_number_idx`, which is why `normalized_events_name_history_idx`
stays although the ENSv1-only Mainnet walk never read it.

## Projection publication

Project is the only projection writer. A normal follow block atomically commits
its affected [owned key families](glossary.md#owned-key-family), undo entries and
[family marker](glossary.md#family-marker). Rebuilds use bounded ranges and keep
the marker unavailable until publication completes. There is no obsolete serving
batch, claim queue, dead-letter queue or separate serving-table refresh.

Replay needs no Interpret-to-Project handoff: Project undoes its journalled
family publications to a trusted base and replays retained canonical input, so
a redo range's deleted normalized rows are not copied anywhere first.

Each family has its own typed keys and state; there is no generic set of old
`*_current` columns. The table list and reducer ownership are in
[per-block publication](glossary.md#per-block-publication). The marker gives the
publication for the whole chain, including unchanged keys. Composition cannot
recover an older name snapshot from the last block that happened to touch a key.
Child registration history remains in `child_registration_events`, derived by
Project from retained canonical events. Historical upgrade migrations remain
append-only; `20260929160000_remove_served_projections.sql` drops the obsolete
serving tables, the old generation-failure audit and the redo handoff tables
from initialized databases, and the current baseline no longer creates them.

The composed name's `declared_summary` carries the current ENSv1 NameWrapper lifecycle
label and [expiry-effective](glossary.md#expiry-effective-namewrapper-fuse-word)
fuse summary together. The underlying normalized
`PermissionScopeChanged` event keeps its expiry-unadjusted interpreted fuse word
unchanged; Project
clears only the rebuildable current summary when the served projection timestamp
passes wrapper expiry. Permission reads derive restrictions from the same family state by
`resource_id`; they do not persist a second per-resource permission fan-out.
Registry-wide approvals use the same rule: Project owns the replayable
[account permission state](glossary.md#account-permission-state) and the
[registry-owner binding](glossary.md#registry-owner-binding). Revoked account
rows remain in the current-state table so losing-fork grants and losing-fork
revocations both rebuild from surviving canonical history. Interpret re-walks
retained raw facts through the [`standard_approval`
derivation](glossary.md#standard-approval-derivation); Project then rebuilds both state legs without a provider
refetch. Storage serving combines those two state legs into effective
registry-operator permission rows without persisting per-resource fan-out.
For namespace-scoped reads, direct and effective registry-operator rows share
one membership rule: a resource is a member when a retained, activated
normalized event for that resource carries the namespace, and both the event
and its `chain_lineage` anchor are canonical, safe, or finalized. Membership
therefore does not need a current name binding, so unnamed and superseded
registrations stay readable through a namespace-filtered [resource
audit](glossary.md#resource-audit-context) read. A resource with no such event
has no namespace membership but remains visible to unscoped reads.

For ENSv2, a latest state-derived `RegistryPathExpired` release removes that resource's effective
permission rows without removing its partial-coverage summary. A later
`RegistrationRenewed` marked as a revival readmits retained grants when the same
resource has an earlier path-expiry release, regardless of whether that release
named a surface. A grant or reservation also readmits the resource. A new
versioned resource receives grants only from its own permission events. An
owner-zero reservation is different: registration keeps both version counters,
including `eacVersionId`, so it reuses the reservation's permission resource ID.
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L29-L34
@ ens_v2@a971bd64) (upstream:
.refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L428-L471 @
ens_v2@a971bd64) (upstream:
.refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L632-L645 @
ens_v2@a971bd64)

Coverage wording is not an exhaustiveness claim. `support_status` and
`unsupported_reason` carry admission separately from projection completeness.
`operator_approval_surfaces_not_ingested` maps to partial, best-effort
permission coverage. Family permission composition uses that broad reason for every
non-wrapper authority class. The serving layer maps each stored reason to the
documented list of unlisted permission surfaces and reports the union for
account-wide or mixed reads.
`wrapper_parent_and_resolver_delegation_not_projected` marks NameWrapper
resources partial: holders, operators, and per-token delegates are projected,
while parent control of a wrapped subname and resolver delegation are not; the
summary's `resource_restrictions` field carries the
[resource restrictions](glossary.md#resource-restrictions) block the API
serves. Readers reject inconsistent typed combinations and
map an unrecognized persisted unsupported reason to unknown partial product
coverage rather than treating it as wrapper support or returning an internal
server error. The adapter-owned mapping requires a full-history Interpret
re-walk and Project rebuild under the rotated interpreter content hash; Ingest
does not rerun only when retained raw facts cover the registry
`ApprovalForAll` range required by the current [compiled watch
plan](glossary.md#compiled-watch-plan). That range was declared by commit
`b22bccee` on 2026-08-31 through
[`ens_v1_registry_l1` manifest version 3](../manifests/mainnet/ethereum/ens/ens_v1_registry_l1/v3.toml)
on Ethereum Mainnet,
[`ens_v1_registry_l1` manifest version 1](../manifests/sepolia/ethereum/ens/ens_v1_registry_l1/v1.toml)
on Sepolia, and
[`basenames_base_registry` manifest version 2](../manifests/mainnet/base/basenames/basenames_base_registry/v2.toml)
on Base Mainnet. A retained database whose Ingest predates that declaration must
have completed the retained-range Ingest redo; otherwise, the [re-derivation
boundary](glossary.md#re-derivation-boundary) must start with Ingest for that
range before Interpret and Project. A
from-zero Ingest under the current compiled watch plan satisfies the
precondition directly.

The ENSv2 expiry Project fold also rotates
the shared interpreter content hash without changing raw facts or
normalized-event semantics; the expiry interpretation slice must not be served
before its paired Project fold is deployed and that coherent replay and rebuild
has completed.

## Snapshot serving

Snapshot selection resolves `at`, explicit `chain_positions`, and consistency
to one concrete set of phase chain positions. Current head, safe, and finalized
positions come from `chain_heads`; timestamp and historical selection use
readable `chain_lineage` rows.

Projection `chain_positions` timestamps are decoded as RFC 3339 instants.
PostgreSQL JSON may spell UTC as `+00:00`; request selectors or retained rows
may carry other numeric UTC offsets and one to nine fractional-second digits.
Snapshot selection normalizes these values to UTC before comparing the
timestamp component of a chain-position identity. Storage reserializes the
normalized instant with `Z` and preserves non-zero fractional seconds. Public
API metadata projects these instants to whole Unix-second strings; opaque
snapshot tokens and cursor positions retain the precision needed for identity
and comparison. Invalid timestamp syntax remains unusable projection state; a valid alternate offset
spelling is not stale state. This is a serving-boundary compatibility rule and
does not change which stored projection rows are authoritative or when they are
rebuilt.

Every selection also requires a live [family marker](glossary.md#family-marker)
with the API's compiled interpreter content hash, at or below the newest stored
head and at most the API's
[publication lag tolerance](glossary.md#publication-lag-tolerance) (one block
by default) behind it. The API reads only projections eligible for the selected
positions and revalidates the Project generation before returning; a
current-state collection checks it instead on its one read snapshot, before its
first read. A head that advances within that tolerance leaves the publication
servable; a replaced publication, or one that stops being servable, before that
check returns `409 stale`.

### History page order

Name history, address history, and `/v1/events` pages sort normalized events by
chain position: block number newest first with events without a block last,
then chain, block hash, transaction index, log index, and event identity, with
an event that has no transaction position before every transaction of its
block. The transaction hash is not an ordering key. `order=asc` is the exact
reverse. The served reads always carry a block window,
the published block of each chain, so the events they return always have a
block number, although the column itself allows NULL for events such as
manifest updates.

For a read that no name, registration, address, or resolver anchors, such as an
unfiltered `/v1/events` page, on one chain,
`normalized_events_chain_block_number_desc_idx` on
`(chain_id, block_number DESC NULLS LAST)` returns rows in the page order: read
forward newest first and backward oldest first. PostgreSQL stops after one page
instead of sorting every matching event; the remaining sort keys only order the
events of one block. Anchored reads start from their anchor's own indexes. The
older ascending `(chain_id, block_number)` index read backward puts events
without a block first, so it cannot serve this order.

A continued page built by the ordinary history page builder also bounds the
scan at the cursor event's block, read in the same transaction that validates
the cursor, so page N does not reread the rows of earlier pages. Name history
with `include=child_registrations` does not get this bound. It builds its own
query with two parts, the name's own events and its direct child registrations
from `child_registration_events`, and continues each part from the cursor
through that part's own index. That bound is added only when the read already
excludes events without a block. An unanchored page query is sent unprepared, so
PostgreSQL plans each page with the actual block values instead of a cached
generic plan's guess at the size of the block range. On the test fixture the
generic plan also reads the index from the cursor block, so this is a margin,
not a requirement. Planning an unanchored page takes about 16 to 19 ms on the
test machine. Anchored pages keep their cached prepared plans.

A window that spans several chains, such as an unanchored read over a namespace
published on two chains, still reads and sorts every matching event, because no
single index returns block order across chains. The prebuild procedure for the
index is in [`ops/events-order-index/README.md`](../ops/events-order-index/README.md).

## Verified lookup storage

Schema-v2 verified lookup has no execution cache, durable trace, reusable
outcome, or persisted request-validation state. Each admitted provider lookup
runs for the current request at the selected block identity. See
[`execution.md`](execution.md).

API verification starts a fresh `REPEATABLE READ, READ ONLY` transaction after
provider calls and revalidates the captured state without advisory or row locks.
Its fixed-`search_path`, security-definer guard checks the same predicates as the
retained locking ledger writer. The API role needs `EXECUTE` only on
`revalidate_resolution_lookup_state_read_only`, which fixes locking to false.
The shared boolean core remains private to the schema owner; clients cannot
choose its locking mode. The API role needs no access to the ledger or its
writer. API requests never create, refresh or clear a
divergence, on either primary databases or physical streaming standbys.
Existing ledger rows remain diagnostic observations and can still be retired
by Project publication and reorg handling. No serving path consumes them.

For non-API direct resolver comparisons, the original eight-argument guard
invokes the shared body with locks enabled; the writer retains its transaction
and mutation behavior.

Lookup composes name topology and inventory in one repeatable-read family
snapshot. The guarded writer receives the captured indexed entries, read rules,
coverage and resolver path. It locks the live family marker and checks its
sequence, block identity and interpreter hash, the execution heads and canonical
positions, and the actual manifest row versions. It also locks Interpret and
Project phase rows through the ledger commit and refuses any redo range that
overlaps the publication. An unrelated phase-row update alone does not invalidate
a captured family generation. A same-height family republish does.

The writer evaluates the captured exact-or-ENSIP-19 indexed answer before
comparing the provider response. The internal snapshot is never client-supplied
request data or a reusable persisted lookup outcome. The current baseline and
the removal schema-migration keep the guarded functions' signatures and grants
while removing their former serving-table inputs.

Ledger rows are durable operational observations, not authority for served
values or a response cache. When a family publication changes an ENS Mainnet or Sepolia exact resolver
to null, Project retires active direct observations for that name on the
publication's chain in the same transaction. A Universal Resolver proxy change
adds names with active disagreements on that chain to the reserved-name summary
refresh candidates, so cutover also retires observations for names without a live
ENSv2 entry and their descendants. Candidates are bounded by active evidence rather
than every name. When a later ENSv2 entry release removes the path below a
second-level name, the same publication refreshes its descendants with active
evidence. The release journal gates this work; the normal summary work list
supplies affected parent names, including resource-only releases, and label hashes
match their descendants. Previously retired observations, other parents with live
entries and other chains are unchanged.
[Universal Resolver ancestor discovery](glossary.md#universal-resolver-ancestor-discovery)
revalidates the exact composed name, Ethereum head, family publication, canonical
positions and Universal Resolver manifest authority. It does not write, compare
or clear by agreement with a request-scoped ancestor-served result. The writer
normalizes the legacy indexed status alias `failed` to `execution_failed`,
matching the Rust evaluator. Retired ledger rows remain available for audit.

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
- `crates/lookup` owns verified lookup behavior and guarded divergence writes;
  Project publication may only clear outdated direct observations when an ENS
  Mainnet exact resolver becomes null.
- `crates/storage` provides the typed persistence and read boundaries above.
- `apps/api` reads phase projections and lookup output; it does not write raw
  facts, interpretation output, projection rows, or legacy execution artifacts.

### Same-node source transport maintenance

The phase runner owns the explicit Sepolia [source transport](glossary.md#source-transport) change described under [same-node Sepolia transport change](chain-intake.md#same-node-sepolia-transport-change). It changes only `ingest_cursors.source_kind` under all phase advisory locks after checking the old and new node interfaces against retained hashes and the next block. Source key, seed, start, progress, raw identities/canonicality, redo obligations and derived output remain unchanged. It grants no new verification independence. This documented exception does not permit arbitrary provider replacement or ordinary startup to rewrite cursor identity.
