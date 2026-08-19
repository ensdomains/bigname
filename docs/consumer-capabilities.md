# Consumer Capabilities

This document maps the consumer-facing capabilities served by the bigname API.
Wire format and route details live in [`api-v2.md`](api-v2.md) and
[`api-v2-routes.md`](api-v2-routes.md).

## Served route sets

| Set | Routes | Intended use |
| --- | --- | --- |
| Lookup | `POST /v2/lookup`, `GET /v2/status` | Batched name/address lookup and indexing readiness. |
| Product reads | `/v2/names/*`, `/v2/addresses/*`, `/v2/permissions`, `/v2/search`, `/v2/events`, `/v2/resolvers/*`, `/v2/namespaces/*` | Name, record, address, permission, event, resolver, and namespace reads. |
| Diagnostics | `/v2/diagnostics/*` | Coverage, binding, authority, record, manifest, and event inspection. |
| GraphQL compatibility | `POST /graphql` | The documented narrow subgraph-compatible operations. |
| Operator health | `GET /healthz` | API process, opaque running-database-instance identity, and phase-runner heartbeat readiness. This is not a product route. |

The v1 REST surface has been removed. In particular,
`POST /v1/identity:lookup` no longer serves the native identity capability.
`POST /v2/lookup` owns batched forward and reverse lookup with the v2 envelope;
it does not preserve the deleted v1 DTOs.

## Capability mapping

| Capability | Route owner | Notes |
| --- | --- | --- |
| Batched forward and reverse lookup | `POST /v2/lookup` | `profile=feed` is the field-budgeted path; `profile=detail` returns the documented full record shape. |
| Indexing readiness | `GET /v2/status` | Per-chain projection progress, stored head, indexing-process liveness, network-head readiness, and required Sepolia completed-Ingest state plus [verification-level evidence](glossary.md#verification-level). |
| Exact name profile | `GET /v2/names/{name}` | Indexed or verified name and record fields, plus [expiry-effective](glossary.md#expiry-effective-namewrapper-fuse-word) ENSv1 NameWrapper lifecycle and fuse data when backed, subject to the route's source rules. |
| Resolver records | `GET /v2/names/{name}/records` | Key-selected record reads plus inventory metadata. |
| Direct subnames | `GET /v2/names/{name}/subnames` | Latest-state direct-subname collection. |
| Name history | `GET /v2/names/{name}/history` | Name, registration, or combined history scope. |
| Names by address | `GET /v2/addresses/{address}/names` | Owner, manager, and registrant relations with optional expansions. |
| Primary name | `GET /v2/addresses/{address}/primary-name` | Indexed tuples and verified ENS coin-type 60 lookup as documented. |
| Address history | `GET /v2/addresses/{address}/history` | Latest-state address-anchored event history. |
| Permission holders | `GET /v2/permissions` | Current resource-anchored permission rows; returned current ENSv1 wrapper registrations carry [expiry-effective](glossary.md#expiry-effective-namewrapper-fuse-word) lifecycle and fuse data without claiming exhaustive wrapper-holder enumeration. |
| Search | `GET /v2/search` | Name search only; no registration, pricing, or availability workflow. |
| Events | `GET /v2/events` | Product event collection. |
| Resolver overview | `GET /v2/resolvers/{chain_id}/{address}` | Resolver metadata, total section counts with deterministic samples capped at 100 items, and a separately paginated record-shaped bound-name collection, including [expiry-effective](glossary.md#expiry-effective-namewrapper-fuse-word) ENSv1 NameWrapper metadata when backed. |
| Namespace metadata | `GET /v2/namespaces/{namespace}` | Product-facing namespace and capability metadata. |
| Pipeline diagnostics | `/v2/diagnostics/*` | Explicit diagnostic tier, separate from product reads. |

## ENSv1→ENSv2 mixed-history ownership

The replacement contract for exact-name and direct-subname current reads is
the per-name current-authority rule in
[`architecture.md`](architecture.md#ensv1ensv2-current-authority). Under that
rule, an [authority proof](glossary.md#authority-proof) first selects one
[authority epoch](glossary.md#authority-epoch), and every current field is then
selected inside that epoch. A migrated name keeps both eras in history while current
registration, control, resolver, expiry, address relations, and permissions
come only from its ENSv2 resource. Retained ENSv1 facts remain history and
provenance; they do not make the current read unsupported and cannot become
current again after an ENSv2 release. Release leaves
[released v2 authority](glossary.md#released-v2-authority), so later ENSv1
events cannot repopulate any current field. Slice 2C applies this rule to the
exact-name projection, the name-detail response, and per-result batch-lookup
records. Slice 2D makes address-name and
permission collections consume that selected current registration, but
they acquire no row-local coverage status or unsupported-reason vocabulary;
callers inspect the exact-name or lookup result for coverage. An explicit
`registration_id` permission query may inspect a superseded ENSv1 registration
as historical/audit data. Once slice 2D is activated, every permission row
carries `authority_context`. `current_for_name` means a `name` filter selected
the row's current registration for that requested name. A row admitted without
a `name` filter, including an explicit-`registration_id` or address-filtered
resource read, is `resource_audit` and makes no current-name claim; an optional
display name does not change that classification. Rows carrying the
[`resource_audit` context](glossary.md#resource-audit-context) remain queryable.
The marker changes only how that permission
response may be interpreted; the per-name ownership rule independently decides
which registration contributes current authority, address relations, and role
summaries. A superseded ENSv1 registration is therefore never selected, while a
current registration queried by resource can still contribute in a separate
name-scoped view.

Slice 1 admits the facts and records `MigrationApplied` as a candidate
interpreter-owned authority boundary. It deliberately defers the ENSv1→ENSv2
`SurfaceBinding` close/open to an activated transition. Every effect whose existence depends on
the per-name
[migration correlation group](glossary.md#migration-correlation-group) carries
`consumer_visibility=candidate` and is excluded from Project staging and all
product event/history reads, even when the effect uses an existing source
family. An independently admitted existing-family event remains byte-for-byte
activated and product-visible; only its correlation association is candidate
and diagnostics-only. Diagnostics may expose both forms. Slice 2A makes
ordinary binding changes arm-scoped and adds the exact-name activated transition
writer, but production continues to emit candidate transitions only. Until the
planned re-derivation boundary, product row membership and DTO fields remain unchanged and exact-name
reads over a corpus containing both families retain the existing
`mixed_exact_name_corpus` public reason. That boundary re-derives the group as
activated, performs the deferred binding transition, and enables the
correlation-dependent event/history rows. Slice 2C consumes the validated
transition through its activated
`MigrationApplied` artifact without re-correlating raw ENSv1→ENSv2 migration
evidence. It
replaces that blanket refusal with the per-name exact-name rule. It
also activates non-boundary correlation groups; those groups never perform a
binding transition or change an authority epoch.

Slice 3B activates direct-subname ownership per child. A child that has not
migrated can remain ENSv1-authoritative below a migrated parent. Once that
child migrates or otherwise obtains a current ENSv2 registration, the ENSv2
parent-child binding replaces the ENSv1 binding. A later release leaves the
child unregistered on the ENSv2 side rather than restoring the retained ENSv1
binding. A Mainnet pair whose ENSv1 and ENSv2 bindings both remain current
after applying those boundaries blocks [Project phase](glossary.md#projection)
publication for that
generation; it is not a tie to resolve by event recency or an ambiguous product
row. A proven Sepolia boundary, or a current
child registration in the admitted migration registry below a proven migrated
parent, follows the same per-name or per-child selection rule. Sepolia overlap
without either proof is instead an expected property of independent test
deployments and remains unsupported under its own reason until a caller or
[deployment profile](glossary.md#deployment-profile) selects one system. Until slice 3B is activated, existing
direct-child projection behavior remains in force.

The Mainnet dual-current assertion runs only after transaction- and block-level
reconciliation; a transient intra-transaction overlap is not a publication
failure. A dual-current result after the applicable proven activated boundary
aborts before `publish::swap`, publishes no partial output, fails readiness for
that Project publication, and returns structured failure evidence. After the
Project transaction rolls back, the phase runner persists that evidence in the
append-only `project_generation_failures` diagnostic audit described in
[`storage.md`](storage.md#projection-publication). Slice 2E introduces that audit
path for the exact-name invariant; slice 3B reuses it for the child invariant and
replaces the existing child recency tie-break. It does not layer the authority
rule on top of that ranking. The Sepolia distinction above is unchanged.

## ENSv1→ENSv2 delivery slices

Each slice includes its behavior tests and fixture provenance. Counts are
estimated hand-written production files; test fixtures, test-only harness
files, and docs are not included.

| Slice | Coherent capability | Estimated production files |
| --- | --- | ---: |
| 1. Schema vocabulary, candidate ENSv1→ENSv2 intake, and replay with no product-visible change | Extend the closed schema-v2 event/derivation vocabulary through a reviewed in-place schema upgrade; admit fixed ENSv1→ENSv2 migration contracts; ratify [migration-registry](glossary.md#migration-registry-wrapperregistry) discovery; keep the independently admitted `registry_announcement` indexability edge ordinary and traversable by the watch plan while attaching candidate correlation provenance; interpret only controller-mediated second-level correlation-dependent identity, topology, role, registration, renewal, and normalized effects as candidate while leaving independently derivable existing-family output ordinary; exclude candidate groups and association/effect tables from Project staging and product event/history reads; defer every ENSv1→ENSv2 migration-driven `SurfaceBinding` transition; and add production provider-trusted Verify support plus declared-level guard fixtures for `ethereum-sepolia`. Child-migration derivation through a parent `WrapperRegistry` landed in slice 3A below as candidate-only output; publication of child authority remains deferred to slice 3B. Restart, full-replay, and live-follow fixtures prove later proxy facts remain retained without changing product behavior. | At least 22 (3 manifest TOML, up to 11 adapter/manifest Rust files, 2 schema contract/check files, at least 1 reviewed versioned schema-migration file, 1 phase-runner Verify module, and up to 4 Project/API/storage visibility modules) |
| 2A. Explicit migration authority transition and arm-scoped ordinary bindings | Add a required `authority_arm` to every binding and closure draft; scope ordinary close, predecessor, and successor behavior to chain, exact logical name, and arm; preserve coexisting ENSv1 and ENSv2 bindings; represent the exact-name cross-arm transition explicitly; and exercise its locked zero/one/multiple predecessor behavior through a code-only activated test seam. Production remains candidate-only and Project behavior does not change. | 3 production modules plus one reviewed schema-migration file |
| 2B. Graveyard, reservation, and renewal semantics | Classify Graveyard cleanup and production reservation seeding without reading cleanup registrations as user leases; establish the remaining renewal rules from deployment evidence. | To be scoped |
| 2C. Exact-name current authority | Consume a validated activated transition or positive ENSv2 child-registration proof to select one authority epoch, then publish every `name_current` field from only that epoch. Name detail exposes the selected exact-name result or the [deployment-profile](glossary.md#deployment-profile)-specific unsupported reason; candidate events remain inert. The resolver route's `bound_names` listing inherits this selection because it reads `name_current` directly — a name is listed only under its selected resolver, and rows classified `current_authority_not_projected` are omitted, per the resolver-route contract in [`api-v2-routes.md`](api-v2-routes.md). Batch lookup results carry the same selection in 2C: a name-keyed or reverse lookup result exposes the selected exact-name outcome or the minimal unsupported record shape, per the lookup contract in [`api-v2-routes.md`](api-v2-routes.md). | To be scoped |
| 2D. Authority fanout across product collections | Apply the selected exact-name authority to address-name, permission, primary-name, search, and product-history fanout, including the address-driven lookup collections whose membership reads address-name projections, while retaining explicit resource reads as audit views. | To be scoped |
| 2E. Post-rollback generation-failure audit | Enforce the reconciled Mainnet dual-current invariant and persist the rolled-back generation failure in a separate append-only diagnostic transaction. | To be scoped |
| 3A. Direct-child correlation | Derive the deferred child-migration shapes that reach no migration controller, where the already-migrated parent's own [migration registry](glossary.md#migration-registry-wrapperregistry) registers the child into itself through the self-call that definition cites; admit the registry a locked child receives from its parent registry so admitted depth is unbounded; derive the child's ENSv1 predecessor from the parent registry's own migration evidence and the registered labelhash rather than inheriting the `.eth` second-level rule, under the separate `wrapper_backed_child_control` anchor defined at [child migration boundary](glossary.md#child-migration-boundary), selected against the child's ENSv1 cleanup rather than the registration; admit both cleanup shapes that definition cites — the `locked_child` path, whose wrapper token is parked in the Graveyard, and the `emancipated_child` path, whose node is unwrapped into it — each only with that ENSv1 predecessor cleanup present, earlier in the registration's own transaction; and reject the clobbered registration, the unmigrated child, factory-only evidence, incomplete parent discovery, and any self-claim lacking ENSv1 predecessor cleanup as non-boundaries, `MigrationHelper` participation being unobservable for the reason cited there and so never a correlation key at all. Correlation reuses `authority_transition`; every child boundary and effect is candidate-only, so no child state, projection, or product row changes — though an admitted child registry does widen Project's delete-and-rebuild scope — and activating a child transition remains an explicit refusal until slice 3B. | 4 |
| 3B. Children publication invariant | Replace the child recency tie-break with per-child authority, retain legitimate unmigrated ENSv1 children, and block publication on unresolved dual-current Mainnet pairs. | To be scoped |

Slices 1, 2A, 2B, and 2C are separately reviewed and separately merged implementation
PRs, but deploy together at the same planned [re-derivation
boundary](glossary.md#re-derivation-boundary), which also carries
[PR #391](https://github.com/ensdomains/bigname/pull/391). The boundary
uses one [interpreter content hash](glossary.md#interpreter-content-hash), one
full source re-walk, and one Project
publication decision for `ethereum-sepolia`. Other chains
retain independent publication decisions. There is no production
interval serving candidate-only data on the `ethereum-sepolia` ENSv1→ENSv2
target.
Candidate-versus-activated state remains a replay/test-surface distinction, and
the acceptance comparison below runs in the test environment against the
boundary fixture corpus. The ordinary registry-announcement edge remains a
watch-plan input, so this one-boundary plan has no ingest hole.

Slice 1 has a mandatory full-re-walk acceptance comparison against the
pre-admission Project publication at a fixed readable chain head. This comparison
isolates slice 1: its control and candidate test runs hold every other
shared-boundary input constant, including PR #391's topology serializer. It is
not a comparison between the actual pre-boundary production publication and the
activated Project publication deployed after the shared boundary. It proves identical
product-visible row membership
and every DTO field for `name_current`, `children_current`,
`address_names_current`, both permission projections and `/v2/permissions`,
resolver and record reads, primary-name and search reads, `/v2/events`, and
name- and address-history reads, plus every GraphQL compatibility operation.
The comparison covers ordered pages, page membership, every REST and Manager
DTO field, summary/count fields, `has_more`, and point responses. Before the
test-only slice-1 re-walk, each normalized-event-backed cursor surface reads a
page and saves its `next_cursor`. After the full Interpret and Project re-walk
publishes, that pre-rewalk cursor is submitted to the post-rewalk test
publication.
For `/v2/events`, name history, address history, and every other product cursor
surface backed by normalized-event row identity, it must resume from the same
normalized-event keyset anchor with identical remaining product rows, pages,
fields, `has_more`, and summary behavior. The anchor may be an unmapped event
absent from the product response, so the corpus interleaves an unmapped event at
a product-page boundary and proves that no visible row is skipped or duplicated.
`/v2/diagnostics/events` must accept its old cursor and continue from the same
stable normalized-event anchor, but its remaining rows and fields may include
the expected new candidate diagnostics. A pre-existing diagnostic row's numeric
`normalized_event_id` may change, while its `event_identity` and pre-existing
semantic fields remain stable apart from those allowed candidate additions.
Fresh post-rewalk cursors are tested
separately on every covered route and must continue normally.
Implementations may preserve numeric `normalized_event_id` values or resolve an
old token through stable `event_identity` plus its stored sort tuple; freshly
issued cursor bytes may differ. Raw facts, candidate
normalized events,
diagnostic event associations and identity/discovery effects, manifest
metadata, internal provenance, cursor-embedded row identities, and content
hashes are expected to change; product behavior is not.
The comparison runs over the complete planned re-walk, so a unit fixture that
filters only `ens_v2_migration_l1` cannot satisfy this gate.

A separate shared-boundary integration gate exercises the final combined
slice-1, slice-2A, slice-2B, slice-2C, and PR-#391 artifact. It inspects the actual widened watch
plan, performs the boundary's mandatory historical fetch and full re-walk, and
compares the published DTOs, pages, summaries, and cursor continuation with the
pre-boundary Project publication. PR #391's exact allowed wire delta is that an
existing Basenames `transport.contract_address` becomes lowercase and remains
`0x`-prefixed; no other PR-#391 product DTO field may change. The other allowed
differences are the slice-2C authority and event/history
activation contracted here, and the planned diagnostic, manifest, provenance,
and content-hash changes. Product cursors issued before this combined boundary
must remain valid, although their non-snapshot remaining rows and fields may
reflect those explicit activated deltas. Any other difference or cursor
rejection blocks the `ethereum-sepolia` publication decision.

The production Verify phase validates `ethereum-sepolia`'s durable ingested
extent through its finalized marker and records `quick_synced`. The configured
dRPC source's persisted intake cursor must match its key, kind, seed basis, and
start block and cover that finalized marker. The phase performs no
independent comparison because the configured dRPC source also supplies intake;
selecting it as a reference would only compare the provider with itself.
Independent verification requires source-role separation and is deferred to
[issue #411](https://github.com/ensdomains/bigname/issues/411), which will
upgrade the chain to `cross_checked`. Project publishes before Verify in the
pipeline sequence, but that publication remains unready and traffic-drained
until Verify succeeds for the target. Slice-1 fixtures prove the Sepolia path
reports `quick_synced` without calling a reference and that a phase cannot
persist a stronger, unearned level, accept a stale or mismatched intake cursor,
or proceed to Live.
Omitting, disabling, or replacing Verify with a no-op is not an acceptable
readiness gate.

The boundary fixture places a migration-created `RegistryCreated` at block N,
restarts Ingest and Interpret, and then emits a registry, role, registration,
renewal, or topology fact from that proxy in a later transaction or block. Full
historical replay and live-follow variants both prove the ordinary announcement
admission keeps the proxy watched, the later raw fact is retained, its
correlation-dependent augmentation is interpreted as candidate, and any output
independently derivable under `ens_v2_registry_l1` remains ordinary and matches
the control test run. The fixture also asserts that the persisted edge appears
in the generated watch plan after restart before either the retained-raw-log
announcement preload or the same-window announcement query adds the proxy, so
neither intake path can mask a missing edge. The product comparison above
remains unchanged. A
same-transaction ordering test alone cannot satisfy slice 1.

Catalog-derived slice-1 fixtures preserve each decoded expiry instead of
reconstructing a fixed premigration delta. They also require ENSv1 registry
resolver- or TTL-clear events only when the cleared value actually changed:
`setRecord` delegates to a helper that compares both stored values before it
emits either event. (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L39 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L40 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L179 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L181 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L184 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L186 @ ens_v1@91c966f)

Slice 1 still fits the approximate production-file budget, but its inertness
contract requires narrow Project staging and API/storage history-selection
plumbing in addition to adapters, manifests, and fixtures. It also requires a
reviewed in-place schema-migration: `MigrationApplied`,
`ContractDiscovered`, `ens_v2_migration`, and the correlation-scoped visibility
provenance are outside the current closed schema-v2 contract. That requirement
is the stop condition for implementation in this change. An empty-schema
replacement is not an alternative for this boundary because it cannot preserve
outstanding cursors whose event identities currently include sequence-assigned
manifest IDs. Slice 1 must also add the reviewed `ethereum-sepolia` production
Verify path and readiness fixtures; the target cannot become ready or serve
traffic by omitting or bypassing that phase. Slices 1 and 2 remain
separately reviewed capabilities but share the deployment boundary above; slice
3 remains a later consumer capability.

The GraphQL compatibility operations read the schema-v2 current projections
and preserve the committed Manager response contract. Name inputs are ENS-normalized and
matched by namehash within the `ens`
namespace. While the `project` phase has not completed at the newest stored chain head,
operations that would return projection rows fail rather than serve the prior
publication. Unsupported name rows are omitted, and
unsupported record inventories preserve the existing empty record shapes.

All top-level v2 collections use the standard `page` object. Latest-state
collections do not claim a frozen snapshot; point-in-time behavior is limited
to the routes and selectors documented in [`api-v2-routes.md`](api-v2-routes.md).

## Replacement boundary

The v2 route set is the current internal API contract. This document records
local route ownership only; it does not claim that an external application has
changed its call sites or that the production public edge exposes v2. The
checked-in Caddy configuration remains on the pre-C3 routing policy, so the v2
REST surface is not publicly reachable until the maintainer-gated C3 edge
flip.
