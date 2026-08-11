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
| Operator health | `GET /healthz` | API process, database, and phase-runner heartbeat readiness. This is not a product route. |

The v1 REST surface has been removed. In particular,
`POST /v1/identity:lookup` no longer serves the native identity capability.
`POST /v2/lookup` owns batched forward and reverse lookup with the v2 envelope;
it does not preserve the deleted v1 DTOs.

## Capability mapping

| Capability | Route owner | Notes |
| --- | --- | --- |
| Batched forward and reverse lookup | `POST /v2/lookup` | `profile=feed` is the field-budgeted path; `profile=detail` returns the documented full record shape. |
| Indexing readiness | `GET /v2/status` | Per-chain projection progress, stored head, indexing-process liveness, and network-head readiness. |
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
| Resolver overview | `GET /v2/resolvers/{chain_id}/{address}` | Resolver metadata and bounded, record-shaped name expansion, including [expiry-effective](glossary.md#expiry-effective-namewrapper-fuse-word) ENSv1 NameWrapper metadata when backed. |
| Namespace metadata | `GET /v2/namespaces/{namespace}` | Product-facing namespace and capability metadata. |
| Pipeline diagnostics | `/v2/diagnostics/*` | Explicit diagnostic tier, separate from product reads. |

## ENSv1→ENSv2 mixed-history ownership

The replacement contract for exact-name and direct-subname current reads is
the per-name current-authority rule in
[`architecture.md`](architecture.md#ensv1ensv2-current-authority). Under that
rule, a migrated name keeps both eras in history while current
registration, control, resolver, expiry, address relations, and permissions
come only from its ENSv2 resource. Retained ENSv1 facts remain history and
provenance; they do not make the current read unsupported and cannot become
current again after an ENSv2 release. Address-name and permission collections
consume that selected current registration for a supported migrated name, but
they acquire no row-local coverage status or unsupported-reason vocabulary;
callers inspect the exact-name or lookup result for coverage. An explicit
`registration_id` permission query may inspect a superseded ENSv1 registration
as historical/audit data. Once slice 2 is activated, every permission row
carries `authority_context`. `current_for_name` means a `name` filter selected
the row's current registration for that requested name. A row admitted without
a `name` filter, including an explicit-`registration_id` or address-filtered
resource read, is `resource_audit` and makes no current-name claim; an optional
display name does not change that classification. Rows carrying
`resource_audit` remain queryable. The marker changes only how that permission
response may be interpreted; the per-name ownership rule independently decides
which registration contributes current authority, address relations, and role
summaries. A superseded ENSv1 registration is therefore never selected, while a
current registration queried by resource can still contribute in a separate
name-scoped view.

Slice 1 admits the facts and records `MigrationApplied` as a candidate
interpreter-owned authority boundary. It deliberately defers the ENSv1→ENSv2
`SurfaceBinding` close/open to slice 2. Every effect whose existence depends on
the per-name
[migration correlation group](glossary.md#migration-correlation-group) carries
`consumer_visibility=candidate` and is excluded from Project staging and all
product event/history reads, even when the effect uses an existing source
family. An independently admitted existing-family event remains byte-for-byte
activated and product-visible; only its correlation association is candidate
and diagnostics-only. Diagnostics may expose both forms. Until slice 2 is
activated, product row membership and DTO fields remain unchanged and exact-name
reads over a corpus containing both families retain the existing
`mixed_exact_name_corpus` public reason. Slice 2 activates the group, performs
the deferred binding transition, enables the correlation-dependent event/history
rows, and replaces that blanket refusal with the per-name exact-name rule. It
also activates non-boundary correlation groups; those groups never perform a
binding transition or change an authority epoch.

Slice 3 activates direct-subname ownership per child. A child that has not
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
[deployment profile](glossary.md#deployment-profile) selects one system. Until slice 3 is activated, existing
direct-child projection behavior remains in force.

The Mainnet dual-current assertion runs only after transaction- and block-level
reconciliation; a transient intra-transaction overlap is not a generation
failure. A dual-current result after the applicable proven activated boundary
aborts before `publish::swap`, publishes no partial generation, fails readiness
for the target generation, and returns structured failure evidence. After the
Project transaction rolls back, the phase runner persists that evidence in the
append-only `project_generation_failures` diagnostic audit described in
[`storage.md`](storage.md#projection-publication). Slice 2 introduces that audit
path for the exact-name invariant; slice 3 reuses it for the child invariant and
replaces the existing child recency tie-break. It does not layer the authority
rule on top of that ranking. The Sepolia distinction above is unchanged.

## ENSv1→ENSv2 delivery slices

Each slice includes its behavior tests and fixture provenance. Counts are
estimated hand-written production files; test fixtures, test-only harness
files, and docs are not included.

| Slice | Coherent capability | Estimated production files |
| --- | --- | ---: |
| 1. Schema vocabulary, candidate ENSv1→ENSv2 intake, and inert replay | Extend the closed schema-v2 event/derivation vocabulary through a reviewed upgrade or full-rebuild path; admit fixed ENSv1→ENSv2 migration contracts; ratify [migration-registry](glossary.md#migration-registry-wrapperregistry) discovery; interpret every catalog event shape into candidate identity, discovery, and normalized effects; carry correlation-scoped visibility through existing-family effects without reclassifying independently admitted output; exclude candidate groups and association/effect tables from Project staging and product event/history reads; and defer every ENSv1→ENSv2 migration-driven `SurfaceBinding` transition. Diagnostics and internal provenance may change, but product rows and DTOs may not. | At least 21 (3 manifest TOML, up to 11 adapter/manifest Rust files, 2 schema contract/check files, at least 1 reviewed upgrade or rebuild mechanism file, and up to 4 Project/API/storage visibility modules) |
| 2. Exact-name current authority and consumer activation | Re-derive every candidate [migration correlation group](glossary.md#migration-correlation-group) as activated and enable its correlation-dependent normalized rows for product event/history reads. Only `authority_transition` groups perform the deferred predecessor-binding close and successor-binding open. Consume that activated boundary, plus a current child registration in an admitted migration registry below a proven migrated parent, to publish one current registration, expiry, resolver, control, address relation, permission summary, and exact-name coverage result while preserving both eras in history. Name detail, lookup, resolver-record, and verified-primary paths expose explicit unsupported reasons; address-name, search, and resolver-bound-name collections, plus name-filtered permission reads, publish only the selected registration and omit a name whose authority cannot be proven. Explicit-`registration_id` and address-filtered permission reads remain resource-centric audit views and carry `authority_context=resource_audit`. Introduce the post-rollback generation-failure audit for the exact-name invariant. The child-registration path does not invent a migration boundary. (upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L169 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L172 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L290 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L303 @ ens_v2@ccaeb58) | At least 13 (up to 4 Project builders, 6 API reason/read modules, 1 phase-runner persistence module, 1 storage diagnostic module, and 1 reviewed schema upgrade file) |
| 3. Direct-subname authority and publication invariant | Replace the recency tie-break with per-child authority, retain legitimate unmigrated ENSv1 children, fail Project publication when both ENSv1 and ENSv2 bindings remain current for one Mainnet pair, reuse the slice-2 post-rollback generation-failure audit, and cover same-transaction parent/child ENSv1→ENSv2 migration through the public subnames behavior. | At least 4 (up to 3 Project scope/builder files and 1 API mapping module) |

Slice 1 has a mandatory full-re-walk acceptance comparison against the
pre-admission generation. It proves identical product-visible row membership
and every DTO field for `name_current`, `children_current`,
`address_names_current`, both permission projections and `/v2/permissions`,
resolver and record reads, primary-name and search reads, `/v2/events`, and
name- and address-history reads, plus every GraphQL compatibility operation.
The comparison covers ordered pages, page membership, every REST and Manager
DTO field, summary/count fields, `has_more`, and cursor payloads as well as point
responses. Raw facts, candidate normalized events, diagnostic event
associations and identity/discovery effects, manifest metadata, internal
provenance, and content hashes are expected to change; product behavior is not.
The comparison runs over the complete planned re-walk, so a unit fixture that
filters only `ens_v2_migration_l1` cannot satisfy this gate.

Catalog-derived slice-1 fixtures preserve each decoded expiry instead of
reconstructing a fixed premigration delta. They also require ENSv1 registry
resolver- or TTL-clear events only when the cleared value actually changed:
`setRecord` delegates to a helper that compares both stored values before it
emits either event. (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L39 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L40 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L179 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L181 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L184 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L186 @ ens_v1@91c966f)

Slice 1 still fits the approximate production-file budget, but its inertness
contract requires narrow Project staging and API/storage history-selection
plumbing in addition to adapters, manifests, and fixtures. It also requires a
schema-migration or a reviewed full schema rebuild: `MigrationApplied`,
`ContractDiscovered`, `ens_v2_migration`, and the correlation-scoped visibility
provenance are outside the current closed schema-v2 contract. That requirement
is the stop condition for implementation in this change. Slices 2 and 3 remain
separate consumer capabilities rather than hidden prerequisites of source
admission.

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
