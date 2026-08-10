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
they do not acquire a new row-local mixed-authority status vocabulary; callers
inspect the exact-name or lookup result for coverage.

Slice 1 will admit the facts and record the interpreter-owned authority boundary;
it will not activate that replacement in public projections or API reason
mapping. Until slice 2 is activated, exact-name reads over a corpus containing
both families retain the existing `mixed_exact_name_corpus` public reason.
Slice 2 replaces that blanket refusal with the per-name exact-name rule.

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

## ENSv1→ENSv2 delivery slices

Each slice includes its behavior tests and fixture provenance. Counts are
estimated hand-written production files; test fixtures, test-only harness
files, and docs are not included.

| Slice | Coherent capability | Estimated production files |
| --- | --- | ---: |
| 1. Schema vocabulary, migration intake, and replay | Extend the closed schema-v2 event/derivation vocabulary through a reviewed upgrade or full-rebuild path; admit fixed migration contracts; ratify [migration-registry](glossary.md#migration-registry-wrapperregistry) discovery; interpret every catalog event shape into identity, discovery, and normalized events, including Graveyard claims and v1-renewal bridge events. No projection or API write path changes. | At least 17 (3 manifest TOML, up to 11 adapter/manifest Rust files, 2 schema contract/check files, and at least 1 reviewed upgrade or rebuild mechanism file) |
| 2. Exact-name current authority | Consume `MigrationApplied`, plus a current child registration in an admitted migration registry below a proven migrated parent, to publish one current binding, registration, expiry, resolver, control, address relation, permission summary, and exact-name coverage result while preserving both eras in history. Name detail, lookup, resolver-record, and verified-primary paths expose explicit unsupported reasons; address-name, permission, search, and resolver-bound-name collections publish only the selected registration and omit a name whose authority cannot be proven. The child-registration path does not invent a migration boundary. (upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L169 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L172 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L290 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L303 @ ens_v2@ccaeb58) | 10 (up to 4 project builders and 6 API reason/read modules) |
| 3. Direct-subname authority | Replace the recency tie-break with per-child authority, retain legitimate unmigrated ENSv1 children, fail Project publication when both ENSv1 and ENSv2 bindings remain current for one Mainnet pair, and cover same-transaction parent/child migration through the public subnames behavior. | 4 (up to 3 project scope/builder files and 1 API mapping module) |

Catalog-derived slice-1 fixtures preserve each decoded expiry instead of
reconstructing a fixed premigration delta. They also require ENSv1 registry
resolver- or TTL-clear events only when the cleared value actually changed:
`setRecord` delegates to a helper that compares both stored values before it
emits either event. (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L39 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L40 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L179 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L181 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L184 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L186 @ ens_v1@91c966f)

Slice 1 still fits the approximate production-file budget and does not require
project or API write-path plumbing, but it does require a schema-migration or a
reviewed full schema rebuild: `MigrationApplied`, `ContractDiscovered`, and
`ens_v2_migration` are outside the current closed schema-v2 constraints. That
requirement is the stop condition for implementation in this change. Slices 2
and 3 remain separate consumer capabilities rather than hidden prerequisites
of source admission.

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
