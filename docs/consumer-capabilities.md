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

Exact-name, direct-subname, history, address-name, and permission reads use
the per-name current-authority rule in
[`architecture.md`](architecture.md#ensv1ensv2-current-authority). A migrated
Mainnet name keeps both eras in history while current registration, control,
resolver, expiry, address relations, and permissions come only from its
ENSv2 resource. Retained ENSv1 facts remain history and provenance; they do
not make the current read unsupported and cannot become current again after
an ENSv2 release.

Direct-subname ownership is evaluated per child. A child that has not
migrated can remain ENSv1-authoritative below a migrated parent. Once that
child migrates or otherwise obtains a current ENSv2 registration, the ENSv2
parent-child arm replaces the ENSv1 arm. A Mainnet pair left current in both
arms after applying those boundaries is explicit unsupported anomaly data,
not a tie to resolve by event recency. Sepolia overlap is instead an expected
property of independent test deployments and remains unsupported under its
own reason until a caller or deployment profile selects one system.

## ENSv1→ENSv2 delivery slices

Each slice includes its behavior tests and fixture provenance. Counts are
estimated hand-written production files; test fixtures, test-only harness
files, and docs are not included.

| Slice | Coherent capability | Estimated production files |
| --- | --- | ---: |
| 1. Migration intake and replay | Admit fixed migration contracts, ratify migration-registry discovery, interpret every catalog event shape into identity, discovery, and normalized events, including Graveyard claims and v1-renewal bridge events. No projection or API write path changes. | 12 (2 manifest TOML, up to 10 adapter/manifest Rust files) |
| 2. Exact-name current authority | Consume `MigrationApplied` to publish one current binding, registration, expiry, resolver, control, address relation, permission summary, and exact-name coverage result while preserving both eras in history. | 6 (up to 4 project builders and 2 API reason/read modules) |
| 3. Direct-subname authority | Replace the recency tie-break with per-child authority, retain legitimate unmigrated ENSv1 children, surface double-current Mainnet pairs as anomalies, and cover same-transaction parent/child migration through the public subnames behavior. | 4 (up to 3 project scope/builder files and 1 API mapping module) |

Slice 1 is bounded to adapters, manifests, and their fixtures and requires no
schema-migration. Slices 2 and 3 are separate consumer capabilities rather
than hidden prerequisites of source admission.

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
