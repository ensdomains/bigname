# Consumer Capabilities

This document maps the consumer-facing capabilities served by the bigname API.
Wire format and route details live in [`api-v2.md`](api-v2.md) and
[`api-v2-routes.md`](api-v2-routes.md).

## Served route sets

| Set | Routes | Intended use |
| --- | --- | --- |
| Lookup | `POST /v2/lookup`, `GET /v2/status` | Batched name/address lookup and indexing readiness. |
| Product reads | `/v2/names/*`, `/v2/addresses/*`, `/v2/permissions`, `/v2/search`, `/v2/events`, `/v2/resolvers/*`, `/v2/namespaces/*` | Name, record, address, permission, event, resolver, and namespace reads. |
| Diagnostics | `/v2/diagnostics/*` | Coverage, binding, authority, record, execution, manifest, and event inspection. |
| GraphQL compatibility | `POST /graphql` | The documented narrow subgraph-compatible operations. |
| Operator health | `GET /healthz` | API-local process and database readiness. This is not a product route. |

The v1 REST surface has been removed. In particular,
`POST /v1/identity:lookup` no longer serves the native identity capability.
`POST /v2/lookup` owns batched forward and reverse lookup with the v2 envelope;
it does not preserve the deleted v1 DTOs.

## Capability mapping

| Capability | Route owner | Notes |
| --- | --- | --- |
| Batched forward and reverse lookup | `POST /v2/lookup` | `profile=feed` is the field-budgeted path; `profile=detail` returns the documented full record shape. |
| Indexing readiness | `GET /v2/status` | Per-chain projection progress, stored head, indexing-process liveness, and network-head readiness. |
| Exact name profile | `GET /v2/names/{name}` | Indexed or verified name and record fields, subject to the route's source rules. |
| Resolver records | `GET /v2/names/{name}/records` | Key-selected record reads plus inventory metadata. |
| Direct subnames | `GET /v2/names/{name}/subnames` | Latest-state direct-subname collection. |
| Name history | `GET /v2/names/{name}/history` | Name, registration, or combined history scope. |
| Names by address | `GET /v2/addresses/{address}/names` | Owner, manager, and registrant relations with optional expansions. |
| Primary name | `GET /v2/addresses/{address}/primary-name` | Indexed tuples and verified ENS coin-type 60 lookup as documented. |
| Address history | `GET /v2/addresses/{address}/history` | Latest-state address-anchored event history. |
| Permission holders | `GET /v2/permissions` | Current resource-anchored permission rows. |
| Search | `GET /v2/search` | Name search only; no registration, pricing, or availability workflow. |
| Events | `GET /v2/events` | Product event collection. |
| Resolver overview | `GET /v2/resolvers/{chain_id}/{address}` | Resolver metadata and bounded name expansion. |
| Namespace metadata | `GET /v2/namespaces/{namespace}` | Product-facing namespace and capability metadata. |
| Pipeline diagnostics | `/v2/diagnostics/*` | Explicit diagnostic tier, separate from product reads. |

The GraphQL compatibility operations read the schema-v2 current projections
and preserve the committed Manager response contract. They do not fall back to
the retained public-schema projections. Name inputs are ENS-normalized and
matched by namehash within the `ens`
namespace. While the `project` phase has not completed at the newest stored chain head,
operations that would return projection rows fail rather than serve the prior
publication. Unsupported name rows are omitted, and
unsupported record inventories preserve the existing empty record shapes.

All top-level v2 collections use the standard `page` object. Latest-state
collections do not claim a frozen snapshot; point-in-time behavior is limited
to the routes and selectors documented in [`api-v2-routes.md`](api-v2-routes.md).

## Retained storage after v1 removal

The deleted v1 identity route was the sole API reader of the
`address_names_current_identity_counts` and
`address_names_current_identity_feed` [sidecars](glossary.md). Their tables,
indexes, functions, and triggers are intentionally retained, but are orphaned
from API reads until the slice-3 schema cleanup.

This slice also retains the worker, the execution crate for worker use, legacy
storage writers, manifest legacy views, and public-schema tables. Their removal
or migration belongs to slice 3 and is not evidence that the deleted v1 API is
still served.

## Replacement boundary

The v2 route set is the current internal API contract. This document records
local route ownership only; it does not claim that an external application has
changed its call sites or that the production public edge exposes v2. The
checked-in Caddy configuration remains on the pre-C3 routing policy, so the v2
REST surface is not publicly reachable until the maintainer-gated C3 edge
flip.
