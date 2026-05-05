# Consumer capabilities

What apps actually need from bigname, and which routes serve it. Identity and resolution semantics live in [`architecture.md`](architecture.md); wire format in [`api-v1.md`](api-v1.md).

## Capability matrix

| Capability | Where it shows up | Routes |
| --- | --- | --- |
| Exact name profile | profile pages, registration views | `GET /v1/names/{namespace}/{name}` (full), `GET /v1/names?namespace=…&name=…` (compact) |
| Names by address | dashboards, search | `GET /v1/names?account=…&relation=token_holder|any` |
| Names by address with role summary | dashboard lists | `GET /v1/addresses/{address}/names?include=role_summary` |
| Direct subnames | subname pages | `GET /v1/names/{namespace}/{name}/children` |
| Record inventory and cache | profile records, editing | `GET /v1/resolutions/{namespace}/{name}` (full), `GET /v1/names/.../records` and `GET /v1/resolve/{name}/records` (compact) |
| Verified record reads | profile, send, address-resolution | `Resolution.verified_queries` plus `GET /v1/explain/resolutions/{namespace}/{name}/execution` |
| Name history | profile history | `GET /v1/history/names/{namespace}/{name}` |
| Address history | activity views | `GET /v1/history/addresses/{address}` |
| Role holders | roles pages | `GET /v1/resources/{resource_id}/permissions` |
| Role history | roles history | `GET /v1/history/resources/{resource_id}` |
| Resolver overview | resolver pages | `GET /v1/resolvers/{chain_id}/{resolver_address}` (full), `.../overview` (compact) |
| Claimed vs verified primary name | dashboards, profile | `GET /v1/primary-names/{address}` |
| Compact name search | search, explorer | `GET /v1/names?prefix=…` or `contains=…` |
| Compact events | activity tables | `GET /v1/events`, history routes with `view=compact` |
| Roles by account/resource/name | resolver and role pages | `GET /v1/roles`, `GET /v1/names/.../roles`, `GET /v1/resources/lookup` |

Compact defaults across these routes hide provenance, full coverage, internal projection identifiers, and raw normalized-event payloads unless the caller opts in via `meta=full`, `view=full`, or an explain route.

`GET /v1/resolve/{name}` and `GET /v1/resolve/{name}/records` are convenience entries to the same `Resolution` and compact-records capabilities. Exact `base.eth` infers `namespace=ens`, `*.base.eth` infers `namespace=basenames`, other supported ENS names infer `namespace=ens`. Inferred Basenames requests use Basenames-local support and don't fall back to ENS.

## Coverage notes

- **`Address.names` with `include=role_summary`** is an additive expansion of the same address-to-surface collection — not a separate route. It adds `role_summary`, `subname_count`, `record_count`, `status`, `expiry`.
- **`Resolution`** is one mixed route. `record_inventory` defines the known record-selector space; `record_cache` is the declared last-known-value view; `verified_queries` is execution-derived.
- **ENSv2 exact-name profile** is supported only on the `sepolia-dev` deployment profile when `ens_v2_registrar_l1` declares `exact_name_profile = "supported"`. That covers exact-name profile reads from the admitted ETHRegistry and ETHRegistrar; it doesn't graduate resolver-profile support, verified resolution, primary names, or history coverage.[^v2-deploy-ethreg]
- **ENSv1 records and resolver overview** require ENS Labs PublicResolver-generation profile admission for complete family coverage and event-to-call parity. Retained generic resolver-local events provide observed selector and cache facts while a profile is `pending`. Malformed topic collisions stay raw without contributing.[^v1-pres] Shared PublicResolver targets don't enumerate current-name fan-in in resolver-overview `bindings`, `aliases`, or event summaries — those return explicit `UnsupportedSummary` with `resolver_binding_enumeration_not_projected`. Exact-name resolver state stays on exact-name routes.
- **Verified resolution and the resolver-profile gate** are independent. For an already supported verified-resolution path, `resolver_family_pending` declared state stays visible in `record_inventory` and `record_cache` but doesn't suppress matching persisted Universal Resolver readback.
- **Basenames declared resolver-profile** support is `L2Resolver`-compatible only. A discovered Base resolver that is watched but has `pending` or `unsupported` profile state stays topology-only. The Mainnet `L1Resolver`, `basenames_execution`, and any offchain gateway don't satisfy declared resolver-profile support.[^bn-l2resolver]
- **ENSv1 dynamic resolver-profile admission** is profile-exact, not latest-only. A resolver with `pending`/`unsupported` profile state may expose only observed selector and cache facts. An admitted legacy generation satisfies only the families listed for that profile.
- **ENSv1 pubkey** evidence is unadmitted. Known PublicResolver generations keep it `unsupported`; unknown resolvers keep it `pending`.
- **ENSv1 reverse/primary `NameChanged` text** is preimage intake only.[^v1-namech] It can attach already-observed forward-node facts to a human-readable name; it doesn't create primary-name truth, exact-name authority, or record support without those forward-node facts.
- **`ENSRegistryOld`** is admitted as migration-aware input under `ens_v1_registry_l1`. Current-registry `NewOwner` migration, suppression of later old-registry topology for migrated nodes, and the root-resolver exception are honored before any old-registry fact contributes to declared reads.[^subgraph-old]
- **`PrimaryName`** is one mixed route. `claimed_primary_name` is declared; `verified_primary_name` is execution-derived. Route-level coverage is `partial` for the ENS and Basenames exact-tuple persisted-readback classes and explicit `unsupported` outside them.

Both mixed routes (`Resolution`, `PrimaryName`) reuse the same `ResultStatus` vocabulary: `success`, `not_found`, `mismatch`, `unsupported`, `invalid_name`, `execution_failed`.

## Out of scope

| Capability | Reason |
| --- | --- |
| Favorites, local services | App-local, not a chain or indexer concern. |
| Name availability | Direct contract / pricing service. |
| Registration pricing | Direct contract. |
| Direct contract workflows | Direct contract. |
| DNSSEC, app images, faucet | App-local. |
| Direct reverse checks not backed by a projection | Use `GET /v1/primary-names/{address}` instead. |

## Deferred

These are off the menu until the relevant projection or fan-in exists:

- resolved-address listing
- `resource_hex` lookup
- selector-specific record history beyond event filters
- linked / alias / wildcard child buckets
- unprojected resolver fan-in

Unsupported filters or sections return explicit `unsupported` state, never silent empty results.

---

[^v2-deploy-ethreg]: (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistry.json:L2 @ ens_v2@554c309)
[^v1-pres]: (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L20 @ ens_v1@91c966f)
[^bn-l2resolver]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L22 @ basenames@1809bbc)
[^v1-namech]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/NameResolver.sol:L10 @ ens_v1@91c966f)
[^subgraph-old]: (upstream: .refs/ens_subgraph/subgraph.yaml:L39 @ ens_subgraph@723f1b6)
