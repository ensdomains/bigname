# API v1

The bigname public read contract. Wire format for the model in [`architecture.md`](architecture.md). Per-route detail in [`api-v1-routes.md`](api-v1-routes.md). Machine-readable spec in [`api-v1.openapi.json`](api-v1.openapi.json).

## Conventions

- All routes live under `/v1`.
- JSON, `snake_case` keys.
- Timestamps are RFC 3339 UTC.
- Path-segment names are normalized, URL-encoded.
- `namespace` is explicit on canonical routes. Convenience routes that infer namespace echo it back in identity fields.
- Opaque internal IDs (UUIDs) are returned as-is. Clients never derive them.

## Query parameters

| Parameter | Meaning |
| --- | --- |
| `at` | Point-in-time selector — RFC 3339 timestamp or chain-position token. |
| `chain_positions` | Explicit per-chain positions, JSON object. |
| `consistency` | `head`, `safe`, `finalized`. |
| `mode` | `declared`, `verified`, `both`. |
| `include` | Route-specific expansions, comma-separated. |
| `view` | `compact` or `full`. |
| `meta` | `none`, `summary`, `full`. |
| `sort` | Route-specific stable sort. |
| `order` | `asc` or `desc`. |
| `cursor` | Opaque pagination cursor. |
| `page_size` | Default `50`, max `200`. |

Each route documents which subset it accepts. Defaults: `consistency=head`, `mode=declared`. With no `at` or `chain_positions`, the route uses the latest stored checkpoint per required chain.

## Snapshot selection

Every read resolves caller input to one `ChainPositions` object before any lookup, then uses that one object for everything in the response.

| Inputs | Result |
| --- | --- |
| `chain_positions` only | use them exactly |
| `at` only | resolve per-chain positions at `consistency` |
| neither | latest available at `consistency` |
| both | reject `400 invalid_input` |

Validation:

- Every chain the route requires must appear in `chain_positions` and use a position slot the route accepts.
- Malformed input, mixed-profile positions (mainnet + Sepolia), duplicate slots, or `chain_id` mismatched against the active profile — `400 invalid_input`.
- Positions below the requested `consistency` floor — `409 conflict`.
- A `(chain_id, block_number, block_hash)` not on stored canonical lineage, or that can't reconcile across chains as one snapshot — `409 conflict`.
- A coherent selector whose required projection rows aren't built — `409 stale`.
- Persisted-readback routes return `stale` or `not_found` when matching output is absent.

The exception: supported ENS verified resolution (`GET /v1/resolutions/{namespace}/{name}` and `GET /v1/resolve/{name}`) may execute on demand against the selected snapshot if no persisted output exists, then persist and return it.

### Cross-chain rules

ENS authoritative positions select on `ethereum-mainnet`. Basenames authoritative positions select on `base-mainnet` (upstream deploys the stack on Base[^bn-readme-base]). An auxiliary chain position is chosen at the same `consistency` with timestamp at or before the authoritative-chain timestamp.

Verified execution doesn't advance positions mid-request.

### Profiles

A runtime serves one profile. Mainnet and `sepolia-dev` chain keys never appear together in one response or one explicit `chain_positions`. The `sepolia-dev` profile supports declared exact-name profile reads against the admitted ETHRegistry/ETHRegistrar deployments[^v2-deploy-ethreg] — it does not enable mainnet, reverse/primary, wrapper authority, migration, Universal Resolver, verified resolution, or execution-explain surfaces.

## Response envelopes

### Full envelope (single resource)

```json
{
  "data": {},
  "declared_state": {},
  "verified_state": null,
  "provenance": {},
  "coverage": {},
  "chain_positions": {},
  "consistency": "head",
  "last_updated": "2026-04-16T00:00:00Z"
}
```

### Full envelope (collection)

Adds:

```json
{
  "page": {
    "cursor": null,
    "next_cursor": null,
    "page_size": 50,
    "sort": "display_name_asc"
  }
}
```

### Compact envelope (app-facing)

```json
{
  "data": [],
  "page": { "cursor": null, "next_cursor": null, "page_size": 50, "sort": "name_asc" },
  "meta": {
    "support_status": "partial",
    "unsupported_filters": [],
    "unsupported_fields": [],
    "total_count": null
  }
}
```

### Rules

- `declared_state` and `verified_state` are always present in the full envelope. If a route doesn't have one, it returns `null`.
- `mode=declared` populates `declared_state`, `verified_state=null`. `mode=verified` inverts. `mode=both` populates both.
- `coverage` is route-level completeness and enumeration basis, not freshness.
- `chain_positions` may carry multiple chains for cross-chain answers.
- `meta` levels: `none` omits it (collection `page` stays); `summary` adds support, unsupported filters/fields, count metadata, snapshot summary; `full` adds the full-envelope coverage, chain positions, consistency, last-updated, and route-level provenance.
- `view=full` returns the full envelope when documented; otherwise `400 invalid_input`.
- Compact responses never sneak raw facts, full provenance, or projection internals through `meta`. Explain detail belongs on explain routes.

## Exact-name snapshot

Routes that share the exact-name snapshot:

- `GET /v1/names/{namespace}/{name}`
- `GET /v1/coverage/{namespace}/{name}`
- `GET /v1/explain/names/{namespace}/{name}/surface-binding`
- `GET /v1/explain/names/{namespace}/{name}/authority-control`
- `GET /v1/resolutions/{namespace}/{name}` — data, declared topology, inventory/cache, coverage, verified support, execution target

Rules:

- The snapshot is resolved once before any lookup, topology build, explain build, or execution.
- Every section uses that one snapshot. No mixing rows from different snapshots.
- A `_current` row whose stored chain context predates the selected snapshot stays eligible only if no newer canonical input exists for the same key through the selected positions, and the chain set matches.
- `coverage` for `{namespace, name}` matches across `GET /v1/names/.../{name}` and `GET /v1/coverage/.../{name}`.
- Explain routes resolve the same `logical_name_id`, `resource_id`, `token_lineage_id`, `surface_binding_id`, and `binding_kind` as the exact-name route.
- `mode=verified|both`: persisted verified output joins only when stored chain positions exactly match. If output is missing for a supported ENS selector, the route executes against the selected snapshot, persists, and returns the outcome. Live execution requires an Ethereum RPC provider on the API process — without one, supported selectors return `409 stale` with a configuration message. No declared-cache fallback.
- `GET /v1/resolve/{name}` infers namespace and uses the canonical default snapshot. It doesn't accept `at`, `chain_positions`, or `consistency`.

## Shared objects

Brief reference. Detailed shapes in [`api-v1-routes.md`](api-v1-routes.md).

### `NameRef`

`logical_name_id`, `namespace`, `normalized_name`, `canonical_display_name`, `namehash`, `resource_id`, `binding_kind`.

### `ResourceRef`

`resource_id`, `authority_epoch`, `token_lineage_id`, `current_resolver`.

### `ChainPositions`

`{ ethereum, base, execution_checkpoint }`. Each position: `chain_id`, `block_number`, `block_hash`, `timestamp`.

### `Provenance`

`normalized_event_ids`, `raw_fact_refs`, `manifest_versions`, `execution_trace_id`, `derivation_kind`. `execution_trace_id` only appears when execution-derived material participates.

### `Coverage`

| Field | Values |
| --- | --- |
| `status` | `full`, `partial`, `observed_only`, `unsupported`, `stale` |
| `exhaustiveness` | `authoritative`, `best_effort`, `observed_only`, `non_enumerable`, `not_applicable` |
| `source_classes_considered` | source families the route consulted |
| `enumeration_basis` | how the route enumerated (e.g. `exact_name_profile`, `primary_name_lookup`) |
| `unsupported_reason` | required when `status=unsupported` |

### `ResultStatus`

`success`, `not_found`, `mismatch`, `unsupported`, `invalid_name`, `execution_failed`. Used on `record_cache.entries`, `verified_queries`, `claimed_primary_name`, `verified_primary_name`. Each route documents the subset it uses.

- `status` is always present.
- Request-identity fields stay present even when `status != success`.
- `unsupported_reason` is required for `unsupported`.
- `failure_reason` may appear on `not_found`, `mismatch`, `invalid_name`, `execution_failed`.
- Concrete value/identity fields appear only when the route established a concrete answer.

### `UnsupportedSummary`

`{ status: "unsupported", unsupported_reason }`. Used when a documented declared subdocument exists but isn't projected for this resource.

### `ExactNameControlSummary`

`registrant`, `registry_owner`, `latest_event_kind`. Narrow current-resource summary — not `ControlVector`, not a permissions ledger. Keys stay present when supported; values may be `null` when the authority epoch doesn't expose that subject.

### `ExactNameResolverSummary`

`chain_id`, `address`, `latest_event_kind`. Topology-only target identity. `chain_id=null, address=null` means "no declared resolver" — not unsupported.

For ENSv1, complete family coverage and resolver-overview support require admission to an ENS Labs PublicResolver-generation profile.[^v1-pres] Retained generic resolver-local events may produce observed cache successes while profile state stays `pending`.

For Basenames, complete family coverage requires `L2Resolver`-compatible profile admission for the discovered Base resolver.[^bn-l2resolver] The ENSv1 profile gate, L1 transport, and offchain gateways don't satisfy this.

### `ResolutionTopology`

| Field | Shape |
| --- | --- |
| `registry_path` | ordered `NameRef[]` from the requested surface toward registry authority. Never empty when topology is supported. |
| `subregistry_path` | toward the nearest declared subregistry ancestor. Empty when none participates. |
| `resolver_path` | ordered `ResolutionResolverHop[]`. Last hop is the selected resolver. |
| `wildcard` | `{ source: NameRef\|null, matched_labels: string[] }`. `null/[]` if wildcard didn't participate. |
| `alias` | `{ final_target: NameRef\|null, hops: NameRef[] }`. `null/[]` if alias didn't participate. |
| `version_boundaries` | `{ topology_version_boundary, record_version_boundary }`. |
| `transport` | `{ source_chain_id, target_chain_id, contract_address, latest_event_kind }`. All `null` for no transport. |

For Basenames, supported transport is `base-mainnet → ethereum-mainnet` through the L1 Resolver.[^bn-l1resolver]

`record_version_boundary` is identical across `topology.version_boundaries`, `record_inventory`, and `record_cache` when those sections are supported.

### `ResolutionRecordSelector`

`record_key`, `record_family`, `selector_key`, `cacheable`. `record_key = record_family + ":" + selector_key` when `selector_key` is non-null. Numeric domains (coin types) stay textual on the wire.

### `ResolutionRecordInventory`

```
record_version_boundary
enumeration_basis: { observed_selectors, capability_declared_families, globally_enumerable }
selectors: ResolutionRecordSelector[]    # sorted by record_key
explicit_gaps: ResolutionRecordGap[]     # sorted by record_key
unsupported_families: ResolutionUnsupportedRecordFamily[]   # sorted by record_family
last_change: HistoryPointer | null
```

May be authoritative for exact lookup while `globally_enumerable=false`. When `topology.resolver_path` ends in the explicit no-resolver hop, inventory is supported with empty selectors and `record_cache.entries[*]` return `not_found` rather than `unsupported`.

### `ResolutionRecordCache`

`record_version_boundary`, `entries: ResolutionRecordCacheEntry[]`. Declared cache uses `success`, `not_found`, `unsupported` only. `value` appears only on `success`, family-native JSON shape.

### Compact summaries

`CompactDomainSummary`, `CompactRecordSummary`, `CompactHistoryEvent`, `RoleRow`, `ResolverOverviewCompact`, `ResolverOverviewBindingItem`, `ResolverOverviewBindingSummary` — see [`api-v1-routes.md`](api-v1-routes.md) for fields.

## Routes

| Route | Purpose |
| --- | --- |
| `GET /v1/namespaces/{namespace}` | Namespace metadata. |
| `GET /v1/manifests/{namespace}` | Active manifest versions and capabilities. |
| `GET /v1/names` | Compact name search, exact lookup, address relations, suggestions. |
| `GET /v1/names/{namespace}/{name}` | Exact name lookup (full envelope). |
| `GET /v1/names/{namespace}/{name}/children` | Direct children. |
| `GET /v1/names/{namespace}/{name}/records` | Compact resolver records. |
| `GET /v1/names/{namespace}/{name}/roles` | Compact role rows for the name's current resource. |
| `GET /v1/coverage/{namespace}/{name}` | Single-name coverage and explain detail. |
| `GET /v1/explain/names/{namespace}/{name}/surface-binding` | Current surface-binding explain. |
| `GET /v1/explain/names/{namespace}/{name}/authority-control` | Current authority/control explain. |
| `GET /v1/explain/resolutions/{namespace}/{name}/execution` | Persisted verified execution explain. |
| `GET /v1/addresses/{address}/names` | Address-to-surface collection. |
| `GET /v1/addresses/{address}/names/count` | Count companion. |
| `GET /v1/history/names/{namespace}/{name}` | Surface or combined history. |
| `GET /v1/history/resources/{resource_id}` | Resource history. |
| `GET /v1/history/addresses/{address}` | Address activity. |
| `GET /v1/events` | Compact event search. |
| `GET /v1/roles` | Compact role rows by account, resource, or name. |
| `GET /v1/resources/lookup` | `{namespace, name}` → current `resource_id`. |
| `GET /v1/resources/{resource_id}/permissions` | Resource-centric effective permissions. |
| `GET /v1/resolvers/{chain_id}/{resolver_address}` | Resolver overview (full). |
| `GET /v1/resolvers/{chain_id}/{resolver_address}/overview` | Compact resolver overview. |
| `GET /v1/resolutions/{namespace}/{name}` | Resolution topology, inventory, cache, verified queries. |
| `GET /v1/resolve/{name}` | Namespace-inferred resolution. |
| `GET /v1/resolve/{name}/records` | Namespace-inferred compact records. |
| `GET /v1/primary-names/{address}` | Claimed and verified primary name. |
| `GET /healthz` | Liveness. Not part of `v1`. |

The running API also serves `GET /openapi.json` and `GET /docs`. They aren't `v1` business routes and don't appear in `api-v1.openapi.json`.

## Sorting and pagination

| Route | Default sort |
| --- | --- |
| `GET /v1/names` | `name_asc` |
| `GET /v1/addresses/{address}/names` | `display_name_asc` |
| `GET /v1/names/{namespace}/{name}/children` | `display_name_asc` |
| `GET /v1/resources/{resource_id}/permissions` | `subject_scope_asc` |
| `GET /v1/roles` | `account_resource_scope_asc` |
| `GET /v1/names/{namespace}/{name}/roles` | `account_scope_asc` |
| `GET /v1/history/names/{namespace}/{name}` | `chain_position_desc` |
| `GET /v1/history/resources/{resource_id}` | `chain_position_desc` |
| `GET /v1/history/addresses/{address}` | `chain_position_desc` |
| `GET /v1/events` | `chain_position_desc` |

Other routes don't honor `cursor` or `page_size`.

Surface-first views break ties on `logical_name_id`. Resource-grouped address views break on `resource_id`. `page.cursor` echoes the applied cursor or `null` on the first page; `page.next_cursor=null` means no further rows at the requested snapshot. Cursors are stable under replay for the same chain positions.

## Errors

```json
{
  "error": {
    "code": "unsupported",
    "message": "the requested route option is not supported",
    "details": {}
  }
}
```

Codes: `invalid_input`, `not_found`, `unsupported`, `stale`, `verification_failed`, `conflict`, `internal_error`.

| Code | When |
| --- | --- |
| `invalid_input` | Malformed selector, unsupported position slot, missing required slot, mixed-profile positions, `at` plus `chain_positions`. |
| `stale` | Coherent selector that can't be served from current projections. |
| `conflict` | Selector whose lineage, canonicality floor, or cross-chain reconciliation can't form one snapshot. |
| `unsupported` | Request can't produce the route contract at all. |

When a route can produce the envelope but a subsection is unsupported, return `200` and surface state through `UnsupportedSummary` or `ResultStatus.unsupported`. Don't use the top-level `unsupported` error in that case.

Persisted-readback routes return their documented `stale` or `not_found` state when matching output is missing. Supported ENS verified resolution executes on demand instead, then returns `409 stale` with a configuration message if the Ethereum RPC provider is unconfigured or can't serve the selected block.

## Versioning

- New optional fields and new routes are additive within `v1`.
- Changing enum meaning, default sort, coverage semantics, or required fields requires `v2`.
- An unsupported capability is reported through `coverage` or `error`. Never silent omission.

---

[^bn-readme-base]: (upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc)
[^v2-deploy-ethreg]: (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistry.json:L2 @ ens_v2@554c309)
[^v1-pres]: (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L20 @ ens_v1@91c966f)
[^bn-l2resolver]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L22 @ basenames@1809bbc)
[^bn-l1resolver]: (upstream: .refs/basenames/src/L1/L1Resolver.sol:L13 @ basenames@1809bbc)
