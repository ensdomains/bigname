# API v1 Routes

Per-route reference. Conventions, snapshot selection, envelopes, shared objects, and errors live in [`api-v1.md`](api-v1.md).

## Namespaces and manifests

### `GET /v1/namespaces/{namespace}`

Manifest-backed metadata for one public namespace.

`declared_state`: `active_manifest_count`, `active_source_families`, `chains`, `normalizer_versions`.

- `200` with empty lists and `active_manifest_count=0` when the namespace is public but has no active manifests.
- `404 not_found` for unsupported namespace.
- Per-manifest capability flags live on the next route.

### `GET /v1/manifests/{namespace}`

Active manifest versions and capability flags. Declared-only.

## Names

### `GET /v1/names/{namespace}/{name}`

Exact name lookup. Full envelope. Uses the exact-name snapshot.

Query: `at`, `chain_positions`, `consistency`.

`data`: surface identity (`logical_name_id`, `namespace`, `normalized_name`, `canonical_display_name`, `namehash`) plus binding (`resource_id`, `token_lineage_id`, `binding_kind`).

`declared_state`:

- `registration`
- `authority`
- `control` — `ExactNameControlSummary | UnsupportedSummary`
- `resolver` — `ExactNameResolverSummary | UnsupportedSummary`
- `record_inventory` — `ResolutionRecordInventory | UnsupportedSummary`
- `history` — `ExactNameHistorySummary | UnsupportedSummary`

Rules:

- Authoritative for supported source classes even when individual sections are unsupported.
- Every declared section is always present as an object. Missing projections return `UnsupportedSummary`.
- `declared_state.authority` falls back to `{resource_id, token_lineage_id, binding_kind}` when no dedicated authority summary is projected.
- For `namespace=ens` on `sepolia-dev`, the promoted exact-name profile is supported, backed by `ens_v2_registry_l1` and `ens_v2_registrar_l1`. Coverage: `status=full`, `exhaustiveness=authoritative`, `enumeration_basis=exact_name_profile`.[^v2-deploy-ethreg][^v2-iperm]
- For `namespace=basenames`, declared truth comes from the Base authority split (`basenames_base_registry`, `basenames_base_registrar`, `basenames_base_resolver`). Claim and transport families don't widen this route.[^bn-readme-base]
- `declared_state.control` is the narrow current-resource summary. Full role/permission lineage stays on the dedicated permissions route.
- Supported `declared_state.resolver` uses `chain_id, address` keying compatible with `GET /v1/resolvers/{chain_id}/{resolver_address}`. Both `null` means no declared resolver — not unsupported.
- `declared_state.record_inventory` shape matches `GET /v1/resolutions/{namespace}/{name}` for the same snapshot.
- `declared_state.history.surface_head` and `resource_head` point at the first canonical row of `GET /v1/history/names/{namespace}/{name}` under `scope=surface` and `scope=resource`.
- `coverage` matches `GET /v1/coverage/{namespace}/{name}` for the same `{namespace, name}` and snapshot.
- No `include` expansions. `verified_state` is `null`.

### `GET /v1/coverage/{namespace}/{name}`

Single-name coverage and explain detail. Same exact-name snapshot.

Query: `at`, `chain_positions`, `consistency`.

`data` identifies the same surface and binding as the exact-name route. The top-level `coverage` equals the inline `coverage` from `GET /v1/names/{namespace}/{name}` for the same request. `verified_state` is `null`.

### `GET /v1/explain/names/{namespace}/{name}/surface-binding`

Surface-binding explain over the same exact-name target.

Query: `at`, `chain_positions`, `consistency`.

`declared_state.surface_binding`: `SurfaceBindingExplainSummary`. `declared_state.history`: `ExactNameHistorySummary | UnsupportedSummary`.

`surface_binding_id` identifies the current `SurfaceBinding` row whose `binding_kind` matches the exact-name answer. No historical binding rows.

### `GET /v1/explain/names/{namespace}/{name}/authority-control`

Authority/control explain over the same exact-name target.

Query: `at`, `chain_positions`, `consistency`.

`declared_state.authority` (same shape and fallback as the exact-name route) and `declared_state.control` (same `ExactNameControlSummary | UnsupportedSummary`).

Row-granular permission lineage stays on `GET /v1/resources/{resource_id}/permissions`.

### `GET /v1/names`

Compact app-facing collection: exact lookup, address-owned lists, owner/registrant/effective-controller relations, name search, suggestions.

Query: `namespace`, `name`, `prefix`, `contains`, `contains_nocase`, `owner`, `account`, `registrant`, `resolver`, `resolved_address`, `relation=token_holder|registrant|effective_controller|any`, `sort=name|expiry_date|registration_date|created_at`, `order=asc|desc`, `include=record_summaries,total_count`, `view=compact|full`, `meta=none|summary|full`, `cursor`, `page_size`.

Defaults: `view=compact`, `meta=summary`, `relation=any` when `account` is supplied, `sort=name`, `order=asc`.

Each compact item is `CompactDomainSummary`.

Rules:

- Without `namespace`, items span supported public namespaces and each carries its own `namespace`.
- `name` is exact lookup by normalized name. With `namespace`, it returns zero or one item.
- `prefix`, `contains`, `contains_nocase` are search filters. They aren't availability checks.
- `owner` is `account` with `relation=token_holder`. Supplying both is `400 invalid_input`.
- `relation=any` returns the deduped union of token-holder, registrant, and effective-controller matches by `(namespace, normalized_name)`.
- `resolver` filters by current declared resolver address where the exact-name resolver summary is projected.
- `resolved_address` requires a declared, replay-stable record-value equality projection. Otherwise non-2xx `unsupported`.
- Sort orders break ties on `(namespace, normalized_name, namehash)`. `null` sort values order after non-null on `asc`, before on `desc`.
- `include=record_summaries` adds compact record counts, known text-key hints, avatar/content-hash presence, known coin-type hints — declared inventory/cache only, no verified execution.
- `include=total_count` populates `meta.total_count` for the filtered set before cursor slicing where supported. Unsupported combinations leave `total_count=null` and add it to `meta.unsupported_fields`.
- `view=full` is reserved and returns `400 invalid_input` until the full item shape is documented.

### `GET /v1/names/{namespace}/{name}/children`

Direct children. Compact by default.

Query: `surface_classes=declared`, `include=counts`, `view=compact|full`, `meta=none|summary|full`, `cursor`, `page_size`.

Each compact item: `name`, `normalized_name`, `label_name`, `labelhash`, `namehash`, `owner`, `registrant`, `subname_count`.

Rules:

- `view=full` returns the existing full-envelope declared child collection.
- `labelhash` is `null` when the projection doesn't carry a stable label hash.
- `owner` and `registrant` are `null` when not projected for that child; this isn't route-level unsupported.
- `include=counts` adds `subname_count` per child where projected. Unprojected fields appear in `meta.unsupported_fields` unless `meta=none`.
- `surface_classes=linked|alias|wildcard` is reserved and returns `unsupported`.
- For `namespace=basenames`, child surfaces come from the admitted Base authority split only.[^bn-readme-base]
- Pages over `display_name_asc`.

### `GET /v1/names/{namespace}/{name}/records`

Compact resolver records over declared inventory/cache and optional verified selectors. Current-projection read; no normalized-event catch-up scans.

Query: `mode=auto|declared|verified|both`, `texts`, `known_text_keys=true|false`, `avatar=true|false`, `content_hash=true|false`, `coin_types`, `include=resolver_address,known_text_keys,avatar,content_hash,coins`, `view=compact|full`, `meta=none|summary|full`.

Defaults: `mode=declared`, `view=compact`, `meta=summary`, `include=resolver_address`.

`data` is `CompactRecordSummary` for `view=compact`.

Rules:

- `resolver_address` is the current declared resolver target. `null` means no declared resolver, not a verified failure.
- `texts` returns requested keys in `text_records` from the selected value source.
- `known_text_keys=true` returns projected inventory metadata, not verified enumeration.
- `avatar=true` is a compact alias for the `avatar` text key.
- `content_hash=true` requests the declared content-hash selector.
- `coin_types` is a comma-separated list of textual coin-type selectors.
- `mode=declared` uses `record_cache` and `record_inventory`. No live execution.
- `mode=verified|both` uses the same supported boundary as `GET /v1/resolutions/{namespace}/{name}`. Supported ENS cache misses execute live through the configured Ethereum RPC provider using `latest`.
- `mode=auto`: an authoritative declared profile uses local inventory/cache. Otherwise supported requested selectors use verified output, including non-persisted on-demand Universal Resolver execution at provider `latest` when no exact-snapshot output exists.
- Without declared selectors, `mode=auto|verified|both` may probe the basic app profile set: `addr:60`, `avatar`, `contenthash`, text keys `description`, `url`, `email`.
- On-demand `latest` calls return inline; they don't create exact-snapshot execution cache rows or block-anchored `raw_call_snapshots`. Use `GET /v1/resolutions/{namespace}/{name}` for persisted exact-block provenance.

### `GET /v1/names/{namespace}/{name}/roles`

Compact role rows for the name's current resource.

Query: `account`, `role_bitmap`, `view=compact|full`, `meta=none|summary|full`, `cursor`, `page_size`.

Resolves the current `resource_id` for `{namespace, name}` at the exact-name snapshot and returns `RoleRow` items.

## Addresses

### `GET /v1/addresses/{address}/names`

Address-to-surface collection. Returns surfaces, not backing resources.

Query: `namespace`, `relation=registrant|token_holder|effective_controller`, `dedupe_by=surface|resource`, `include=role_summary`, `cursor`, `page_size`.

Each item: `logical_name_id`, `namespace`, `normalized_name`, `canonical_display_name`, `namehash`, `resource_id`, `binding_kind`, `relation_facets`. With `include=role_summary`, also `role_summary: RoleSummary`, `subname_count`, `record_count`, `status`, `expiry`.

Rules:

- `dedupe_by=surface` is the default. `dedupe_by=resource` is grouping-only — it doesn't change coverage or turn the route into a resource collection.
- Default sort is `display_name_asc`.
- `include=role_summary` is additive: it groups current `permissions_current` rows by `subject` with `(scope, effective_powers)` pairs. Row-granular grant lineage stays on the dedicated permissions route.
- `subname_count` reuses declared-direct-child semantics from the children route.
- `status` and `expiry` mirror the current `ControlVector.status` and `ControlVector.expiry` for the item's `resource_id`.
- `record_count` counts distinct stable declared selectors at the current version boundary (same shape as `Resolution.record_inventory`).
- For `namespace=basenames`, address-name membership and relations come from the Base authority split. Reverse-claim and transport state don't add rows.[^bn-readme-base]

### `GET /v1/addresses/{address}/names/count`

Count companion to the address relation filters.

Query: `namespace`, `relation=token_holder|registrant|effective_controller|any`, `prefix`, `contains`, `contains_nocase`, `resolver`.

```json
{
  "data": { "address": "0x…", "namespace": "ens", "relation": "token_holder", "count": 0 },
  "meta": { "support_status": "partial", "unsupported_filters": [] }
}
```

`relation=any` counts the deduped union. Filter support matches `GET /v1/names`. The count is over the filtered set before any cursor slice.

## Resources and roles

### `GET /v1/resources/lookup`

Compact lookup from `{namespace, name}` to current `resource_id`.

Query: `namespace`, `name` (both required), `view=compact|full`, `meta=none|summary|full`.

```json
{
  "data": {
    "namespace": "ens",
    "name": "alice.eth",
    "normalized_name": "alice.eth",
    "resource_id": "00000000-0000-0000-0000-000000000000",
    "resource_hex": null
  }
}
```

`resource_id` is the stable API key. `resource_hex` is deferred unless a stable projected field is documented; clients must not derive it from `resource_id`, `namehash`, token id, or calldata.

### `GET /v1/resources/{resource_id}/permissions`

Resource-centric current effective permission rows.

Query: `subject`, `scope`, `cursor`, `page_size`.

Each item: `resource_id`, `subject`, `scope`, `effective_powers`, `grant_source`, `revocation_source`, `inheritance_path`, `transfer_behavior`.

Rules:

- `resource_id` is the truth anchor. Surface names and resolver addresses appear as explanatory context only.
- `effective_powers` is server-computed post-scope-modifier. Clients don't apply NameWrapper fuse masks themselves.
- Resolver-scoped permissions are rows in this collection with resolver-scope detail, not a separate truth system.
- For ENSv1 wrapper-backed resources, current NameWrapper fuses are folded into `effective_powers` before publication. Burned fuses remove any power that depends on the prohibited operation; rows whose powers are fully masked are omitted.[^v1-nw-fuses]
- A wrapper-backed answer is `coverage.status=full` only when the current fuse modifier was applied. If the projection can't prove current fuse state, the route fails closed instead of returning unmasked powers.
- Pages over `subject_scope_asc`. Declared-state only.

### `GET /v1/roles`

Compact role rows by account, resource, or name.

Query: `account`, `resource_id`, `namespace`, `name`, `role_bitmap`, `view=compact|full`, `meta=none|summary|full`, `cursor`, `page_size`.

Defaults: `view=compact`, `meta=summary`, sort `account_resource_scope_asc`.

Each item is `RoleRow`.

Rules:

- At least one of `account`, `resource_id`, or `{namespace, name}` is required.
- `{namespace, name}` resolves through `GET /v1/resources/lookup` semantics, then reads current effective permission rows for that resource.
- `account` filters by effective permission subject. It doesn't search owner, registrant, or address-name relations unless those subjects also exist in `permissions_current`.
- `role_bitmap` filters only when the projection exposes it.
- `effective_powers` remains the API-owned post-scope result. Don't infer powers from `role_bitmap` alone.

## Resolvers

### `GET /v1/resolvers/{chain_id}/{resolver_address}`

Resolver overview. Full envelope. No query parameters.

`data` identifies the resolver target. `declared_state`:

- `bindings` — `ResolverOverviewBindingSummary | UnsupportedSummary`
- `aliases` — `ResolverOverviewBindingSummary | UnsupportedSummary`
- resolver-scoped permissions
- role-holder summary
- resolver event summary

Rules:

- Supported enumerable `bindings` include every current resolver-linked binding whose target matches the route, regardless of `binding_kind`.
- `aliases` reuses the same `{status, count, items}` envelope, narrowed to `binding_kind=resolver_alias_path`.
- For an enumerable target with no current alias binding, `aliases` returns `{status:"supported", count:0, items:[]}`.
- For ENSv1 PublicResolver-generation targets, `bindings`, `aliases`, and resolver event fan-in summaries return `UnsupportedSummary` with `unsupported_reason="resolver_binding_enumeration_not_projected"` rather than enumerating every name pointing at a shared resolver. Exact-name resolver state stays on exact-name and resolution routes.
- A discovered ENSv1 target with `pending` or `unsupported` profile state, or an admitted legacy generation without the requested family, returns explicit `UnsupportedSummary`.
- For Basenames, a discovered target requires `L2Resolver`-compatible `supported` profile state. The ENSv1 gate, L1 transport, and offchain gateways don't satisfy this.[^bn-l2resolver]

### `GET /v1/resolvers/{chain_id}/{resolver_address}/overview`

Compact resolver overview using the same target.

Query: `include=nodes,aliases,roles,events`, `view=compact|full`, `meta=none|summary|full`.

Defaults: `view=compact`, `meta=summary`, `include=nodes,aliases,roles,events`.

Rules:

- `counts.{nodes,aliases,role_holders,events}` are present only when the section is projected. Unsupported sections appear in `meta.unsupported_fields` unless `meta=none`.
- `nodes` and `aliases` are `null` when their fan-in is unprojected. Unsupported fan-in is never rendered as a supported zero count.
- `roles` is the compact role-holder list from resolver-scoped permission rows when projected.
- `events` is a compact event list from canonical normalized events when projected.

## Resolution

### `GET /v1/resolutions/{namespace}/{name}`

Mixed declared + verified resolution. The canonical route.

Query: `at`, `chain_positions`, `consistency`, `mode=declared|verified|both`, `records`.

`data` matches `GET /v1/names/{namespace}/{name}`.

Populated `declared_state`:

- `topology` — `ResolutionTopology | UnsupportedSummary`
- `record_inventory` — `ResolutionRecordInventory | UnsupportedSummary`
- `record_cache` — `ResolutionRecordCache | UnsupportedSummary`

Populated `verified_state`:

- `verified_queries`

Example fully-supported declared shape:

```json
{
  "topology": {
    "registry_path": [
      {"logical_name_id":"ens:alice.eth","namespace":"ens","normalized_name":"alice.eth","canonical_display_name":"alice.eth","namehash":"0x...","resource_id":"...","binding_kind":"declared_registry_path"}
    ],
    "subregistry_path": [],
    "resolver_path": [
      {"logical_name_id":"ens:alice.eth","namespace":"ens","normalized_name":"alice.eth","canonical_display_name":"alice.eth","resource_id":"...","chain_id":"ethereum-mainnet","address":"0x...","latest_event_kind":"ResolverChanged"}
    ],
    "wildcard": {"source": null, "matched_labels": []},
    "alias": {"final_target": null, "hops": []},
    "version_boundaries": {
      "topology_version_boundary": {"logical_name_id":"ens:alice.eth","resource_id":"...","normalized_event_id":null,"event_kind":null,"chain_position":{...}},
      "record_version_boundary":   {"logical_name_id":"ens:alice.eth","resource_id":"...","normalized_event_id":null,"event_kind":null,"chain_position":{...}}
    },
    "transport": {"source_chain_id": null, "target_chain_id": null, "contract_address": null, "latest_event_kind": null}
  },
  "record_inventory": { "...": "..." },
  "record_cache":     { "...": "..." }
}
```

Rules:

- Uses the exact-name snapshot for data, declared sections, coverage, verified support, and execution target.
- `mode=verified|both`: persisted verified output is eligible only when its stored chain positions match the selected snapshot exactly. When matching output is missing for a supported ENS Universal Resolver selector, the route executes on demand against the selected snapshot, persists the trace and outcome, and returns it.[^v1-iur]
- All declared sections are always present as objects when `declared_state` is populated. Missing projections return `UnsupportedSummary`.
- Callers round-trip the surfaced `record_key` strings in `records`. `record_family` and `selector_key` are explanatory.
- `record_inventory` defines the known selector space and version boundary. It does not imply global enumeration.
- `record_cache` is the declared last-known-value view over that space. It never implies verified execution ran.
- For ENSv1 and Basenames, a current resolver target alone doesn't claim complete `record_inventory`, `record_cache`, or resolver-overview support. Retained resolver-local events may produce selector-level cache successes; complete family coverage requires resolver-profile admission.[^v1-pres][^bn-l2resolver]
- `record_version_boundary` is identical across `topology.version_boundaries.record_version_boundary`, `record_inventory.record_version_boundary`, and `record_cache.record_version_boundary` when those sections are supported together.
- `record_cache.entries[*]` and `verified_queries[*]` always echo `record_key` even when status isn't `success`.
- `records` is comma-separated. In `mode=declared` it's optional; if supplied, `record_cache` narrows. In `mode=verified|both`, it's required. Duplicates or malformed selectors return `400 invalid_input`.
- `verified_queries` returns one result per requested selector in request order. Unsupported selector families, unsupported verified path classes, and namespaces without a verified entrypoint return `200` with `verified_queries[*].status=unsupported`. Never silent declared-cache fallback.
- Declared resolver-profile gaps don't suppress verified execution for an otherwise supported Universal Resolver path.

#### Verified support classes

ENS supports three exact-surface classes against the same declared topology snapshot:

| Class | Conditions |
| --- | --- |
| Direct | `resolver_path[0].logical_name_id == data.logical_name_id`, `wildcard.source=null`, `alias.final_target=null`, all `transport=null`. |
| Alias-only non-direct | Same as direct, but `alias.final_target` non-null with non-empty `hops`. |
| Wildcard-derived | `wildcard.source` non-null with non-empty `matched_labels`, `resolver_path[0].logical_name_id == wildcard.source.logical_name_id`, `alias.final_target=null`, `subregistry_path=[]`, all `transport=null`. |

Other ENS classes (non-alias ancestor-selected, linked-subregistry ancestor-selected, transport-assisted, CCIP-participating) return `verified_queries[*].status=unsupported`.

Basenames supports one class: exact-surface transport-assisted direct path through the L1 Resolver. `transport.source_chain_id="base-mainnet"`, `transport.target_chain_id="ethereum-mainnet"`, `transport.contract_address="0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31"`. CCIP-Read participation is in scope because upstream `L1Resolver` emits `OffchainLookup` for non-`base.eth` requests and verifies the callback through `resolveWithProof`.[^bn-l1resolver-flow] Other classes return `unsupported`.

For `mode=verified|both`, top-level `provenance` summarizes the request-scoped trace; `verified_queries[*]` may carry per-selector provenance. When a supported ENS selector needs on-demand execution and the Ethereum RPC provider is unconfigured or can't serve the selected block, return `409 stale` with a configuration message — not declared cache.

Deeper execution explanation lives on `GET /v1/explain/resolutions/{namespace}/{name}/execution`. Per-selector verified misses don't change route-level `coverage`.

### `GET /v1/resolve/{name}`

Namespace-inferred convenience for the canonical resolution route.

Query: `mode=declared|verified|both`, `records`.

Inference on the normalized name:

- exact `base.eth` → `namespace=ens`
- `*.base.eth` → `namespace=basenames`
- other supported ENS names → `namespace=ens`

Rules:

- The canonical route is preferred when callers know the namespace.
- Inferred namespace is echoed through `data.namespace` and `data.logical_name_id`.
- `at`, `chain_positions`, `consistency` aren't on this route. The canonical default snapshot applies (head, latest stored checkpoint); supported ENS on-demand execution targets that.
- Selector identity is namespace-local after inference. `*.base.eth` interprets `records` against the Basenames selector space.
- Inference and verified support are independent. `*.base.eth` doesn't fall back to `namespace=ens` outside the Basenames support class.

### `GET /v1/resolve/{name}/records`

Namespace-inferred compact records. Default `mode=auto`. Current-projection read.

Query: same as `GET /v1/names/{namespace}/{name}/records`.

Defaults: `mode=auto`, `view=compact`, `meta=summary`, `include=resolver_address,known_text_keys,avatar,content_hash,coins`. The default also turns on the common app-facing sections so one request returns resolver address, known text keys, avatar, content hash, and known coin addresses where available.

Without declared selectors, `mode=auto` probes the basic app profile set and returns successful fallback text rows plus the ETH coin row when available. It doesn't claim `known_text_keys` inventory support from those probes. No `at`, `chain_positions`, or `consistency`.

### `GET /v1/explain/resolutions/{namespace}/{name}/execution`

Persisted verified execution explain.

Query: `records` (required).

`data` matches the surface from the resolution route. `declared_state` is `null`.

`verified_state`:

- `execution: ResolutionExecutionExplainSummary`
- `verified_queries`

Rules:

- Verified-only; doesn't duplicate declared topology, inventory, or cache.
- Duplicate or malformed `records` return `400 invalid_input`.
- Keyed by the same exact surface and selector set the resolution route would use. Explains the persisted answer.
- Public verified-resolution support boundary matches the resolution route.
- `verified_queries` reuses selector-scoped result objects, request order, and `ResultStatus`.
- `verified_state.execution.execution_trace_id == provenance.execution_trace_id`.
- `verified_state.execution.steps` is the persisted ordered step summary — not raw calldata, gateway payloads, or a replayable dump.
- The route doesn't trigger fresh execution and doesn't synthesize from declared topology. With no persisted answer for the requested surface and selector set, returns `404 not_found`.

## History and events

### `GET /v1/history/names/{namespace}/{name}`

Canonical normalized-event history for one logical name anchor.

Query: `scope=surface|resource|both` (default `both`), `view=compact|full`, `meta=none|summary|full`, `cursor`, `page_size`.

| `scope` | Anchored by |
| --- | --- |
| `surface` | the requested `logical_name_id` |
| `resource` | any `resource_id` ever bound to that surface |
| `both` | union |

Observed and orphaned events are excluded. `view=compact` returns `CompactHistoryEvent` rows. `view=full` returns the full normalized-event row. Pages over `chain_position_desc`.

### `GET /v1/history/resources/{resource_id}`

Same contract anchored on `resource_id`. `resource_id` must be a UUID; otherwise `400 invalid_input`.

| `scope` | Anchored by |
| --- | --- |
| `resource` | the requested resource |
| `surface` | any `logical_name_id` ever bound to it |
| `both` | union |

### `GET /v1/history/addresses/{address}`

Canonical normalized-event history for the address-derived anchor set.

Query: `namespace`, `relation=registrant|token_holder|effective_controller`, `scope=surface|resource|both` (default `both`), `view=compact|full`, `meta=none|summary|full`, `cursor`, `page_size`.

Reuses the normalized-event history contract. `namespace` and `relation` filter which surfaces and resources contribute anchors; they don't change row shape, ordering, or coverage.

### `GET /v1/events`

Compact event search across name, address, resource, type, and block filters.

Query: `namespace`, `name`, `address`, `resource`, `resource_id`, `type`, `relation=token_holder|registrant|effective_controller|any`, `from_block`, `to_block`, `view=compact|full`, `meta=none|summary|full`, `cursor`, `page_size`.

Defaults: `view=compact`, `meta=summary`, sort `chain_position_desc`.

Each row is `CompactHistoryEvent`.

Rules:

- `name` requires `namespace`, otherwise `400 invalid_input`.
- `address` selects events whose projected anchor relates to the address under the requested `relation`. Same anchor selection as `GET /v1/history/addresses/{address}`.
- `resource` and `resource_id` both select events anchored on the resource. Supplying both is `400 invalid_input`. `resource_hex` isn't accepted.
- `type` filters by normalized event type or compact alias. Unsupported aliases return non-2xx `unsupported`.
- `from_block` and `to_block` apply to canonical chain position. They don't trigger raw fact scans.

## Primary names

### `GET /v1/primary-names/{address}`

Claimed and verified primary name for one `(address, namespace, coin_type)` tuple.

Query: `mode=declared|verified|both`, `coin_type` (required), `namespace` (required).

`data`: `address`, `namespace`, `coin_type`.

`declared_state.claimed_primary_name`. `verified_state.verified_primary_name`.

| Object | Statuses |
| --- | --- |
| `claimed_primary_name` | `success`, `not_found`, `unsupported`, `invalid_name` |
| `verified_primary_name` | the above plus `mismatch`, `execution_failed` |

Rules:

- Head-only. No `at` or `consistency`.
- `claimed_primary_name` is the candidate only; it never implies verification.
- For ENS, the admitted claim source is `ens_v1_reverse_l1` at `0xa58E81fe9b61B5c3fE2AFD33CF304c454AbFc7Cb`.[^v1-revreg] The route doesn't trigger fresh reverse lookups while serving the declared response. Missing or unsupported claims don't fall back to registry, resolver, or other surfaces.
- `claimed_primary_name.name` comes only from the exact requested `primary_names_current(address, coin_type, namespace)` row's declared normalized claim-identity source. Never synthesized from manifest presence, resolver-backed identity, or another tuple.
- For Basenames, the admitted claim family is `basenames_base_primary` at `0x79ea96012eea67a83431f1701b3dff7e37f9e282`.[^bn-revreg] Claim intake only — does not replace the Base registry/registrar/resolver families.
- `claimed_primary_name.raw_claim_name` may appear only when `status=invalid_name` for the exact requested tuple, copied verbatim. Blank or whitespace-only is `not_found`; `invalid_name` is for nonblank claims that fail normalization.
- `claimed_primary_name.provenance` is exact-tuple declared provenance. It strips any verified-primary lookup/invalidation hook material and omits `execution_trace_id`.
- `verified_primary_name` shape: `{status, name?, unsupported_reason?, failure_reason?, provenance?}`. `name` is `NameRef` and appears only for `success` or `mismatch`. `raw_claim_name` never appears here.
- `verified_primary_name.provenance` is `{execution_trace_id, manifest_versions}` for the same tuple. `execution_trace_id` equals top-level `provenance.execution_trace_id`.
- `verified_primary_name` is authoritative only on `success`. `mismatch` means the claim normalized and resolved for the requested coin type, but to a different address.
- Verified persisted-readback uses execution identity `request_type=verified_primary_name`, request key `{namespace}:{normalized_address}:{coin_type}` (lowercased address). `primary_names_current(address, coin_type, namespace)` is the only claim-side anchor.
- Invalid address syntax, missing `namespace` or `coin_type`, or a malformed tuple returns `400 invalid_input`.
- Unsupported public namespace returns `404 not_found`.
- No declared or verified answer for the tuple returns `200` with `status=not_found`.
- Unsupported claim surfaces or verified entrypoints return `200` with the corresponding object `status=unsupported`.

#### Route-level coverage

Local to the requested tuple. Not the single-name `Coverage` object.

| Class | `status` | `exhaustiveness` | `source_classes_considered` |
| --- | --- | --- | --- |
| ENS supported tuple | `partial` | `non_enumerable` | `["ens_v1_reverse_l1","ens_execution"]` |
| Basenames supported tuple | `partial` | `non_enumerable` | `["basenames_base_primary","basenames_execution"]` |
| Out of class | `unsupported` | `not_applicable` | `[]` |

`enumeration_basis=primary_name_lookup` for all three. Out-of-class verified objects use `verified_primary_name.status=unsupported`.

## Examples

Dashboard owned names:

```
GET /v1/names?namespace=ens&account=0x0000…&relation=token_holder&contains=ali&sort=expiry_date&order=asc&page_size=50
```

Name search:

```
GET /v1/names?namespace=ens&prefix=alic&sort=name&order=asc&page_size=10&meta=none
```

Exact compact lookup:

```
GET /v1/names?namespace=ens&name=alice.eth&include=record_summaries
```

Subnames:

```
GET /v1/names/ens/alice.eth/children?include=counts&page_size=50
```

Resolver records:

```
GET /v1/resolve/alice.eth/records
GET /v1/resolve/alice.eth/records?include=resolver_address,known_text_keys,avatar,content_hash,coins&texts=avatar,com.twitter&coin_types=60,0
```

History:

```
GET /v1/history/names/ens/alice.eth?view=compact&scope=both&page_size=25
```

Address activity:

```
GET /v1/events?address=0x0000…&relation=any&namespace=ens&page_size=25
```

Roles:

```
GET /v1/roles?account=0x0000…&page_size=50
GET /v1/roles?resource_id=00000000-0000-0000-0000-000000000000&page_size=50
GET /v1/names/ens/alice.eth/roles?page_size=50
```

Resolver overview:

```
GET /v1/resolvers/ethereum-mainnet/0x0000…/overview?include=nodes,aliases,roles,events
```

---

[^v2-deploy-ethreg]: (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistry.json:L2 @ ens_v2@554c309)
[^v2-iperm]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L34 @ ens_v2@554c309)
[^bn-readme-base]: (upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc)
[^v1-nw-fuses]: (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L31 @ ens_v1@91c966f)
[^bn-l2resolver]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L22 @ basenames@1809bbc)
[^v1-iur]: (upstream: .refs/ens_v1/contracts/universalResolver/IUniversalResolver.sol:L44 @ ens_v1@91c966f)
[^v1-pres]: (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L20 @ ens_v1@91c966f)
[^bn-l1resolver-flow]: (upstream: .refs/basenames/src/L1/L1Resolver.sol:L154 @ basenames@1809bbc)
[^v1-revreg]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L15 @ ens_v1@91c966f)
[^bn-revreg]: (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L12 @ basenames@1809bbc)
