# API v1 Contract

Status: Phase 0 baseline

This document freezes the external `v1` read contract strongly enough for API, projection, and SDK work to proceed in parallel.

## 1. Conventions

- all routes live under `/v1`
- responses are JSON with `snake_case` keys
- timestamps are RFC 3339 UTC strings
- semantic identities are strings; opaque internal IDs are never inferred by clients
- `namespace` is always explicit for name-based reads
- names in path segments are normalized names, URL-encoded as plain text
- every externally visible answer includes provenance, coverage, chain position context, and consistency

### Common query parameters

- `at`: point-in-time selector, either an RFC 3339 timestamp or a chain-position token
- `consistency`: `head`, `safe`, or `finalized`
- `mode`: `declared`, `verified`, or `both`
- `include`: comma-separated expansions; default is route-specific
- `cursor`: opaque pagination cursor
- `page_size`: default `50`, max `200`

Routes declare which subset of these parameters they honor. Unlisted parameters are reserved for additive route support and do not widen the shipped declared-state contract.

Defaults:

- `consistency=head`
- `mode=declared`

### Snapshot Selection Rules

| Inputs | Rule |
| --- | --- |
| `chain_positions` only | use the supplied positions exactly |
| `at` only | resolve per-chain positions at the requested `consistency` |
| neither | use the latest available positions at the requested `consistency` |
| both `at` and `chain_positions` | reject with `invalid_input` |

Validation rules:

- if `chain_positions` is supplied, every chain required by the route must be present
- if `chain_positions` is supplied, unsupported chain keys for that route are rejected
- if `consistency` is supplied with explicit `chain_positions`, the server validates that each supplied position satisfies that consistency floor or returns `conflict`

Cross-chain rules:

- ENS authoritative positions are selected on Ethereum L1
- Basenames authoritative positions are selected on Base
- when a route also needs an auxiliary chain, choose the auxiliary position at the same requested consistency with timestamp less than or equal to the authoritative-chain timestamp
- verified execution runs against the resolved positions only; it does not advance to a newer head mid-request

## 2. Shared Response Envelope

Single-resource reads return:

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

Collection reads replace `data` with an array and add:

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

Rules:

- `declared_state` and `verified_state` are always present in the response envelope
- routes without declared or verified semantics return `null` for that top-level section
- routes that support both declared and verified semantics use `mode` to decide which sections are populated:
  `declared` populates `declared_state` and returns `verified_state=null`
  `verified` populates `verified_state` and returns `declared_state=null`
  `both` populates both sections
- `coverage` explains completeness and enumeration basis, not just freshness
- `chain_positions` may contain multiple chains for cross-chain answers
- route-level `coverage` and subdocument support are separate: a read may be authoritative for exact lookup while one or more declared summary sections still return explicit unsupported objects
- top-level `provenance` is a route-level summary; mixed declared+verified routes may add section-local `provenance` objects where declared and execution derivations differ

## 3. Shared Objects

### `NameRef`

- `logical_name_id`
- `namespace`
- `normalized_name`
- `canonical_display_name`
- `namehash`
- `resource_id`
- `binding_kind`

### `ResourceRef`

- `resource_id`
- `authority_epoch`
- `token_lineage_id`
- `current_resolver`

### `RoleSummary`

- `subjects`
- `subjects[*].subject`
- `subjects[*].scopes`
- `subjects[*].scopes[*].scope`
- `subjects[*].scopes[*].effective_powers`

Use this object for the `role_summary` expansion on `GET /v1/addresses/{address}/names`. It is the per-resource summary view of the current effective permission rows for the same `resource_id`. Row-granular permission lineage such as `grant_source`, `revocation_source`, `inheritance_path`, and `transfer_behavior` stays on `GET /v1/resources/{resource_id}/permissions`.

### `UnsupportedSummary`

- `status`: always `unsupported`
- `unsupported_reason`

Use this object when a declared-state subdocument is part of the route contract but is not yet projected. The field stays present; unsupported detail is never omitted silently.

### `ResultStatus`

- `success`
- `not_found`
- `mismatch`
- `unsupported`
- `invalid_name`
- `execution_failed`

Use this status vocabulary for:

- `declared_state.record_cache.entries[*]`
- `verified_state.verified_queries[*]`
- `declared_state.claimed_primary_name`
- `verified_state.verified_primary_name`

Rules:

- every result object above always includes `status`
- route-specific request identity fields stay present even when `status` is not `success`
- `unsupported_reason` is required when `status=unsupported`
- `failure_reason` may appear for `not_found`, `mismatch`, `invalid_name`, or `execution_failed`
- value and identity fields appear only when the route established a concrete record value or concrete name target for that result
- not every status applies to every result object; routes document the subset they use

### `Coverage`

- `status`: `full`, `partial`, `observed_only`, `unsupported`, `stale`
- `exhaustiveness`: `authoritative`, `best_effort`, `observed_only`, `non_enumerable`, `not_applicable`
- `source_classes_considered`
- `enumeration_basis`
- `unsupported_reason`

This shared object is the route-level `coverage` summary on every response. For the same exact-name target and snapshot, `GET /v1/names/{namespace}/{name}` and `GET /v1/coverage/{namespace}/{name}` return the same top-level `Coverage` object.

### `Provenance`

- `normalized_event_ids`
- `raw_fact_refs`
- `manifest_versions`
- `execution_trace_id`
- `derivation_kind`

### `ChainPositions`

- `ethereum`
- `base`
- `execution_checkpoint`

Each position object contains:

- `chain_id`
- `block_number`
- `block_hash`
- `timestamp`

## 4. Initial Route Set

These routes define the baseline `v1` surface. Later additions must be additive within `v1`.

The current API binary ships only the declared-state subset below. Queued routes remain part of the frozen `v1` contract so projection and SDK work can proceed without changing wire semantics later.

| Route | Purpose | Contract state |
| --- | --- | --- |
| `GET /v1/namespaces/{namespace}` | Namespace metadata and support status | shipped declared-state |
| `GET /v1/names/{namespace}/{name}` | Exact name lookup | shipped declared-state |
| `GET /v1/names/{namespace}/{name}/children` | Declared child collection by default | shipped declared-state |
| `GET /v1/history/names/{namespace}/{name}` | Surface or combined history | shipped declared-state |
| `GET /v1/history/resources/{resource_id}` | Resource history | shipped declared-state |
| `GET /v1/history/addresses/{address}` | Address activity across related surfaces and resources | queued declared-state |
| `GET /v1/manifests/{namespace}` | Active manifest versions and capabilities | shipped declared-state |
| `GET /v1/addresses/{address}/names` | Address-to-surface collection | queued declared-state |
| `GET /v1/resources/{resource_id}/permissions` | Resource-centric effective permissions | queued declared-state |
| `GET /v1/resolvers/{chain_id}/{resolver_address}` | Resolver overview | queued declared-state |
| `GET /v1/resolutions/{namespace}/{name}` | Resolution topology, inventory, and verified reads | queued mixed declared+verified |
| `GET /v1/primary-names/{address}` | Claimed and verified primary-name answer | queued mixed declared+verified |
| `GET /v1/coverage/{namespace}/{name}` | Single-name coverage and explain details | queued declared-state |

## 5. Route-Level Semantics

### `GET /v1/namespaces/{namespace}`

Returns manifest-backed metadata for one public namespace.

`declared_state` includes:

- `active_manifest_count`
- `active_source_families`
- `chains`
- `normalizer_versions`

Rules:

- return `200` with empty lists and `active_manifest_count=0` when the namespace is public but has no active manifests yet
- return `404 not_found` when the namespace is not a supported public namespace
- use `GET /v1/manifests/{namespace}` for per-manifest capability flags and manifest-version detail

### `GET /v1/names/{namespace}/{name}`

Returns:

- `data` surface identity: `logical_name_id`, `namespace`, `normalized_name`, `canonical_display_name`, `namehash`
- `data` binding identifiers: `resource_id`, `token_lineage_id`, `binding_kind`
- `declared_state.registration`
- `declared_state.authority`
- `declared_state.control`
- `declared_state.resolver`
- `declared_state.record_inventory`
- `declared_state.history`

Rules:

- the exact-name route is authoritative for supported source classes even when one or more declared summary sections are still unsupported
- every declared summary section above is always present as an object
- if a section is not yet projected, it returns `UnsupportedSummary`
- `declared_state.authority` may fall back to `{resource_id, token_lineage_id, binding_kind}` when a dedicated authority summary is not yet projected but the current binding is known
- for the same `{namespace}`, `{name}`, and snapshot selection, the top-level `coverage` object matches `GET /v1/coverage/{namespace}/{name}`
- the shipped exact-name route does not support `include` expansions; history, permissions, resolution, and primary-name reads stay on their dedicated routes
- `verified_state` is `null` for the shipped exact-name route

### `GET /v1/coverage/{namespace}/{name}`

This route is queued but frozen now to unblock single-name coverage and explain reads.

Returns the declared-state coverage answer for one exact public surface.

`data` identifies the same single surface and current binding as `GET /v1/names/{namespace}/{name}`.

`declared_state` carries explain-oriented detail for that same single-name coverage answer.

Supported query parameters:

- `at`
- `consistency`

Rules:

- this route honors only `at` and `consistency` from the common query set; if `at` is omitted, the common snapshot defaults apply and the route reads the latest available positions at `consistency=head` unless the caller supplies another supported `consistency`
- this route is declared-state only and `verified_state` is `null`
- the top-level `coverage` field is the shared `Coverage` object for the requested name and snapshot
- for the same `{namespace}`, `{name}`, and snapshot selection, that top-level `coverage` object must match the inline `coverage` returned by `GET /v1/names/{namespace}/{name}`
- `declared_state` explains `coverage.status`, `coverage.exhaustiveness`, `coverage.source_classes_considered`, `coverage.enumeration_basis`, and `coverage.unsupported_reason`; it does not redefine them
- the initial contract defines no `include` expansions for this route

### `GET /v1/addresses/{address}/names`

This route is queued but frozen now to unblock address-read work.

Returns surfaces, not backing resources.

Supported filters in the first declared-state contract:

- `namespace`
- `relation=registrant|token_holder|effective_controller`
- `dedupe_by=surface|resource`
- `include=role_summary`

Each item includes:

- `logical_name_id`
- `namespace`
- `normalized_name`
- `canonical_display_name`
- `namehash`
- `resource_id`
- `binding_kind`
- `relation_facets`

When `include=role_summary` is requested, each item also adds:

- `role_summary`: `RoleSummary`
- `subname_count`
- `record_count`
- `status`
- `expiry`

Rules:

- `dedupe_by=surface` is the default truth model
- `dedupe_by=resource` changes grouping only; it does not change coverage semantics or turn the route into a resource collection
- the default sort remains `display_name_asc`
- `include=role_summary` is additive; it does not change supported filters, default `dedupe_by`, enumeration basis, route-level coverage meaning, default sort, cursor behavior, or item identity
- the `role_summary` expansion derives from the current item `resource_id` plus the existing resource-permissions truth family; it does not introduce a separate address-role ledger
- `role_summary` groups the current `GET /v1/resources/{resource_id}/permissions` rows by `subject`; each grouped subject keeps the current `(scope, effective_powers)` pairs for that `resource_id`, while row-granular grant and revocation detail stays on the dedicated permissions route
- `subname_count` counts the same declared direct child surfaces returned by `GET /v1/names/{namespace}/{name}/children` by default; it does not include linked, alias-derived, or wildcard-observed child buckets
- `status` and `expiry` mirror the current `ControlVector.status` and `ControlVector.expiry` values for the item `resource_id`
- `record_count` counts the distinct stable declared record selectors for the item `resource_id` at its current version boundary; in the first shipped slice this is the number of selectors that belong to the same declared record-inventory answer shape used by `Resolution.record_inventory`, not a count of raw resolver slots, cached values, or verified query results
- the added fields `role_summary`, `subname_count`, `record_count`, `status`, and `expiry` are optional expansion fields only and do not replace the required surface identity and relation facets

### `GET /v1/resources/{resource_id}/permissions`

This route is queued but frozen now to unblock resource-centric declared reads.

Returns current effective permission rows anchored to one `resource_id`.

Supported filters:

- `subject`
- `scope`

Each item includes:

- `resource_id`
- `subject`
- `scope`
- `effective_powers`
- `grant_source`
- `revocation_source`
- `inheritance_path`
- `transfer_behavior`

Rules:

- `resource_id` is the truth anchor; surface names or resolver addresses may appear only as explanatory context
- resolver-scoped permissions remain rows in this same collection with resolver-specific scope detail; they are not a separate truth system
- `GET /v1/addresses/{address}/names?include=role_summary` is the per-resource summary form of this same collection: it groups current rows by `subject`, retains each grouped subject's `scope` plus `effective_powers`, and leaves row-granular lineage on this dedicated route
- this route is declared-state only and `verified_state` remains `null`

### `GET /v1/names/{namespace}/{name}/children`

Defaults to declared direct children only.

Optional query parameters:

- `surface_classes=declared`
- `include=counts`

Rules:

- requesting `linked`, `alias`, or `wildcard` surface classes is reserved for additive expansion and currently returns `unsupported`

### `GET /v1/history/names/{namespace}/{name}`

Returns canonical normalized-event history for one logical name anchor.

Supported query parameters:

- `scope=surface|resource|both` with default `both`

Rules:

- `scope=surface` returns events anchored by the requested `logical_name_id`
- `scope=resource` returns events anchored by any `resource_id` ever bound to that surface
- `scope=both` returns the union of those anchor sets
- observed and orphaned events are excluded from the shipped history routes
- `declared_state` is `{}` for history routes; the normalized-event rows themselves are the declared answer

### `GET /v1/history/resources/{resource_id}`

Returns canonical normalized-event history for one resource anchor.

Supported query parameters:

- `scope=surface|resource|both` with default `both`

Rules:

- `resource_id` must be a UUID or the route returns `400 invalid_input`
- `scope=resource` returns events anchored by the requested `resource_id`
- `scope=surface` returns events anchored by any `logical_name_id` ever bound to that resource
- `scope=both` returns the union of those anchor sets
- observed and orphaned events are excluded from the shipped history routes
- no dedicated address-history route ships in the current subset; the queued route below reuses these same anchor and coverage semantics rather than inventing a second history contract

### `GET /v1/history/addresses/{address}`

This route is queued but frozen now to unblock address activity reads.

Returns canonical normalized-event history for one address-derived anchor set.

Supported query parameters:

- `namespace`
- `relation=registrant|token_holder|effective_controller`
- `scope=surface|resource|both` with default `both`

Rules:

- address history reuses the existing normalized-event history contract; it does not introduce a separate address-history ledger or projection family
- `namespace` and `relation` filter which related surfaces and resources contribute anchors for the requested address across current and historical matches; they do not change history row shape, ordering, or coverage meaning
- `scope=surface` returns events anchored by any `logical_name_id` selected for the requested address across current and historical matches under the active filters
- `scope=resource` returns events anchored by any `resource_id` selected for the requested address across current and historical matches under the active filters
- `scope=both` returns the union of those anchor sets
- observed and orphaned events are excluded from this route
- this route follows the shared history default sort `chain_position_desc`
- `declared_state` is `{}` for history routes; the normalized-event rows themselves are the declared answer

### `GET /v1/resolvers/{chain_id}/{resolver_address}`

This route is queued but frozen now to unblock resolver-overview reads.

`data` identifies the resolver target. `declared_state` groups:

- current bindings
- alias mappings
- resolver-scoped permissions
- role-holder summary
- resolver event summary

Rules:

- resolver overview is declared-state only and `verified_state` remains `null`
- counts for nodes, aliases, and role holders live inside those declared summaries rather than as a separate truth system
- any declared summary that is not yet projected returns `UnsupportedSummary`

### `GET /v1/resolutions/{namespace}/{name}`

This route freezes one mixed declared+verified envelope for resolution reads.

Supported query parameters:

- `at`
- `consistency`
- `mode=declared|verified|both`
- `records`

`data` identifies the same surface and current binding as `GET /v1/names/{namespace}/{name}` for the requested snapshot.

When `declared_state` is populated, it includes:

- `topology`
- `record_inventory`
- `record_cache`

When `verified_state` is populated, it includes:

- `verified_queries`

Rules:

- `topology`, `record_inventory`, and `record_cache` are always present as objects when `declared_state` is populated; any declared section that is not yet projected returns `UnsupportedSummary`
- `record_inventory` defines the known record-selector space, explicit gaps, and the current version boundary for the requested surface; it does not imply global record enumeration
- `record_cache` is the declared last-known-value view over that same selector space and version boundary; it never implies that verified execution was run
- selector-level declared cache results live in `record_cache.entries`
- `record_cache.entries[*]` and `verified_queries[*]` always echo the applicable `record_key`, even when the selector status is not `success`
- `records` is a comma-separated list of explicit record selectors; selectors use the stable `record_key` strings surfaced by `record_inventory`, and the contract permits additive selector families without changing the envelope shape
- in `mode=declared`, `records` is optional; if supplied, `record_cache` narrows to the requested selectors, otherwise it returns every cacheable selector visible at the current version boundary
- in `mode=verified` or `mode=both`, `records` is required and duplicate selectors are rejected with `400 invalid_input`
- malformed selector syntax returns `400 invalid_input`
- if the exact surface does not exist for the requested namespace and snapshot, return `404 not_found`
- `verified_queries` returns one result object per requested selector in request order
- `verified_queries[*].status` uses the shared `ResultStatus` vocabulary; the initial resolution contract uses `success`, `not_found`, `unsupported`, and `execution_failed`
- unsupported selector families, unsupported resolver families, or namespaces without a verified entrypoint return `200` with `verified_queries[*].status=unsupported`; they do not silently downgrade to declared cache values
- supported verified queries that execute but do not produce a trustworthy answer return `status=execution_failed` with `failure_reason`
- for `mode=verified` or `mode=both`, top-level `provenance` includes the request-scoped execution trace summary and each `verified_queries[*]` item may carry narrower provenance for the specific selector result
- route-level `coverage` explains declared completeness for topology, inventory, and cache at the requested snapshot; per-selector verified misses or failures do not change that shared route-level `coverage` object by themselves

### `GET /v1/primary-names/{address}`

Supported query parameters:

- `at`
- `consistency`
- `mode=declared|verified|both`
- `coin_type`
- `namespace`

This route is keyed by one `(address, namespace, coin_type)` tuple. `namespace` and `coin_type` are required.

`data` identifies the requested tuple:

- `address`
- `namespace`
- `coin_type`

When `declared_state` is populated, it returns:

- `claimed_primary_name`

When `verified_state` is populated, it returns:

- `verified_primary_name`

Rules:

- `claimed_primary_name` is the declared claim candidate only; it never implies that the requested address actually verifies to that name
- `claimed_primary_name.status` uses the shared `ResultStatus` vocabulary; the initial declared contract uses `success`, `not_found`, `unsupported`, and `invalid_name`
- `verified_primary_name.status` uses the same `ResultStatus` vocabulary; the initial verified contract uses `success`, `not_found`, `mismatch`, `unsupported`, `invalid_name`, and `execution_failed`
- `claimed_primary_name` and `verified_primary_name` always include `status` when their containing section is populated
- when a concrete claim target exists, `claimed_primary_name` includes the resolved surface identity fields; if the raw claim exists but cannot be normalized, it returns `status=invalid_name`, keeps the raw claim text in `raw_claim_name`, and omits normalized identity fields
- `verified_primary_name` is authoritative only when `status=success`
- `status=mismatch` means the claim normalized and resolved, but the verified target address for the requested `coin_type` did not equal the requested `{address}`; the result keeps the candidate name identity and the mismatching resolved target
- invalid address syntax, missing required `namespace` or `coin_type`, or a malformed query tuple returns `400 invalid_input`
- an unsupported public namespace returns `404 not_found`
- no declared or verified primary-name answer for the requested tuple returns `200` with `status=not_found`; it does not turn the route into `404`
- unsupported claim surfaces or unsupported verified entrypoints return `200` with the corresponding object `status=unsupported`
- top-level `provenance` summarizes the declared claim inputs and, when `verified_state` is populated, the verification trace; `claimed_primary_name` and `verified_primary_name` may each carry narrower provenance objects
- route-level `coverage` explains completeness of the declared claim surface for the requested tuple; a verification mismatch or absence does not by itself change that coverage summary

## 6. Sorting And Pagination Defaults

- address collections default to `display_name_asc`
- child collections default to `display_name_asc`
- history reads default to `chain_position_desc`
- ties break on `logical_name_id` for surfaces and `resource_id` for resource views

Cursor pagination must be stable under replay for the same requested chain positions.

## 7. Error Model

Every non-2xx response returns:

```json
{
  "error": {
    "code": "unsupported",
    "message": "the requested route option is not supported",
    "details": {}
  }
}
```

`error.code` values:

- `invalid_input`
- `not_found`
- `unsupported`
- `stale`
- `verification_failed`
- `conflict`
- `internal_error`

Rules:

- use non-2xx `unsupported` only when the request cannot produce the route contract at all for the requested shape
- when a mixed route can produce the envelope but one declared or verified subsection is unsupported, return `200` and surface that state through `UnsupportedSummary` or the shared `ResultStatus` vocabulary instead of raising a route-level `unsupported` error

## 8. Versioning Rules

- new optional fields are additive within `v1`
- new routes are additive within `v1`
- changing enum meaning, default sort, coverage semantics, or required fields requires `v2`
- if a capability is unsupported for a namespace or source class, return it explicitly in `coverage` or `error`, never through silent omission
