# ADR 0006: API v2 Product Surface

Status: Proposed
Date: 2026-06-10

## Context

A holistic review of the `v1` API (2026-06-10) confirmed the fragmentation that
ADR 0003 diagnosed but never resolved: the scope worksheet behind it
(`docs/internal/api-surface-flattening-scope-decisions.md`) still has an empty
"Final Direction" section, and the vocabulary/envelope decisions it deferred were
never made. Measured against the implementation, the `v1` contract today has:

- 24 routes producing 8 distinct response envelope shapes in practice (full
  single, full collection, compact collection, compact single, the profile-route
  hybrid whose arity changes with `meta`, the identity batch shape, the status
  wrapper, and `/healthz`).
- 54 OpenAPI component schemas, roughly 30 used exactly once, 2 orphaned, and
  about 10 documented shapes that exist only as untyped `JsonObject`.
- The same concept spelled differently per route: the name string as `name` /
  `normalized_name` / `canonical_display_name` (with `name` itself meaning
  display-name on some routes and normalized-name on others); controlling
  addresses as `owner` / `owner_address` / `registrant` / `registry_owner` /
  `account` / `subject` / `manager_address`; expiry as `expiry_date` (RFC 3339)
  vs `expiration` (unix integer) vs `expiry`; four relation enums; nine status
  vocabularies; six sort vocabularies in two styles; two pagination objects.
- Indexer-internal vocabulary on every default read: `logical_name_id`,
  `resource_id`, `token_lineage_id`, `surface_binding_id`, `binding_kind`,
  `normalized_event_id`, `raw_fact_refs`, `manifest_versions`,
  `derivation_kind`, `exhaustiveness`, `enumeration_basis`,
  `source_classes_considered`, the `execution_checkpoint` pseudo-chain slot, and
  projection table names leaking into `meta.value_source` and error messages.
- Contract rot: `verification_failed` and `authority_epoch` are documented but
  never emitted; `resource_hex` and `role_bitmap` are hardcoded `null` on the
  wire; `CompactDomainSummary` documents fields the builder never produces;
  `/v1/events` declares roughly 11 query parameters that exist only to be
  rejected; seven routes advertise a `view` enum with one legal value.

The two driving consumers need the opposite of this:

- partner-1 (`docs/partners/partner-1-indexing-requirements.md`) needs two
  primitives — forward name-to-record and reverse address-plus-coin-type-to-names,
  batched — with a flat record shape in common vocabulary, `has_more` /
  `total_count` / cursor pagination, per-response staleness metadata, defined
  empty-vs-not-found semantics, no caller-side namespace fan-out, and p95 under
  10 ms. `POST /v1/identity:lookup` already satisfies this and meets the latency
  target (`docs/partners/partner-1-identity-facade-benchmarks.md`).
- The first-party app (capability scan in ADR 0003) needs profile, records,
  owned-names-with-role-summary, subnames, search, and history reads — and does
  not call the `v1` API yet.

That last fact is the strategic window: there are approximately zero live
consumers of the current contract. partner-1 is pre-shim and the app is on
ENSJS/GraphQL. Breaking the API costs nothing today and becomes permanently
expensive at partner cutover.

The review also confirmed what is worth keeping: snapshot-pinned reads, the
indexed-vs-onchain-verified distinction, and explicit unsupported semantics are
real differentiators — partner-1's shadow-comparison migration depends on
exactly those properties. The defect is that they leak on every route by
default, under invented names, in overlapping vocabularies. And one route —
`identity:lookup` — already demonstrates the target ergonomics: flat DTOs,
common vocabulary, sane batch semantics, fastest path in the system. This ADR
makes the exception the rule.

## Decision

Publish API `v2` as the product contract, designed around three rules:

1. **One vocabulary.** Every domain concept has exactly one wire name, drawn
   from common ENS/blockchain usage, defined in the naming dictionary below, and
   used identically on every route.
2. **One envelope.** Every route returns `data`, plus `page` on collections,
   plus `meta`. No full/compact dual shapes, no query-time shape switching.
3. **Three tiers.** Lookup primitives (the partner path), product reads (the app
   path), and diagnostics (the only home of pipeline vocabulary). Product
   responses carry zero indexer internals; the route path — not a query knob —
   decides which tier a response belongs to.

`v1` is frozen at the current contract and sunset before partner-1 cutover (see
Rollout).

### Naming dictionary (normative)

One name per concept, applied on every `v2` route:

| `v2` name | Meaning | Replaces (`v1`) |
| --- | --- | --- |
| `name` | the ENSIP-15 normalized name string | `normalized_name`, `logical_name_id` (derivable as `namespace:name`) |
| `display_name` | display form of the name | `canonical_display_name` |
| `owner` | token/registry owner | `token_holder`, `owner`, `owner_address`, `registry_owner` |
| `manager` | controller/manager | `effective_controller`, `manager_address` |
| `registrant` | registrant | `registrant` (unchanged) |
| `relation` | address-to-name relation enum: `owner`, `manager`, `registrant`, `any` | four divergent relation/role enums incl. `owned`/`managed`/`both` |
| `expires_at` | expiry, RFC 3339 | `expiry_date`, `expiration` (unix), `expiry` |
| `registered_at` | current registration start, RFC 3339 | `registration_date` |
| `created_at` | first observation of the name, RFC 3339 | `created_at` (now defined and distinguished from `registered_at`) |
| `addresses` | coin-type-to-address map, string keys | `coin_addresses`, `coin_type_addresses` |
| `text_records` | text-key-to-value map | `text_records` (unchanged) |
| `content_hash` | contenthash value | `content_hash` (unchanged) |
| `resolver` | `{chain_id, address}` | `resolver_address`, `current_resolver`, declared resolver summaries |
| `chain_id` | numeric EVM chain id (`1`, `8453`); string-keyed in maps | string chain ids (`"ethereum-mainnet"`), position slot keys |
| `network` | display slug (`ethereum`, `base`) | `network` (unchanged, display-only) |
| `registration_id` | the one opaque stable handle for a registration lifecycle | `resource_id`, `resource_hex`, `resource`, `token_lineage_id`, `surface_binding_id` |
| `finality` | `latest`, `safe`, `finalized` (JSON-RPC block-tag vocabulary) | `consistency` = `head`/`safe`/`finalized` |
| `source` | `indexed`, `verified`, `both` | `mode` = `declared`/`verified`/`both`/`auto`; `declared_state`/`verified_state` |
| `as_of` | per-chain `{block_number, block_hash, timestamp}`, keyed by `chain_id` | `chain_positions` (and the `execution_checkpoint` pseudo-slot is diagnostics-only) |
| `scope` (history) | `name`, `registration`, `both` | `surface`, `resource`, `both` |
| `status` | one result vocabulary: `ok`, `not_found`, `invalid_name`, `unsupported`, `stale`, `failed` | `ResultStatus`, `IdentityStatus`, `NameRecordStatus`, `unnormalizable_input` (folds into `invalid_name`) |
| `completeness` | `full`, `partial`, `unsupported` | `coverage.status` on product routes (full taxonomy moves to diagnostics) |
| `powers` | effective permission powers | `effective_powers` |

Rules:

- Timestamps are RFC 3339 UTC everywhere, including the lookup route. partner-1
  requested unix-seconds `expiration`; the partner shim performs that format
  mapping. Field semantics, not field format, are the requirement per the
  partner document.
- JSON map keys are strings (`"60"`, `"8453"`); `chain_id` as an object field is
  a JSON number.
- `token_id` stays a decimal string (uint256).
- Pipeline vocabulary (`projection`, `sidecar`, `manifest`, `normalized event`,
  `raw fact`, table names) must not appear in product-route field names, enum
  values, or error messages.

### Route catalog

Name-shaped routes infer the namespace from the name itself (the existing
`profiles/` and `identity:lookup` inference: exact `base.eth` is `ens`,
`*.base.eth` is `basenames`, other supported names are `ens`), accept an
optional `?namespace=` override, and always echo the resolved `namespace` in the
response. This collapses the `v1` dual grammar of `/v1/names/{ns}/{name}` vs
`/v1/profiles/names/{name}`.

Tier 1 — lookup primitives:

| Route | Purpose |
| --- | --- |
| `POST /v2/lookup` | Batched forward (name) and reverse (address + coin type) resolution. `profile=feed` is the latency path; `profile=detail` returns full records. Replaces `POST /v1/identity:lookup`. |
| `GET /v2/status` | Per-chain indexing readiness: `chains: {<chain_id>: {latest_block, indexed_block, lag_blocks, lag_seconds, status}}`. |

Tier 2 — product reads:

| Route | Purpose |
| --- | --- |
| `GET /v2/names/{name}` | Name profile: the flat record shape plus registration summary. Replaces `/v1/names/{ns}/{name}` + `/v1/profiles/names/{name}`. |
| `GET /v2/names/{name}/records` | Resolver records. `?source=indexed\|verified\|both`, `?keys=` selector filter. |
| `GET /v2/names/{name}/subnames` | Direct subnames. `?include=counts`. Replaces `children`. |
| `GET /v2/names/{name}/history` | Name history. `?scope=name\|registration\|both`. |
| `GET /v2/names/{name}/permissions` | Permission rows for the name's current registration. Replaces `/v1/resources/{id}/permissions`, `/v1/roles`, `/v1/names/.../roles`, `/v1/resources/lookup`. `registration_id` stays available as a response field and filter. |
| `GET /v2/addresses/{address}/names` | Names related to an address. `?relation=owner\|manager\|registrant\|any`, `?include=role_summary`. |
| `GET /v2/addresses/{address}/primary-name` | Primary name. `?coin_type=` (default `60`), `?source=`. Replaces `/v1/primary-names/{address}`. |
| `GET /v2/addresses/{address}/history` | Address activity history. |
| `GET /v2/search` | Name search and suggestions: `?q=`, `?namespace=`, `?limit=`. Split out of `/v1/names`; no availability or pricing semantics. |
| `GET /v2/events` | Compact event search across name, address, registration, type, and block filters. |
| `GET /v2/resolvers/{chain_id}/{address}` | Resolver overview (numeric `chain_id`). Replaces `/v1/resolvers/.../overview`. |
| `GET /v2/namespaces/{namespace}` | Namespace metadata. `?include=manifests` absorbs `/v1/manifests/{namespace}`. |

Tier 3 — diagnostics (the only routes carrying pipeline vocabulary):

| Route | Purpose |
| --- | --- |
| `GET /v2/diagnostics/names/{name}/coverage` | Full coverage taxonomy: `exhaustiveness`, `enumeration_basis`, `source_classes_considered`, `unsupported_reason` detail. |
| `GET /v2/diagnostics/names/{name}/binding` | Surface-binding explain (binding ids, binding kind, anchors). |
| `GET /v2/diagnostics/names/{name}/authority` | Authority/control explain (token lineage, control vectors, permission lineage). |
| `GET /v2/diagnostics/names/{name}/records` | Record inventory and cache internals: selectors, explicit gaps, unsupported families, version boundaries, value sources. |
| `GET /v2/diagnostics/names/{name}/execution` | Persisted verified-execution explain: trace id, steps, digests, CCIP participation. |

`GET /healthz`, `GET /`, `GET /docs`, and `GET /openapi.json` remain non-contract
helpers.

Deleted from the public catalog (capability absorbed as noted): the `profiles/`
prefix, `/v1/coverage/*` and `/v1/explain/*` (moved under diagnostics),
`/v1/resources/*` and the roles routes (merged into permissions),
`/v1/manifests/*` (merged into namespaces), and exact-name filtering via
`/v1/names?name=` (owned by `GET /v2/names/{name}`).

### Envelope

One success shape for every route:

```json
{
  "data": {},
  "page": {
    "cursor": null,
    "next_cursor": null,
    "page_size": 50,
    "total_count": 123,
    "has_more": true
  },
  "meta": {
    "as_of": {
      "1": {
        "block_number": 19000000,
        "block_hash": "0x...",
        "timestamp": "2026-06-10T00:00:00Z"
      }
    },
    "completeness": "partial",
    "unsupported_fields": ["manager"],
    "unsupported_reason": "resolver_family_pending",
    "source": "indexed"
  }
}
```

Rules:

- `data` is an object on single-resource routes and an array on collections.
- `page` appears on collections only and is identical everywhere, including
  partner-1's `total_count` and `has_more`. Per-input pagination on
  `POST /v2/lookup` uses this same object inside each result.
- `total_count` is nullable. It is populated where an indexed count sidecar
  makes it cheap (the reverse-lookup count path) or where the caller opts in
  via `include=total_count`; routes must not run unconditional full counts on
  the request path to fill it.
- `meta` defaults to the summary above: `as_of` always; `completeness`,
  `unsupported_fields`, and `unsupported_reason` only when the read is not
  clean; `source` when the route supports `?source=`. `?meta=none` strips
  `meta` for byte-sensitive feed reads. There is no `meta=full` on product
  routes — deeper detail is a diagnostics route, not a query knob.
- There are no `declared_state`/`verified_state` parallel trees.
  `source=indexed` returns indexed values; `source=verified` returns the same
  shape from verified execution; `source=both` returns `data` (indexed) plus a
  `verified` sibling section in the same shape, present only in that mode.
  No permanently-null required fields.
- `view` does not exist in `v2`.

The flat record shape (used by `/v2/lookup` detail results, `/v2/names/{name}`,
and as the row shape on address-name collections):

```
name, display_name, namespace, namehash, chain_id, network,
owner, manager, registrant, expires_at, registered_at, created_at,
resolver: {chain_id, address}, primary_name,
addresses: {"60": "0x..."}, text_records: {...}, content_hash,
token_id, registration_id, status, unsupported_fields
```

Reverse results add `is_primary` and `relations` (the subset of
`owner`/`manager`/`registrant` that matched). Optional fields are omitted when
no backed value exists; `unsupported_fields` lists fields the index could not
prove without inventing a value — the explicit-unsupported guarantee is
unchanged, only its spelling.

Event rows (history and events routes) use one shape:
`{type, name, namespace, registration_id, chain_id, block_number, timestamp,
transaction_hash, log_index, data}` with the friendly `type` vocabulary
(`registration`, `renewal`, `transfer`, `authority`, `resolver`, `record`,
`primary_name`, `permission`). Raw upstream event kinds are diagnostics-only.
Permission rows use `{address, scope, powers, registration_id, name}`.

### Parameters

| Parameter | Applies to | Values |
| --- | --- | --- |
| `at` | projection-read routes | RFC 3339 timestamp; selects the snapshot at or before it |
| `finality` | projection-read routes | `latest` (default), `safe`, `finalized` |
| `source` | names, records, primary-name | `indexed` (default), `verified`, `both` |
| `namespace` | name-inferred and collection routes | explicit override / filter |
| `include` | route-documented expansions | per-route allowlist |
| `meta` | all | `summary` (default), `none` |
| `sort`, `order` | every paginated route | route-documented field set + `asc`/`desc`; one style |
| `cursor`, `page_size` | every paginated route | opaque cursor; default 50, max 200 |

Rules:

- Snapshot selection (`at` + `finality`) is uniform across projection-read
  routes — including history and events, which could not pin a snapshot in
  `v1`. Exact multi-chain block pinning (the `chain_positions` JSON selector)
  is diagnostics-only.
- Cursors are opaque and versioned but not bound to the route path string, so
  route evolution does not invalidate outstanding cursors. Cursors remain
  stable under replay for the same snapshot.
- No advertised-but-rejected parameters. If a filter is unimplemented it is
  absent from the contract and listed under deferred capabilities in
  `docs/consumer-capabilities.md`, not reserved in the schema.
- `POST /v2/lookup` body: `{inputs: [...], profile, namespace?}`, where each
  input is `{name}` or `{address, coin_type?, relation?, page_size?, cursor?}`.
  Input order is preserved, one result per input, batch limit 1000
  (configurable via `BIGNAME_API_LOOKUP_BATCH_LIMIT`, with the existing
  `BIGNAME_API_IDENTITY_BATCH_LIMIT` honored as an alias during the bridge
  window). `profile=shadow` is
  removed from the public contract; it survives as an undocumented
  compatibility alias of `detail` until partner-1 shadow comparison completes.

### Error model and statuses

The error envelope is unchanged: `{"error": {"code", "message", "details"}}`.
The mapping is fixed and uniform:

| Code | HTTP | Meaning |
| --- | --- | --- |
| `invalid_input` | 400 | malformed input, unnormalizable path name, bad parameter combination |
| `not_found` | 404 | single-resource GET with no answer |
| `unsupported` | 422 | the route cannot produce its contract for this input |
| `stale` | 409 | coherent selector not yet served by projections |
| `conflict` | 409 | selector cannot form one canonical snapshot |
| `internal_error` | 500 | unexpected failure |

Rules:

- `unsupported` moves from 400 to 422; 409 carries two distinct codes.
- `verification_failed` is deleted (it was never emitted). Verified-execution
  failures surface as `status: "failed"` on the affected section with a
  `failure_reason`, or `stale` when the RPC provider cannot serve the selected
  block.
- One not-found philosophy: single-resource GETs return 404; collections return
  200 with empty `data`; batch lookup results carry in-band `status` per input
  (a batch never 404s). Empty arrays mean known-empty, never unknown.
- One result-status vocabulary everywhere: `ok`, `not_found`, `invalid_name`,
  `unsupported`, `stale`, `failed`, with `unsupported_reason` required when
  `unsupported` and `failure_reason` permitted on `failed`/`not_found`.
- Error messages must not name internal tables, projections, or sidecars.

### Deleted wire surface

Gone from `v2` with no replacement: `resource_hex` and `role_bitmap` (hardcoded
`null` in `v1`), `authority_epoch` (documented, never emitted),
`verification_failed` (documented, never emitted), reserved `view=full`
parameters, the `resolved_address` and `role_bitmap` advertised-but-rejected
filters, the reserved `/v1/events` parameter block, the `resource` vs
`resource_id` alias pair, `contains_nocase`, top-level and section
`provenance` on product routes (diagnostics-only), and routine
`normalized_name`/`canonical_display_name` peers.

### What this ADR deliberately keeps

- Snapshot-pinned reads, multi-chain coherence, and `stale`/`conflict`
  semantics — renamed (`finality`, `as_of`), not removed.
- Verified execution, CCIP-Read support, Basenames transport, and on-demand
  execution on the records/profile/primary-name paths — behind `source=`.
- Explicit unsupported semantics — as `meta.completeness`,
  `unsupported_fields`, and per-item `status`, with the full taxonomy intact on
  diagnostics routes. "Never silent omission" is unchanged.
- History, events, subnames, permissions, resolver overview, namespace
  metadata — reshaped, not cut.
- Namespace inference with explicit override, per ADR 0003's namespace rules,
  now uniform instead of route-exceptional.

This answers the open items in
`docs/internal/api-surface-flattening-scope-decisions.md`: provenance public —
no, diagnostics-only (Q3); coverage public — simplified on product routes, full
taxonomy on diagnostics (Q4); one envelope — yes (Q5); record cache metadata —
diagnostics-only (Q8); selector-state distinctions — kept, via the unified
status vocabulary (Q9); execution traces — diagnostics-only (Q12); search/exact
split — yes (Q20); typed unsupported inside 200s — yes, as `meta` +
per-item `status` (Q31); `view=full` — deleted (Q33); `mode` public — renamed
`source`, only on routes where verified execution is first-class (Q34);
canonicality explicit in public behavior — yes, via `stale`/`conflict` (Q39);
coherent multi-chain snapshots — yes, with exact pinning diagnostics-only
(Q40).

## Upstream anchors

This ADR introduces no new claims about ENSv1, ENSv2, or Basenames behavior. It
renames and restructures bigname-owned API vocabulary only. The ENSIP-15
normalization boundary, namespace inference rules, resolver-profile gating, and
all upstream-anchored semantics in `docs/api-v1.md` and
`docs/consumer-capabilities.md` carry forward unchanged with their existing
citations. The `finality` vocabulary adopts the Ethereum JSON-RPC block-tag
terms already used by `consistency`'s `safe`/`finalized` values.

## Consequences

Positive:

- One vocabulary and one envelope: a consumer learns the record shape and the
  `data`/`page`/`meta` contract once. The partner record shape and the app
  profile shape become the same object.
- The partner path is the front door (`POST /v2/lookup`), not a side-route
  exception, and its proven latency characteristics are unaffected (the
  feed-profile read path and sidecars do not change).
- Pipeline internals stop leaking: product DTOs carry no projection, manifest,
  or lineage vocabulary; diagnostics routes own all of it explicitly.
- Contract rot is eliminated structurally: no reserved parameters, no
  documented-but-never-emitted fields or codes, and generated OpenAPI continues
  to derive from the route table.
- `docs/api-v1.md` ceases to grow; `v2` docs start from the dictionary and one
  envelope instead of per-route exceptions.

Negative / trade-offs:

- Two contracts exist during the bridge window; the route table and handlers
  serve both until `v1` is removed.
- Some field names originate in projection rows in `crates/storage`
  (`declared_summary` passthroughs); `v2` requires a mapping layer at the API
  boundary or projection-side renames, coordinated with Storage and Domain.
- Diagnostics routes become load-bearing for operators and shadow comparison;
  they need the same contract discipline as product routes, just a different
  audience.
- Folding roles/permissions into one route narrows specialist query shapes;
  account-anchored role search moves to `?address=` filters on
  `/v2/names/{name}/permissions` and `/v2/addresses/{address}/names?include=role_summary`,
  which must be validated against the roles-page capability before `/v1/roles`
  is removed.

New failure modes:

- A single envelope serializer bug affects every route at once; envelope
  conformance tests are required from the first slice.
- Namespace inference on all name routes makes the inference table
  correctness-critical; it needs exhaustive tests including the `base.eth`
  exact-match exception.

## Rollout

Doc-first, then code-concurrent slices. Ownership follows
`docs/internal/workstreams.md`: Projections and API own routes, DTOs, OpenAPI,
and contract tests; Storage and Domain review the boundary mapping for
projection-originated field names; Verified Execution reviews `source=` and
diagnostics execution surfaces; Conformance and Fixtures own capability-mapping
tests.

1. Accept or revise this ADR; record the outcome as the Final Direction in
   `docs/internal/api-surface-flattening-scope-decisions.md`.
2. Write `docs/api-v2.md` and `docs/api-v2-routes.md` from the dictionary and
   route catalog above; generate `docs/api-v2.openapi.json` from the route
   table. `docs/api-v1.md` is frozen except for corrections.
3. Implement `v2` routes over the existing read layer (the shared exact-name
   funnel in `apps/api/src/support/snapshots.rs` and the route-definition
   table). ADR 0003 slices 3–6 (snapshot service, record read model,
   support-state consolidation) remain valid implementation work and become
   `v2` enablers; this ADR supplies the target model those slices were missing.
4. Add envelope-conformance, dictionary-conformance (no banned `v1` spellings
   on `v2` routes), and product-route denylist tests (no pipeline vocabulary)
   alongside the existing OpenAPI assertions.
5. Point the partner-1 shim and the first app integration at `v2` only. Rerun
   the partner latency benchmarks against `POST /v2/lookup` before cutover.
6. Sunset `v1`: docs stop advertising it at `v2` availability; routes are
   removed before partner-1 cutover, while the consumer count is still ~zero.
   No long deprecation window is owed to a contract nobody integrated against.

Sequencing with the 2026-06 remediation
(`docs/internal/remediation-2026-06.md`, a temporary tracking doc): the
remediation completes before `v2` implementation begins (planning decision,
2026-06-10). Steps 1–2 (docs only) conflict with nothing and may proceed
during the remediation; step 3 starts after the remediation closes out, so
`v2` inherits the corrected semantics (primary-name status classification,
ENSIP-15 input validation at the path boundary, SQL keyset pagination)
instead of re-fixing them. Because the remediation runs first, three
execution notes follow. The first two are advisory, not gates: remediation
work that `v2` later replaces is an accepted cost (planning decision,
2026-06-10), and the remediation does not wait on this ADR's acceptance.

1. WS-G's "accepted-but-inert `meta=full` / `include=record_summaries`" item
   may take either branch; the disclose-and-document branch is cheaper since
   `v2` deletes both knobs.
2. WS-G's pagination/cursor rewrite ideally targets the `v2` cursor rules
   (opaque, versioned, not route-path-bound); if it ships route-bound cursors
   instead, `v2` re-cuts them.
3. At remediation teardown, remaining P2/P3 `v1`-contract polish items are
   re-triaged against `v2` supersession: work that only reconciles `v1` knobs
   or docs that `v2` deletes is dropped with a rationale line, not done.

`v1` behavior fixes that converge implementation on the documented `v1`
contract are corrections, not contract changes — the `v1` freeze does not
block them.

## Alternatives considered

**Reshape `v1` in place instead of publishing `v2`.** Cheaper on path strings,
but `docs/api-v1.md` § Versioning explicitly requires a major version for
changed enum meanings, defaults, and required fields, and an in-place rewrite
would make the frozen docs retroactively false. The prefix costs nothing;
honoring the repo's own versioning policy keeps the contract docs trustworthy.

**Keep the dual full/compact envelope model and only rename fields.** Renaming
alone fixes vocabulary but preserves eight envelope shapes, the `meta`-dependent
arity changes, and the `view`/`mode`/`meta` knob matrix — the structural half of
the fragmentation. Rejected because the envelope count, not the spelling, is
what forces consumers to learn three API philosophies.

**Adopt GraphQL for the product surface.** partner-1 accepts either transport,
and GraphQL would solve field selection. Rejected for now: the latency path is
already met with REST + sidecars, the app capability set is small and
enumerable, and a second query engine is a larger complexity budget than the
problem justifies. The flat record shape ports to GraphQL later if field
selection becomes a real need.

**Keep `resource_id` routes public.** Resource-anchored routes
(`/v1/resources/*`) expose the indexer's internal identity model as API
structure. Rejected: `registration_id` survives as a field and filter for the
stability guarantees consumers actually need (dedupe, permission joins), without
requiring callers to navigate an ontology the partner and app never asked for.

**Drop diagnostics from the public API entirely.** Largest simplification, but
it would delete the explain/coverage surfaces that shadow comparison and
operational debugging rely on, and ADR 0003 already rejected hiding
auditability. Rejected in favor of confining, not deleting.

## References

- `docs/adrs/0003-api-surface-flattening-plan.md` (target model this ADR
  completes)
- `docs/adrs/0004-conceptual-deduplication-gate.md`
- `docs/internal/api-surface-flattening-scope-decisions.md`
- `docs/partners/partner-1-indexing-requirements.md`
- `docs/partners/partner-1-identity-facade-benchmarks.md`
- `docs/api-v1.md`, `docs/api-v1-routes.md`, `docs/consumer-capabilities.md`
