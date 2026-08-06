# API v2 Routes

Per-route reference for the development-time `/v2` surface accepted in
[ADR 0006](adrs/0006-api-v2-product-surface.md). Contract principles,
dictionary, envelope, status vocabulary, finality rules, cursor rules, and
error shape live in [`api-v2.md`](api-v2.md).

Routes below use the `/v2` prefix. C2 removes the former `/v1` API without
renaming these routes; the public edge remains a separate C3 change.

`GET /healthz` remains the unversioned operator health contract outside the
versioned product routes. `GET /`, `GET /docs`, and `GET /openapi.json` are not
served.

## Shared Route Rules

Name-shaped routes infer the namespace from the name itself: exact `base.eth`
is `ens` because upstream treats it as the L1 root domain handled by the
Mainnet L1Resolver (upstream: .refs/basenames/src/L1/L1Resolver.sol:L13 @ basenames@1809bbc)
(upstream: .refs/basenames/src/L1/L1Resolver.sol:L154 @ basenames@1809bbc);
`*.base.eth` is `basenames`, the Base-issued subdomain space
(upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc); other
supported names are `ens`. Name-shaped routes accept optional `namespace` as
an override and always echo the resolved `namespace` in the response.

Single-resource GETs return `404 not_found` when no answer exists. Collections
return `200` with empty `data` when the result set is known-empty. Batch lookup
uses in-band result `status` and never returns `404` for one missing input. The
primary-name route is the documented single-resource exception: valid tuples
with no claim, unsupported verification, or mismatched verification return
`200` with in-band `status`.

All collection routes use the standard `page` object: `cursor`,
`next_cursor`, `page_size`, nullable `total_count`, and `has_more`.

The top-level latest-state collections are `GET /v2/names/{name}/subnames`,
`GET /v2/names/{name}/history`, `GET /v2/permissions`,
`GET /v2/addresses/{address}/names`,
`GET /v2/addresses/{address}/history`, `GET /v2/search`, `GET /v2/events`,
and `GET /v2/diagnostics/events`. They omit `meta.as_of` and
`meta.as_of_token`, and their cursors bind the collection anchor, namespace,
filters, and sort without claiming a frozen snapshot. Newly issued cursors
carry no snapshot token; a legacy cursor's snapshot component is ignored. They
accept omitted or explicit `finality=latest`. An `at` selector returns `400 invalid_input` with
`at is not supported because collection routes read latest state`;
`finality=safe` or `finality=finalized` returns `400 invalid_input` with
`finality must be latest because collection routes read latest state`.
Issue #188 option 1 remains the storage follow-up: revision-bound cursors with
explicit cursor-expired semantics. These restrictions lift when that storage
contract exists.

Field ownership:

- Shared record, lookup, primary-name, event, and count concepts are dictionary
  fields in `api-v2.md`.
- Lookup-only transport fields are route-local: `id` is caller correlation
  inside the echoed `input`, `kind` is the result discriminator, `profile` and
  `inputs` are request controls, `record` holds a single name result, `records`
  holds reverse result rows, and `changed`, `input_name`, and `reason` live
  inside `normalization` only.
- Name-filter request fields are route-local: `q` is shared by search and
  address-name collections, `match` is search-only, and `dedupe` is
  address-name-only.
- Records-route containers are route-local: `records`, `inventory`,
  `known_keys`, `unset_keys`, `unsupported_keys`, and `value` are the per-key
  answer and inventory shape for one resolver-record route, not shared domain
  vocabulary.
- Permission lineage containers are route-local: `lineage`, `grant`,
  `revocation`, `inheritance_path`, and `transfer_behavior` exist only on
  `include=lineage` for `/v2/permissions`.
- Primary-name containers are route-local: `answers` holds the returned
  source answer entries, and `raw_claim_name` preserves an invalid reverse
  claim exactly as observed for that tuple.
- Role-summary containers are route-local: `grants` groups
  `{grant_scope, powers}` entries under one `address` inside
  `role_summary`.
- Namespace metadata containers are route-local: `networks` is the
  product-facing list of public chain mappings for one namespace.
- Resolver overview containers are route-local: `bound_names` is the nested
  names collection inside one resolver overview object.
- Ops status containers are route-local: `/v2/status` owns `chains`,
  `latest_block`, `indexed_block`, `safe_block`, `finalized_block`,
  `lag_blocks`, `lag_seconds`, `pending_invalidation_count`,
  `pending_invalidation_count_capped`, `dead_letter_count`, `network_block`, `network_head_observed_at`,
  `network_head_age_seconds`, `network_head_status`,
  `ingestion_lag_blocks`, and `ingestion_lag_seconds`.
- Diagnostic-only field names are route-local to diagnostics unless they are
  already dictionary fields. Diagnostics may use pipeline vocabulary because
  their tier is explicitly separate from product reads.

## Tier 1: Lookup Primitives

### `POST /v2/lookup`

- Method/path: `POST /v2/lookup`
- Tier: lookup primitive.
- Purpose: batched forward name-to-record and reverse address-plus-coin-type
  resolution. `profile=feed` is the latency path; `profile=detail` returns
  full records.
- Request parameters: body `{inputs, profile, namespace?}`. Each input is
  `{id?, name}` or `{id?, address, coin_type?, relation?, page_size?, cursor?}`.
  Reverse inputs default to `coin_type=60` when omitted. Reverse `relation`
  accepts a comma-separated set of `owner`, `manager`, and `registrant`; `any`
  is the normalized all-three set. Reverse rows match when any listed relation
  matches. Batch limit is 1000 and is configurable with
  `BIGNAME_API_LOOKUP_BATCH_LIMIT`.
- Response shape: the common envelope. `data` is an array of result objects,
  not an object wrapper. The array contains one result per input in caller
  order. Each result is `{input, kind, status, unsupported_reason?,
  failure_reason?, normalization?, record?, records?, page?}`. `input` echoes
  the caller-supplied input, including `id` when supplied; omitted `id` is not
  synthesized. `kind` is `name` or `address`. Name results use `record` for the
  single record object. Reverse results use `records` for zero or more record
  rows with `is_primary` and `relations` in addition to the shared record
  fields. Reverse `input.relation` echoes the normalized relation set; `any`
  serializes as `owner,manager,registrant` and reordered sets use canonical
  dictionary order. `profile=feed` returns a documented core-field subset of
  the same record object; it does not introduce another DTO.
- Pagination behavior: top-level `page` is absent. Reverse inputs use the
  standard `page` object inside each result. Detail and feed use identical
  pagination semantics; feed only reduces returned fields. Reverse inputs
  default `page_size` to 50 and use the common max of 200. Relation filters
  that cannot be satisfied by one storage role (including exact `owner`, exact
  `registrant`, and partial relation sets such as `owner,manager`) may return
  an as-filled page with `has_more=true` when the API reaches its bounded
  post-filter scan cap; clients continue with the returned `next_cursor`.
- Status semantics: per-result `status` uses the common result vocabulary.
  Name misses are in-band `not_found`; invalid names are in-band
  `invalid_name`. Reverse misses return `status=ok` with an empty `records`
  array for the input. Lookup record-level reason values are mapped to product
  vocabulary before serialization; current values include `read_failed`,
  `exact_name_profile_not_supported`, `mixed_exact_name_corpus`, and
  `unsupported_reason_missing`.
- Snapshot behavior: lookup selects the current schema-v2 phase head and reads
  `bigname_phase` name, inventory, and address-name projections published for
  one completed projection-phase generation. Because projection publication is
  incremental, an unchanged row target may precede the selected head; it may
  not be ahead, and a same-height target must match the selected hash. Lookup
  revalidates both `chain_heads` and that generation after the read. An
  invalid target, phase lag, or mid-request head/projection change returns `409
  stale`.
- Replaces (v1): `POST /v1/identity:lookup`.

### `GET /v2/status`

- Method/path: `GET /v2/status`
- Tier: lookup primitive.
- Purpose: per-chain indexing readiness.
- Request parameters: none.
- Response shape: `data.status` plus `data.chains`, keyed by `chain_id`.
  The schema-v2 project phase does not use the legacy invalidation queue, so
  `data.pending_invalidation_count` is `0`,
  `data.pending_invalidation_count_capped` is `false`, and
  `data.dead_letter_count` is `0`. Each chain entry carries `latest_block`,
  `indexed_block`, `safe_block`,
  `finalized_block`, `lag_blocks`, `lag_seconds`, `network_block`,
  `network_head_observed_at`, `network_head_age_seconds`,
  `network_head_status`, `ingestion_lag_blocks`, `ingestion_lag_seconds`, and
  route-local ops `status`.
- Storage mapping: the chain set is the union of
  `bigname_phase.chain_heads` and `bigname_phase.chain_phase_state`.
  `latest_block`, `safe_block`, and `finalized_block` map to the corresponding
  `chain_heads` positions. `indexed_block` maps to the `project` phase's
  `current_block_number`. `lag_seconds` compares the timestamps of the exact
  latest-head and project-current hashes in `bigname_phase.chain_lineage`.
  Missing head, project, or lineage rows preserve the existing nullable fields.
  If the phase schema has not been created yet, API startup uses an empty
  expected-chain set and this route returns the same empty, `degraded` status
  shape instead of preventing the API process from starting.
- The existing per-chain `status` field also maps the `project` phase
  lifecycle, redo marker, and newest per-chain
  `bigname_phase.service_heartbeats` timestamp.
  The most recent completed `project` publication remains the indexed position
  while the next live-follow pass is running. A running phase is therefore
  eligible for `ready` when that completed publication is present and its
  block and time lag are within `BIGNAME_API_STATUS_MAX_BLOCK_LAG` and
  `BIGNAME_API_STATUS_MAX_LAG_SECS`. Readiness also requires the publication's
  interpreter content hash to match this API build and, at the same height as
  the stored head, its block hash to match exactly. A generation mismatch is
  `degraded`. A running phase without a completed publication is `degraded`;
  lag beyond either threshold is `stale`. A failed
  `project` phase is `stale`, while a paused or redoing phase is `degraded`.
  Missing phase-runner heartbeat evidence is `degraded`; a heartbeat older
  than `BIGNAME_API_PHASE_HEARTBEAT_MAX_AGE_SECS` is `stale`. This phase-only
  threshold defaults to 60 seconds so a long database statement between
  five-second runner heartbeat opportunities does not reuse the stricter
  legacy worker-health threshold.
- `lag_blocks` and `lag_seconds` are independently nonnegative. Each field
  clamps its own canonical-versus-projected difference at `0`.
- Pagination behavior: none.
- Status semantics: route-local ops `status` is `ready`, `degraded`, or
  `stale`. This is the only non-result `status` enum in `v2`. `project`
  publication lag beyond either configured threshold, or a fresh provider
  comparison beyond either configured ingestion-lag threshold, is `stale`.
  Missing stored readiness or a provider observation
  whose `network_head_status` is `stale`, `unavailable`, `pending`, or
  `unconfigured` is `degraded` when its projection is within the configured
  thresholds. Excessive projection lag takes precedence and is `stale`;
  `network_head_status` still reports the provider state. The provider head is
  refreshed asynchronously
  under a timeout and cache TTL; this route reads only the cache and never
  waits for provider I/O. If the latest refresh fails after a successful one,
  `network_head_status` becomes `unavailable` immediately while the last head,
  observation time, age, and lag values remain as cached evidence.
- Replaces (v1): `GET /v1/status`.

## Tier 2: Product Reads

### `GET /v2/names/{name}`

- Method/path: `GET /v2/names/{name}`
- Tier: product read.
- Purpose: name-profile read, using the flat record shape plus registration summary.
- Request parameters: path `name`; query `namespace`, `at`, `finality`,
  `source`. `source` accepts `indexed` or `verified`; omitting it is identical
  to `source=indexed`. This name-profile route does not accept `source=auto`.
- Response shape: `data` is one flat record object using dictionary fields.
  The registration summary is not nested; it is represented by
  `registration_id`, `token_id`, `owner`, `manager`, `registrant`,
  `registered_at`, `created_at`, `expires_at`, and `registration_status` on
  the same object when backed. An ENSv1 wrapper-backed row also carries
  `wrapper_state` with the current [`wrapped`](glossary.md#wrapped-namewrapper-state),
  [`emancipated`](glossary.md#emancipated-namewrapper-state), or
  [`locked`](glossary.md#locked-namewrapper-state) lifecycle value. The field
  follows the upstream NameWrapper lifecycle names.
  (upstream: .refs/ens_v1/contracts/wrapper/README.md:L32 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/wrapper/README.md:L34 @ ens_v1@91c966f)
  The field
  is omitted after an emancipated or locked wrapper position expires; a plain
  wrapped position remains `wrapped` because expiry clears its fuses without
  clearing its owner. (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L843 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L848 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L849 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L852 @ ens_v1@91c966f)
  Fuse-effect gating accepts the full upstream `uint64` expiry domain. A valid
  `MAX_EXPIRY` therefore keeps the lifecycle value active at representable
  served block timestamps even when `expires_at` is omitted because that public
  timestamp cannot be represented.
  (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L57 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L843 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L848 @ ens_v1@91c966f)
  A `.eth` second-level name keeps its lifecycle value during the registrar
  grace period, even though owner modification and transfer powers stop at the
  earlier grace boundary; `wrapper_state` is not itself a complete permission
  summary. (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L48 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L218 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L221 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L820 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L825 @ ens_v1@91c966f)
  `manager` is omitted when no forward-read source can derive it; it is not
  emitted as a permanent null placeholder. The
  name-profile portion uses `name`, `display_name`, `namespace`, `namehash`, `resolver`,
  `addresses`, `text_records`, `content_hash`,
  `primary_name`, `primary_address`, `chain_id`, `network`, `status`, and
  `unsupported_reason`/`failure_reason`/`unsupported_fields` when those fields
  are served. With `source=verified`, the resolver-record-backed fields
  `addresses`, `text_records`, `content_hash`, and `primary_address` are built
  by a fresh schema-v2 lookup at the current readable position, using the same
  verified path as `/v2/names/{name}/records`; indexed resolver-record values
  are not substituted into those fields. The registration and identity summary
  fields (`registration_id`, `token_id`, `owner`, `manager`, `registrant`, dates,
  `registration_status`, `wrapper_state`, `name`, `display_name`, `namespace`, `namehash`,
  `resolver`, `primary_name`, `chain_id`, and `network`) remain indexed
  projection values because they are not resolver records. Verified responses
  include `meta.as_of`/`meta.as_of_token` for the positions used by the fresh
  lookup. For cross-chain resolution, the authoritative position is admitted
  from the selected product snapshot and the auxiliary position is the
  canonical execution position retained by the projected row; it may be older
  than the newest `chain_heads` position selected for that chain, but never
  newer; an anchor at the same height must have the same block hash. They create
  no legacy trace or reusable execution outcome. Provider connect, DNS, TLS,
  connection-reset, and other transport failures abort the whole request with
  `500 internal_error`; they are not flat-record `status=stale` results. On a
  `200` name-profile response,
  `status` is the flat-record result: `ok` for clean indexed reads; `failed`,
  `stale`, or `unsupported` may appear only when `source=verified` cannot serve
  the verified sections, with `failure_reason` or `unsupported_reason` carrying
  the product reason when available;
  `not_found` and `invalid_name` are unreachable in-record.
- Pagination behavior: none.
- Status semantics: valid names with no name-profile data return `404 not_found`.
  Invalid path names return `400 invalid_input`.
- Replaces (v1): `GET /v1/names/{namespace}/{name}` and
  `GET /v1/profiles/names/{name}`.

### `GET /v2/names/{name}/records`

- Method/path: `GET /v2/names/{name}/records`
- Tier: product read.
- Purpose: resolver records.
- Request parameters: path `name`; query `namespace`, `at`, `finality`,
  `source=indexed|verified|auto`, `keys`, `include=inventory`.
- Response shape: `data` returns resolver record values using `namespace`,
  `resolver`, `addresses`, `text_records`, and `content_hash`. `keys` is a
  comma-separated
  record-key allowlist using the existing app key grammar: `addr:<coin_type>`,
  `text:<key>`, `avatar`, and `contenthash`. Requested-key outcomes are also
  returned in route-local `records`, keyed by the requested key; each value is
  `{status, value?, unsupported_reason?, failure_reason?}`. `source=verified`
  and verified fallback from `source=auto` execute a fresh schema-v2 lookup on
  every request. They do not read or write the legacy execution cache. A direct
  live/indexed disagreement may update the guarded resolution divergence
  ledger; restored agreement may clear a matching active row, while wildcard
  and CCIP-Read answers do not mutate it. A selected position
  that the provider cannot serve is reported per key as `status=stale` with a
  `failure_reason`; a completed provider response timeout or malformed result
  remains an in-band `status=failed` result.
  `meta.as_of` and `meta.as_of_token` report the authoritative and actual
  hash-pinned execution positions returned by the lookup engine. On a
  cross-chain path, the execution position may be an older canonical projected
  position than the auxiliary `chain_heads` position initially selected by the
  route. It may not be newer, and a position at the same height must have the
  same block hash; violations are stale before provider execution. Provider
  connect, DNS, TLS, connection-reset, and other transport
  failures abort the whole request with `500 internal_error`; they are not
  per-key stale answers and `source=auto` does not return a partial blend.
  Product records use product reason vocabulary: retained-selector misses use
  `value_not_retained`, and phase-unsupported record families use
  `record_family_not_supported`.
  `source=auto` blends per key: indexed answers are used where they satisfy the
  requested key, and only the remaining supported keys fall back to verified
  lookup. A Basenames auto read remains Base-scoped when no fallback key
  remains; it selects the Ethereum resolution-auxiliary position only when it
  will attempt that verified fallback. If projection movement removes the last
  fallback key while the expanded snapshot is being selected, the request
  returns `409 stale` so a retry can return an indexed Base-scoped response.
  Explicit `keys` and the inventory-derived default verified selector set are
  both limited to 200 record keys. When omitted `keys` would derive more than
  200 keys, `source=verified` returns `422 unsupported` before any provider call;
  callers can supply `keys` to select a smaller set. The verified flat
  name-profile has the same 200-key server-derived limit and returns `422
  unsupported` because that route has no key selector.
  `include=inventory` adds route-local
  `inventory: {known_keys, unset_keys, unsupported_keys}`. Deep inventory
  internals stay on diagnostics.
- Pagination behavior: none.
- Status semantics: a missing name returns `404 not_found`. Missing, unset, or
  unsupported requested record values are reported with the common result
  `status` vocabulary inside the record answer rather than by changing the
  envelope.
- Replaces (v1): `GET /v1/names/{namespace}/{name}/records` and record
  sections of `GET /v1/profiles/names/{name}`.

### `GET /v2/names/{name}/subnames`

- Method/path: `GET /v2/names/{name}/subnames`
- Tier: product read.
- Purpose: direct subnames.
- Request parameters: path `name`; query `namespace`, `include=counts`,
  `cursor`, `page_size`, and optional `finality=latest`. `at` and historical
  `finality` values are rejected by the shared latest-state collection rule.
- Response shape: `data` is an array of dedicated subname rows in dictionary
  vocabulary: `name`, `display_name`, `namespace`, `namehash`, `labelhash`,
  `owner`, `registrant`, `registration_status`, `registered_at`,
  `created_at`, and `expires_at`. Resolver records are not included here;
  use `GET /v2/names/{name}/records` for `resolver`, `addresses`,
  `text_records`, and `content_hash`.
  `include=counts` adds `subname_count`, the row's direct subname count.
- Pagination behavior: standard collection pagination by
  `display_name` ascending.
- Snapshot behavior: the parent and subname rows are selected from current
  state. The response omits `meta.as_of` and `meta.as_of_token`, and its cursor
  carries no snapshot validity claim. True as-of child enumeration is deferred
  to the revision-bound storage follow-up.
- Status semantics: no direct subnames returns `200` with empty `data`.
  Missing parent names return `404 not_found`.
- Replaces (v1): `GET /v1/names/{namespace}/{name}/children`.

### `GET /v2/names/{name}/history`

- Method/path: `GET /v2/names/{name}/history`
- Tier: product read.
- Purpose: name history.
- Request parameters: path `name`; query `namespace`,
  `scope=name|registration|both`, `cursor`, `page_size`, and optional
  `finality=latest`. `at` and historical `finality` values are rejected by the
  shared latest-state collection rule.
- Response shape: `data` is an array of dedicated lean event rows:
  `{type, name, namespace, registration_id, block_number, timestamp,
  transaction_hash, log_index}`. `registration_id` is present only when the
  event row is registration-resource anchored. Rows never include before/after
  state, raw normalized-event payloads, or a `data` change object. Friendly
  `type` vocabulary: `registration`, `renewal`, `release`, `expiry`,
  `transfer`, `authority`, `resolver`, `record`, `primary_name`, `permission`.
  Raw upstream or pipeline event kinds are diagnostics-only and are not emitted
  by this product route.
- Pagination behavior: standard newest-first collection pagination by chain
  position. The cursor is bound to the resolved namespace, parent name, scope,
  and sort. Product event-type filtering is applied after loading the storage
  page, so `page_size` is an upper bound on returned product rows; a page may
  contain fewer than `page_size` rows when non-product normalized events are
  interleaved.
- Scope behavior: `scope=name` reads name-surface events only,
  `scope=registration` reads registration-resource events associated with the
  requested name, and `scope=both` reads both sets. `scope` defaults to `both`.
- Snapshot behavior: the parent anchor and history rows are selected from
  current state. The response omits `meta.as_of` and `meta.as_of_token`, and
  its cursor carries no snapshot validity claim. True as-of history
  enumeration is deferred to the revision-bound storage follow-up.
- Status semantics: no matching history returns `200` with empty `data`.
  Missing names return `404 not_found`.
- Replaces (v1): `GET /v1/history/names/{namespace}/{name}`.
  Registration-id anchored history from `GET /v1/history/resources/{resource_id}`
  moves to `GET /v2/events?registration_id=...`. `scope=registration` on this
  route is limited to registration lifecycles associated with the requested
  name.

### `GET /v2/permissions`

- Method/path: `GET /v2/permissions`
- Tier: product read.
- Purpose: flat permission rows by name, registration, or address, including
  registrations that are no longer a name's current one.
- Request parameters: at least one of `name`, `registration_id`, or `address`;
  filters are combinable. Query `namespace`, `include=lineage`, `cursor`,
  `page_size`, and optional `finality=latest`. `at` and historical `finality`
  values are rejected by the shared latest-state collection rule.
- Response shape: `data` is an array of permission rows
  `{address, grant_scope, powers, registration_id, name}`. `include=lineage`
  adds route-local `lineage` per row:
  `{grant, revocation?, inheritance_path?, transfer_behavior?}`. Product lineage
  is a bounded summary; deep provenance stays on diagnostics authority/events
  routes. Lineage objects expose only allowlisted fields: `kind`,
  `registration_id`, `resolver: {chain_id, address}`, and `powers` when those
  fields apply. `kind` values are `event`, `permission`,
  `registration_authority`, `registration_rebound`, `ens_v1_authority`,
  `resolver_root_fallback`, and `registry_root_fallback`. Diagnostics-only
  storage keys such as event provenance, upstream/root resources,
  contract-instance ids, changed powers, and manifest versions are omitted.
  `grant_scope` is `{kind, detail}`. Detail is `{}` for `root`, `registry`,
  and `registration`;
  `{resolver: {chain_id, address}}` for `resolver` with numeric `chain_id`;
  `{chain_id, manager}` for `record_manager`; `{predecessor_registration_id}`
  for `migration_derived`; and `{transport}` for `transport_derived`.
  `migration_derived` and `transport_derived` are
  [reserved surface](glossary.md#reserved-surface) with no producer: the shapes
  are documented so the enum stays exhaustive, but no admitted source family
  emits either kind, so neither appears in a response today.
  `transport_derived` describes an abandoned cross-chain ENSv2 design and never
  will.
- Pagination behavior: standard collection pagination.
- Snapshot behavior: a `name` filter resolves its current registration anchor,
  and permission rows come from current state. The response omits `meta.as_of`
  and `meta.as_of_token`; completeness metadata remains available. Its cursor
  carries no snapshot validity claim. True as-of permission enumeration is
  deferred to the revision-bound storage follow-up.
- Status semantics: no matching permission rows returns `200` with empty
  `data`, including when a `name` filter has no registration anchor in the
  current state. Unsupported filter combinations return `422 unsupported`.
  An absent or older projection-owned permission
  publication version returns `409 stale` before permission rows are decoded.
  A publication revision change while rows and summaries are read also returns
  `409 stale`. The version and revision are schema/publication compatibility and
  request-coherence guards, not freshness watermarks. When `name` or
  `registration_id` binds the read to a registration, the projection-owned
  per-registration permission summary classifies the result: full support adds
  no completeness metadata, missing or partial support returns
  `meta.completeness=partial` with
  `unsupported_reason=permission_support_unknown`, and an ENSv1 wrapper returns
  `meta.completeness=unsupported` with
  `unsupported_reason=wrapper_holder_permissions_not_supported`. An
  address-only read is always at least `partial` with the wrapper reason because
  zero-row wrapper registrations are absent from the permission-row fan-out; a
  missing or partial summary for a returned registration changes the reason to
  `permission_support_unknown`. Projected rows are not suppressed by these
  classifications.
- Replaces (v1): `GET /v1/resources/{resource_id}/permissions`,
  `GET /v1/roles`, `GET /v1/names/{namespace}/{name}/roles`, and
  `GET /v1/resources/lookup`.

### `GET /v2/addresses/{address}/names`

- Method/path: `GET /v2/addresses/{address}/names`
- Tier: product read.
- Purpose: names related to an address.
- Request parameters: path `address`; query `namespace`, `relation`, `q`,
  `sort=name|expires_at|registered_at`, `order=asc|desc`,
  `dedupe=name|registration`, `include=role_summary`, `cursor`, `page_size`,
  and optional `finality=latest`. `at` and historical `finality` values are
  rejected by the shared latest-state collection rule.
  `q` applies prefix matching to the dictionary `name` field case-insensitively:
  the prefix is lowercased to match the normalized name, and full Unicode
  normalization of partial prefixes is a follow-up. This route does not accept
  `match`. `relation` accepts a comma-separated set of v2 vocabulary values
  `owner`, `manager`, and `registrant`; `any` normalizes to all three values.
  Rows match when any listed relation matches. The storage relations map as
  token-holder -> `owner`, effective-controller -> `manager`, and
  registrant -> `registrant`. `dedupe=name` groups by name surface and is the
  default; `dedupe=registration` groups by registration resource.
- Response shape: `data` is an array of record-shaped rows with `name`,
  `display_name`, `namespace`, `namehash`, `owner`, `registrant`,
  `registration_status`, `registered_at`, `created_at`, and `expires_at`.
  Address-name rows add `is_primary` and `relations`, where `relations` is the
  subset of `owner`, `manager`, and `registrant` that matched. `is_primary` is
  evaluated against that row namespace's coin-type-60 primary-name claim, not a
  route-wide namespace shortcut. Resolver records are not included; use
  `GET /v2/names/{name}/records` for resolver data.
  `include=role_summary` adds
  `role_summary: [{address, grants: [{grant_scope, powers}]}]` grouped by the
  permission subject address and `record_count` when record inventory exists
  for the row. `record_count` counts the known record selectors for the name's
  current registration, including unsupported-family selectors and excluding
  explicit gaps. `grant_scope` uses the same shape documented for
  `GET /v2/permissions`.
- Pagination behavior: standard collection pagination. Cursors are bound to
  address, optional namespace filter, normalized relation set, `q`, dedupe
  mode, sort, and order.
- Snapshot behavior: address-name rows come from current state. The response
  omits `meta.as_of` and `meta.as_of_token`; completeness metadata for
  `include=role_summary` remains available. Its cursor carries no snapshot
  validity claim. True as-of address-name enumeration is deferred to the
  revision-bound storage follow-up.
- Status semantics: no related names returns `200` with empty `data`.
  Malformed addresses return `400 invalid_input`. `include=role_summary`
  conditionally returns `409 stale` when the compatible projection-owned
  permission publication version is absent or old; the same address-name read
  without that expansion remains available. The expansion also returns `409
  stale` when the permission publication revision changes while it is assembled.
  The expansion batch-loads projection-owned permission summaries for every
  registration on the served page. If all are full, no completeness metadata is
  added. A missing or partial summary returns `meta.completeness=partial`,
  `meta.unsupported_fields=["role_summary"]`, and
  `unsupported_reason=permission_support_unknown`. An ENSv1 wrapper summary
  uses the same `partial` response classification and unsupported field with
  `unsupported_reason=wrapper_holder_permissions_not_supported`. Projected
  grants remain in `role_summary`, but the expansion is non-authoritative;
  therefore an empty wrapper summary is not a proven empty permission set.
  Missing summary metadata takes precedence when a page contains both cases.
- Replaces (v1): `GET /v1/addresses/{address}/names` and address-relation
  uses of `GET /v1/names`.

### `GET /v2/addresses/{address}/primary-name`

- Method/path: `GET /v2/addresses/{address}/primary-name`
- Tier: product read.
- Purpose: primary name for an address.
- Request parameters: path `address`; query `coin_type` default `60`,
  `namespace` default `ens`, and `source`. This is a current-state read and
  does not accept `at` or `finality`.
- Response shape: `data` is
  `{address, coin_type, namespace, answers, verification?}` with `coin_type`
  serialized as a JSON number. `answers` is an
  array of `{source, status, name?, raw_claim_name?, unsupported_reason?,
  failure_reason?}` entries. When `source` is omitted, the route returns one
  entry for each answer source in stable `indexed`, then `verified` order;
  unsupported sources are represented by an entry with `status=unsupported`,
  not omitted.
  Supplying `source=indexed` or `source=verified` narrows the `answers` array
  to that source for single-source callers; every indexed entry comes from
  `bigname_phase.primary_names_current`, regardless of source selection. A
  successful stored raw claim is normalized for the indexed product name even
  when its raw spelling was not already normalized. `verification` is
  `{status, name?, unsupported_reason?, failure_reason?}` and appears when the
  fresh lookup produces a verification outcome. As an explicit exception,
  it also appears when the request includes the `verified` source and the
  live reverse claim fails the pre-forward normalization gate. That gate result
  represents the fresh hash-pinned reverse lookup and normalization decision,
  not a forward resolver call or a persisted execution trace.
  An indexed-only response never includes `verification`. The `verified` answer
  entry is the source-specific payload; `verification` is the typed comparison
  summary and must not contradict that entry. Claimed-vs-verified remains one
  call without `declared_state`/`verified_state`. When a served head is
  available, `meta.as_of` and `meta.as_of_token` record the served positions
  for staleness attribution and shadow-diff correlation. ENS/60 verification
  uses the schema-v2 lookup engine's current readable Ethereum position and
  pins its reverse and optional forward calls to that block hash. It persists
  neither a legacy trace/outcome nor a divergence row. When `source` is omitted,
  the indexed claim is read from `bigname_phase.primary_names_current` and
  returned beside the verified answer only when the current `chain_heads`
  position and exact completed `project` publication generation match the
  lookup before verified execution and remain unchanged after the indexed
  read; otherwise the request returns `409 stale`. Live results never change
  the indexed answer. Basenames verified primary-name lookup is unsupported;
  indexed Basenames responses remain Base-scoped.
- Pagination behavior: none.
- Snapshot behavior: current-state read over chain-derived primary-name state.
  The route does not accept `at` or `finality`. Successful responses carry
  `meta.as_of` and `meta.as_of_token` for indexed state or the current readable
  Ethereum position used by fresh ENS/60 verification. No metadata field
  implies cache reuse or a persisted execution identity. Provider transport
  failures abort the request with `500 internal_error`; they are not verified
  answer entries with `status=stale`.
- Status semantics: answer entries use in-band `status`. Valid tuples with no
  indexed claim return an `indexed` entry with `status=not_found`. Unsupported,
  not-found, failed, and mismatched verified outcomes return `200` with the
  corresponding `verified` entry status. When the requested output includes the
  verified source, a successful live claim whose raw spelling
  differs from its normalized form produces a verified answer and
  `verification` with `status=not_found` and
  `failure_reason=claim_not_normalized`. An unnormalizable live claim
  leaves a missing indexed tuple as `status=not_found` and produces a verified
  answer plus `verification` with `status=not_found` and
  `failure_reason=claim_name_not_normalizable`. A completed JSON-RPC failure,
  malformed response, or configured provider or CCIP-Read gateway response
  timeout produces an in-band verified `status=failed` result. Missing provider
  configuration or a selected-block rejection returns whole-request `409
  stale`. A provider or gateway connect-phase timeout, DNS failure, TLS
  failure, connection reset, or other transport failure returns whole-request
  `500 internal_error`; no trace or outcome is persisted, so the next read
  retries. Malformed addresses return `400 invalid_input`.
  `source=indexed` does not enter verified-execution rate or concurrency
  admission; omitted `source` and `source=verified` do because they run the
  fresh lookup.
- Replaces (v1): `GET /v1/primary-names/{address}`.

### `GET /v2/addresses/{address}/history`

- Method/path: `GET /v2/addresses/{address}/history`
- Tier: product read.
- Purpose: address activity history.
- Request parameters: path `address`; query `namespace`, `relation`,
  `scope=name|registration|both`, `cursor`, `page_size`, and optional
  `finality=latest`. `at` and historical `finality` values are rejected by the
  shared latest-state collection rule.
  `namespace` defaults to `ens` when omitted. `relation` accepts a
  comma-separated set of `owner`, `manager`, and `registrant`; `any`
  normalizes to all three values. Rows match when any listed relation matches.
- Response shape: `data` is an array of compact event rows using the shared
  friendly `type` vocabulary.
- Pagination behavior: standard collection pagination.
- Snapshot behavior: address-history rows come from current state. The response
  omits `meta.as_of` and `meta.as_of_token`, and its cursor carries no snapshot
  validity claim. True as-of/finality row-bounding is deferred to the
  revision-bound storage follow-up.
- Status semantics: no matching activity returns `200` with empty `data`.
  Malformed addresses return `400 invalid_input`.
- Replaces (v1): `GET /v1/history/addresses/{address}`.

### `GET /v2/search`

- Method/path: `GET /v2/search`
- Tier: product read.
- Purpose: name search and suggestions. No availability or pricing semantics.
- Request parameters: query `q`, `match=prefix|contains` default `prefix`,
  `namespace`, `cursor`, `page_size`, and optional `finality=latest`. `at` and
  historical `finality` values are rejected by the shared latest-state
  collection rule.
- Response shape: `data` is an array of record-shaped name search results in
  dictionary vocabulary.
- Pagination behavior: standard collection pagination.
- Snapshot behavior: search rows come from current state. The response omits
  `meta.as_of` and `meta.as_of_token`, and its cursor carries no snapshot
  validity claim. True as-of search enumeration is deferred to the
  revision-bound storage follow-up.
- Status semantics: no matches returns `200` with empty `data`. `q` is
  required; a missing or empty `q` returns `400 invalid_input`.
- Replaces (v1): search, suggestion, and exact-name-filter uses of
  `GET /v1/names`; exact name profiles move to `GET /v2/names/{name}`.

### `GET /v2/events`

- Method/path: `GET /v2/events`
- Tier: product read.
- Purpose: compact event search across name, address, registration, type, and
  block filters.
- Request parameters: query `namespace`, `name`, `address`,
  `registration_id`, `type`, `from_block`, `to_block`, `cursor`, `page_size`,
  and optional `finality=latest`. `at` and historical `finality` values are
  rejected by the shared latest-state collection rule. When `name` is present
  and `namespace` is omitted, namespace is inferred from the name; `namespace`
  defaults to `ens` only when there is no name filter.
- Response shape: `data` is an array of compact event rows with friendly
  `type` vocabulary. Raw upstream event kinds are diagnostics-only.
- Pagination behavior: standard collection pagination.
- Snapshot behavior: event rows come from current state. The response omits
  `meta.as_of` and `meta.as_of_token`, and its cursor carries no snapshot
  validity claim. True as-of/finality row-bounding is deferred to the
  revision-bound storage follow-up.
- Status semantics: no matching events returns `200` with empty `data`.
  Malformed filters return `400 invalid_input`.
- Replaces (v1): `GET /v1/events` compact event search.

### `GET /v2/resolvers/{chain_id}/{address}`

- Method/path: `GET /v2/resolvers/{chain_id}/{address}`
- Tier: product read.
- Purpose: resolver overview for numeric `chain_id` and resolver `address`.
- Request parameters: path `chain_id`, `address`; query `include` for
  route-documented sections, `at`, `finality`, `cursor`, `page_size`.
- Response shape: `data` is a resolver overview in product vocabulary. The
  route includes route-local `bound_names: {data, page}`, a nested collection
  of record-shaped name rows that replaces resolver-based name filtering.
  `include=aliases` exposes binding rows as `{namespace, name, display_name,
  namehash}` and resolver alias rows as `{namespace, from_name, to_name,
  from_display_name?, to_display_name?, state, resolver: {chain_id, address},
  to_registration_id?}`. `to_name` is `null` when the latest alias state is
  `removed` or `unknown`. `include=events` exposes `{count, by_type}` where
  `by_type` aggregates raw resolver event kinds that map to the same friendly
  `type` vocabulary as `GET /v2/events`; raw kinds without a product event type
  remain included in `count` but are excluded from `by_type`.
- Pagination behavior: standard collection pagination applies to the
  nested `bound_names.page` object. The top-level response has no `page`.
- Snapshot behavior: the resolver overview and bound names read
  `bigname_phase` projections from one completed projection-phase generation. A row
  target may precede the selected position when the row was unchanged by later
  incremental publications; it may not be ahead, and a same-height target must
  match the selected hash. The projection-phase generation is revalidated after the
  read, and an invalid target or changed generation returns `409 stale`.
- Status semantics: an otherwise valid current/latest resolver with no overview
  row returns `404 not_found`. For `at`, `safe`, or `finalized`, a missing
  current projection cannot prove historical absence and returns `409 stale`.
  Bound-name listings under `at`, `safe`, or `finalized` are drawn from
  current-state projections: a name bound at the requested position but
  unbound since is absent from the listing rather than flagged, consistent
  with the coverage `exhaustiveness: not_asserted` disclosure.
  A resolver overview with no bound names returns `200` with an empty
  bound-names section. Malformed `chain_id` or `address` returns `400
  invalid_input`.
- Replaces (v1): `GET /v1/resolvers/{chain_id}/{resolver_address}/overview`
  and the `GET /v1/names?resolver=...` filter.

### `GET /v2/namespaces/{namespace}`

- Method/path: `GET /v2/namespaces/{namespace}`
- Tier: product read.
- Purpose: namespace metadata and supported-capability summary in product
  vocabulary.
- Request parameters: path `namespace`.
- Response shape: `data` is `{namespace, capabilities, networks}`.
  `capabilities` is a product-facing object keyed by capability name; each
  value is `{completeness, unsupported_reason?}` using the common
  completeness vocabulary. `networks` is an array of `{network, chain_id?}`
  entries when the namespace has public chain mappings. Control-plane metadata
  omits `meta.as_of` and `meta.as_of_token`.
- Pagination behavior: none.
- Status semantics: unsupported public namespaces return `404 not_found`.
- Replaces (v1): `GET /v1/namespaces/{namespace}`. Operational namespace
  internals move to the diagnostics namespace route documented below.

## Tier 3: Diagnostics

Diagnostics are the only routes that may carry pipeline vocabulary. Product
route vocabulary restrictions do not apply to the diagnostic payloads below.

Diagnostic snapshot rules:

- `/v2/diagnostics/names/{name}/coverage`,
  `/v2/diagnostics/names/{name}/binding`,
  `/v2/diagnostics/names/{name}/authority`,
  `/v2/diagnostics/names/{name}/records`,
  and `/v2/diagnostics/names/{name}/execution` accept `at` and `finality` and
  carry `meta.as_of`/`meta.as_of_token` because they explain one selected
  snapshot.
- `/v2/diagnostics/events` follows the shared latest-state collection rule: it
  omits snapshot metadata and rejects `at` and historical `finality`.
- Diagnostics execution selection uses the exact name, `keys`, and selected
  snapshot. Omitting `at` selects the latest persisted execution artifact.
  RFC 3339 `at` selects the newest persisted artifact whose requested chain
  positions are at or before the selected positions. If multiple artifacts
  match, the deterministic tie-break is newest `finished_at`, then greatest
  `execution_trace_id`.
- `/v2/diagnostics/namespaces/{namespace}/manifests` omits `meta.as_of` and
  `meta.as_of_token`; it is control-plane metadata.

### `GET /v2/diagnostics/names/{name}/coverage`

- Method/path: `GET /v2/diagnostics/names/{name}/coverage`
- Tier: diagnostics.
- Purpose: full coverage taxonomy.
- Request parameters: path `name`; query `namespace`, `at`, `finality`.
- Response shape: `data` includes `exhaustiveness`, `enumeration_basis`,
  `source_classes_considered`, and `unsupported_reason` detail.
- Pagination behavior: none.
- Status semantics: missing names return `404 not_found`; unsupported coverage
  classes return diagnostic detail rather than product simplification.
- Replaces (v1): `GET /v1/coverage/{namespace}/{name}`.

### `GET /v2/diagnostics/names/{name}/binding`

- Method/path: `GET /v2/diagnostics/names/{name}/binding`
- Tier: diagnostics.
- Purpose: surface-binding explain.
- Request parameters: path `name`; query `namespace`, `at`, `finality`.
- Response shape: `data` includes binding ids, binding kind, and anchors.
- Pagination behavior: none.
- Status semantics: missing names return `404 not_found`.
- Replaces (v1): `GET /v1/explain/names/{namespace}/{name}/surface-binding`.

### `GET /v2/diagnostics/names/{name}/authority`

- Method/path: `GET /v2/diagnostics/names/{name}/authority`
- Tier: diagnostics.
- Purpose: authority/control explain.
- Request parameters: path `name`; query `namespace`, `at`, `finality`.
- Response shape: `data` includes token lineage, control vectors, and
  permission lineage.
- Pagination behavior: none.
- Status semantics: missing names return `404 not_found`.
- Replaces (v1): `GET /v1/explain/names/{namespace}/{name}/authority-control`.

### `GET /v2/diagnostics/names/{name}/records`

- Method/path: `GET /v2/diagnostics/names/{name}/records`
- Tier: diagnostics.
- Purpose: record inventory and cache internals.
- Request parameters: path `name`; query `namespace`, `at`, `finality`, and
  optional `keys`. `keys` uses the same record-key grammar as
  `/v2/names/{name}/records`: `addr:<coin_type>`, `text:<key>`, `avatar`, and
  `contenthash`.
- Response shape: `data` is
  `{record_inventory, record_cache, value_sources, comparison,
  comparison_explicit_gaps?}`.
  `record_inventory` is
  `{record_version_boundary, enumeration_basis, selectors, explicit_gaps,
  unsupported_families, last_change}` using the existing diagnostic selector
  row fields `record_key`, `record_family`, `selector_key`, and `cacheable`.
  `record_cache` is `{record_version_boundary, entries}` where each entry is
  `{record_key, record_family, selector_key, status, value?,
  unsupported_reason?, failure_reason?}`. `value_sources` summarizes the
  indexed or verified origin per key. `comparison` is keyed by `record_key` and
  carries side-by-side `{indexed, verified}` record answers for the former
  `mode=both` workflow. Without `keys`, `comparison` defaults to the first 16
  inventory-derived supported record keys in deterministic order. The indexed
  `record_inventory` and `record_cache` sections remain complete. When more
  than 16 default comparison keys are available, `comparison_explicit_gaps`
  lists each uncompared selector as
  `{record_key, record_family, selector_key, gap_reason}` with
  `gap_reason=diagnostics_comparison_default_limit_exceeded`. With `keys`, the
  comparison is scoped exactly to the requested keys. On-demand verified
  comparison execution is chunked to at most 4 in-flight selector RPCs per
  diagnostics request burst. Identity objects in these diagnostics use dictionary
  spellings (`namespace`, `name`, `display_name`, `registration_id`), while
  pipeline-only identifiers such as `normalized_event_id` keep their pipeline
  names per the tier-3 rule.
- Pagination behavior: none.
- Status semantics: missing names return `404 not_found`.
- Replaces (v1): record inventory/cache diagnostics formerly embedded in
  `GET /v1/profiles/names/{name}` and
  `GET /v1/names/{namespace}/{name}/records`, including the former
  `mode=both` comparison.

### `GET /v2/diagnostics/names/{name}/execution`

- Method/path: `GET /v2/diagnostics/names/{name}/execution`
- Tier: diagnostics.
- Purpose: persisted verified-execution explain.
- Request parameters: path `name`; query `namespace`, `at`, `finality`, and
  required `keys`.
  `keys` uses the same record-key grammar as `/v2/names/{name}/records`. The
  route is verified-only; callers select the persisted artifact by exact name,
  requested keys, and selected snapshot. The route rejects duplicate or
  malformed keys with `400 invalid_input`.
- Response shape: `data` includes trace id, steps, digests, and CCIP
  participation. Identity objects in the payload use dictionary spellings
  (`namespace`, `name`, `display_name`, `registration_id`), while pipeline-only
  identifiers keep their pipeline names per the tier-3 rule.
- Pagination behavior: none.
- Status semantics: missing persisted execution artifacts return
  `404 not_found`.
- Replaces (v1): `GET /v1/explain/resolutions/{namespace}/{name}/execution`.

### `GET /v2/diagnostics/namespaces/{namespace}/manifests`

- Method/path: `GET /v2/diagnostics/namespaces/{namespace}/manifests`
- Tier: diagnostics.
- Purpose: active manifest versions, source families, deployment epochs, and
  capability flags.
- Request parameters: path `namespace`.
- Response shape: `data` is the active manifest summary in diagnostics
  vocabulary.
- Pagination behavior: none.
- Status semantics: unsupported public namespaces return `404 not_found`.
- Replaces (v1): `GET /v1/manifests/{namespace}`.

### `GET /v2/diagnostics/events`

- Method/path: `GET /v2/diagnostics/events`
- Tier: diagnostics.
- Purpose: raw normalized-event rows: upstream event kinds, event identity, and
  full provenance.
- Request parameters: query `namespace`, `name`, `address`,
  `registration_id`, `type`, `from_block`, `to_block`, `cursor`, `page_size`,
  and optional `finality=latest`. `at` and historical `finality` values are
  rejected by the shared latest-state collection rule. When `name` is present
  and `namespace` is omitted, namespace is inferred from the name; `namespace`
  defaults to `ens` only when there is no name filter.
- Response shape: `data` is an array of raw normalized-event rows in
  diagnostics vocabulary:
  `{normalized_event_id, event_identity, namespace, name?, registration_id?,
  event_kind, source_family, manifest_version?, source_manifest_id?,
  chain_position, transaction_hash, log_index, raw_fact_ref, derivation_kind,
  canonicality_state, before_state?, after_state?, provenance, coverage}`.
- Pagination behavior: standard collection pagination.
- Snapshot behavior: diagnostic event rows come from current state. The
  response omits `meta.as_of` and `meta.as_of_token`, and its cursor carries no
  snapshot validity claim. True as-of/finality row-bounding is deferred to the
  revision-bound storage follow-up.
- Status semantics: no matching rows returns `200` with empty `data`.
- Replaces (v1): `view=full` on `GET /v1/history/names/{namespace}/{name}`,
  `GET /v1/history/resources/{resource_id}`, and
  `GET /v1/history/addresses/{address}`.
