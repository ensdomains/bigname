# API v2 Routes

> **Prefix note (#315):** the public route prefix is now `/v1`. This file's
> name (`api-v2-routes`) is historical, naming the ADR 0006 contract
> generation; it is not the wire prefix. A rename of the contract docs is
> tracked separately.

Per-route reference for the `/v1` surface accepted in
[ADR 0006](adrs/0006-api-v2-product-surface.md). Contract principles,
dictionary, envelope, status vocabulary, finality rules, cursor rules, and
error shape live in [`api-v2.md`](api-v2.md).

Routes below use the `/v1` prefix. The former `/v1` API listed under each
route's "Replaces (v1)" line was removed in C2; this contract took over the
prefix in #315, and the public edge serves it.

`GET /healthz` remains the unversioned operator health contract outside the
versioned product routes. `GET /` serves a static landing page
(`apps/api/src/home.html`) and `GET /docs` serves the static API reference
page (`apps/api/src/docs.html`); both describe this contract and neither is
part of it. `GET /openapi.json` is not served.

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

The history collections, `/v1/events`, name history (with or without
`include=child_registrations`), and address history, read history as it stood
at one block. The first page records, for every chain in the request scope,
the block the served Project publication stood at, by number and hash. That
block is the walk's [cursor bound](glossary.md#cursor-bound). The cursor also
carries each of those chains' Interpret and Project redo counters and a digest
of the active manifest revisions. The request scope is the requested
namespace's chains, or the chains of every active public namespace when none
is selected. Every page of the walk reads events at or below the bound and
judges which events belong to the collection from evidence at or below it, so
the rows, their order, and `page.total_count` stay the same on every page,
however many blocks are published during the walk. Address history has one
exception: the four relation kinds in the [known
limitation](#history-collection-filters) of the history collection filters are
judged from current state, so when such a relation between the address and a
name ends after the bound block, a later page of the walk loses that name's
remaining rows and reports a smaller `page.total_count`. The cursor does not
expire when that happens. `meta.as_of` on every page,
the first included, is the bound: its block number, hash, and timestamp per
chain, not the API's current progress. The one current-state field on these
pages is the display name on `/v1/events` and address history rows, which is
read from current name state after the page. Cursor fields are not
authenticated, so a client can edit the binding, for example to an older
readable bound block. An edited binding is validated exactly like a minted
one, and a binding that passes serves a consistent read at that bound while
every expiry rule below still applies.

A history cursor expires, returning `409 stale` with a message to restart
without a cursor, when any of these holds: the bound block is no longer
readable on some chain (a reorg replaced it), an Interpret or Project redo
counter of a chain in scope changed, the manifest revisions changed, the
served publication is behind the bound, the scope's chain set changed, or the
chain's readable head or its served publication reached the walk's
[classification horizon](glossary.md#classification-horizon) on some chain,
the next declaration start at which Project may classify a resolver
differently. The head counts because Project's publications become visible
before its recorded position moves: Project commits its projection swap first
and the phase runner records the new position afterwards, so for a while, and
indefinitely after a crash in that gap, the served position lags the data the
reads see. Project only ever targets the readable head, so the head reaches
the horizon at or before Project publishes there, and a cursor sitting just
below a horizon can expire one block early. For the same reason a first page
read while the head has reached a horizon that Project has not yet published
answers with the retry message until Project publishes there. The cursor records the horizon per
chain; one that differs from the horizon the
manifests give for its bound also expires the cursor, and one at or below the
bound block is malformed and returns `400 invalid_input`. This is
conservative: a reorg or redo entirely above the bound also
expires the cursor, because the redo counters do not say which blocks were
rewritten. New blocks, a Project run in progress, and a Project publication
that lands during the read never expire it while the head and the publication
stay below the classification horizon. An Interpret or Project redo in progress returns
`409 stale` without data; once it finishes, a cursor issued before it has a
changed counter and must restart. Each request checks all of this before it
reads and again, in a fresh transaction, after its page transaction. A first
page whose check after the read fails returns `409 stale` with a message to
retry the same request, since it has no cursor to drop. A cursor issued before
this rule returns `409 stale` with a message saying so and must be restarted;
a cursor carrying both the older publication token and a block binding is
malformed and returns `400 invalid_input`.

The other product collections, `GET /v1/names`, subnames, address names, and
permissions, read current state. Their cursors bind anchors, filters, sorting,
the served project publication (including same-height replacement) and
manifest revisions. Counts and rows use the same filters; time-dependent
expiry filtering retains the first page's evaluation time. These reads
revalidate the publication before returning and disclose `meta.as_of`. A
changed or unavailable publication, or an older unbound cursor, returns `409
stale` and requires restarting without a cursor. A first page, which has no
cursor to drop, whose publication changed while it was read returns the same
`409 stale` with a message saying so; retrying the same request reads the new
publication. No historical projection is retained by a pagination token.

Both kinds of binding conservatively cover the requested namespace, or all
active public namespaces when none is selected. A count spanning namespaces
requires readable publications for all of them; otherwise it returns `409
stale` rather than a misleading partial total. Registry and resolver
collections use the current-state publication fence in addition to their
documented selected-chain position.

These collections still reject `at`, `finality=safe`, and
`finality=finalized` with `400 invalid_input`; omitted or explicit
`finality=latest` is accepted. `meta.as_of_token` is omitted because neither
kind of cursor can be replayed through `at`.

Search and diagnostic-event cursors retain their existing latest-state behavior
without a publication-validity claim. Search discloses request-scope `meta.as_of`
and `meta.as_of_completeness`; diagnostic events omit snapshot metadata. The
per-input `POST /v1/lookup` cursor contract is documented separately. Historical
collection replay remains deferred.

At the planned ENSv1→ENSv2 [re-derivation
boundary](glossary.md#re-derivation-boundary), slices 1 and 2 deploy
together with [PR #391](https://github.com/ensdomains/bigname/pull/391) under one
[interpreter content hash](glossary.md#interpreter-content-hash), one full
source re-walk, and one
[Project publication](glossary.md#projection) decision for
`ethereum-sepolia`. Production makes only that activated Project publication;
candidate-versus-activated behavior remains a replay and
acceptance-test distinction, not a production serving interval. Other chains
retain their ordinary independent publication decisions.

In the test environment, the slice-1 acceptance gate saves each normalized-
event-backed route's `next_cursor` at a fixed readable chain head, performs and
publishes the full Interpret and Project re-walk, and submits that old cursor to
the post-re-walk test publication. The control and candidate test runs hold
every other shared-boundary input constant, including PR #391's topology
serializer. For
`/v1/events`, name history, and address history, the re-walk runs as a redo
that raises the redo counters their cursors carry, so the old cursor must
return the `409 stale` restart described under [Shared Route
Rules](#shared-route-rules), even though the re-walk preserves product
behavior. A redo counter says that history was rewritten, not which blocks, so
every redo expires those cursors. Any other product cursor surface backed by
normalized-event row identity must resume from the same normalized-event keyset
anchor with identical remaining product rows, pages, fields, `has_more`, and
summary behavior. Fresh post-re-walk history cursors must walk identical product
pages. Because a keyset anchor may be an unmapped event absent from the
response, the corpus places an unmapped normalized event at a product-page
boundary and proves no visible row is skipped or duplicated. This default
product-event exception does not remove an explicitly requested `type` from
cursor anchor validation.
`/v1/diagnostics/events` must accept its old cursor
and continue from the same stable normalized-event anchor, but its remaining
rows and fields may include the expected new candidate diagnostics.
The numeric `normalized_event_id` of a pre-existing diagnostic row may change
across the re-walk; its `event_identity` and pre-existing semantic fields remain
stable, apart from the explicitly allowed candidate diagnostic additions.
Implementations may preserve numeric normalized-event IDs or resolve the old
token through stable `event_identity` plus its stored sort tuple; these are
alternative storage strategies. Freshly issued cursor bytes may differ. The
gate separately verifies fresh post-re-walk cursors on every covered route.

That identical-product rule applies to a re-walk whose declared
contract preserves product behavior. An intentional [interpreter content
hash](glossary.md#interpreter-content-hash) change may instead have a documented
field and route-membership delta. For the
[#348](https://github.com/ensdomains/bigname/issues/348) and
[#529](https://github.com/ensdomains/bigname/issues/529) changes, an existing late
ENSv2 resolver `RecordChanged` or `RecordVersionChanged` keeps its
`event_identity`, gains `logical_name_id`, keeps `resource_id=null`, and updates
the corresponding `raw_fact_ref.interpreter_state_key` attribution field. Its
`before_state` may also become the preceding `after_state` from the
logical-name/resource-null state stream that the event now joins. Issue #348
retains the surface from registry/root evidence; issue #529 retains a surface
observed only by resolver `AliasChanged` before a batch boundary. Those events
may consequently enter name-filtered diagnostics and product history. An
outstanding cursor has no continuation guarantee across this behavior-changing
boundary and may be rejected. Consumers must discard pre-#348/#529 cursors and
restart from the first page; fresh post-publication cursors continue normally.

The [#613](https://github.com/ensdomains/bigname/issues/613) interpreter change
keeps the original [pre-surface](glossary.md#pre-surface) ENSv1 registry `ResolverChanged` row unchanged,
then adds a name- and resource-linked, [state-derived](glossary.md#state-derived-normalized-event) `ResolverChanged` when the
first active [name surface](glossary.md#surface-name-surface) is learned. Product
events or name history may therefore gain one historical resolver row, while
diagnostics may gain each linked resource copy. A cursor issued before this
change has no continuation guarantee and may be rejected. Consumers must
discard pre-#613 cursors and restart from the first page; fresh post-publication
cursors continue normally.

These boundaries do not claim fresh/resumed parity for the known pre-existing
exception: when a resolver-emitted resource equals `namehash(N)`,
named-resource and alias preimages can share one retained [interpreter state
key](glossary.md#interpreter-state-key), so resumed interpretation can lose the
named-resource resolver hint and diverge from a fresh walk
([#560](https://github.com/ensdomains/bigname/issues/560); evidence is checked
in as an ignored collision probe).
If an ended resource retains a resolver pointer to the emitter, its rebuildable
record-inventory projection may change. The event remains resource-less and
does not restore `name_current.resource_id`, so the released or expired name's
name and record routes still expose no current record inventory.

Position-bound history cursors are a client-visible boundary too. A
`/v1/events`, name history, or address history cursor issued before them
carries the older publication token and no block binding. The route answers it
once with `409 stale` and a message saying it was issued before
position-bound pagination; the client restarts without a cursor, and the new
cursor keeps working as blocks are published. During a rolling deploy an
instance without this change refuses the new cursors in the same way, so a
client may be asked to restart more than once until every instance runs it.
The current-state collections keep their publication-bound cursors and are
unchanged by this boundary.

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
- Record-answer containers are route-local: `records` and `value` are the
  per-key answer shape for one resolver-record route, not shared domain
  vocabulary. The inventory container `inventory: {known_keys, unset_keys,
  unsupported_keys, abi_content_types, abi_unsupported_reason?}` has one shape
  on the two routes that serve it,
  `GET /v1/names/{name}/records?include=inventory` and `POST /v1/lookup` with
  `include=inventory`; the records route defines its meaning and the lookup
  route serves it unchanged, per name, so a batch of names is one request.
- Permission lineage containers are route-local: `lineage`, `grant`,
  `revocation`, `inheritance_path`, and `transfer_behavior` exist only on
  `include=lineage` for `/v1/permissions`.
- Primary-name containers are route-local: `answers` holds the returned
  source answer entries, and `raw_claim_name` preserves an invalid reverse
  claim exactly as observed for that tuple.
- Role-summary containers are route-local: `grants` groups
  `{grant_relation?, grant_scope, powers}` entries under one `address` inside
  `role_summary`.
- Namespace metadata containers are route-local: `networks` is the
  product-facing list of public chain mappings for one namespace.
- Resolver overview containers are route-local: `bound_names` is the nested
  names collection inside one resolver overview object.
- Ops status containers are route-local: `/v1/status` owns `chains`,
  `latest_block`, `indexed_block`, `safe_block`, `finalized_block`,
  `lag_blocks`, `lag_seconds`, `pending_invalidation_count`,
  `pending_invalidation_count_capped`, `dead_letter_count`, `network_block`, `network_head_observed_at`,
  `network_head_age_seconds`, `network_head_status`,
  `ingestion_lag_blocks`, and `ingestion_lag_seconds`.
- Diagnostic-only field names are route-local to diagnostics unless they are
  already dictionary fields. Diagnostics may use pipeline vocabulary because
  their tier is explicitly separate from product reads.

### Caching headers on indexed single-resource reads

`GET /v1/names/{name}`, `GET /v1/names/{name}/records`,
`GET /v1/resolvers/{chain_id}/{address}`, and
`GET /v1/addresses/{address}/primary-name` answer an indexed read with two
HTTP caching headers derived from the response itself:

- `ETag: W/"<response-body-hash>"` — a weak validator derived from the complete
  serialized JSON response body using Keccak-256. It changes when the served
  representation changes, including after a projection rebuild or API change
  at the same chain positions. Identical bodies share a validator whether read
  at latest state or pinned with `at`. The validator is opaque to clients and
  is separate from `meta.as_of_token`, which remains the snapshot selector.
- `Cache-Control: public, max-age=12, stale-while-revalidate=48` — one
  Ethereum slot of freshness, after which a browser or edge revalidates with
  `If-None-Match`; an edge may keep serving the held body for four more slots
  while it revalidates.

A request whose `If-None-Match` lists that validator (weak or strong form, or
`*`) receives `304 Not Modified` with the same two headers and no body. The
headers appear only on `200` responses whose body carries `meta.as_of_token`,
and only for indexed reads: `source=verified` and `source=auto` execute against
a provider per request and are never cached, and the primary-name route
qualifies only when `source=indexed` is explicit, because its default answer
set includes the verified source. Errors, `POST /v1/lookup`, and every
collection route carry neither header.

## Tier 1: Lookup Primitives

### `POST /v1/lookup`

- Method/path: `POST /v1/lookup`
- Tier: lookup primitive.
- Purpose: batched forward name-to-record and reverse address-plus-coin-type
  resolution. `profile=feed` is the latency path; `profile=detail` returns
  full records.
- Request parameters: body `{inputs, profile, namespace?, include?}`. Each
  input is `{id?, name}` or `{id?, address, coin_type?, relation?, page_size?,
  cursor?}`. `include` is the comma-separated expansion string the `GET`
  routes take, and allows `inventory` only; it requires `profile=detail`
  (`profile=feed` with `include` is `400 invalid_input`, since feed is the
  field-budgeted path) and applies to name inputs; reverse rows never carry
  it.
  Reverse inputs default to `coin_type=60` when omitted. Reverse `relation`
  accepts a comma-separated set of `owner`, `manager`, and `registrant`; `any`
  is the normalized all-three set. Reverse rows match when any listed relation
  matches. `relation=resolves_to` stands alone and answers the names whose
  current `addr:<coin_type>` resolver record resolves to the input address for
  the input `coin_type`, with the same matching rule, ENSIP-19 default-address
  fallback, and exclusions as `GET /v1/addresses/{address}/names?relation=resolves_to`
  for one decimal coin type (lookup inputs take a numeric `coin_type` only and
  have no `evm` form);
  combining it with an authority relation or `any` returns `400
  invalid_input`, and `any` never includes it. Batch limit is 1000 and is
  configurable with `BIGNAME_API_LOOKUP_BATCH_LIMIT`.
- Response shape: the common envelope. `data` is an array of result objects,
  not an object wrapper. The array contains one result per input in caller
  order. Each result is `{input, kind, status, unsupported_reason?,
  failure_reason?, normalization?, record?, records?, page?}`. `input` echoes
  the caller-supplied input, including `id` when supplied; omitted `id` is not
  synthesized. `kind` is `name` or `address`. Name results use `record` for the
  single record object. Reverse results use `records` for zero or more record
  rows with `is_primary` and `relations` in addition to the shared record
  fields; a `resolves_to` row carries `relations: ["resolves_to"]` and
  `resolution: {coin_type, record_key}`, and its `is_primary` and
  `primary_address` follow the input `coin_type`. Reverse `input.relation`
  echoes the normalized relation set; `any`
  serializes as `owner,manager,registrant` and reordered sets use canonical
  dictionary order. `profile=feed` returns a documented core-field subset of
  the same record object; it does not introduce another DTO.
  `profile=detail` records carry `authority` (`ens_v1` or `ens_v2`) when the
  projection selected an ENSv1/ENSv2 arm for the name, and `migrated_at` when
  that `ens_v2` authority was proven by an ENSv1→ENSv2 migration transition;
  both apply to name results and reverse rows alike and are omitted on feed
  records and on `status=unsupported` records. Reverse inputs accept no
  `authority` filter yet; filter client-side or use
  `GET /v1/addresses/{address}/names?authority=`.
  A name result classified as `registration_status=unregistered` always omits
  `registration_id`. It also omits `resolver` and resolver-record fields unless
  it is
  an ownerless ENSv1 or Basenames registry row whose current registry resolver
  pointer is retained (a [serving resource](glossary.md#serving-resource)), or
  an ENSv2 TLD with a current
  [root-registry resolver pointer](glossary.md#root-registry-resolver-pointer)
  and no projected authority.
  That classified row serves its resolver without acquiring registration
  identity or control. Indexed records are served when its serving resource has
  supported inventory. The TLD row keeps `status=unsupported` with
  `current_authority_not_projected` and the full detail shape; only the
  resolver and resolver-record fields are added.
  `profile=detail` name results and reverse rows take `addresses`,
  `text_records`, `content_hash`, and `primary_address` only from a record
  inventory whose coverage is authoritative. When the serving resolver's
  inventory row is `unsupported`, those four fields are omitted and listed in
  the record's `unsupported_fields`, exactly as when no inventory exists;
  `status` and `resolver` keep following the name row. The per-key reason is
  served by `GET /v1/names/{name}/records`.
  With `include=inventory`, each `profile=detail` name result whose record
  has a current registration and a record inventory row on its serving
  resource also carries `record.inventory: {known_keys, unset_keys,
  unsupported_keys, abi_content_types, abi_unsupported_reason?}`, the records
  route's container with the records route's meaning: `known_keys` and
  `unset_keys` come from a record inventory whose coverage is authoritative,
  and an `unsupported` inventory row lists every product key it knows about
  under `unsupported_keys` with the other two empty. No `keys` allowlist
  applies, so `unsupported_keys` holds only keys the row itself carries.
  `abi_content_types` and `abi_unsupported_reason` have the records route's
  meaning too, and the whole batch reads their evidence at once rather than
  once per name. The container is omitted, not empty, on a record with no
  serving inventory: a `status=unsupported` or `unregistered` record, a
  reservation, or a name whose serving resource has no inventory row; the
  four value fields are then listed in `unsupported_fields` as before. This
  is the batch form of the records route's inventory for a caller holding many
  names, bounded by the batch limit above; per-key values, `source=verified`,
  and the `keys` allowlist stay on the records route.
  See [registration status](api-v2.md#status-vocabulary) for the upstream
  basis.
  An ownerless ENSv2 reservation does not meet this exception, even if identity
  attached to a resource or record inventory was retained for audit, unless it
  is a root-registry TLD reservation served through the root-registry resolver
  pointer above. This intentionally differs from ENSv2, which stores and
  returns a reservation resolver until expiry.
  (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L255-L258 @ ens_v2@a971bd64)
  (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L461-L478 @ ens_v2@a971bd64)
- Pagination behavior: top-level `page` is absent. Reverse inputs use the
  standard `page` object inside each result. Detail and feed use identical
  pagination semantics; feed only reduces returned fields. Reverse inputs
  default `page_size` to 50 and use the common max of 200. A reverse cursor
  binds the deployment-derived public namespace set and is rejected if that
  set changes. Relation filters that cannot be satisfied by one storage role
  (including exact `owner`, exact `registrant`, and partial relation sets such
  as `owner,manager`) may require multiple broad candidate batches to assemble
  one response page. The API retains the selected [projection
  generation](glossary.md#projection-generation) across those batches. Before
  issuing a second or later broad batch, it revalidates that generation and
  returns retryable `409 stale` if it changed. The API may return an as-filled
  page with `has_more=true` when it reaches the bounded post-filter scan cap;
  clients continue with the returned `next_cursor`. A `resolves_to` input pages
  `address_records_current` in name order (not primary-first), its cursor
  binds the address, coin type, relation, and public namespace set, and its
  `total_count` is null.
- Status semantics: per-result `status` uses the common result vocabulary.
  Name misses are in-band `not_found`; invalid names are in-band
  `invalid_name`. Name-only and exact-scope latest reads return retryable `409
  stale` when the selected namespace is undergoing an Interpret redo. Reverse
  misses return `status=ok` with an empty `records`
  array for the input. Lookup record-level reason values are mapped to product
  vocabulary before serialization; current values include `read_failed`,
  `exact_name_profile_not_supported`, `mixed_exact_name_corpus`, and
  `unsupported_reason_missing`. The contracted per-name authority replacement
  is documented in
  [`architecture.md`](architecture.md#ensv1ensv2-current-authority). When its
  exact-name consumer slice is activated, `conflicting_current_ens_authority`
  covers Mainnet overlap without a provable boundary.
  `independent_ens_deployments_overlap` covers
  ordinary Sepolia overlap without a proven ENSv1→ENSv2 migration boundary; a proven
  Sepolia boundary follows the same per-name authority rule. The exact
  [shared ENS infrastructure](glossary.md#shared-ens-infrastructure) names—root,
  `eth`, `reverse`, and `addr.reverse`—instead select ENSv2 when the ENSv2 arm is
  current and ENSv1 evidence, current or historical, exists without proof.
  When that shared-infrastructure rule selects ENSv2, it overrides the ordinary
  no-proof handling below, so those names carry neither Mainnet's
  `conflicting_current_ens_authority` nor Sepolia's
  `independent_ens_deployments_overlap`.
  Historical ENSv2 evidence alone does not qualify, and `.reverse` descendants
  do not inherit the exception. These values replace the
  blanket mixed-corpus reason; intake from the planned [ENSv2 migration source
  family](glossary.md#source-family) alone does not add them. An address lookup
  returns `409 conflict` when the deployment has no ready public namespace.
  After the authority replacement is activated, an unsupported mixed-history
  name result retains `input`, `kind`, and a `record` containing only `name`,
  `display_name`, `namespace`, `namehash`, `status`, and
  `unsupported_reason`. It omits registration, control, lifecycle, resolver,
  record, relation, permission, and primary-name fields from both source
  families rather than presenting either binding as current.
- Snapshot behavior: lookup selects the current schema-v2 phase head and reads
  `bigname_phase` name, inventory, and address-name projections published for
  one completed projection-phase generation. For each reverse result, the
  readable name fetched with the candidate row is the common source for the
  emitted normalized and display names, label-derived fields, primary-name
  ordering, the `is_primary` result, and the reverse cursor. A `resolves_to`
  input additionally requires each `address_records_current` row it serves to
  be published at or before the selected head and omits unreadable current name
  rows. Unsupported rows are also omitted except for the documented TLD
  [root-registry resolver pointer](glossary.md#root-registry-resolver-pointer)
  case with `current_authority_not_projected`; that case serves resolver records
  without claiming registration authority. Public reverse lookup with no explicit
  namespace derives its snapshot scope from the namespaces served by the
  deployment, excluding a namespace
  while its selected authority chain has Interpret `redo_in_progress=true`,
  regardless of redo mode. A running Interpret redo rewrites previously served
  identity history batch by batch, so a page read during the redo can be
  incomplete even while Project still reports its prior completed head. Because
  projection publication is incremental, an unchanged
  row target may precede the selected head; it may not be ahead, and a
  same-height target must match the selected hash. Lookup revalidates both
  `chain_heads` and that generation after the read. Before that check, public
  reverse lookup also reloads the active manifest declarations and Interpret
  redo state captured during namespace derivation. An active manifest
  declaration change returns `409 conflict`. A redo that begins mid-request, an
  invalid target, phase lag, readiness change, or other mid-request
  head/projection change returns the existing retryable `409 stale`, never a
  partial page. For a bare public reverse request, the request scope contains
  every active public namespace's chains even when the readable namespace set
  excludes one of them. Readable chains appear in `meta.as_of`; an excluded
  request-scope chain appears in `meta.as_of_completeness` with
  `completeness=unsupported` and
  `unsupported_reason=temporarily_unavailable`. Name-only lookup does not use
  public namespace derivation: its inferred or explicit namespace determines
  the chain scope, and out-of-scope chains are absent from both disclosure maps.
  In a mixed name-and-address batch, reverse suppression takes precedence over
  a real `meta.as_of` entry for the same chain. `meta.as_of_token` can still
  contain the position actually used by the name input.
- Replaces (v1): `POST /v1/identity:lookup`.

### `GET /v1/status`

- Method/path: `GET /v1/status`
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
  When the phase schema is present, API startup checks every phase-schema
  relation, function, and type its serving paths read: relations by name, both
  guarded [verified lookup](glossary.md#verified-lookup) functions by exact
  signature, and the `canonicality_state` type. If any are missing, the API
  refuses to start and its diagnostic names every missing identity.
- The existing per-chain `status` field also maps the `project` phase
  lifecycle and redo marker, the Interpret `redo_in_progress` marker, and the
  newest per-chain
  `bigname_phase.service_heartbeats` timestamp. A phase row that startup
  settled while its chain was unconfigured is not eligible for `ready` until
  genuine phase completion or completed-state revalidation clears that marker.
  It reports `degraded` unless a stronger `stale` condition applies, such as a
  genuinely failed phase or an expired heartbeat. An active Interpret redo
  makes the chain `degraded`; `data.status` continues to report the worst
  readiness across all chains.
  Ethereum Sepolia is ineligible for `ready` unless its `ingest` phase
  remains `completed` and its [verification](glossary.md#verification-level)
  phase is `completed` at a known level at or above the `quick_synced` floor; unknown stored levels fail closed. A failed Ingest or Verify, or an ordinary completed Verify without that
  evidence, is `stale`; an idle, running, paused, or missing Ingest or Verify is
  `degraded`. An expired runner heartbeat remains `stale` while either required
  phase is incomplete. Chains without this requirement omit those Ingest and
  Verify evidence checks but still honor the settlement marker and the
  Project-lifecycle rule below.
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
  five-second runner heartbeat opportunities.
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

### `GET /v1/names`

- Method/path: `GET /v1/names`
- Tier: product read.
- Purpose: the namespace-wide listing of current names by registration expiry
  — the "which names expire between t1 and t2" sweep. It lives under
  `/v1/names` rather than a `/v1/registrations` route because its rows are
  names in the dictionary shape `GET /v1/search` serves, each carrying its
  selected current registration, and because no route addresses a registration
  as a resource of its own: registrations appear only as `registration_id` on
  history and permission rows.
- Request parameters: query `namespace` (required), `expires_after`,
  `expires_before`, `sort=expires_at`, `order=asc|desc`, `cursor`,
  `page_size`, and optional `finality=latest`. `at` and historical `finality`
  values are rejected by the shared latest-state collection rule.
  `namespace` is required: the listing is one namespace's index scan, and a
  missing namespace returns `400 invalid_input`; an unsupported one returns
  `404 not_found`. At least one of `expires_after` and `expires_before` is
  required so the request can never be an unbounded scan; both are RFC 3339 UTC
  timestamps. `expires_after` is inclusive and `expires_before` exclusive, so
  consecutive windows tile without overlap or gap; `expires_after` must be
  earlier than `expires_before`. `sort` defaults to `expires_at` and accepts
  nothing else; `order` defaults to `asc`. Any other value, a non-RFC 3339
  bound, or an equal or inverted pair returns `400 invalid_input`.
- Response shape: `data` is an array of the same record-shaped rows
  `GET /v1/search` serves: `name`, `display_name`, `namespace`, `namehash`,
  `owner`, `registrant`, `registration_status`, `registered_at`, `created_at`,
  and `expires_at`. Every row has an `expires_at` inside the window. A name
  whose exact-name authority is unsupported is omitted, as on search, because
  a listing row carries no `unsupported_reason`.
- Coverage: the listing reads `name_current` rows whose
  `declared_summary.registration.expiry` is the projection's numeric lease
  expiry (unix seconds) — the form the exact-name builder writes for registrar
  leases, ENSv2 registrations, and wrapped subnames — and relies on the partial
  expression index `name_current_registration_expiry_idx` on
  `(namespace, (registration.expiry)::double precision, logical_name_id)`
  guarded by `jsonb_typeof(registration.expiry) = 'number'`
  (`schema-v2/baseline/06_projections.sql`; migration
  `20260911120200_name_current_registration_expiry_idx.sql` for a phase schema
  installed before the baseline carried it). A row whose only expiry is stored
  in another form (an RFC 3339 string at `control.expiry`, or no expiry at all)
  is outside this listing by design; `GET /v1/names/{name}` still serves its
  `expires_at`.
- Released names: the listing means "registrations whose expiry falls in this
  window", whether the registration is live, in grace or released. A released
  name keeps the lapsed registration's expiry, so it appears in every window
  that covers that old expiry: every released ENSv1 `.eth` lease, and an ENSv2
  registration that lapsed by path expiry, whose registry entry still holds
  the expiry. An ENSv2 registration ended by an explicit release loses its
  expiry with the entry, so it is outside every window; `GET /v1/names/{name}`
  serves it as `released` without `expires_at`. A released row has
  `registration_status: released`, its old `expires_at`, and
  no `owner` or `registrant`. The row shape has no `lapsed_registration` field;
  `GET /v1/names/{name}` serves that block, with the last holder, for a released
  ENSv1 name. A client that wants only held names filters rows on
  `registration_status`.
- Pagination behavior: standard collection pagination by `expires_at` in the
  requested order, ties broken by namespace, name, and namehash. Cursors are
  bound to namespace, both bounds, and order. `page.total_count` is `null`.
- Snapshot behavior: the page and its counts use the captured current
  publication. The response discloses `meta.as_of`; continuation cursors bind
  the publication and return `409 stale` requiring a restart when it changes.
  A first page whose publication changes during the read returns `409 stale`
  too and can simply be retried.
  Historical replay through `at` is not supported.
- Status semantics: an empty window returns `200` with empty `data`.

### `GET /v1/names/{name}`

- Method/path: `GET /v1/names/{name}`
- Tier: product read.
- Purpose: name-profile read, using the flat record shape plus registration summary.
- Request parameters: path `name`; query `namespace`, `at`, `finality`,
  `source`, `include=counts`. `source` accepts `indexed` or `verified`; omitting it is identical
  to `source=indexed`. This name-profile route does not accept `source=auto`.
  `include=counts` adds `subname_count`, the name's direct readable subname
  count (the same per-parent aggregate `GET /v1/names/{name}/subnames` reports
  as `page.total_count`), and `record_count`, the known record-selector count
  of the current registration's record inventory with the same meaning as on
  address-name rows; `record_count` is omitted when the row has no current
  record inventory. Neither count is added to the `status=unsupported`
  identity-only object. There is no `event_count`: bigname keeps no
  precomputed per-name event total, and counting history rows on the request
  path would be an unbounded scan, so the expansion does not offer one. Any
  other `include` value returns `400 invalid_input`.
  Counts require the current publication on the name's chain. A historical
  selection at another position, or a publication change during the read,
  returns `409 stale`; historical profiles without counts retain their existing behavior.

- Response shape: `data` is one flat record object using dictionary fields.
  The registration summary is not nested; it is represented by
  `registration_id`, `token_id`, `owner`, `manager`, `registrant`,
  `registered_at`, `created_at`, `expires_at`, and `registration_status` on
  the same object when backed. For a `.eth` second-level name
  `registration_id` is the BaseRegistrar lease whether or not the name is
  wrapped; see
  [registration identity of wrapped names](api-v2.md#registration-identity-of-wrapped-names).
  A released ENSv1 name keeps its lapsed `expires_at`, serves no current
  `registrant`, and carries
  `lapsed_registration: {registrant?, held_through?, released_at?}` with the
  holder the lease had when it lapsed; see
  [lapsed registration](api-v2.md#lapsed-registration). The block is omitted
  for every name that is not released. An ENSv1 wrapper-backed row also carries
  `wrapper_state` with the current [`wrapped`](glossary.md#wrapped-namewrapper-state),
  [`emancipated`](glossary.md#emancipated-namewrapper-state), or
  [`locked`](glossary.md#locked-namewrapper-state) lifecycle value and the typed
  `wrapper_fuses` object defined in [`api-v2.md`](api-v2.md#naming-dictionary).
  The tristate is bigname vocabulary derived from the enforcing NameWrapper
  guards, not an upstream enum. Both fields are omitted after an emancipated or
  locked wrapper position expires; a plain wrapped position remains `wrapped`
  with `wrapper_fuses.fuses=0` and every named boolean false because expiry
  clears its fuses without clearing its owner.
  (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L843 @ ens_v1@91c966f)
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
  emitted as a permanent null placeholder. `authority` names the protocol arm
  the current registration fields come from (`ens_v1` or `ens_v2`, read from
  the projection's selected [authority epoch](glossary.md#authority-epoch)); it
  is omitted for Basenames names and on the `status=unsupported` identity-only
  object. `migrated_at` is present only when `authority=ens_v2` was proven by
  an activated `MigrationApplied` [migration
  boundary](glossary.md#migration-boundary): it is the RFC 3339 block time of
  that proof event, read through the event's block in the chain lineage. A name
  first registered in ENSv2 has `authority=ens_v2` and no `migrated_at`. The
  name-profile portion uses `name`, `display_name`, `namespace`, `namehash`, `resolver`,
  `subregistry`, `addresses`, `text_records`, `content_hash`,
  `primary_name`, `primary_address`, `chain_id`, `network`, `status`, and
  `unsupported_reason`/`failure_reason`/`unsupported_fields` when those fields
  are served. `subregistry` is `{chain_id, address}` of the ENSv2 registry the
  name's current subregistry pointer targets, bounded to the selected
  position; it is omitted when the name has no current pointer, including
  every ENSv1-only and Basenames name, and when the latest pointer was cleared.
  The same field appears on batch-lookup name results and on subname rows,
  where it reads the latest pointer. With `source=verified`, the resolver-record-backed fields
  `addresses`, `text_records`, `content_hash`, and `primary_address` are built
  by a fresh schema-v2 lookup at the current readable position, using the same
  verified path as `/v1/names/{name}/records`; indexed resolver-record values
  are not substituted into those fields. The registration and identity summary
  fields (`registration_id`, `token_id`, `owner`, `manager`, `registrant`, dates,
  `registration_status`, `wrapper_state`, `wrapper_fuses`, `authority`,
  `migrated_at`, `name`, `display_name`, `namespace`, `namehash`,
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
  `status` is the flat-record result: `ok` for clean indexed reads; `failed`
  and `stale` may appear only when `source=verified` cannot serve the verified
  sections. `unsupported` is keyed on the projected row's own coverage status,
  not on a list of reasons: any row whose `coverage.status=unsupported` returns
  `200` with `status=unsupported` and the minimal identity-only object below.
  The single exception is `current_authority_not_projected`, which keeps the
  ratified partial `status=ok` described at the end of this section. Every other
  unsupported reason downgrades, including
  `conflicting_current_ens_authority` and
  `independent_ens_deployments_overlap` for a mixed-history read with no
  provable current authority, and `ensv2_exact_name_profile_shadow`, which
  reaches consumers as `exact_name_profile_not_supported`. The rule fails closed
  at both edges: an unsupported row that names no reason downgrades, and so does
  an unsupported reason this build does not recognize, so a reason added to the
  projection later serves `unsupported` by default rather than silently serving
  `ok` ([#487](https://github.com/ensdomains/bigname/issues/487)). A proven
  migration boundary returns the selected ENSv2 registration and `status=ok`;
  it does not expose the retained ENSv1 registration as current. `failure_reason`
  or `unsupported_reason` carries the product reason when available;
  `not_found` and `invalid_name` are unreachable in-record. The unsupported
  object retains only `name`, `display_name`, `namespace`,
  `namehash`, `status`, and `unsupported_reason`; registration, control,
  lifecycle, resolver, record, relation, permission, and primary-name fields
  from both source families are omitted.
  A row classified as `registration_status=unregistered` always omits
  `registration_id`. It also omits `resolver` and resolver-record fields unless
  it is
  an ownerless ENSv1 or Basenames registry row whose current registry resolver
  pointer is retained (a [serving resource](glossary.md#serving-resource)), or
  an ENSv2 TLD with a current
  [root-registry resolver pointer](glossary.md#root-registry-resolver-pointer)
  and no projected authority.
  For that classified row, indexed name detail serves the resolver and records
  present in its serving resource's inventory. `source=verified` executes lookup
  through the surviving resolver when the ordinary lookup capability supports
  it. Neither path acquires registration identity or control.
  Indexed `addresses`, `text_records`, `content_hash`, and `primary_address`
  come only from a record inventory whose coverage is authoritative (`full` or
  `projected` with no `unsupported_reason`). An `unsupported` inventory row,
  such as one behind a resolver whose implementation is not an admitted
  profile, omits those four fields and lists them in `unsupported_fields`
  exactly as a missing inventory does; `status` and `resolver` keep following
  the name row, and `GET /v1/names/{name}/records` serves the per-key reason.
  An ownerless ENSv2
  reservation does not meet this exception, even if identity attached to a
  resource or record inventory was retained for audit, unless it is a
  root-registry TLD reservation served through the root-registry resolver
  pointer above. This intentionally
  differs from ENSv2, which stores and returns a reservation resolver until
  expiry.
  See [registration status](api-v2.md#status-vocabulary) for the upstream
  basis.
  (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L255-L258 @ ens_v2@a971bd64)
  (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L461-L478 @ ens_v2@a971bd64)
  For `source=indexed`, a row classified as
  `current_authority_not_projected` remains `status=ok` for the identity and
  registration fields that can be served, but omits `resolver`; retained
  resolver-pointer evidence is not presented as current authority. The one
  exception is an ENSv2 TLD whose current
  [root-registry resolver pointer](glossary.md#root-registry-resolver-pointer)
  is its serving resource: the row stays `current_authority_not_projected` and
  unregistered, `authority` stays absent, and `resolver` and the resolver-record
  fields are served from that pointer and its inventory.
  An ownerless ENSv1 or Basenames registry row with a zero [getter-visible
  owner](glossary.md#getter-visible-owner) is instead supported and unregistered.
  When a current event-linked nonzero registry resolver pointer survives, name
  detail includes that resolver while registration and control fields remain
  absent.
- Pagination behavior: none.
- Status semantics: valid names with no name-profile data return `404 not_found`.
  Invalid path names return `400 invalid_input`.
- Replaces (v1): `GET /v1/names/{namespace}/{name}` and
  `GET /v1/profiles/names/{name}`.

### `GET /v1/names/{name}/records`

- Method/path: `GET /v1/names/{name}/records`
- Tier: product read.
- Purpose: resolver records.
- Request parameters: path `name`; query `namespace`, `at`, `finality`,
  `source=indexed|verified|auto`, `keys`, `include=inventory`.
- Response shape: `data` returns `namespace`, `resolver`, and route-local
  `records`, plus `inventory` with `include=inventory`. `records` is the only
  value shape on this route and is always present in a successful response:
  an object keyed by record key whose values are
  `{status, value?, unsupported_reason?, failure_reason?, meta?}`. This route
  does not serve the flat `addresses`, `text_records`, or `content_hash` maps;
  they remain on `GET /v1/names/{name}` and on `POST /v1/lookup` with
  `profile=detail`. `keys` is a comma-separated record-key allowlist using the
  existing app key grammar: `addr:<coin_type>`, `text:<key>`, `avatar`, and
  `contenthash`. With `keys`, `records` holds one answer per requested key.
  When `keys` is omitted, empty, or only whitespace, `records` answers the
  default key set: every record key in that grammar found in the served record
  inventory row's selectors, entries, and explicit gaps, parsed and
  deduplicated. The phase inventory reader currently records no explicit gaps,
  so in practice the set comes from selectors and entries and `unset_keys` is
  empty. Only an inventory row the name may serve contributes (the
  current-registration rule below). Coverage is not consulted while the set is
  derived: an `unsupported` row still contributes its keys, which
  `source=indexed` and unkeyed `source=auto` answer with the row's reason and
  `source=verified` answers from verified execution. A record family listed only as unsupported
  names no key, so it never enters the set; request its keys with `keys` to
  read the per-key reason. The default set adds no derived keys: an ENSIP-19
  derived `addr:<coin_type>` answer appears without `keys` only when the
  inventory itself enumerates that key. `records` is `{}` when the request has
  no key to answer, that is, no served inventory row or a row with no key in
  the grammar. `{}` does not prove that the resolver has no records, that an
  inventory row exists, or that verified reads are supported. Explicitly
  requested keys are always answered, never replaced by `{}`. Answers
  serialize in lexicographic byte order of the record key, so
  `addr:2147483648` precedes `addr:60`. That order is the server's
  deterministic serialization; JSON object member order carries no meaning and
  clients need not preserve it. Exact indexed and verified answers omit
  `meta`. An indexed ENSIP-19 answer derived from the projected
  default-address rule includes
  `meta={basis:"derived", rule:"ensip19_default_address",
  source_record_key:"addr:2147483648"}` for both `ok` and authoritative
  `not_found`.
  `source=verified`
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
  A name with no current registration returns no declared resolver or retained
  record values and does not execute verified lookup unless it is
  an ownerless ENSv1 or Basenames registry row whose current registry resolver
  pointer is retained (a [serving resource](glossary.md#serving-resource)), or
  an ENSv2 TLD with a current
  [root-registry resolver pointer](glossary.md#root-registry-resolver-pointer)
  and no projected authority.
  That classified row serves its resolver and any records present in its serving
  resource's inventory. Verified lookup runs through the surviving resolver
  when the ordinary lookup capability supports it. An ownerless ENSv2
  reservation does not meet this exception unless it is a root-registry TLD
  reservation served through the root-registry resolver pointer above.
  `include=inventory` does not expose
  inventory retained for a former or audit-only resource, and such inventory
  contributes no default keys: without `keys` the response carries
  `records: {}` even when that retained inventory lists more than 200
  selectors, while explicitly requested keys still receive their per-key
  answers. This intentionally
  omits the resolver that ENSv2 can store and return for an unexpired
  reservation.
  See [registration status](api-v2.md#status-vocabulary) for the upstream
  basis.
  (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L255-L258 @ ens_v2@a971bd64)
  (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L461-L478 @ ens_v2@a971bd64)
  Product records use product reason vocabulary: retained-selector misses use
  `value_not_retained`, and phase-unsupported record families use
  `record_family_not_supported`.
  Indexed record values are served only from a record inventory whose coverage
  is authoritative: coverage `status` is `full` or `projected` and it names no
  `unsupported_reason`. A name whose serving resolver's inventory row is
  `unsupported` (for example an ENSv2 name behind a resolver whose
  implementation is not an admitted profile) publishes no record values even
  when the projection retained entries for diagnostics. `source=indexed` then
  reports every requested key as `status=unsupported` with the row's own reason,
  mapped through the shared name-level vocabulary: a reason this build does not
  recognize or that carries pipeline wording becomes
  `unsupported_reason_unrecognized`, and a row naming no reason reports
  `indexed_record_inventory_not_authoritative`. Without `keys`, every key of
  the default set receives that same answer. `resolver` still reports the
  declared registry pointer, which is registry evidence rather than a record.
  `include=inventory` lists every product key the row knows about (selectors,
  entries, explicit gaps, and the requested keys) under `unsupported_keys`;
  `known_keys` and `unset_keys` are empty because an unsupported row can assert
  neither presence nor absence, and `abi_content_types` is `null` with
  `abi_unsupported_reason=inventory_not_authoritative` for the same reason.
  With `keys`, `source=auto` treats those keys
  as unsatisfied and executes verified lookup for them; without `keys` it
  stays an indexed read and returns the `unsupported` answers above.
  `source=verified` is unaffected by
  indexed coverage. The divergence ledger applies the same refusal: a verified
  answer over an unsupported inventory is compared against an `unsupported`
  indexed result, never against a retained entry value.
  A name whose current ENSv2 resolver is a declared
  [ENSv1 mirror resolver](glossary.md#ensv1-mirror-resolver-ensv1_mirror_resolver)
  keeps that mirror as `data.resolver`, and its indexed `records` and
  `include=inventory` come from the ENSv1
  resolver the mirror's registry walk selects for the name (the exact node's,
  else the nearest ancestor's), read for the queried node exactly as Project
  derived it ([`projections.md`](projections.md#resolver-and-records)); the
  response shape does not change, and an ancestor-derived answer is usually
  empty because it is the ancestor resolver's storage for this node. When no
  consulted node has a projected resolver, or the selected ancestor resolver is
  declared `ensip10_extended_resolver`, the inventory is unsupported with
  `mirrored_resolver_not_projected`, and `source=auto` with `keys` falls back
  to verified lookup as for any unsupported inventory.

  Representative keyed answers are:

  ```json
  {
    "records": {
      "contenthash": {
        "status": "ok",
        "value": "0xe3010170"
      },
      "addr:0": {
        "status": "ok",
        "value": "0x001122"
      },
      "addr:2": {
        "status": "not_found"
      }
    }
  }
  ```

  Coin types in record keys are decimal strings.
  Contenthash and address answers are scalar lowercase, `0x`-prefixed hex.
  This route flattens both projected `{encoding,bytes}` address values and
  projected scalar address values to that same public string; the exact-name
  detail route, `profile=detail` lookup, and GraphQL resolver address fields
  apply the same normalization.
  A zero-length byte payload makes the exact stored answer `not_found` and
  omits `value`, so a cleared exact value answers `not_found` unless a
  documented derived-record rule supplies a replacement answer. The ENSIP-19
  default-address rule below is one such rule.
  With `keys`, `source=auto` blends per key: indexed answers are used where
  they satisfy the requested key, and only the remaining supported keys fall
  back to verified lookup. Without `keys` (omitted, empty, or only
  whitespace), `source=auto` is an indexed read of the default key set: it
  makes no verified fallback, reports `meta.source=indexed`, and returns the
  same answers as `source=indexed`. A verified read has two closed refusal
  reasons of its own, reported per key on this route and as `data.unsupported_reason` on the verified
  exact-name detail route: `verified_records_not_supported` when the projected
  row carries no topology the engine admits (a bound name without a record
  inventory row, a null-resolver row outside the discovery shape below, an
  out-of-class shape, or no admitted execution entrypoint), and
  `exact_name_authority_not_verifiable` when the row's selected
  [authority arm](glossary.md#authority-epoch) is outside the
  `verified_authority_arms` the selected `ens_execution` manifest declares
  (`manifests.md` § `verified_authority_arms`; absent means `["ens_v1"]`, so
  an `ens_v2`-selected name is refused on Mainnet and
  admitted on the official `sepolia` profile). Neither refusal dispatches a provider call.
  Bound ENS names of either arm with a non-null exact resolver carry a
  projected direct topology (`execution.md` § Resolver-record lookup), so an
  admitted arm executes the direct route and compares against the indexed
  inventory like any other direct route. [Universal Resolver ancestor
  discovery](glossary.md#universal-resolver-ancestor-discovery) applies when a
  readable ENS name on the deployment profile's Ethereum L1 (Mainnet under
  `manifests/mainnet`, Sepolia under `manifests/sepolia`) has a null projected
  exact resolver, a projected name identity and DNS wire name, no alias,
  linked-subregistry, projected wildcard, or cross-chain transport path, and an
  admitted Universal Resolver manifest entrypoint on that chain
  (`ens_execution`, checked in for both profiles; see the
  [official Sepolia deployment](sepolia-deployment.md)). Verified ENS reads follow
  the same rules on both chains; only the chain, and therefore the
  `BIGNAME_API_CHAIN_RPC_URLS` entry they need (`ethereum-mainnet=` or
  `ethereum-sepolia=`), differs. This makes the indexed null-resolver miss
  unsatisfying. `source=auto` therefore executes the requested
  keys through verified lookup, and `source=verified` uses the same route. The
  Universal Resolver walks to the nearest nonzero ancestor resolver and accepts
  an ancestor only when it implements ENSIP-10
  `(upstream: .refs/ens_v1/contracts/universalResolver/RegistryUtils.sol:L25-L38 @ ens_v1@91c966f)`
  `(upstream: .refs/ens_v1/contracts/universalResolver/AbstractUniversalResolver.sol:L63-L88 @ ens_v1@91c966f)`.
  These responses use `meta.source=verified`; `data.resolver` remains null
  because it reports the exact registry resolver, not the ancestor used during
  live execution. `ResolverNotFound(bytes)` is a chain-proven per-key
  `not_found` with `failure_reason=resolver_not_found` only when its embedded
  DNS name equals the request name; it covers both no resolver and a nearest
  non-extended ancestor. Other reverts remain failed. Every successfully
  decoded call for one name and block must return the same effective resolver.
  A `ResolverNotFound` outcome cannot coexist with a successfully decoded
  effective resolver; either inconsistency fails the request closed. Ordinary
  selector-local failed or unsupported results remain mixed per key. This route
  has no indexed comparison: success and
  live `not_found` write and clear no divergence rows, while CCIP-required
  answers remain `unsupported` with `offchain_lookup_required` and likewise
  write nothing. ENS verified record resolution continues not to follow CCIP-Read.
  Basenames and other chains and namespaces do not enter this route. Outside this
  null-exact-resolver class, exact indexed `ok` answers and authoritative
  ENSIP-19 derived answers satisfy auto without a provider request. Within this
  class, all requested keys execute through verified lookup because retained
  exact inventory predates the resolver-clear boundary. If no admitted
  Universal Resolver entrypoint is available at execution time, the requested
  keys are explicitly `unsupported`; auto never turns that inability to execute
  into an indexed null-resolver `not_found`. For
  `addr:<coin_type>`, exact `ok`
  wins. An exact entry normalized to `not_found`, including empty address
  bytes, or a missing exact entry may read projected
  `addr:2147483648` only when the selected resolver's manifest-authorized
  [resolver read feature](glossary.md#resolver-read-feature) is present and
  `chainFromCoinType(coin_type) > 0`; coin type `2147483648` itself never
  recurses. A derived answer is normalized through the requested getter's
  verified decode: for coin type `60`, a 20-byte zero default becomes derived
  `not_found`; for EVM-range multicoin selectors, the same non-empty bytes remain
  an `ok` value. Exact stored records retain their stored value except that an
  ENSv1 or Basenames `addr:60` value of exactly 20 zero bytes is normalized to `not_found`
  before this derived rule runs. That covers an `AddressChanged(node,60,...)` payload of 20 zero
  bytes and a retained
  legacy-only normalized `AddrChanged(node,address(0))` behind an ENSv1 registry, registrar,
  or wrapper resolver pointer, or a Basenames registry resolver pointer. ENSv2-origin
  attribution, another coin type, another nonempty byte length, and nonzero addresses retain
  their values. The exact entry remains but omits `value`. Indexed and auto
  retain exact `not_found` even when an authorized nonzero default exists;
  verified returns the same absence. Name detail's `addresses["60"]` and
  `primary_address`, and default derivation metadata, remain absent. Empty or missing exact data keeps
  permitted fallback. The private observation marker and inventory provenance
  do not appear in product responses or record diagnostics.
  (upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L22-L24 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L47-L70 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L36-L40 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L81-L84 @ ens_v1@91c966f)
  (upstream: .refs/basenames/src/L2/resolver/AddrResolver.sol:L43-L66 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/resolver/AddrResolver.sol:L76-L82 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/resolver/AddrResolver.sol:L108-L110 @ basenames@1809bbc)
  (upstream: .refs/basenames/src/L2/resolver/AddrResolver.sol:L93-L99 @ basenames@1809bbc)
  (upstream: .refs/basenames/src/L2/resolver/AddrResolver.sol:L116-L121 @ basenames@1809bbc)
  Other
  default-source `ok` values yield the requested-key value, while authoritative
  absence yields derived `not_found`. An unsupported or
  non-authoritative source leaves auto unsatisfied and triggers ordinary
  verified lookup. Explicit `source=indexed` reports that case as
  `unsupported`.
  (upstream: .refs/ens_v1/contracts/utils/ENSIP19.sol:L9-L38 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L36-L40 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L68-L85 @ ens_v1@91c966f)
  (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L685-L697 @ ens_v2_sepolia_20260629@ccaeb58)
  The admitted legacy Basenames resolver is unflagged because its exact-storage
  getter does not implement that fallback. The fallback-bearing upgradeable
  Basenames resolver proxy is not yet admitted.
  (upstream: .refs/basenames/test/Fork/BaseMainnetConstants.sol:L9-L14 @ basenames@1809bbc)
  (upstream: .refs/basenames/lib/ens-contracts/contracts/resolvers/profiles/AddrResolver.sol:L35-L61 @ basenames@1809bbc)
  A non-derived indexed `not_found` is admitted only from a record inventory
  whose coverage carries no unsupported reason and whose coverage `status` is
  `full` or `projected`. `projected` is admitted
  because a supported schema-v2 inventory is complete for its current resolver
  over retained supported facts: the project phase includes both record events
  already linked to the name and ENSv1 resolver events whose `logical_name_id`
  is null but which match the same chain, node hash, and emitting resolver. It
  then keeps everything
  strictly after that resolver last bumped the node's record version and thereby
  cleared its earlier records (the `record_version_boundary` reported below).
  Resolver selection time is not a lower bound on writes. These rules follow
  the registry's current-resolver lookup
  (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L137 @ ens_v1@91c966f)
  and the resolver's version-, node-, and key-scoped text storage
  (upstream: .refs/ens_v1/contracts/resolvers/profiles/TextResolver.sol:L28 @ ens_v1@91c966f).
  Once that inventory exists, a key's absence is therefore absence from the
  retained attributable history rather than an unfinished build. When an
  ENSv1 registry resolver selection predates the
  [name surface](glossary.md#surface-name-surface), first-surface
  materialization supplies the linked pointer without requiring a repeated
  selection; a latest zero-address selection remains a clear. The row's
  `exhaustiveness: not_asserted` disclaims a claim about complete *history*,
  which is a weaker statement than `full` and does not weaken this admission.
  Node-keyed `ens_v1_resolver_l1` records written before the name surface
  existed enter that attributable history only when the selected pointer's
  source family is `ens_v1_registry_l1`, `ens_v1_registrar_l1`, or
  `ens_v1_wrapper_l1`. A selected `ens_v2_registry_l1` or `ens_v2_root_l1`
  pointer may also admit them when its target resolver's final classification
  is supported `ens_v1_resolver_l1` from an applicable exact declaration and
  the classifying manifest's namespace matches the pointer's namespace. Under
  that guard, absence from the projected inventory is authoritative
  `not_found`. A node-keyed `basenames_base_resolver` row with no
  `logical_name_id` is likewise attributable only through a
  `basenames_base_registry` pointer on the same chain, node, and resolver
  emitter. Basenames keeps the current resolver by node, authorizes its
  registrar controller and reverse registrar independently of the node owner,
  and stores text by record version, node, and key.
  (upstream: .refs/basenames/src/L2/Registry.sol:L173-L180 @ basenames@1809bbc)
  (upstream: .refs/basenames/src/L2/L2Resolver.sol:L193-L199 @ basenames@1809bbc)
  (upstream: .refs/basenames/lib/ens-contracts/contracts/resolvers/ResolverBase.sol:L7-L24 @ basenames@1809bbc)
  (upstream: .refs/basenames/lib/ens-contracts/contracts/resolvers/profiles/TextResolver.sol:L7-L36 @ basenames@1809bbc)
  Other ENSv2-family pointers do not attribute this node-keyed history.
  An inventory in any other coverage state is not authoritative, and the
  request falls through to verified lookup or an explicit unsupported answer
  rather than reporting absence from the index as absence on chain.
  A Basenames auto read remains Base-scoped when no fallback key
  remains; it selects the Ethereum resolution-auxiliary position only when it
  will attempt that verified fallback. If projection movement removes the last
  fallback key while the expanded snapshot is being selected, the request
  returns `409 stale` so a retry can return an indexed Base-scoped response.
  Explicit `keys` and the default key set are both limited to 200 record keys.
  More than 200 explicit keys is `400 invalid_input`. The limit applies to the
  default set after parsing and deduplication, so a key repeated across
  selectors, entries, and explicit gaps counts once and an inventory key
  outside the grammar does not count. When the default set would hold more
  than 200 keys, every source (`indexed`, `verified`, and `auto`) returns
  `422 unsupported` before any lookup or provider call; a set of exactly 200
  keys is served. The route never truncates the set; callers can supply `keys`
  to select a smaller one. For the verified flat
  name-profile, the limit applies to inventory-derived selectors before the
  route adds its synthetic primary-address request. A 200-selector inventory
  may therefore produce 201 provider keys when `addr:60` was absent; an
  inventory with more than 200 selectors still returns `422 unsupported`.
  `include=inventory` adds
  `inventory: {known_keys, unset_keys, unsupported_keys, abi_content_types,
  abi_unsupported_reason?}`, the container `POST /v1/lookup` also serves per
  name with the same meaning. Deep inventory internals stay on diagnostics.
  All three sources read the same record inventory row for the default key
  set and `include=inventory`: the served inventory at the selected snapshot
  on whichever chain the deployment indexes, with the same binding,
  serving-resource, chain-position, record-version-boundary, and snapshot
  checks for every source. Reading inventory does not admit verified
  execution. `source=verified` and verified fallback still pass the lookup
  engine's topology, authority-arm, admitted-position, and provider checks, so
  a name those checks refuse answers each default key with the same per-key
  refusal it gives the same explicit keys (`verified_records_not_supported` or
  `exact_name_authority_not_verifiable`), and those checks may refuse before
  any provider call. A
  verified request with explicit `keys` reads the same inventory, so
  `include=inventory` carries its lists, and inventory that is ahead of the
  selected snapshot fails the request as it does an indexed read.
  `abi_content_types` lists the ABI content types written to the name's
  current resolver storage. They come from the ABI-change events among the
  record writes the inventory already selects for the name, so the resolver
  selection, record-version reset, exact or default record link, ENSv1 mirror,
  and canonicality rules that decide the other keys also decide which ABI
  writes count: a write made before the resolver was selected counts, a write
  on a resolver the name has since left does not (until the name selects it
  again), and a write before a `VersionChanged` reset does not. Whether the
  selected storage has an ABI event bigname indexes comes from the selected
  resolver's classification in the resolver projection, read in the same
  statement that confirms the inventory row the route loaded (same resource,
  record version boundary key, chain positions, and recompute time) is still
  the published row. A change to the classification's manifest, declaration,
  admission namespace, or upgrade evidence makes Project republish every
  inventory row that depends on the resolver, so while the row is still
  published the classification read with it decides admission the same way
  it did when the row was built; the remaining family changes do not change
  whether ABI observations are admitted, so the answer is unaffected. A
  resolver row stamped at a newer target than the inventory row is expected
  and does not change the answer either. Each item is a
  canonical unsigned decimal string of a single-bit `uint256` content type,
  listed once, in ascending numeric order; content types can exceed 64 bits,
  so decode each item as a big integer. A listed content type means a write
  was observed, not that its ABI is currently non-empty. ENS removes an ABI by
  storing empty bytes and emits the same event as for a set, so a caller that
  needs the bytes reads them through the resolver's resolution interface and
  treats a successfully decoded empty value as unset, not as a failed call. A
  node-keyed ENSv1 resolver exposes `ABI(node, contentType)` directly
  (upstream: .refs/ens_v1/contracts/resolvers/profiles/ABIResolver.sol:L10-L26 @ ens_v1@91c966f);
  an ENSv2 record resolver and an ENSv1 mirror answer the encoded `ABI` call
  only through `resolve(dnsEncodedName, data)` and do not expose the getter
  (upstream: .refs/ens_v2/contracts/src/resolver/AbstractRecordResolver.sol:L110-L145 @ ens_v2@a971bd64)
  (upstream: .refs/ens_v2/contracts/src/resolver/AbstractMirrorResolver.sol:L67 @ ens_v2@a971bd64).
  (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L150-L161 @ ens_v2@a971bd64)
  An empty list means the selected resolver storage has an ABI event bigname
  indexes and none of the selected writes is an ABI write. It is not proof
  that no ABI is stored. When the index cannot list the content types,
  `abi_content_types` is `null` and `abi_unsupported_reason` names the cause;
  a caller must not read `null` as an empty list:
  - `inventory_not_available`: the name has no record inventory on its
    serving resource. Only this route serves it; the lookup route omits the
    whole container instead.
  - `inventory_not_authoritative`: the inventory row is `unsupported`, so it
    cannot speak for the resolver's storage.
  - `abi_observations_not_supported`: the selected resolver storage has no
    ABI-change event that bigname admits. The declared direct
    PublicResolverV2 classification (role `public_resolver_v2`) admits only
    address, text, contenthash, and version events, and the Basenames
    resolver manifests declare no
    `ABIChanged`, so both answer this way even when their inventory is
    supported and the other keys are served. Both contracts inherit ENS's
    ABI resolver and do emit `ABIChanged` on chain; the gap is bigname's
    admission, not the contracts.
    (upstream: .refs/ens_v2/contracts/src/resolver/PublicResolverV2.sol:L23-L26 @ ens_v2@a971bd64)
    (upstream: .refs/basenames/src/L2/L2Resolver.sol:L29-L31 @ basenames@1809bbc)
  - `abi_observations_stale`: a write the inventory selected is no longer
    retained as canonical, activated evidence, for example after a
    reorganization or an Interpret redo removed it; the list is withheld
    rather than shortened until Project rebuilds the inventory. The same
    reason covers an inventory row that Project replaced after the route
    loaded it and before it read the resolver's classification: the answer is
    withheld rather than computed from the older row and the newer
    classification, and a retry reads the new row.
  - `abi_content_type_not_single_bit`: a selected ABI write names a content
    type that is zero or has more than one bit set. The ENS setters reject
    such types, so only a nonstandard resolver emits them; bigname neither
    splits such a mask into single-bit types nor lists the remaining types
    without it. The same reason covers a selected write whose content type
    cannot be read as a canonical unsigned decimal of at most 256 bits: a
    missing, non-text, non-decimal, or zero-padded value, or one above the
    uint256 range.
    (upstream: .refs/ens_v1/contracts/resolvers/profiles/ABIResolver.sol:L21-L22 @ ens_v1@91c966f)
    (upstream: .refs/ens_v2/contracts/src/resolver/AbstractRecordResolver.sol:L211-L216 @ ens_v2@a971bd64)

  `abi_unsupported_reason` is omitted whenever `abi_content_types` is a list.
  ABI records stay outside the record-key grammar: `known_keys` never lists
  them, `keys=abi:<content_type>` is still rejected, and record counts do not
  include them.
- Pagination behavior: none.
- Status semantics: a missing name returns `404 not_found`. Missing, unset, or
  unsupported requested or default record values are reported with the common result
  `status` vocabulary inside the record answer rather than by changing the
  envelope. A proven migration uses
  only the selected ENSv2 resolver; the retained ENSv1 resolver is historical.
  A name whose exact-name projection is unsupported exposes no resolver values.
  Every unsupported reason except `current_authority_not_projected`
  short-circuits `source=indexed`, `source=verified`, and `source=auto` before
  provider execution and reports each requested or inventory-derived key as
  `status=unsupported` with the name-level reason:
  `conflicting_current_ens_authority` or
  `independent_ens_deployments_overlap` for a mixed-history name with no
  provable current authority, and otherwise the same public reason name detail
  serves for that row. The reason reaches this route through the shared
  name-level vocabulary name detail uses, so one projection reason yields one
  public reason on every route. Verified execution does not choose a resolver
  for an unsupported name. `current_authority_not_projected` also
  short-circuits those three sources before provider execution, but keeps its
  documented behavior: the response has no resolver values and reports each
  requested or inventory-derived key as `status=unsupported`
  with `inventory_not_available`. A `current_authority_not_projected` TLD
  whose current
  [root-registry resolver pointer](glossary.md#root-registry-resolver-pointer)
  is its serving resource does not enter that short circuit either: the
  response carries that resolver and each key classifies through the serving
  resource's inventory, so a TLD pointed at an
  [ENSv1 mirror resolver](glossary.md#ensv1-mirror-resolver-ensv1_mirror_resolver)
  reports `mirrored_resolver_not_projected` per key when the mirror's registry
  walk finds no projected ENSv1 resolver.
  A supported ownerless registry name does not enter that short circuit merely because its control
  state is unregistered. Indexed reads use the [serving resource](glossary.md#serving-resource)'s inventory, verified reads select
  the surviving resolver, and `source=auto` follows the ordinary indexed/verified blend. Owner zero
  or registry-self alone therefore does not produce `inventory_not_available`.
  Direct verified lookup compares against the same exact-or-derived indexed
  evaluator before the guarded resolution-divergence-ledger write. Agreement
  can therefore clear an older exact-key false miss; provider output remains
  request-scoped and is never copied into inventory or another projection.
  When projection changes a formerly direct resolver to null, projection
  publication retires active observations for that old direct resolver as stale
  evidence. This cleanup does not compare a live ancestor-served answer with the
  former indexed miss and is not a wildcard divergence write.
- Replaces (v1): `GET /v1/names/{namespace}/{name}/records` and record
  sections of `GET /v1/profiles/names/{name}`.

For an admitted [record-ID resolver generation](architecture.md), declared
records use the existing inventory response shapes. Values belong to the
currently selected record, including pre-link writes; shared records may appear
for several names. A zero exact link selects the default record when one exists.
Link changes participate in inventory provenance without being presented as
record-version resets. Permissions keep their existing resolver/resource scope
shapes: a numeric resolver record ID is never a bigname permission resource ID.

#### Public record-field completeness

The product record-key grammar is deliberately closed. The same grammar applies
to the product and record-diagnostic routes; a family outside it is rejected as
`400 invalid_input`, not returned with an invented or incomplete value.

| Registry or resolver field family | Public status | Contract |
| --- | --- | --- |
| Address records | Served | `addr:<coin_type>`, with the coin type limited to an unsigned 64-bit integer; see the [coin-type selector divergence](upstream.md#verified-resolution-addr-coin-type-selector-narrowing). ENS defines both the legacy Ethereum-address getter and the multicoin getter. (upstream: .refs/ens_v1/contracts/resolvers/profiles/IAddrResolver.sol:L4-L11 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/IAddressResolver.sol:L4-L15 @ ens_v1@91c966f) |
| Text records | Served | `text:<key>`, with the key limited to the closed selector grammar: non-empty, no ASCII whitespace, no commas (commas separate multiple record keys in request parameters). Each request item is trimmed of boundary whitespace before the grammar check, so `text:display ` selects the `display` key; a key that fails the grammar after that trim is rejected as `400 invalid_input`, and an on-chain text key containing whitespace or commas is not requestable through this route; see the [text selector-key divergence](upstream.md#verified-resolution-text-selector-key-narrowing). ENS defines text records by node and unconstrained string key. (upstream: .refs/ens_v1/contracts/resolvers/profiles/ITextResolver.sol:L4-L19 @ ens_v1@91c966f) |
| Avatar | Served | `avatar`, as the dedicated public selector for the `avatar` text key. (upstream: .refs/ens_v1/contracts/resolvers/profiles/ITextResolver.sol:L4-L19 @ ens_v1@91c966f) |
| Content hash | Served | `contenthash`. The current Basenames admission has a narrower event family; see the [Basenames contenthash divergence](upstream.md#basenames-contenthash-admission-narrowing). (upstream: .refs/ens_v1/contracts/resolvers/profiles/IContentHashResolver.sol:L4-L10 @ ens_v1@91c966f) |
| Registry TTL | Validated and discarded | `NewTTL` is decoded to validate admitted logs but produces no normalized event or public record key. The LLL-era low-byte validation exception is documented in the [registry-word divergence](upstream.md#ensv1-lll-era-registry-word-decoding). ENS declares the TTL event and getter as `uint64`. (upstream: .refs/ens_v1/contracts/registry/ENS.sol:L14-L15 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENS.sol:L49-L57 @ ens_v1@91c966f) |
| Registry owner | Served outside the grammar | No record key. `NewOwner` and `Transfer` are retained as normalized authority events. `GET /v1/names/{name}` carries the selected current owner in its optional `owner` field; its history route exposes a retained authority change as `type=authority`. Ownership is never requestable as a record key. (upstream: .refs/ens_v1/contracts/registry/ENS.sol:L6-L9 @ ens_v1@91c966f) |
| Registry resolver | Served outside the grammar | No record key. `NewResolver` is retained as the node's resolver-binding event. `GET /v1/names/{name}` carries a serveable current binding in its optional `resolver` object (`chain_id` and `address`); its history route exposes a retained change as `type=resolver`. A resolver address is not itself a requestable record. (upstream: .refs/ens_v1/contracts/registry/ENS.sol:L11-L12 @ ens_v1@91c966f) |
| ABI records | Content types served outside the grammar | No record key and no value. With `include=inventory`, the records and lookup routes list the content types of the name's selected ABI writes as `abi_content_types` (see the records route above); the ABI bytes are never served, and `keys=abi:<content_type>` is rejected. ENS defines ABI records by node and accepted content-type mask, and each write names one single-bit content type. (upstream: .refs/ens_v1/contracts/resolvers/profiles/IABIResolver.sol:L4-L16 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/ABIResolver.sol:L16-L26 @ ens_v1@91c966f) |
| Public keys | Outside the grammar | No public key. ENS defines a secp256k1 public-key record. (upstream: .refs/ens_v1/contracts/resolvers/profiles/IPubkeyResolver.sol:L4-L12 @ ens_v1@91c966f) |
| Interface declarations | Outside the grammar | No public key. ENS defines an interface-ID-to-implementer lookup. (upstream: .refs/ens_v1/contracts/resolvers/profiles/IInterfaceResolver.sol:L4-L22 @ ens_v1@91c966f) |
| Reverse-claim name records | Served outside the grammar | No record key. The primary-name projection takes an indexed claim value from one of two event paths, chosen by the event that keys the address, coin type, and namespace tuple. (1) When the reverse-registrar adapter interprets `NameForAddrChanged`, it emits the tuple's `ReverseChanged` and a `RecordChanged` row carrying `primary_claim_source`; the claim attaches to that tuple. ENSv1's standalone reverse registrar emits `NameForAddrChanged` when it stores an address's name. (upstream: .refs/ens_v1/contracts/reverseRegistrar/StandaloneReverseRegistrar.sol:L28-L30 @ ens_v1@91c966f) (2) The ENSv1 `addr.reverse` ReverseRegistrar declared by the Mainnet and canonical `sepolia` [deployment profiles](glossary.md#deployment-profile) emits no name: it emits `ReverseClaimed` with the reverse node, sets that node's registry resolver, and calls the resolver's `setName`, which emits `NameChanged`. (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L76-L84 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L123-L131 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/NameResolver.sol:L13-L19 @ ens_v1@91c966f) For such a tuple the projection joins the `ReverseClaimed` reverse node to the latest retained `NameChanged` or record-version reset on the node's current registry resolver, as described in [projections.md](projections.md#primary-names). A Sepolia or Mainnet `setName` through an admitted event-emitting PublicResolver therefore yields an indexed `claim_status = success` with the claimed name; a blank name or a version reset yields `not_found`. The joined `RecordChanged` row itself still carries no `primary_claim_source`. When the reverse node's resolver is [event-silent](glossary.md#event-silent) (it stores the name without emitting `NameChanged`), there is nothing to join, so the indexed answer is `not_found` unless reverse-resolver [hydration](glossary.md#hydration) is admitted for that resolver; the canonical `sepolia` profile admits none. Either way the indexed value is a declared claim only: forward verification stays on the request-scoped [verified lookup](glossary.md#verified-lookup) path. |
| General resolver name records | History only; outside the grammar | No public key or current value surface. Every resolver-family `NameChanged` is retained as an unattributed normalized `RecordChanged` in the `name` family, regardless of resolver or node type; a write for an `<addr>.addr.reverse` node therefore remains unattributed. When the row is associated with a materialized name, `GET /v1/names/{name}/history` exposes the change as `type=record` without its stored name value. The record routes reject the `name` family. The primary-name projection reads these rows only through the reverse-node join in the row above: a `NameChanged` for a node that no retained `ReverseClaimed` tuple names, or on a resolver that is not that node's current registry resolver, contributes nothing. ENSv1 defines `NameChanged` generically by node and name. (upstream: .refs/ens_v1/contracts/resolvers/profiles/INameResolver.sol:L4-L11 @ ens_v1@91c966f) |
| Resolver record versions | Outside the grammar | No public key. ENS keeps a per-node record version on the resolver and bumps it on `clearRecords`, emitting `VersionChanged`; the indexed record inventory retains that event as the boundary that invalidates older record values, but the version number itself is not served. (upstream: .refs/ens_v1/contracts/resolvers/ResolverBase.sol:L8 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/ResolverBase.sol:L20-L22 @ ens_v1@91c966f) |
| DNS record sets | Outside the grammar | No public key. ENS defines DNS record-set update/delete events and a wire-format getter. (upstream: .refs/ens_v1/contracts/resolvers/profiles/IDNSRecordResolver.sol:L4-L24 @ ens_v1@91c966f) |
| DNS zone hashes | Outside the grammar | No public key. ENS defines a DNS zone-hash update event and getter. (upstream: .refs/ens_v1/contracts/resolvers/profiles/IDNSZoneResolver.sol:L4-L15 @ ens_v1@91c966f) |
| Legacy content and multihash | Outside the grammar | No public key. ENS retains these getters and setters as deprecated resolver functions. (upstream: .refs/ens_v1/contracts/resolvers/Resolver.sol:L86-L93 @ ens_v1@91c966f) |
| ENSv1 arbitrary data records | Outside the grammar | No public key. ENS defines string-keyed arbitrary byte data. (upstream: .refs/ens_v1/contracts/resolvers/profiles/IDataResolver.sol:L5-L21 @ ens_v1@91c966f) The pinned ENSv1 `PublicResolver` source composes `DataResolver`. (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L20-L30 @ ens_v1@91c966f) bigname's ENSv1 resolver-family manifest admits and normalizes `DataChanged`, but its Mainnet PublicResolver admission rows include `DataResolver` in none of their declared resolver compositions; see [ENS mainnet admission](manifests.md#ens-mainnet). This is an admitted-generation composition limit and a grammar limit, not an event-admission limit. |
| ENSv2 generic data resources | Outside the grammar | No public key. The historical June Sepolia resolver ABI exposed `DataChanged` and `NamedDataResource`; their normalized-event exclusion is documented in the [ENSv2 admission divergence](upstream.md#ensv2-data-event-admission-narrowing). (upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/PermissionedResolverImpl.json:L360-L375 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/PermissionedResolverImpl.json:L505-L519 @ ens_v2@a971bd64) |

### `GET /v1/names/{name}/subnames`

- Method/path: `GET /v1/names/{name}/subnames`
- Tier: product read.
- Purpose: direct subnames.
- Request parameters: path `name`; query `namespace`, `q`,
  `sort=name|expires_at|registered_at`, `order=asc|desc`,
  `include_expired=true|false`, `include=counts`, `cursor`, `page_size`, and
  optional `finality=latest`. `at` and historical `finality` values are
  rejected by the shared latest-state collection rule.
  `q` applies prefix matching to the served `name` under exactly the rules
  documented for `q` on `GET /v1/addresses/{address}/names`: the whole value is
  normalized as an ENSIP-15 name prefix (`q=AL` matches `alpha.parent.eth`),
  one trailing dot marks a label boundary (`q=alpha.` matches
  `alpha.parent.eth` but not `alphax.parent.eth`), an empty `q` is absent, and
  input the normalizer rejects returns `400 invalid_input`. The comparison is
  byte-wise against the served name, so a [non-name form](glossary.md#non-name-form)
  row matches only a prefix of its placeholder or escaped text.
  `sort` defaults to `name` and `order` to `asc`. `expires_at` and
  `registered_at` order by the child's own registration timestamps, read the
  same way `GET /v1/addresses/{address}/names` reads them; a child with no
  current name row or no such timestamp sorts after every dated row ascending
  and before every dated row descending. Ties, and the whole `name` sort, break
  by served name and then by child identity. Any other `sort` or `order` value
  returns `400 invalid_input`.
  `include_expired` defaults to `true`, which is the route's prior behaviour:
  released children and children whose `expires_at` has passed are listed with
  their `registration_status` and `expires_at` as served. `include_expired=false`
  omits a child whose current registration status is `released` or whose
  `expires_at` is earlier than the database's transaction time when the page is
  read. A child with no registration or no expiry — an unregistered subname, or
  one under a parent that carries no expiry — is not expired and stays. bigname
  applies no grace period here: the comparison is against the served
  `expires_at`. Any other value returns `400 invalid_input`.
- Response shape: `data` is an array of dedicated subname rows in dictionary
  vocabulary: `name`, `display_name`, `namespace`, `namehash`, `labelhash`,
  `owner`, `registrant`, `registration_status`, `registered_at`,
  `created_at`, and `expires_at`. Registry events prove the child node and its
  labelhash but not the label, so two [non-name
  forms](glossary.md#non-name-form) are reachable here. A child whose label has
  never been observed carries `[<labelhash-without-0x>].<parent-name>` in
  `display_name` and the same stored bytes in `name`. The same placeholder serves
  a child whose label was observed — from the chain or the proof-checked rainbow
  import — but whose text fails ENSIP-15 normalization: the node commits to the
  raw label bytes, so serving the text as a name would name a different node,
  and escaping the valid string would reproduce the same misleading text. When a
  label is observed as
  bytes that are not valid UTF-8, or that contain a NUL, the row instead carries
  the whole stored child name — those label bytes, a dot, and the parent's —
  encoded by PostgreSQL's `escape` rule: a NUL as `\000`, each byte above `0x7f`
  as a backslash and three octal digits, a backslash doubled, and every other
  byte verbatim. The rule runs over the whole string, so a non-ASCII parent
  portion is octal-escaped along with the label. Neither form is reserved syntax
  — a label really spelled `[<64 hex digits>].<parent>`, or really spelled like
  escape output such as `\377bad`, produces the same string — so distinguish
  rows by `namehash` and `labelhash` rather than by parsing the served text.
  Both forms come from ENSv1 and Basenames registry edges; an ENSv2 child
  bigname cannot name is absent from the page instead. Neither form is
  addressable, and neither may be fed
  back into a name-shaped route. Resolver records are not included here;
  use `GET /v1/names/{name}` for `resolver`, `addresses`, `text_records`, and
  `content_hash`, or `GET /v1/names/{name}/records` for per-key record
  answers.
  `include=counts` adds `subname_count`, the row's direct subname count.
  `subregistry` is `{chain_id, address}` of the ENSv2 registry the child's
  current subregistry pointer targets, omitted when there is none.
- Pagination behavior: standard collection pagination in the requested sort
  and order. Cursors are bound to namespace, parent, `q`, `include_expired`,
  sort, and order; a cursor replayed under different controls returns
  `400 invalid_input`. A cursor must also carry the current publication and
  expiry evaluation time; older cursors without them return `409 stale` and
  require restarting without a cursor.
  `page.total_count` is populated with the parent's direct readable subname
  count — the same bounded per-parent aggregate that already annotates the page,
  so it costs no extra scan — when the page admits every child, that is
  when `q` is absent and `include_expired` is not `false`. A narrowed page
  reports the exact filtered total, computed with the same child-name and expiry
  predicates as its list, before the cursor.
- Snapshot behavior: the parent, child rows and filtered count use one
  revalidated publication, disclosed in `meta.as_of`. Continuations retain its
  identity and expiry evaluation time. A changed publication returns `409 stale`
  requiring a restart without the cursor; a first page whose publication changes
  during the read can simply be retried. Historical child enumeration is not
  supported.
- Status semantics: no direct subnames returns `200` with empty `data`.
  Missing parent names return `404 not_found`. Each child appears at most once,
  from the relation its own selected authority names. ENSv1 relations that are
  unreachable through the parent's ENSv1→ENSv2 migration path are omitted. A
  parent on the `unwrapped`, `unlocked_wrapped`, or `emancipated_child` path
  retains no ENSv1 children. A parent on the `locked_wrapped` or `locked_child`
  path retains only a [migratable child](glossary.md#migratable-child): one
  whose label has never had a reserved, registered, or renewed entry in that
  parent's [migration `WrapperRegistry`](glossary.md#migration-registry-wrapperregistry), whose current
  expiry-effective fuse word has `PARENT_CANNOT_CONTROL` set and `IS_DOT_ETH`
  clear, and whose current ENSv1 registry owner is nonzero. The wrapper fuse
  and expiry evidence remains effective across an ENSv1 binding rotation.
  (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L276-L277 @ ens_v1@91c966f)
  The child's own
  [authority arm](glossary.md#authority-epoch) still chooses between the remaining ENSv1 and ENSv2 candidates.
  An unknown activated migration-path value blocks the Project generation as a
  data-integrity failure instead of silently hiding relations. A child whose
  arms disagree with no authority proof is omitted entirely. On every ENS
  [deployment profile](glossary.md#deployment-profile) (Mainnet and Sepolia), an ENSv1 relation that survives
  parent reachability and
  was asserted after a proven ENSv2 child authority began blocks Project
  publication for that generation,
  though a positive ENSv2 registration in a locked parent's migration registry
  is itself entry history and therefore filters the ENSv1 relation before this
  assertion. The dual-current assertion remains a defensive generation check:
  an unmigrated parent can expose this contradiction, but no ordinary on-chain
  parent-and-child ENSv1→ENSv2 shape reaches it after parent reachability and
  migration-registry history are applied.
  (upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L146-L164 @ ens_v2@a971bd64)
  (upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L293-L307 @ ens_v2@a971bd64)
  This route therefore never chooses one
  by recency, emits two rows for one logical
  child, or adds a row-local unsupported shape.
  A V1 child with getter-visible owner zero is omitted unless a current
  event-linked nonzero resolver independently establishes read reachability.
  Such a row has owner zero and no registrant or control registration; clearing
  the resolver removes it.
- Replaces (v1): `GET /v1/names/{namespace}/{name}/children`.

### History collection filters

`GET /v1/names/{name}/history`, `GET /v1/addresses/{address}/history`, and
`GET /v1/events` share one filter and pagination vocabulary in addition to
their route-specific anchors:

An unrecognized namespace returns `404 not_found` before publication selection.
A recognized namespace with no available publication returns retryable `409 stale`.

- `order=asc|desc` selects the keyset direction over the shared chain-position
  sort. `desc` (newest first) is the default; `asc` is the exact reverse of the
  same total order, so the two directions enumerate identical row sets and an
  `asc` first page is the oldest-first anchor of the collection.
- `type` accepts one friendly event type or a comma-separated set, for example
  `type=registration,renewal`. Parts are trimmed, empty parts are ignored,
  duplicates collapse, and the set is canonicalized into the friendly
  vocabulary order. A part outside the vocabulary or an entirely empty set
  returns `400 invalid_input`. Rows match when their `type` is in the set.
- `from_timestamp` and `to_timestamp` are inclusive RFC 3339 bounds (`Z` or a
  numeric offset; fractional seconds allowed). They are resolved to block
  ranges from readable chain lineage, never through RPC: for every chain the
  deployment can serve, `from_timestamp` maps to the first readable block whose
  timestamp is at or after the bound and `to_timestamp` to the last readable
  block whose timestamp is at or before it, and rows are kept when their
  `block_number` falls inside that chain's inclusive range. A chain with no
  block satisfying a bound contributes no rows, so a window entirely after the
  last indexed block matches nothing and returns `200` with empty `data`. Rows
  without a chain position never match a timestamp window. `from_timestamp`
  greater than `to_timestamp`, or a value that is not RFC 3339, returns `400
  invalid_input`. On `/v1/events` the resolved window intersects an explicit
  `from_block`/`to_block` range.
- Every read is bound to one block per chain: the served publication's block
  on a first page, and the [cursor bound](glossary.md#cursor-bound) on a
  continuation, which is the same block the first page read. Rows stop at that
  block, and history anchor expansion is bounded to it too: bindings,
  NameWrapper links, registrar grants, historical ownership matches, and
  resolver record writes attributed to a registration through its resolver
  pointers are judged from evidence at or below the bound block of their
  chain, so evidence recorded after that block cannot introduce older events
  into a later page of the same walk. A pointer or record link above the bound
  block neither attributes an older write nor closes an earlier pointer's window.
  Bindings and ownership before `from_timestamp` remain valid anchors; the
  timestamp window filters event rows.
- Two inputs are read from current state. An address's relations are read from
  the current relation rows, and a row is admitted when the event Project
  cites for it lies at or below the bound block. The name's attachment to
  the row's resource is judged at or below the bound block too: the row is
  admitted only when some [surface binding](glossary.md#surface-binding) of
  that name to that resource on the row's chain was written at or below the
  bound block, whatever event the row cites and including the same-holder
  case below. The row's current binding may be newer: a registry owner moved
  away and restored rebinds the name to the same resource. A current row cited
  above the bound block is also admitted when every registration event
  between the bound block and the cited one is a same-holder token
  transfer: the cited event is a `TokenControlTransferred` whose sender and
  recipient are both the address, every `RegistrationGranted`,
  `RegistrationReleased` and `TokenControlTransferred` on that event's resource
  after the bound block and up to it is such a transfer too, and no ENSv2
  `RegistrationReserved` on that resource lies in the range. For an effective controller, no
  `AuthorityTransferred`, `SurfaceBound` or `PermissionChanged` on that
  resource may lie in the range either. Project cites the latest registration
  event for the registrant, token holder and fallback controller relations, so
  a transfer from the holder to itself moves the cited event without changing
  the holder, and the earliest transfer in the range names the address as its
  sender, so the address held the name at the bound block. The rule reads
  only the `TokenControlTransferred` rows the adapters emit: an ENSv1 or
  Basenames registrar `Transfer` with a nonzero sender and recipient becomes
  such a row carrying both as logged
  (bigname: `crates/adapters/src/schema_v2/protocol/v1/registrar.rs:136-140`)
  (bigname: `crates/adapters/src/schema_v2/protocol/v1/registrar.rs:197-203`)
  (bigname: `crates/adapters/src/schema_v2/protocol/v1/registrar.rs:216-222`).
  For Basenames the self-transfer is valid upstream: the registrar inherits
  Solady's ERC-721
  (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L5 @ basenames@1809bbc),
  whose `transferFrom` checks only that `from` is the current owner and does
  not reject `to == from`
  (upstream: .refs/basenames/lib/solady/src/tokens/ERC721.sol:L252-L309 @ basenames@1809bbc).
  The ENSv1 pin does not vendor the OpenZeppelin ERC-721 the BaseRegistrar
  inherits, so the ENSv1 evidence covers authorization and expiry only: the
  BaseRegistrar authorizes a transfer through its owner, approved or operator
  check
  (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L42-L50 @ ens_v1@91c966f),
  whose `ownerOf` call rejects an expired name and otherwise returns the owner
  (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L71-L76 @ ens_v1@91c966f).
  A relation that began after the bound block has a grant or a transfer to
  the address in the range and is not admitted. The probe also refuses an ENSv2
  `RegistrationReserved` in the range: reserving an expired name burns the
  previous holder's token
  (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registry/PermissionedRegistry.sol:L481-L486 @ ens_v2_sepolia_20260916@366de741)
  and emits `LabelReserved` with no owner and no mint
  (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registry/PermissionedRegistry.sol:L490-L491 @ ens_v2_sepolia_20260916@366de741),
  and bigname keeps no `TokenControlTransferred` row for a burn, so the
  reservation is the only row that marks the end of that holding. A relation
  that ended after that block has no current row, so it is reproduced only
  from the three historical shapes the ownership matcher knows: a
  `RegistrationGranted` registrant and a `TokenControlTransferred` recipient
  on a token-backed, Basenames or ENSv2 resource, and an
  `AuthorityTransferred` owner on a registry-only or ENSv2 resource. The
  second current input is the resolver's current classification row, which
  decides whether an ENSv2 resolver pointer attributes node-keyed writes on
  that resolver.
- Known limitation: four relation kinds that ended after the bound block
  are not reproduced, so the address's read loses that name's events: an ENSv1
  `.eth` registry controller from `AuthorityTransferred` on a token-backed
  resource, a token holder whose only evidence is the grant, an effective
  controller that fell back to the token holder, and an ENSv1 effective
  controller from a resource-scoped `PermissionChanged` grant (registrar
  registration, transfer or surface snapshot, registry owner, or NameWrapper
  holder). The controller is set only by a `PermissionChanged` whose scope is
  the name's resource
  (bigname: `crates/project/src/builders/address_names.rs:238-248`). Upstream
  reports an ENSv2 role change as an account's roles within an access control
  resource of the emitting contract
  (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/access-control/interfaces/IEnhancedAccessControl.sol:L17-L27 @ ens_v2_sepolia_20260916@366de741),
  and bigname normalizes every such change to a registry, registry root or
  resolver scope keyed by that contract
  (bigname: `crates/adapters/src/schema_v2/protocol/permissions.rs:35-46`). The
  other `PermissionChanged` rows with ENSv2 provenance come from the
  `ens_v2_migration_l1` reading of BaseRegistrar `ControllerAdded` and
  `ControllerRemoved`, and carry a `registrar_controller` scope with no resource
  (bigname: `crates/adapters/src/schema_v2/protocol/migration.rs:224-245`). So
  ENSv2 `PermissionChanged` rows never set this controller. This is a bigname
  normalization rule, not an upstream one. A relation that ended after the
  bound block and began again before the current row was written counts as
  ended: its current row cites the transfer, grant or state-derived
  `SurfaceBound` that restored it, above the bound block. Wrapper grace-period and expiry transitions have no
  cited event of their own, so a relation row gated by them can appear or
  vanish between pages of the same read.
- Cursors bind the order and every filter above. The cursor `sort` token
  encodes the direction, and its filters carry the canonical `type` set and
  the canonical UTC spelling of each timestamp bound, so a cursor issued by one
  query cannot continue a query with a different direction, type set, or
  window; the mismatch returns `400 invalid_input`. Equivalent spellings of the
  same bound (`+01:00` versus `Z`) continue the same query. Name history also
  binds [`include=child_registrations`](#direct-child-registrations-includechild_registrations),
  which changes which rows the collection holds; the payload flags
  `include=data` and `include=raw` are not bound.
- `page.total_count` is populated for anchored history reads: name history and
  address history always, and `/v1/events` when `name`, `registration_id`,
  `address`, or `resolver` bounds the read. The count runs inside the same repeatable-read
  transaction as the page over exactly the page's filters (scope, type set,
  block and timestamp windows, product visibility, and duplicate suppression),
  so it agrees with what paging would enumerate. Every page reads at the
  walk's [cursor bound](glossary.md#cursor-bound), so the count is the count at
  that block and is the same on every page of one walk, except when an
  address-history relation in the known limitation above ends during the walk:
  a later page then loses that name's rows and counts fewer. With
  `include=child_registrations`, the count covers the combined collection of
  name rows and direct child registration rows, each event counted once. It is capped: counting stops
  after 10,000 product-visible rows and a larger result reports
  `total_count=null`. With `include=total_count`, anchored history reads instead
  run an exact, uncapped count in the same read transaction and return that total.
  Clients needing a name event counter can request its history with
  `page_size=1&include=total_count`; no event rows need to be enumerated. This
  option may cost more on names with a long history. Unanchored `/v1/events` reads (namespace, type, and block
  or timestamp windows only) never count and always report `total_count=null`.

### History event payloads (`include=data`, `include=raw`)

The same three collections accept independent payload flags `include=data`
and `include=raw`, alongside the exact count flag `include=total_count`.
They can be combined in any order. Name history also accepts
[`include=child_registrations`](#direct-child-registrations-includechild_registrations),
which adds rows rather than fields; address history and `/v1/events` reject
it. Other values return `400 invalid_input`.
Without either payload flag, rows keep their lean shape and carry none of the
fields below; requesting an exact total does not expand event rows.

Every event row on the three collections carries `id`, an opaque
64-character identifier that is unique per row and identical for the same
event wherever it appears — on `/v1/events`, in name history, in address
history, and across pages — so a consumer merging feeds can de-duplicate on
it. Nothing else in the row is an identity: rows derived from interpreter
state rather than from one log have no `transaction_hash` or `log_index`,
and several rows can share one log. `id` is stable across pages, snapshots,
and a redo that re-derives the same blocks on the same chain fork; a reorg
that replaces a block yields new rows with new ids, and a
[re-derivation boundary](glossary.md#re-derivation-boundary) may change
every id, so it is a merge key, not a durable reference to store.

`include=data` adds two fields to every returned row:

- `contract_address`: the lower-cased address of the contract that emitted the
  underlying log. It is `null` for rows derived from interpreter state rather
  than from one on-chain log.
- `data`: an object derived from the stored normalized event's before/after
  state, translated into dictionary vocabulary. Only fields the row actually
  carries are present; absent or null source values are omitted rather than
  serialized as `null`, so `data` may be `{}`. Addresses are lower-cased.
  Contract pointers use the `{chain_id, address}` shape with the row's own
  numeric `chain_id`; a zero-address pointer means "cleared" and is omitted, so
  a `resolver` row whose `data` has no `resolver` records a clearing. Unix
  expiry values become RFC 3339 `expires_at`.

`include=raw` is a separate, explicit opt-in for explorer and diagnostic use.
It adds one field:

- `kind`: the raw storage event kind behind the friendly `type`, such as
  `RegistrationGranted`, `LabelRegistered`, `RecordChanged`, or
  `EACRolesChanged`. This is the one pipeline term the product tier exposes,
  so an explorer that wants the upstream event name for a badge or a raw-type
  facet does not have to fall back to diagnostics. It is never part of
  `include=data`: product clients that render the friendly payload do not see
  storage vocabulary unless they ask for it.

Per friendly `type`, `data` may contain:

| `type` | `data` fields |
| --- | --- |
| `registration` | `registrant`, `owner`, `expires_at`, `resolver: {chain_id, address}`, `subregistry: {chain_id, address}` |
| `renewal`, `release` | `expires_at` |
| `expiry` | `expires_at`, `fuses` (uint32 word when the change came through NameWrapper) |
| `transfer` | `from`, `to`, `fuses` |
| `authority` | `owner` (the new registry owner), `from` (the previous owner when the row retains it) |
| `resolver` | `resolver: {chain_id, address}` (absent when the pointer was cleared) |
| `record` | `key`, `value`, `coin_type` (number, for `addr:<coin_type>` keys). `key` is the stored record key; history retains writes outside the public record grammar (for example `name` or `abi:<content_type>`), so `key` may name a family the records route does not serve. `value` is present only when the write's value was retained: text values are strings, other families are hex strings. A record-version reset (raw kind `RecordVersionChanged`, visible with `include=raw`) carries no fields. |
| `primary_name` | `address`, `coin_type` (number) |
| `permission` | `address` (the subject), `powers` (product power vocabulary, as on permission rows), `fuses` (uint32 word for NameWrapper fuse changes) |
| `subregistry` | `subregistry: {chain_id, address}` (absent when the link was cleared) |

Example row from `GET /v1/names/alice.eth/history?include=data&type=record`:

```json
{
  "type": "record",
  "name": "alice.eth",
  "namespace": "ens",
  "registration_id": null,
  "block_number": 19000106,
  "timestamp": "2026-06-10T00:00:06Z",
  "transaction_hash": "0x...",
  "log_index": 12,
  "contract_address": "0x231b0ee14048e9dccd1d247744d114a4eb5e8e63",
  "data": { "key": "addr:60", "coin_type": 60, "value": "0x..." }
}
```

The same row from `GET /v1/names/alice.eth/history?include=raw&type=record`
carries the lean fields plus `"kind": "RecordChanged"` and no
`contract_address` or `data`; `include=data,raw` carries all three.

`data` never exposes before/after state, raw fact references, or other
pipeline fields; `GET /v1/diagnostics/events` remains the raw surface.

### `GET /v1/names/{name}/history`

- Method/path: `GET /v1/names/{name}/history`
- Tier: product read.
- Purpose: name history.
- Request parameters: path `name`; query `namespace`,
  `scope=name|registration|both`, `type`, `order=asc|desc`, `from_timestamp`,
  `to_timestamp`, `include=data|raw|total_count|child_registrations`, `cursor`, `page_size`, and optional
  `finality=latest`. `at` and historical `finality` values are rejected by the
  shared latest-state collection rule. `type`, `order`, and the timestamp
  window follow the [history collection filters](#history-collection-filters);
  `include=data` and `include=raw` add the [history event
  payloads](#history-event-payloads-includedata-includeraw);
  `include=child_registrations` adds the [direct child
  registrations](#direct-child-registrations-includechild_registrations).
- Response shape: `data` is an array of dedicated lean event rows:
  `{id, type, name, namespace, registration_id, block_number, timestamp,
  transaction_hash, log_index}`, `id` being the opaque row identity of the
  [shared payload contract](#history-event-payloads-includedata-includeraw).
  Without `include=child_registrations`, `name` is always the requested name,
  including on a row that the registration scope reached through a resource
  whose event carries a different name or none. With it, every row also
  carries `subject` (`name` or `child`) and a child row's `name` is the
  child's name; see [direct child
  registrations](#direct-child-registrations-includechild_registrations). `registration_id` carries actual registration
  lifecycle identity and is `null` when the event is not associated with a
  registration; reservation facts never carry one. The shared event-identity
  contract, including the committed companion change that adds `resource_id`
  for the underlying resource identity, is documented under
  [`GET /v1/events`](#get-v2events). Rows never include before/after
  state or raw normalized-event payloads; without `include=data` they carry no
  `data` or `contract_address`, without `include=raw` they carry no `kind`,
  and with those flags they carry exactly the documented fields. Friendly
  `type` vocabulary: `registration`, `renewal`, `release`, `expiry`,
  `transfer`, `authority`, `resolver`, `record`, `primary_name`, `permission`,
  `subregistry` (an ENSv2 registration's subregistry link set or cleared).
  Raw upstream or pipeline event kinds are not emitted by this product route
  except as the `kind` field behind the explicit `include=raw` opt-in; the
  diagnostics events route remains the raw surface. Slice 1 excludes every correlation-dependent normalized
  row with `consumer_visibility=candidate`, including a familiar event kind whose
  existence depends on correlation under an existing source family; diagnostics
  may expose those rows. An [independently admitted
  event](glossary.md#independently-admitted-event) remains byte-for-byte
  activated and product-visible. Its separate
  candidate association is diagnostics-only and cannot suppress, duplicate, or
  reclassify that ordinary row. Only slice 2 consumer activation enables the
  per-source-log mapping specified for [`GET /v1/events`](#get-v2events) when an
  activated event is associated with the requested name or registration.
  Manifest or schema-vocabulary activation alone changes no name-history
  response. Candidate visibility is filtered in
  storage before summary calculation, keyset pagination, page-size limiting,
  cursor construction, or the later product-type mapping; candidate rows cannot
  consume a page slot or move an existing cursor. When one V1 registry resolver
  log is linked to both the registry resource retained for reads and a distinct
  control resource, this product route returns the control-resource row once.
  The additional normalized row that retains the registry resource link remains available from
  `GET /v1/diagnostics/events`; product suppression happens before cursor
  validation, summary calculation, and pagination. Without a distinct control
  resource, the sole registry-resource row remains product-visible.
  (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L89-L94 @ ens_v1@91c966f)
- Pagination behavior: keyset pagination by chain position, newest first
  unless `order=asc`. The cursor is bound to the resolved namespace, parent
  name, scope, direction, `type` set, timestamp window, and whether
  `include=child_registrations` was requested, so a cursor from a request with
  the option returns `400 invalid_input` on a request without it, and the
  reverse. Product event-type
  filtering, including an explicit `type` set, is applied before page
  construction, so `page_size`, `next_cursor`, and `has_more` describe
  product-visible events. A nonterminal page contains `page_size` rows; only
  the terminal page may be shorter. `page.total_count` is the capped anchored
  count described under the shared history filters: exact up to 10,000 rows,
  `null` beyond, unless `include=total_count` requests an uncapped exact total.
- Scope behavior: `scope=name` reads name-surface events only,
  `scope=registration` reads registration-resource events associated with the
  requested name, and `scope=both` reads both sets. `scope` defaults to `both`.
  A V1 ownerless row linked only to the registry resource retained for reads is
  visible through name history with `registration_id=null` when it carries the
  name's `logical_name_id`. Name history returns a
  [pre-surface](glossary.md#pre-surface) owner row on a
  registry resource that was ever bound to the name under `scope=both` or
  `scope=registration`, even when the row was stored before the
  [name surface](glossary.md#surface-name-surface) existed and carries no name
  attribution. `scope=name` returns only rows carrying the name's
  `logical_name_id`. A row on a resource that was never bound to the name is
  reachable through `GET /v1/diagnostics/events` via the registry resource
  recorded internally at
  `name_current.provenance.read_reachability.serving_resource_id`.
  `include=child_registrations` is independent of `scope`: the collection is
  the rows `scope` selects plus the direct child registrations, under
  `scope=name`, `scope=registration`, and `scope=both` alike, and child rows
  are returned even when the scope selects no rows of the name itself.
- Snapshot behavior: the first page reads at the served publication and binds
  its block as the walk's [cursor bound](glossary.md#cursor-bound). Every page
  reads at that bound, counts at it, and discloses it as `meta.as_of`, as the
  [shared history rule](#shared-route-rules) describes. A continuation keeps
  working while new blocks are published and returns `409 stale` requiring a
  restart when the bound no longer holds. A first page whose check after the
  read fails returns `409 stale` too and can simply be retried.
  Historical replay through `at` is not supported.
- Status semantics: no product-visible matches return `200` with empty `data`,
  `page.next_cursor=null`, and `page.has_more=false`. Missing names return
  `404 not_found` on the first page. The first page judges existence at the
  bound it binds: a name whose surface was first observed above the bound block
  of its chain, because a publication landed between capturing the bound and
  looking the name up, is missing too. A continuation does not look the name up
  in current state again: the cursor already binds the name, and its rows come
  only from the name's bindings, NameWrapper links, and registrar grants at or
  below the [cursor bound](glossary.md#cursor-bound), and with
  `include=child_registrations` also from the name's child membership rows at
  or below it.
  `include=child_registrations` on `eth` or `base.eth` returns `400
  invalid_input` before the name is looked up. Request and cursor validation,
  including the cursor's binding, precede the first `redo_in_progress` check,
  so malformed requests retain `400`; a cursor issued before position-bound
  pagination then returns `409 stale` requiring a restart. The route captures
  the redo state before parent lookup. An Interpret redo in progress on any
  chain (the check is collection-wide, regardless of the requested namespace
  or name) or a Project redo in progress on a chain in scope returns `409
  stale`. The route checks the Interpret state again inside the
  repeatable-read page transaction, and after the page repeats the whole check
  in a fresh transaction: both redo counters and flags, the bound block, the
  manifest revisions, the classification horizon, a publication at or above
  the bound, and both that publication and the readable head below that
  horizon. A missing
  parent returns `404` only when that check passes; otherwise the route
  returns `409 stale`. A well-formed cursor whose event anchor is gone returns
  `400 invalid_input` when the check passes and `409 stale` when it does not.
- Replaces (v1): `GET /v1/history/names/{namespace}/{name}`.
  Registration-id anchored history from `GET /v1/history/resources/{resource_id}`
  moves to `GET /v1/events?registration_id=...`. `scope=registration` on this
  route is limited to registration lifecycles associated with the requested
  name. For a wrapped `.eth` name those are its BaseRegistrar leases: the read
  follows each published NameWrapper binding of the name to the lease it
  wrapped, so registrar rows recorded before the name was known are included,
  and NameWrapper rows report the lease as their `registration_id`. A `.eth`
  lease granted with `registerOnly` while the name stayed bound to a
  registry-only resource (a registrar token transferred without `reclaim`) has
  no binding of its own; the read reaches it, and every earlier such lease,
  through the name its `RegistrationGranted` row carries.
  (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L118-L175 @ ens_v1@91c966f)
  Events on a registry-only control resource use the registrar lease that existed
  at the event's position, established by activated grant and release evidence on
  the same chain branch. They do not borrow the latest lease from current name
  state. This includes resolver changes recorded before name materialization:
  a resource still under registry-only control needs no token lineage or active
  name binding to identify the lease at that event. A resolver change on an
  unbound read resource after that control ended still has no registration
  handle. This includes a dormant registry resolver first materialized by wrapping:
  the registry read observation has no authority key, even when the pre-surface
  handoff emitted no closing row on the old registry resource.
  During a released gap, or before a lease is proved, those control
  events have no registration handle. A record event with no resource belongs to that
  lease only while the registry-only binding is active and the lease exists at
  the record's position. The same selection governs row IDs, registration filters,
  counts and cursor anchors.

#### Direct child registrations (`include=child_registrations`)

`include=child_registrations` merges the registrations of the requested name's
direct children (its [direct child
registrations](glossary.md#direct-child-registration)) into its history: one collection, one chain-position order,
one cursor. It adds rows, not fields. A child row has exactly the fields any
`registration` row has, and `include=data` and `include=raw` expand it the same
way.

A direct child registration is a product-visible `registration` row (stored
kind `RegistrationGranted` or `LabelRegistered`) that meets all of these
conditions:

- The name the row carries, as attributed when the event happened, is exactly
  one label below the requested name, in the same namespace and on the same
  chain.
- That child name passes normalization. A label that fails normalization
  still has an identity row in bigname, but it is not served as a name, so
  its registrations are excluded.

The test is structural and historical. It uses the name each event carries and
the child's label path. It never uses the parent's current subregistry, the
current child list of `GET /v1/names/{name}/subnames`, or current name state.
So the collection keeps:

- children that were released or expired since;
- grants made under a registry that the parent has since unlinked or replaced.
  When a registry moves from one parent to another, its earlier grants stay in
  the earlier parent's history and its later grants belong to the new parent;
- every registration of a child, including a registration after a release.

Excluded:

- grandchildren and deeper descendants;
- ENSv1 and Basenames registry subnames created through `setSubnodeOwner`.
  That call emits `NewOwner`, which bigname stores as `subregistry` and
  `authority` rows, not as a registration, so these subnames have no
  `registration` row to include. They remain visible in the child's own
  history.
  (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L75-L84 @ ens_v1@91c966f)
- the children of `eth` and `base.eth`. Every second-level registration of the
  `.eth` registrar or the Basenames registrar is a child of one of these two
  names, so the option returns `400 invalid_input` when the requested name is
  `eth` or `base.eth`, in either namespace. This is a scope limit of the
  option, not missing data: bigname indexes those registrations, including
  ENSv2 `.eth` registry grants, but does not offer them as one parent's
  stream. The check compares the resolved name, not a suffix, so
  `alice.eth` and `alice.base.eth` are served normally.

Child rows follow the same bound as the name's own rows: the option reads only
membership rows at or below the bound block of the parent's chain (the served
publication on a first page, the [cursor bound](glossary.md#cursor-bound) on
a continuation), inside any `from_timestamp`/`to_timestamp` window. Membership
comes from each event's own name at its own position, so evidence recorded
after the bound block cannot add an older child row. The name's own rows are
the rows the read returns without the option, including the resolver record
writes attributed at the bound block.

In practice the rows come from ENSv2 registries. A row is a stored grant
observation, not one registration action, and each keeps its own `id` and
position. One ENSv2 registration can produce several rows: the grant from the
registry's `LabelRegistered` log, a linked copy when the registry later emits
`TokenResource` for that token, and a reachability grant each time the label
becomes reachable under this name, for example when the parent links a registry
that already holds the label, or when `ParentUpdated` completes the link.
(upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registry/interfaces/IRegistryEvents.sol:L18 @ ens_v2_sepolia_20260916@366de741)
(upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L45 @ ens_v2_sepolia_20260916@366de741)
(upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registry/PermissionedRegistry.sol:L145-L150 @ ens_v2_sepolia_20260916@366de741)
(upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registry/PermissionedRegistry.sol:L172-L179 @ ens_v2_sepolia_20260916@366de741)
A label registered before its registry was reachable under this name appears
at the position where it became reachable; its original grant keeps its own
position and the name it had then. `total_count` counts rows, so it can exceed
the number of distinct children or of registration transactions, and grouping
rows by transaction does not reproduce it.

Row shape with the option:

- Every row carries `subject`: `child` for a direct child registration and
  `name` for every other row.
- A child row's `name` is the child's name. A `subject=name` row keeps the
  requested name, as without the option.
- `id`, `registration_id` (the child's own registration), and every other field
  are the same values the row has in the child's own history and on
  `GET /v1/events`.

One event can qualify both ways. The registration scope follows a name's
resources through history, so a grant made to a former child that later
became this name's own resource is both a child registration and a row of the
name's scope. The collection is a set of events: such an event appears once,
with `subject=child`. The same rule decides page rows, `total_count`, and
cursor validation.

The option does not change the meaning of the other controls:

- `type` and the timestamp window apply to the combined collection. Child rows
  are `registration` rows, so a `type` set without `registration` returns no
  child rows.
- `order` reverses the combined order exactly.
- `total_count` counts the combined collection, capped or exact as described
  under the [history collection filters](#history-collection-filters).
- The cursor binds the option.
- Responses carry no `ETag` or `Cache-Control`, as on every history
  collection.

The rows come from a Project-maintained list of each name's historical child
registration events, published with the other projections. A deployment that
introduces it rebuilds Project from full history before serving the option; see
[`projections.md`](projections.md#child-registration-events).

### `GET /v1/permissions`

- Method/path: `GET /v1/permissions`
- Tier: product read.
- Purpose: flat permission rows by name, registration, or address, including
  registrations that are no longer a name's current one. An `address` anchor
  answers “what resources does account X operate?” for both direct and
  effective account-wide grants.
- Request parameters: at least one of `name`, `registration_id`, or `address`;
  filters are combinable intersections. A `name` resolves its current
  `registration_id`; an explicit `registration_id` must match it when both are
  supplied, and `address` then restricts the permission subject. An explicit or
  name-implied `namespace` filters registrations before pagination; an
  address-only request without `namespace` continues to span all namespaces.
  For the limited solo beta, address-only operator discovery retains a known
  scalability limitation: a small page can still scan or sort an owner's
  resource keys, and sparse namespace eligibility or cursor filtering can
  require many eligibility lookups. The accepted optimization reduces payload
  work in measured finite cases; it does not establish a production latency
  SLO, a cold-cache guarantee, or a bound for arbitrary owner sizes. Issue #861
  remains open for scalability. Name or explicit registration filters select
  one resource and avoid this broad discovery branch. This limitation does not
  change which rows are eligible or the cursor ordering.
  An explicit namespace must be public; an unknown value returns `404 not_found`
  before cursor decoding.
  Direct rows and effective registry-operator rows share the namespace
  membership rule described below.
  Query `include=lineage`, `cursor`,
  `page_size`, and optional `finality=latest`. `at` and historical `finality`
  values are rejected by the shared latest-state collection rule. An explicit
  namespace filters rows and their summary evidence before pagination. Resource audit
  reads establish namespace from canonical activated interpreted events, so an
  unnamed or superseded registration does not need a current name binding. A
  registration outside the namespace returns an empty page without its resource
  restrictions or permission support metadata.
- Response shape: `data` is an array of permission rows
  `{address, grant_relation?, grant_scope, powers, registration_id, record_resource?, name?,
  authority_context, wrapper_state?, wrapper_fuses?}`.
  `record_resource` is present only for a grant on an ENSv2 record-ID resolver
  (see [record links](#get-v2resolverschain_idaddress)), where the granted
  resource is not a name but a setter argument — the record the holder may
  set. It is the argument as the interpreter decoded it: `{kind: "address",
  hash, coin_type}`, `{kind: "text" | "data", hash, key}`, `{kind: "abi",
  hash, content_type}`, `{kind: "interface", hash, interface_id}`, or, when
  one argument authorizes several setters, `{kind: "argument", hash,
  selectors: [...]}` of those shapes. `hash` is the on-chain resource, the
  keccak of the argument. Only readings whose setter (`set_addr`, `set_text`,
  `set_data`, `set_abi`, `set_interface`) the row's `powers` still hold are
  served — an `admin_set_*` role is authority to grant that setter, not the
  setter, and does not count; a revoked family drops out, and a multi-setter
  argument left with one held reading is served as that reading — so a row
  with no held reading, like a grant whose argument was never observed,
  carries no `record_resource`. A text or data key is served as `key` when its
  bytes are valid UTF-8, contain no NUL byte, and are not empty or whitespace
  only — the same cutoff the interpreter applies when it names the record
  (`crates/adapters/src/schema_v2/common.rs`, `event_string_selector`) —
  and otherwise as `key_bytes`, the raw bytes as lowercase `0x` hex; a key
  with other control characters is still `key`. `coin_type` and `content_type` are
  numbers, as everywhere else in the API; an argument the chain carried beyond
  64 bits is served as its decimal string under `coin_type_decimal` or
  `content_type_decimal` instead.
  (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L307-L338 @ ens_v2@a971bd64)
  (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L252-L259 @ ens_v2@a971bd64) The two wrapper fields use the same atomic,
  [expiry-effective](glossary.md#expiry-effective-namewrapper-fuse-word)
  contract as name detail and appear only for a returned current ENSv1 wrapper
  registration. For a current ENSv1 NameWrapper registration the rows are the
  ERC-1155 token holder, each owner-wide operator the holder approved for as
  long as that approval stands, and the per-token approved delegate. Operator
  rows carry the holder's powers because NameWrapper authorizes the holder and
  its operators identically; the delegate carries only
  `extend_subname_expiry`; every row applies the expiry-effective fuse mask, and
  rows revoke on transfer, unwrap, approval revocation, and expiry. Request-
  relative completeness metadata below remains authoritative.
  (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L214-L238 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L37-L47 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L105-L117 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L137-L150 @ ens_v1@91c966f)
  A resource-bound read (`name` or `registration_id`) also returns a top-level
  `restrictions` object beside `data`, `page`, and `meta`: the
  [resource restrictions](glossary.md#resource-restrictions) of the selected
  registration (shape in [api-v2.md](api-v2.md#resource-restrictions)). It is
  omitted for address-only reads and when the registration has no
  resource-level constraint model, its NameWrapper position has expired with a
  cleared owner, or its wrapped token has been burnt or unwrapped.
  `authority_context` is required on every row and records how that row was
  admitted under the per-name ownership rule. `powers` values come from the
  [permission powers vocabulary](api-v2.md#permission-powers-vocabulary), which
  names every value and the on-chain role bit or NameWrapper fuse behind it.
  `include=lineage`
  adds route-local `lineage` per row:
  `{grant, revocation?, inheritance_path?, transfer_behavior?}`. Product lineage
  is a bounded summary; deep provenance stays on diagnostics authority/events
  routes. Lineage objects expose only allowlisted fields: `kind`,
  `registration_id`, `resolver: {chain_id, address}`, `powers`, and
  `relation` when those fields apply. `relation` names the NameWrapper relation
  behind an `ens_v1_authority` grant or revocation: `holder`, `operator`, or
  `token_approval`. `kind` values are `event`, `permission`,
  `registration_authority`, `registration_rebound`, `ens_v1_authority`,
  `resolver_root_fallback`, and `registry_root_fallback`. Diagnostics-only
  storage keys such as event provenance, upstream/root resources,
  contract-instance ids, changed powers, and manifest versions are omitted.
  `grant_scope` is `{kind, detail}`. Detail is `{}` for `root`, `registry`,
  and `registration`;
  `{resolver: {chain_id, address}}` for `resolver` with numeric `chain_id`;
  `{chain_id, manager}` for `record_manager`; and
  `{chain_id, authority_kind, authority_contract, owner}` for the
  [`account` permission scope](glossary.md#account-permission-scope). Effective
  account rows carry `grant_relation=operator` and
  `powers=["registry_control"]`; direct rows omit `grant_relation` and are
  otherwise byte-for-byte compatible. For example, a direct row remains:

  ```json
  {
    "address": "0xdirect",
    "grant_scope": {"kind": "registration", "detail": {}},
    "powers": ["registration_control"],
    "registration_id": "018f...",
    "name": "example.eth",
    "authority_context": "current_for_name"
  }
  ```

  An effective registry operator is:

  ```json
  {
    "address": "0xoperator",
    "grant_relation": "operator",
    "grant_scope": {
      "kind": "account",
      "detail": {
        "chain_id": 1,
        "authority_kind": "registry",
        "authority_contract": "0xregistry",
        "owner": "0xregistry-owner"
      }
    },
    "powers": ["registry_control"],
    "registration_id": "018f...",
    "name": "example.eth",
    "authority_context": "current_for_name"
  }
  ```

- Pagination behavior: standard collection pagination with fixed sort
  `address_registration_scope_asc` and keyset
  `(subject, resource_id, scope)`. Direct and account
  scopes share that order. The account key is
  `account:{chain_id}:{authority_kind}:{authority_contract}:{owner}`. The
  opaque cursor binds the exact normalized collection anchor: normalized
  `address`, normalized `name` when supplied, the selected public
  `registration_id` (the one named, else the registration the name serves,
  which for a wrapped `.eth` name is the lease and never the NameWrapper
  resource that holds its rows),
  namespace when explicit or implied by a name (and namespace absence for an
  address-only request, matching its all-namespace result set),
  `include=lineage`, the fixed sort, the last keyset tuple, and the captured
  publication. Malformed cursor
  encoding, a different bound filter anchor or sort, and a missing or malformed
  keyset tuple are rejected. A well-formed edited keyset tuple is accepted as a
  caller-supplied resume position; the cursor is opaque but is not
  cryptographically signed. A name-anchored cursor is rejected for a different
  name or a registration-only request, even when both names resolve to the same
  registration. Crossing from direct to operator rows neither duplicates nor
  omits a row.
- Snapshot behavior: a `name` filter resolves its current registration anchor
  and permission rows under the same revalidated publication, disclosed in
  `meta.as_of`. Completeness metadata remains available. Continuations bind the
  publication; a change returns `409 stale` requiring a restart without the
  cursor. A first page whose publication changes during the read returns
  `409 stale` too and can simply be retried. Historical permission enumeration
  is not supported.
- Status semantics: no matching permission rows returns `200` with empty
  `data`, including when a `name` filter has no registration anchor in the
  current state. Unsupported filter combinations return `422 unsupported`;
  pairing `name` with a `registration_id` that is not that name's selected
  current registration is not one of them. It is a supported query that selects
  nothing, so it returns `200` with empty `data`; its reason is the explicitly
  requested registration's support classification under the resource-bound
  rule.
  A supplied `name` that is missing or unrecognized, whose current name is
  marked unsupported, or that resolves to a current name not bound to a
  registration resource cannot select a supported current registration. Its
  request-relative empty result returns `meta.completeness=partial` with
  `unsupported_reason=permission_support_unknown`; it does not prove that the
  name has no permission rows. A resolved current name paired with an explicitly
  different `registration_id` is a supported empty intersection. Its empty page
  uses that explicit registration's resource-bound support classification,
  including the wrapper list or `permission_support_unknown` when
  applicable. A `registration_id` outside an explicit `namespace` instead
  returns an empty page without permission support metadata.
  Rows of a wrapped `.eth` name carry the BaseRegistrar lease as
  `registration_id`, the same handle name detail serves.
  `GET /v1/permissions?registration_id=<lease>` for a wrapped name returns the
  permission rows of the NameWrapper resource that currently controls the name,
  matched through the name's current `registration_id`; a name registered
  through the NameWrapper, whose wrap recorded no lease, is matched the same
  way. Registering through the NameWrapper registers the lease to the wrapper
  and wraps it in one call, and the registrar mints and emits before the wrap,
  so the controller event that creates the registration comes after
  `NameWrapped` and the wrap records no lease.
  (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L289-L305 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L130-L152 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L894-L902 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/deployments/mainnet/WrappedETHRegistrarController.json:L656 @ ens_v1@91c966f)
  `restrictions.registration_id` is the lease on every page of a read
  bound to it, including an empty page filtered by `address`.
  The NameWrapper resource itself is not the name's registration, so pairing
  the name with it is the supported empty intersection described above, and
  reading it alone returns an empty page without permission support metadata.
  An unrecognized namespace returns `404 not_found`. A publication change
  during the read returns `409 stale`, as described above.
  When `name` or `registration_id` binds the read to a registration, the
  projection-owned per-registration permission summary classifies the result.
  When a permission surface is not listed, the response carries
  `meta.completeness=partial`,
  `unsupported_reason=permissions_partially_listed`, and
  `meta.unlisted_permission_surfaces`, the sorted codes defined in
  [api-v2.md](api-v2.md): `ens_v2_registry_operators`, `registrar_approvals`,
  `resolver_approvals`, and `wrapper_parent_control`. The list shrinks as later
  parts of issue #605 add these surfaces. An unwrapped registrar- or registry-held registration reports
  `["registrar_approvals","resolver_approvals"]`.
  (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L108-L118 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L42-L50 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L78-L103 @ ens_v1@91c966f) An
  ENSv1 NameWrapper resource reports
  `["resolver_approvals","wrapper_parent_control"]`:
  its holder, operators, and delegate are rows, while the parent name's control
  over a non-emancipated wrapped subname and resolver operator/delegate
  approvals are not enumerated; the NameWrapper `Ownable` owner is a
  deployment-wide administrator, not a per-registration permission.
  (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L565-L589 @ ens_v1@91c966f)
  An ENSv2 registry registration reports
  `["ens_v2_registry_operators","resolver_approvals"]`: its direct role holders
  are rows, while operators the owner approved on the ENSv2 registry and
  `PublicResolverV2` operators and delegates are not. It has no BaseRegistrar
  token, so it never reports `registrar_approvals`.
  (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L575-L592 @ ens_v2@a971bd64)
  (upstream: .refs/ens_v2/contracts/src/resolver/PublicResolverV2.sol:L51-L59 @ ens_v2@a971bd64)
  (upstream: .refs/ens_v2/contracts/src/resolver/PublicResolverV2.sol:L174-L184 @ ens_v2@a971bd64)
  Independently proven full support omits all three fields. Missing or
  unrecognized summary metadata returns `meta.completeness=partial` with
  `unsupported_reason=permission_support_unknown`, no list, and takes
  precedence. A request that mixes wrapper, non-wrapper, or ENSv2 registrations
  reports the sorted union. An address-only read always reports all four codes,
  including for zero rows or a page with no wrapper or ENSv2 row, unless
  indeterminate support wins. Returned rows do not define the request denominator: zero rows
  do not prove that no account can mutate the selected name or registration.
  Projected rows are not suppressed by these classifications and remain useful,
  but neither the page nor a role summary is an authoritative permission
  enumeration while the partial marker is present. Registrar ERC-721 approvals,
  resolver approvals/delegates, and parent control of non-emancipated wrapped
  subnames remain absent. ENSv2 registry operators also remain absent, and an
  ENSv2 registration names that gap as `ens_v2_registry_operators`.
  (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L575-L592 @ ens_v2@a971bd64) A `name` filter
  resolves only the selected current registration: a migrated name returns its
  ENSv2 permission rows, while an explicit `registration_id` can still select a
  retained historical ENSv1 registration for audit. An ENSv2 reservation does
  not select permission rows by `name` because current-registration
  classification has no owner evidence: the owner-zero branch emits
  `LabelReserved`, while minting the owner token and granting resource roles
  occur only in the registered branch.
  (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L464-L472 @ ens_v2@a971bd64)
  If bigname retains permission evidence attached to a resource for audit, an explicit
  `registration_id` read remains available with `resource_audit`; that marker
  does not claim the evidence is live for the reserved name. Every
  permission row carries the required `authority_context` field.
  An address-filtered request discovers effective registry operators and returns
  one row per currently matching resource. Name and `registration_id` filters
  expose the same rows for the selected current authority resource. This includes
  an ENSv1 or Basenames registrar-token transfer without reclaim when the original
  registry owner remains nonzero and has approved the operator: the name selects
  the registry-only resource, and its name, resource and address reads agree.
  The registrar token's separate resource does not replace that selection. The
  rows still carry the registrar lease as `registration_id`, the handle name
  detail serves, and the resource read is the read by that handle; the
  registry-only resource holds the rows but is not a public registration
  handle and selects none by its own id. This also holds before name materialization
  when activated, canonical registry authority and registrar grant observations
  identify the same node on the same chain; absent logical names alone prove no
  relationship. Both mapping directions use the captured publication: later
  activated observations cannot change the advertised or selected handle before
  Project publishes them. During a released gap with no live lease, retained
  registry grants instead use their registry resource as a followable
  `resource_audit` handle; this does not claim a live name registration, and the
  released lease does not select that control or a successor's grants.
  An ordinary registry-owned subname with no registrar lease also uses its
  registry resource as the registration handle. That handle selects its grants both while the name is current and as
  a resource audit after the current name row disappears.
  (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L75-L84 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L171-L175 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L17-L20 @ ens_v1@91c966f)
  (upstream: .refs/basenames/src/L2/Registry.sol:L49-L52 @ basenames@1809bbc)
  Applicability reads the current
  [registry-owner binding](glossary.md#registry-owner-binding) described in
  [`projections.md`](projections.md#permissions), rather than
  deriving it from events. A registry-contract generation move, owner change,
  zero owner, revocation, or orphaned account or binding lineage makes the row
  absent. `include=lineage` exposes only the bare `lineage.grant={"kind":"event"}`
  marker and does not expose registry-binding provenance.
  `current_for_name` means a `name` filter selected the row's current
  registration for that requested name. A row admitted without a `name` filter,
  including an explicit-`registration_id` or address-filtered resource read, is
  `resource_audit` and makes no current-name claim even when it has an optional
  display `name`. Combining `name` with `registration_id` returns rows only when
  that registration is the selected current registration; a superseded pair is
  empty. Rows carrying `resource_audit` remain available in this collection.
  The marker changes only how that response may be interpreted; it does not
  change the registration's eligibility in a separate name-scoped view. The
  per-name ownership rule independently decides which registration contributes
  current authority, address relations, and role summaries, so a superseded
  ENSv1 registration is never selected while a current registration queried by
  resource can still contribute elsewhere. The collection adds no row-local
  coverage status or unsupported-reason vocabulary. A `name` filter whose
  exact-name projection is unsupported therefore selects no registration and
  returns `200` with empty `data`; callers use name detail or batch lookup for
  the explicit reason.
- Replaces (v1): `GET /v1/resources/{resource_id}/permissions`,
  `GET /v1/roles`, `GET /v1/names/{namespace}/{name}/roles`, and
  `GET /v1/resources/lookup`.

### `GET /v1/addresses/{address}/names`

- Method/path: `GET /v1/addresses/{address}/names`
- Tier: product read.
- Purpose: names related to an address.
- Request parameters: path `address`; query `namespace`, `relation`,
  `authority=ens_v1|ens_v2`, `coin_type`, `q`,
  `sort=name|expires_at|registered_at`, `order=asc|desc`,
  `dedupe=name|registration`, `include=role_summary`, `cursor`, `page_size`,
  and optional `finality=latest`. `at` and historical `finality` values are
  rejected by the shared latest-state collection rule.
  `authority` keeps only rows whose current name row selected that protocol
  arm; it is a primary-key probe of the name row per candidate relation row.
  Any other value returns `400 invalid_input`. Rows with no selected arm
  (Basenames) match neither value.
  `is_migrated=true|false` optionally selects whether the current name has the
  same proven ENSv1→ENSv2 transition used by `migrated_at`: an ENSv2 authority
  selected by a migration proof with a retained event and block timestamp.
  Native ENSv2 registrations do not satisfy `is_migrated=true`. It combines
  with the other filters and is rejected with `relation=resolves_to`.
  The ownership collection always returns an exact `page.total_count` before
  applying its cursor, with the same relations, prefix, authority, migration
  predicate and deduplication as the rows. For registration counts use
  `relation=registrant&dedupe=registration`; a name count uses `dedupe=name`.
  This GET route supplies exact totals even for single relations whose
  `POST /v1/lookup` result count remains unknown.
  `q` applies prefix matching to the dictionary `name` field. The API treats
  the complete `q` value as an ENSIP-15 name prefix and normalizes it with the
  same normalizer used for indexed names before comparing it directly with the
  stored normalized name. An empty `q` is treated as absent. A partial final
  label is accepted when it is valid as a standalone ENSIP-15 label, so `q=AL`
  normalizes to `al`. When `q` has exactly one trailing dot, the preceding
  nonempty name is normalized and the dot is then restored, so
  `q=ALICE.` matches `alice.eth` but not `alicex.eth`. An empty name before the
  boundary marker, multiple trailing dots, an empty interior label, or any
  other input rejected by bigname's name validation atop ENSIP-15 returns
  `400 invalid_input`. This route does not accept `match`.
  `relation` accepts a comma-separated set of v2 vocabulary values
  `owner`, `manager`, and `registrant`; `any` normalizes to all three values.
  Rows match when any listed relation matches. The storage relations map as
  token-holder -> `owner`, effective-controller -> `manager`, and
  registrant -> `registrant`. `dedupe=name` groups by name surface and is the
  default; `dedupe=registration` groups by registration resource.
  `relation=resolves_to` is the resolver-record relation: the names whose
  current `addr:<coin_type>` resolver record resolves to the path address, read
  from the `address_records_current` projection rather than the authority
  relations. It stands alone: `resolves_to` combined with `owner`, `manager`,
  `registrant`, or `any` returns `400 invalid_input`, and `any` never includes
  it, because it is coin-type scoped and its rows are not authority claims.
  `coin_type` is accepted only with `relation=resolves_to`; supplying it with
  any other relation returns `400 invalid_input`. It takes one of two forms.
  A decimal ENSIP-9/SLIP-44 coin type (default `60`) reads that one coin type.
  A name matches when its serving resolver's indexed inventory answers
  `addr:<coin_type>` with a 20-byte non-zero value equal to the path address,
  using the same rule as `GET /v1/names/{name}/records`: the exact entry, or
  for an EVM coin type (`60`, or an ENSIP-11 coin type `0x80000000 | chain_id`)
  the ENSIP-19 default EVM address `addr:2147483648` when the resolver declares
  that read feature and no exact entry for the coin type shadows it.
  `coin_type=evm` reads every EVM coin type in one request, so a caller can
  find names that resolve to the address on chains it does not know in
  advance. For example, a name whose only matching record is Base's
  `addr:2147492101` is absent from the default coin-60 read but present here.
  The EVM coin types are exactly `60` and every coin type from `2147483648`
  (`0x80000000`, the ENSIP-19 default) through `4294967295` (`0xffffffff`),
  the set ENSIP-19 treats as EVM
  (upstream: .refs/ens_v1/contracts/utils/ENSIP19.sol:L9-L38 @ ens_v1@91c966f).
  A name matches when at least one of its stored `addr:<coin_type>` records for
  a coin type in that set holds exactly the path address. The path address is
  parsed as a `0x`-prefixed 20-byte hexadecimal address and normalised to
  lowercase, so mixed-case and checksummed spellings are accepted (the
  checksum is not validated). Matching compares the exact 20-byte value: it
  establishes the same 20-byte value, not common ownership of an account
  across chains, and padded values or other encodings are not accepted. The
  read enumerates stored
  records rather than per-chain answers, so the ENSIP-19 default record is
  never expanded into the chains it would answer: it matches once, as coin
  type `2147483648`, whenever its own value matches, including when exact
  entries shadow it for some chains. Coin types outside the set are never
  matched by `evm`, even when their stored value is 20 bytes; this includes
  legacy SLIP-44 coin types of EVM-compatible chains such as `61`. Read them
  with a decimal `coin_type`. `evm` is therefore a deliberate narrowing: it
  does not discover every stored coin type. The value is spelled exactly
  `evm`. Boundary whitespace is trimmed as for every query value, so
  `coin_type=evm ` is `evm`; `EVM` or any other spelling returns
  `400 invalid_input`, as `relation=ANY` does. An empty or whitespace-only
  `coin_type` is treated as omitted and reads coin type `60`. A comma list is
  not a supported form and returns `400 invalid_input`.
  Zero-address and cleared records never match. Names with a retained serving
  resource can match without a current owner or registration; the result does
  not invent authority for them. For such names, `dedupe=registration` and
  cursor identity use the serving resource as the grouping key while
  registration fields remain absent. `namespace`, `authority`, `q`, `sort`,
  `order`, `dedupe`,
  and `include=role_summary` apply as for the authority relations.
- Response shape: `data` is an array of record-shaped rows with `name`,
  `display_name`, `namespace`, `namehash`, `owner`, `registrant`,
  `registration_status`, `registered_at`, `created_at`, and `expires_at`.
  Address-name rows also return `permission_resource_id`, the handle
  `GET /v1/permissions?registration_id=` resolves to the permission authority
  resource behind the row's inline summary. While the name retains a current
  registration identity it is that registration's `registration_id`, including
  when other name coverage is unsupported. This preserves a followable resource
  audit without asserting current name support. It agrees with supported name
  detail: for a wrapped `.eth` name it is the BaseRegistrar lease, although its
  permission rows live on the NameWrapper resource, which is not a
  public registration handle. A wrapped subname has no lease and keeps its
  NameWrapper resource. When no current registration claims the resource,
  the value is the resource UUID itself, which the permissions route reads as a
  resource audit. It remains available without `include=role_summary`. A
  `relation=resolves_to` row whose name has only a retained serving resource,
  and therefore no permission authority, omits it.
  Address-name rows add `is_primary` and `relations`, where `relations` is the
  subset of `owner`, `manager`, and `registrant` that matched, or
  `["resolves_to"]` on a `relation=resolves_to` read. A `resolves_to` row also
  carries `resolution: {coin_type, record_key}`: the coin type asked about and
  the resolver record key that answered (`addr:<coin_type>`, or
  `addr:2147483648` when the ENSIP-19 default EVM address answered). On a
  `coin_type=evm` read the row omits `resolution` and instead carries
  `resolutions: [{coin_type, record_key}]`, one entry per EVM coin type whose
  stored record matched the address, ascending by coin type, where
  `record_key` is the stored record that matched (`addr:<coin_type>`). The
  list names matches only. It is not every coin type the name's resolver has
  records for, and it is not a claim about live resolution on chains the
  stored records do not name. One row is still returned per name (or per
  registration), never one per coin type. A row carries at most 100
  `resolutions`. When a row on the returned page matched more than 100
  distinct EVM coin types, whether by its name or, with `dedupe=registration`,
  across its registration group, the whole request returns `422 unsupported`
  with no partial data or truncation, even with `page_size=1`; read that
  address one decimal `coin_type` at a time instead. A coin type that several
  names in one registration group matched counts once. Whoever controls a
  name's resolver can store records that hold any address, so without this
  bound one name could make every `coin_type=evm` page for that address carry
  an unbounded list. The bound caps each row's aggregated matches and response
  size, not the stored rows the read scans to count them. With `dedupe=registration`,
  `resolutions` is the union of the matches of every name in the registration
  group. The reverse index is built so that every name in a group is expected
  to read the same record inventory, so the union normally equals each
  member's own matches. When it does not, `is_primary` still counts only the
  representative name's own matches. The row's name
  fields, `include=counts`, and `is_primary` belong to the
  [representative name](glossary.md#representative-name), the group member
  that sorts first by name text, then by namespace and namehash. Rows also
  carry `authority` and `migrated_at` with the same meaning as on
  `GET /v1/names/{name}`: the selected `ens_v1`/`ens_v2` arm, and the block time
  of the migration boundary that proved an `ens_v2` arm. `is_primary` is
  evaluated against that row namespace's coin-type-60 primary-name claim, not a
  route-wide namespace shortcut; a `resolves_to` row evaluates it against the
  requested `coin_type`'s claim instead, so a name resolving to the address on
  another EVM chain is marked primary by that chain's claim. On a
  `coin_type=evm` read a row is primary when the row namespace's successful
  claim for any coin type in that row's own matches names it; a claim for a
  coin type the row did not match never counts, and under
  `dedupe=registration` only the representative name's own matches count. The
  claims are read with the same [canonicality](glossary.md#canonicality) and
  [hydration](glossary.md#hydration) fallback rules as
  `GET /v1/addresses/{address}/primary-name`. The claim is compared in the same normalized
  form the indexed answer from `GET /v1/addresses/{address}/primary-name`
  publishes, so a successful claim recorded in a non-normalized spelling still
  marks its name primary. A spelling the projection already recorded as its
  normalized form is instead compared verbatim, so such a claim marks a row
  primary only where the published spelling is exactly that row's name. A
  successful claim whose stored spelling does not normalize likewise marks no
  row primary, and the primary-name route reports it as `invalid_name`.
  Resolver records are not included; use `GET /v1/names/{name}/records` for
  resolver data.
  `include=role_summary` adds
  `role_summary: [{address, grants: [{grant_relation?, grant_scope, powers}]}]` grouped by the
  permission subject address, `restrictions` (the same
  [resource restrictions](glossary.md#resource-restrictions) object
  `GET /v1/permissions` returns for the row's registration, omitted when none
  applies), and `record_count` when record inventory exists
  for the row. `record_count` counts the known record selectors for the name's
  current registration, including unsupported-family selectors and excluding
  explicit gaps. `grant_scope` uses the same shape documented for
  `GET /v1/permissions`. Direct grants omit `grant_relation`; effective
  registry-operator grants carry `grant_relation=operator`, the account scope,
  and `powers=["registry_control"]`. Operator grants expand roles for resources
  already on the page but never add or remove address-name membership rows.
  The include supports at most 1,000 grant rows across all returned summaries,
  counting each grant again when multiple names share a resource. This is a
  total nested expansion budget, not a per-name or per-subject limit. Overflows
  return a whole-request `422 unsupported` with no partial data or truncation.
  Omit the include, then paginate
  `GET /v1/permissions?registration_id=<permission_resource_id>` for each selected
  resource. Preserve `namespace` only if it was explicitly present on the names
  request; do not add `name` or `address` filters. This reads the same supported
  permission relation, including supported rows on an unsupported name anchor.
  Existing unsupported permission families remain unsupported. Reducing the name
  page can help, but one resource can exceed the limit by itself. Each request
  binds its own publication: a publication change between the names request and
  a later permissions request can alter grants. The 1,000-row cap bounds returned grant
  rows, not bytes or total database work. The name page maximum remains 200.
  `include=counts` adds `subname_count`, the row's
  direct readable subname count (one bounded per-parent aggregate over the
  page's names), and the same `record_count`; the two expansions combine as
  `include=counts,role_summary`. No `event_count` is offered, for the reason
  given on `GET /v1/names/{name}`. Any other `include` value returns
  `400 invalid_input`.
- Pagination behavior: standard collection pagination. Cursors are bound to
  address, optional namespace filter, normalized relation set, `authority`, `is_migrated`,
  `q`, dedupe mode, sort, and order; a `resolves_to` cursor additionally binds
  the coin-type selector (the canonical decimal coin type, or `evm`), so a
  single-coin cursor never resumes an `evm` read or the reverse, and a cursor
  minted for one relation set never resumes another. `resolves_to` reads
  return `page.total_count: null`.
- Snapshot behavior: the page and its counts use the captured current
  publication. The response discloses `meta.as_of`; continuation cursors bind
  the publication and return `409 stale` requiring a restart when it changes.
  A first page whose publication changes during the read returns `409 stale`
  too and can simply be retried.
  Historical replay through `at` is not supported.
- Status semantics: no related names returns `200` with empty `data`.
  Malformed addresses return `400 invalid_input`. Unsupported public namespaces
  return `404 not_found`. `include=role_summary`
  uses the same publication fence as the base collection, and current-state
  publication changes produce `409 stale`. The grant-budget `422 unsupported`
  is returned only after that fence passes, so a publication change during an
  overflowing read is also `409 stale`. The same holds for the
  `coin_type=evm` `422 unsupported` for a row past 100 matched coin types. The expansion batch-loads
  projection-owned permission summaries for every
  registration on the served page. A page with any unlisted permission surface
  returns `meta.completeness=partial`,
  `meta.unsupported_fields=["role_summary"]`,
  `unsupported_reason=permissions_partially_listed`, and the sorted union of
  its registrations' `meta.unlisted_permission_surfaces`, using the same codes
  as `GET /v1/permissions`.
  Projected
  grants remain in `role_summary`, but the expansion is non-authoritative;
  therefore an empty summary is not a proven empty permission set. Missing or
  unrecognized summary metadata takes precedence and uses
  `permission_support_unknown`.
  Current address relations and
  `role_summary` are built only from the selected registration. A migrated
  name therefore stops relating its superseded ENSv1 holder or controller to
  the current name row. This collection adds no row-local mixed-authority
  status. When a current address relation is provable, the standing exception
  in [`api-v2.md`](api-v2.md#cursors-and-pagination) still lists the row even if other name
  coverage is unsupported. When no current authority can be proven, no current
  address relation can be established and the name is structurally absent;
  callers use name detail or batch lookup for its explicit coverage reason.
  A name bigname has never materialized as a
  [name surface](glossary.md#surface-name-surface) is likewise absent even
  when its registry owner is proven. On the ENSv1 arm a surface comes from a
  label-bearing registrar or NameWrapper event; a node known only from registry
  owner events — a subname written with `setSubnodeOwner` or
  `setSubnodeRecord`, which emit the labelhash and not the label, and never
  wrapped — has a registry-only resource but no surface, whether or not a
  label preimage for it exists
  ([ADR 0002](adrs/0002-surface-resource-identity.md), ENSv1 authority-anchor
  rules)
  (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L49-L58 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L75-L83 @ ens_v1@91c966f).
  Such a name has no current name row for any relation or `is_migrated` value
  to list, and this route never serves a
  [non-name form](glossary.md#non-name-form) for it. Its only listing is the
  row on its parent's `GET /v1/names/{name}/subnames`, named from a proven
  preimage or by the placeholder. The same rule applies to reverse inputs of
  `POST /v1/lookup`.
- Replaces (v1): `GET /v1/addresses/{address}/names` and address-relation
  uses of `GET /v1/names`.

### `GET /v1/addresses/{address}/primary-name`

- Method/path: `GET /v1/addresses/{address}/primary-name`
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
  when its raw spelling was not already normalized, unless the projection
  already recorded that spelling as its normalized form, in which case the
  stored spelling is published unchanged. `verification` is
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
  position and captured `project` publication generation match the
  lookup before verified execution and remain unchanged after the indexed
  read; otherwise the request returns `409 stale`. Live results never change
  the indexed answer. Basenames verified primary-name lookup is unsupported;
  indexed Basenames responses remain Base-scoped.
- Pagination behavior: none.
- Snapshot behavior: current-state read over chain-derived primary-name state.
  The route does not accept `at` or `finality`. Successful responses carry
  `meta.as_of` and `meta.as_of_token` for indexed state or the current readable
  Ethereum L1 position used by fresh ENS/60 verification: chain `1` under the
  Mainnet deployment profile, chain `11155111` (token slot `ethereum-sepolia`)
  under the Sepolia profile. No metadata field
  implies cache reuse or a persisted execution identity. Provider transport
  failures abort the request with `500 internal_error`; they are not verified
  answer entries with `status=stale`. The projected-claim reads that decide
  whether a claimed name may be verified at all fail closed the same way: if
  those reads error the request returns `500 internal_error` rather than
  proceeding to live resolution, because a failed read is not evidence that no
  claim exists. The one exception is a missing exact-name projection: the
  verified source then reports the same in-band `unsupported` answer the indexed
  source reports for it, rather than failing, since neither source can speak for
  a projection that is not deployed.
- Verifiable authority: forward verification is refused for a name whose selected
  [authority arm](glossary.md#authority-epoch) is outside the
  `verified_authority_arms` the selected `ens_execution` manifest declares
  (`manifests.md` § `verified_authority_arms`; absent means `["ens_v1"]`). The
  refusal is an in-band `verified` entry with `status=unsupported` and
  `unsupported_reason=exact_name_authority_not_verifiable`, and no forward
  resolver call is dispatched. The same reason is served when no admitted
  execution entrypoint is declared for the chain at all. The refusal follows the name, not the source that
  named it: it applies both to a projected claim and to a name the live reverse
  leg returns, and in the live case the check runs after the reverse leg and
  before the forward call, so a refused name costs no forward dispatch and no
  CCIP-read follow. A present supported `name_current` row without its selected
  [authority arm](glossary.md#authority-epoch) is a projection anomaly and
  receives the same refusal. A name for which the current projection read finds
  no exact-name row in `name_current` is instead admitted to live forward
  verification. The projection cannot state which authority applies to a name
  outside indexed coverage, and live verification is the only answer path for
  it. A subname known only through offchain or wildcard live resolution has no
  exact-name row because a live provider answer does not materialize one;
  separately observed wildcard names can have projected surfaces.

  A missing readable row cannot generally hide a later authority arm from the
  execution this route performs. Live ENS/60 primary-name verification executes
  against the Ethereum L1 of the selected [deployment
  profile](glossary.md#deployment-profile): Mainnet, whose profile admits no
  ENSv2 registry, so there is no later arm to hide; or Sepolia, through the
  Sepolia profile's `ens_execution` Universal Resolver and `ens_v1_registry_l1`
  registry declarations, with the same reverse leg, the same pre-forward
  authority gate, the same hash pinning to the readable Sepolia head, and the
  same provider limits. The official Sepolia manifest admits both `ens_v1` and `ens_v2`
  through its Universal Resolver proxy; the [deployment inventory](sepolia-deployment.md)
  records the root binding and rollout verification. The
  Sepolia evidence below explains the projected authority and the ENSv1-path
  behavior that route relies on. An
  unwrapped ENSv1→ENSv2 migration clears the migrated node's ENSv1 resolver
  `(upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L111-L118 @ ens_v2@a971bd64)`,
  an unlocked wrapped ENSv1→ENSv2 migration does the same
  `(upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L146 @ ens_v2@a971bd64)`,
  and the locked path clears it when the name permits that change
  `(upstream: .refs/ens_v2_sepolia_20260629/contracts/src/migration/LockedWrapperReceiver.sol:L135-L144 @ ens_v2_sepolia_20260629@ccaeb58)`.
  The deployment script installs the ENSv2-backed wildcard resolver at the
  ENSv1 `eth` node
  `(upstream: .refs/ens_v2_sepolia_20260629/contracts/deploy/00_ENSV2Resolver.ts:L60-L81 @ ens_v2_sepolia_20260629@ccaeb58)`
  `(upstream: .refs/ens_v2/contracts/src/resolver/ENSV2Resolver.sol:L13-L14 @ ens_v2@a971bd64)`.
  The ENSv1 Universal Resolver walks up to an ancestor resolver
  `(upstream: .refs/ens_v1/contracts/universalResolver/RegistryUtils.sol:L25-L38 @ ens_v1@91c966f)`
  and accepts that resolver through ENSIP-10
  `(upstream: .refs/ens_v1/contracts/universalResolver/AbstractUniversalResolver.sol:L63-L87 @ ens_v1@91c966f)`,
  so the ENSv1 path for a migrated name serves live ENSv2 state. Upstream tests
  prove that path directly; the end-to-end suite defines the shared ENSv1 and
  ENSv2 resolution check and invokes it before and after ENSv1→ENSv2 migration
  of unwrapped, unlocked wrapped, and locked names
  `(upstream: .refs/ens_v2_sepolia_20260629/contracts/test/integration/ENSV2Resolver.test.ts:L94-L124 @ ens_v2_sepolia_20260629@ccaeb58)`
  `(upstream: .refs/ens_v2/contracts/test/e2e/migration.test.ts:L133-L147 @ ens_v2@a971bd64)`
  `(upstream: .refs/ens_v2/contracts/test/e2e/migration.test.ts:L454-L458 @ ens_v2@a971bd64)`
  `(upstream: .refs/ens_v2/contracts/test/e2e/migration.test.ts:L546-L553 @ ens_v2@a971bd64)`
  `(upstream: .refs/ens_v2/contracts/test/e2e/migration.test.ts:L606-L613 @ ens_v2@a971bd64)`.
  The following paragraph records historical ENSv1 routing evidence, rather than the current V2 proxy route. The `eth`-node redirect was scripted intent plus a deployed Sepolia resolver in
  the pinned checkout
  `(upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/ENSV2Resolver.json:L2 @ ens_v2@a971bd64)`;
  the [`ens_v2` pin](../.refs/MANIFEST.toml) is scoped to the admitted
  2026-06-29 Sepolia deployment's archived evidence (upstream's 2026-07-30
  redeploy is not admitted) and does not establish a Mainnet redirect
  `(upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/.deployment.json:L4 @ ens_v2@a971bd64)`
  `(upstream: .refs/ens_v2/contracts/deployments/sepolia/.deployment.json:L4 @ ens_v2@a971bd64)`.

  There is one known narrow exception. A locked name migrated with
  `CANNOT_SET_RESOLVER` burned keeps its ENSv1 resolver entry because the
  ENSv1→ENSv2 migration cannot clear it. When that entry names a listed
  PublicResolver, the ENSv1 side continues to serve its retained records while
  ENSv2 selects the replacement resolver
  `(upstream: .refs/ens_v2_sepolia_20260629/contracts/src/migration/LockedWrapperReceiver.sol:L137-L175 @ ens_v2_sepolia_20260629@ccaeb58)`.
  The ENSv1 PublicResolver derives ordinary write authority from the registry or
  wrapped token owner and their approvals, while separately authorizing its
  trusted ETH controller and reverse registrar
  `(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L114-L128 @ ens_v1@91c966f)`,
  requires that authority for record writes
  `(upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L47-L65 @ ens_v1@91c966f)`,
  and keeps existing records readable without that write check
  `(upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L68-L84 @ ens_v1@91c966f)`.
  Moving the wrapped token to the graveyard therefore freezes ordinary writes
  by the former name owner, but it does not revoke those separately trusted
  callers.
  This locked-name `CANNOT_SET_RESOLVER` case is not reachable under the
  Mainnet profile, which has no ENSv2 arm. Under the Sepolia profile, whose
  verified route executes through the Sepolia Universal Resolver, a name outside
  indexed coverage is admitted and can verify those retained records. Inside
  coverage, the name's `ens_v2`-selected row causes the refusal above, so the
  exposure closes as indexing coverage completes.
- Status semantics: answer entries use in-band `status`. Valid tuples with no
  indexed claim return an `indexed` entry with `status=not_found`. A stored
  successful claim whose spelling does not normalize returns an `indexed` entry
  with `status=invalid_name` and `raw_claim_name`, the same answer the
  projection's own `invalid_name` classification produces; that row marks no
  name primary on the address-name and reverse-lookup collections. Unsupported,
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
  timeout produces an in-band verified `status=failed` result; so does the
  shared CCIP-Read gateway budget running out, whatever phase the in-flight
  gateway request was in, including a connection that had not completed when
  the budget expired. Missing provider
  configuration or a selected-block rejection returns whole-request `409
  stale`. A provider or gateway connect-phase timeout, DNS failure, TLS
  failure, connection reset, or other transport failure within a request's own
  timeouts returns whole-request
  `500 internal_error`; no trace or outcome is persisted, so the next read
  retries. Malformed addresses return `400 invalid_input`.
  `source=indexed` does not enter verified-execution rate or concurrency
  admission; omitted `source` and `source=verified` do because they run the
  fresh lookup. Forward verification requires the claimed name's selected
  exact-name authority and declines in three cases. A claim whose exact-name
  projection is unsupported returns the in-band unsupported result carrying
  that projection's own public reason, the same reason name detail serves for
  the row. A present supported row with no selected authority arm is a projection
  anomaly and returns `exact_name_authority_not_verifiable`. A claim the
  projection supports whose selected authority arm is not listed in the
  selected `ens_execution` manifest's `verified_authority_arms` returns that
  same reason. Mainnet lists only `ens_v1`; the official Sepolia manifest
  lists both arms and therefore admits an `ens_v2`-selected claim. The refusal case needs a deployment profile that can support an
  ENSv2 selection at all; where the deployment profile shadows the ENSv2 arm,
  the name is already unsupported and takes the first case instead. None of the
  three cases dispatches a forward resolver call. A live reverse claim has
  already used its two reverse-leg provider calls before the name-level refusal
  is known. A consumer reads `unsupported_reason` to distinguish a projected
  coverage refusal from the shared authority-not-verifiable refusal.
- Replaces (v1): `GET /v1/primary-names/{address}`.

### `GET /v1/addresses/{address}/history`

- Method/path: `GET /v1/addresses/{address}/history`
- Tier: product read.
- Purpose: address activity history.
- Request parameters: path `address`; query `namespace`, `relation`,
  `scope=name|registration|both`, `type`, `order=asc|desc`, `from_timestamp`,
  `to_timestamp`, `include=data|raw|total_count`, `cursor`, `page_size`, and optional
  `finality=latest`. `at`
  and historical `finality` values are rejected by the shared latest-state
  collection rule. `type`, `order`, and the timestamp window follow the
  [history collection filters](#history-collection-filters); `include=data`
  and `include=raw` add the [history event
  payloads](#history-event-payloads-includedata-includeraw).
  `namespace` defaults to `ens` when omitted. `relation` accepts a
  comma-separated set of `owner`, `manager`, and `registrant`; `any`
  normalizes to all three values. Rows match when any listed relation matches.
- Response shape: `data` is an array of compact event rows using the shared
  friendly `type` vocabulary and the event-identity contract documented under
  [`GET /v1/events`](#get-v2events). The correlation-scoped candidate
  visibility rule from name history also applies here: slice 1 changes no
  address-history row,
  and slice 2 consumer activation admits correlation-dependent rows. An
  independently admitted ordinary row remains visible while its candidate
  association remains diagnostics-only. Storage applies
  the visibility predicate before deriving address anchors from ownership or
  control events, constructing selectors, validating cursors, calculating
  summaries, or selecting and paginating final rows. A candidate row therefore
  cannot expose an older activated row by broadening the anchor set. If those
  anchors reach both rows emitted for one V1 registry resolver log, product
  history keeps the control-resource row and suppresses the additional row
  carrying the registry resource link before cursor validation and pagination; raw diagnostics keeps
  both. Without a distinct control resource, the sole registry-resource row
  remains visible.
  (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L89-L94 @ ens_v1@91c966f)
- Snapshot behavior: the first page reads at the served publication and binds
  its block as the walk's [cursor bound](glossary.md#cursor-bound). Every page
  reads at that bound, counts at it, and discloses it as `meta.as_of`, as the
  [shared history rule](#shared-route-rules) describes. A continuation keeps
  working while new blocks are published and returns `409 stale` requiring a
  restart when the bound no longer holds. A first page whose check after the
  read fails returns `409 stale` too and can simply be retried.
  One relation input is not bound: when a relation in the [known
  limitation](#history-collection-filters) of the history collection filters
  ends after the bound block, a continuation drops that name's remaining rows
  and its `page.total_count` shrinks by them, without expiring the cursor.
  Historical replay through `at` is not supported.
- Pagination behavior: product event-type filtering, including an explicit
  `type` set, runs before keyset page construction (newest first unless
  `order=asc`), so `page_size`, `next_cursor`, and `has_more` describe
  product-visible events. The cursor is bound to the address, namespace,
  relation set, scope, direction, `type` set, and timestamp window. A
  nonterminal page contains `page_size` rows; only the terminal page may be
  shorter. `page.total_count` follows the shared anchored-count contract:
  capped by default, exact and uncapped with `include=total_count`.
- Status semantics: no product-visible matches return `200` with empty `data`,
  `page.next_cursor=null`, and `page.has_more=false`. Address, namespace, and
  cursor-binding validation precede the first `redo_in_progress` check, so
  malformed addresses retain `400 invalid_input` and unsupported public
  namespaces retain `404 not_found`; a cursor issued before position-bound
  pagination then returns `409 stale` requiring a restart. The route checks
  `redo_in_progress` before deriving address relation anchors and rechecks the
  Interpret state inside the repeatable-read page transaction. After the
  transaction commits, the route resolves display names, revalidates the
  captured Interpret redo state, and repeats the whole check of the
  [shared history rule](#shared-route-rules) in a fresh transaction before
  returning data. The Interpret check is collection-wide: an active Interpret
  redo on any chain returns `409 stale` with no `data` page, regardless of the
  requested namespace; an active Project redo on a chain in scope does too.
  The same response applies when a redo began between the checks. A
  well-formed cursor whose event anchor is gone returns `400 invalid_input`
  when the whole check passes and `409 stale` when it does not.
- Replaces (v1): `GET /v1/history/addresses/{address}`.

### `GET /v1/search`

- Method/path: `GET /v1/search`
- Tier: product read.
- Purpose: name search and suggestions. No availability or pricing semantics.
- Request parameters: query `q`, `match=prefix|contains` default `prefix`,
  `namespace`, `cursor`, `page_size`, and optional `finality=latest`. `at` and
  historical `finality` values are rejected by the shared latest-state
  collection rule.
- Response shape: `data` is an array of record-shaped name search results in
  dictionary vocabulary. Each result is built only from the selected current
  registration: a migrated name uses its ENSv2 owner, registrant, status, and
  expiry. A name whose exact-name projection is unsupported is omitted from
  search results whatever the reason, including a mixed-history name with no
  provable current authority. Search carries no row-local status or
  unsupported-reason field, so it omits such a name rather than serving
  registration fields no selected authority backs; callers use name detail or
  batch lookup when they need an omitted name's explicit coverage reason. The
  omission is applied before paging, so returned counts, page order, and cursor
  continuation all reflect the same filtered set.
- Pagination behavior: standard collection pagination. Without an explicit
  namespace, the cursor binds the deployment-derived namespace set and is
  rejected if that set changes.
- Snapshot behavior: search rows come from current state. The response reports
  readable request-scope positions in `meta.as_of` and reports each suppressed
  request-scope chain in `meta.as_of_completeness` with
  `completeness=unsupported` and
  `unsupported_reason=temporarily_unavailable`. A bare request accounts for
  every active public namespace's chains. An explicit namespace accounts only
  for that namespace's chains, even when another public namespace is readable.
  Returned search rows do not change that denominator. The response omits
  `meta.as_of_token`, and its cursor carries no snapshot validity claim. True
  as-of search enumeration is deferred to the
  revision-bound storage follow-up. Bare search reloads the active manifest
  declarations, selected authority chain heads, project generations, and
  Interpret redo state after reading its page; a redo that begins mid-request
  or another captured-state change returns the existing retryable `409 conflict`,
  never a partial page. Explicit-namespace search likewise captures its
  request-scope metadata before reading the page and reloads it afterward; a
  head, completed publication generation, or readiness change returns `409
  conflict` rather than attributing the page to a later position. An explicit
  namespace returns retryable `409 stale` whenever an Interpret redo is among
  the failed admission terms, including when the redo begins during the read.
  A Project publication that becomes ready between admission reads with no
  redo involved is instead a readiness change and returns `409 conflict`.
- Status semantics: no matches returns `200` with empty `data`. `q` is
  required; a missing or empty `q` returns `400 invalid_input`. The API treats
  `q` as an ENSIP-15 name fragment, normalizes it, and then applies the selected
  `match=prefix|contains` byte comparison directly to stored names. Invalid
  normalization returns `400 invalid_input`. A single trailing dot remains
  preserved as a label boundary after the preceding nonempty name is normalized,
  matching the address-names `q` behavior documented above. With
  `match=contains`, one leading dot is also accepted when it is followed by a
  nonempty fragment that does not begin with another dot. The following fragment
  is normalized as usual, and the leading dot is preserved for matching. Thus
  `.eth`, `eth.`, `.eth.`, and `th.e` are accepted contains fragments, while `.`
  and `..` return `400 invalid_input`. Leading-boundary support is specific to
  `match=contains`; `match=prefix` fragment behavior is unchanged and does not
  accept a leading dot. An explicit
  recognized namespace bypasses public namespace derivation and reads its
  current rows without a deployment-readiness gate, while its metadata still
  discloses a request-scope chain suppressed by that gate. A disclosure change
  during the read returns `409 conflict`. Bare search excludes a namespace
  while its selected authority chain has Interpret
  `redo_in_progress=true`, regardless of redo mode, and returns `409 conflict`
  when no public namespace is ready or when its captured deployment state
  changes during the read.
- Replaces (v1): search, suggestion, and exact-name-filter uses of
  `GET /v1/names`; exact name profiles move to `GET /v1/names/{name}`.

### `GET /v1/events`

For a registrar lease first identified by a later readable observation, registration time remains the original numeric grant time. Compact product history omits only snapshots with both `state_derived=true` and `registrar_surface_snapshot=true`, before pagination and cursor validation. Diagnostics retains the marked snapshot at its later readable trigger; original resource-only history and all unmarked events remain unchanged. See [storage semantics](storage.md).

- Method/path: `GET /v1/events`
- Tier: product read.
- Purpose: compact event search across name, address, contract, registration,
  resolver, type, and block filters.
- Request parameters: query `namespace`, `name`, `address`, `resolver`,
  `contract_address`, `registration_id`, `type`, `from_block`, `to_block`, `from_timestamp`,
  `to_timestamp`, `order=asc|desc`, `include=data|raw|total_count`, `cursor`, `page_size`,
  and optional `finality=latest`. `at` and historical `finality` values are rejected by the
  shared latest-state collection rule. When `name` is present and `namespace`
  is omitted, namespace is inferred from the name; `namespace` defaults to
  `ens` only when there is neither a name filter nor a `resolver` filter (a
  resolver contract serves whichever namespaces bind to it, so a bare
  `resolver` read spans every namespace). `type` (one value or a
  comma-separated set), `order`, and the timestamp window follow the [history
  collection filters](#history-collection-filters); the resolved timestamp
  window intersects an explicit `from_block`/`to_block` range. `include=data`
  and `include=raw` add the [history event
  payloads](#history-event-payloads-includedata-includeraw).
  `resolver=<chain_id>:<address>` names one resolver contract by numeric chain
  id (`1`, `8453`, `11155111`, `84532`) and EVM address; the address is
  case-insensitive and cursors bind its lower-cased form. Any other shape —
  a bare address, an unsupported chain id, or a malformed address — returns
  `400 invalid_input`. The filter matches rows the resolver contract emitted
  (record writes, permission changes, and other resolver-side events, judged by
  the row's emitting contract) plus `resolver` pointer rows whose new or
  previous pointer is that contract, so a resolver's timeline includes the
  names pointing at it. It composes with every other filter and with
  `include=data`, whose `contract_address` is the emitting contract. This
  filter lives on `/v1/events` rather than as `include=events` on the resolver
  overview because it is an unbounded, paginated, redo-guarded history read
  with the shared `order`/`type`/timestamp vocabulary, whereas the overview's
  `include=events` is a bounded `{count, by_type}` summary; nesting a second
  paginated collection beside `bound_names` would give the overview two
  independent cursors.
  `contract_address` keeps only events whose source log was emitted by
  that contract (a registry, registrar, or resolver address, compared
  case-insensitively); it combines with every other filter (including
  `resolver`, applied as AND), and the cursor binds it like the others.
- Response shape: `data` is an array of compact event rows with friendly
  `type` vocabulary. Raw upstream event kinds appear only as `kind` behind the
  explicit `include=raw` opt-in. Event-row
  identity uses two fields with distinct meanings. `registration_id` is actual
  registration lifecycle identity: it is set only when the event is associated
  with a registration lifecycle and is `null` otherwise. `resource_id` carries
  the underlying resource identity, including non-registration resources such
  as reserved entries. A reservation fact carries `resource_id` but never
  `registration_id`; resource identity is not registration authority. The
  product `registration_id` filter likewise excludes V1 ownerless rows linked
  only to the registry resource retained for reads. Raw diagnostics keeps that
  resource attribution. The filter still returns resource-less events for names
  bound to the requested registration, but only events inside a binding of that
  registration that was open at the event's position on the event's own chain
  branch, so an older registration does not acquire a later registration's name
  events. Record writes on the name's resolver that carry no resource are kept
  through the same binding, or through Project's attribution of the write to
  that registration's own records or to the records of a NameWrapper resource
  whose `NameWrapped` row recorded the lease; attribution to another
  registration of the same name does not admit the write, its count, or its
  cursor anchor. A handle is a public registration only when a
  registration grant backs it: directly, through a NameWrapper binding that
  recorded the lease it wrapped, or, for a wrapped name with no registrar
  lease, through the NameWrapper binding itself. A reservation, a
  registry-only resource, or a NameWrapper resource that wraps a lease selects
  no product rows; that also governs counts, summaries and cursor anchors. The served API currently exposes the old single-field shape, with
  `registration_id` only; the field change is the committed contract and lands
  in an immediate companion change. The
  slice-2 consumer activation contract maps each
  [migration correlation group's](glossary.md#migration-correlation-group)
  renewal-bridge and correlated registrar normalized rows to `renewal` and
  `expiry`, Graveyard claims to `release`, and controller membership changes
  to `permission`. Mapping is per normalized source event and resource anchor,
  not per transaction. A synchronized renewal emits one row for each real
  lifecycle owner/resource participating in renewal, so one synchronized
  renewal transaction contains two `renewal` rows — the renewal-bridge arm and
  the ENSv1-registrar arm — and two `expiry` rows. Reservation-scoped state
  changes remain reservation/resource facts and do not produce registration
  renewal rows; no synthetic collapsed renewal is created. The candidate
  `MigrationApplied` and
  `ContractDiscovered` kinds have no product event type. During slice 1, every
  correlation-dependent row carrying `consumer_visibility=candidate` is excluded
  even if its familiar event kind would otherwise map above or its source family
  is `ens_v2_registry_l1`. An event admitted independently by an existing family
  stays byte-for-byte activated and product-visible; its candidate
  `migration_event_associations` row is diagnostics-only and never changes the
  ordinary event's inclusion or multiplicity. Diagnostics may expose candidate
  rows and associations immediately. Schema-vocabulary,
  manifest-family, backfill, and interpretation activation alone change no
  `/v1/events` or product-history response; only slice 2 changes visibility.
  The shared visibility predicate runs before keyset pagination, page-size
  limiting, cursor construction, and product-type mapping. A V1 registry
  resolver log linked to both the registry resource retained for reads and a
  distinct control resource appears once in this product route: the
  additional row carrying the registry resource link is suppressed before cursor validation and
  pagination, while the control-resource row remains visible and raw
  diagnostics retain both normalized rows. Without a distinct control resource,
  the sole registry-resource row remains product-visible.
  (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L89-L94 @ ens_v1@91c966f)
  (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/registrar/AbstractETHRegistrar.sol:L84 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/registrar/AbstractETHRegistrar.sol:L91 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/registrar/AbstractETHRegistrar.sol:L92 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/registrar/AbstractETHRegistrar.sol:L93 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L212 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L226 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L227 @ ens_v2@a971bd64) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/registrar/ETHRenewerV1.sol:L106 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/registrar/ETHRenewerV1.sol:L107 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/registrar/ETHRenewerV1.sol:L111 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/registrar/ETHRenewerV1.sol:L132 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/registrar/ETHRenewerV1.sol:L134 @ ens_v2_sepolia_20260629@ccaeb58) (upstream: .refs/ens_v1/contracts/ethregistrar/IBaseRegistrar.sol:L8 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/ethregistrar/IBaseRegistrar.sol:L9 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/ethregistrar/IBaseRegistrar.sol:L20 @ ens_v1@91c966f) (upstream: .refs/ens_v2/contracts/src/migration/Graveyard.sol:L157 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/migration/Graveyard.sol:L160 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/migration/Graveyard.sol:L162 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/migration/Graveyard.sol:L169 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/ETHRenewerV1.json:L110-L158 @ ens_v2@a971bd64)
- Pagination behavior: keyset pagination, newest first unless `order=asc`.
  Product event-type filtering, including an explicit `type` set, runs before
  page construction, so `page_size`, `next_cursor`, and `has_more` describe
  product-visible events. The cursor is bound to the namespace, every anchor
  and block filter (including `resolver`), the direction, the `type` set, and
  the timestamp window. A nonterminal page contains `page_size` rows; only the
  terminal page may be shorter. `page.total_count` follows the shared history
  rule: populated (exact up to 10,000 rows, `null` beyond) when `name`,
  `registration_id`, `address`, or `resolver` anchors the read; opting into
  `include=total_count` removes that cap. It is always `null` for unanchored reads.
- Snapshot behavior: the first page reads at the served publication and binds
  its block as the walk's [cursor bound](glossary.md#cursor-bound). Every page
  reads at that bound, counts at it, and discloses it as `meta.as_of`, as the
  [shared history rule](#shared-route-rules) describes. A continuation keeps
  working while new blocks are published and returns `409 stale` requiring a
  restart when the bound no longer holds. A first page whose check after the
  read fails returns `409 stale` too and can simply be retried.
  Historical replay through `at` is not supported.
- Status semantics: no product-visible matches return `200` with empty `data`,
  `page.next_cursor=null`, and `page.has_more=false`. Filter and cursor-binding
  validation precede the first `redo_in_progress` check, so malformed requests
  retain `400 invalid_input`; a cursor issued before position-bound pagination
  then returns `409 stale` requiring a restart. The route checks
  `redo_in_progress` before deriving name, registration, or address anchors and
  rechecks the Interpret state inside the repeatable-read page transaction.
  After that transaction commits, the route resolves display names, revalidates
  the captured Interpret redo state, and repeats the whole check of the
  [shared history rule](#shared-route-rules) in a fresh transaction before
  returning data. The Interpret check is collection-wide: an active Interpret
  redo on any chain returns `409 stale` with no `data` page, regardless of the
  requested filters or namespace; an active Project redo on a chain in scope
  does too. The same response applies when a redo began between the checks. A
  well-formed cursor whose event anchor is gone returns `400 invalid_input` when
  the whole check passes and `409 stale` when it does not.
- Replaces (v1): `GET /v1/events` compact event search.

### `GET /v1/resolvers/{chain_id}/{address}`

- Method/path: `GET /v1/resolvers/{chain_id}/{address}`
- Tier: product read.
- Purpose: resolver overview for numeric `chain_id` and resolver `address`.
- Request parameters: path `chain_id`, `address`; query `include` for
  route-documented sections, `at`, `finality`, `cursor`, `page_size`.
- Response shape: `data` is a resolver overview in product vocabulary. The
  route includes route-local `bound_names: {data, page}`, a nested collection
  of record-shaped name rows that replaces resolver-based name filtering.
  `data.mirror` is present only when the resolver is a declared
  [ENSv1 mirror resolver](glossary.md#ensv1-mirror-resolver-ensv1_mirror_resolver):
  `{kind: "ensv1_registry", registry: {chain_id, address}}` names the ENSv1
  registry whose resolvers answer for the names bound to it; the overview's
  own `counts` and sections still describe this resolver's bindings, not the
  mirrored ENSv1 resolvers.
  Those rows use the same optional, atomic `wrapper_state` and `wrapper_fuses`
  contract as exact-name detail; the fields are present only for a current
  ENSv1 NameWrapper registration at the served projection timestamp.
  Once exact-name authority is activated, `bound_names` includes a logical
  name only under the resolver selected by its current registration. A
  migrated name is absent from its superseded ENSv1 resolver's listing; a
  mixed-history name with no provable current authority is omitted from all
  resolver listings rather than forced to `ok`. This nested collection adds no
  row-local mixed-authority status, so callers use name detail or batch lookup
  for the explicit coverage reason. A row classified as
  `current_authority_not_projected` is also absent from `bound_names`; retained
  resolver-pointer evidence does not establish listing membership. The
  exception is an ENSv2 TLD whose current
  [root-registry resolver pointer](glossary.md#root-registry-resolver-pointer)
  is its serving resource: like the ownerless ENSv1 or Basenames row below, it
  is eligible only where the resolver family's binding-enumeration capability
  is supported.
  An ownerless ENSv2 reservation is likewise absent, other than a root-registry
  TLD reservation served through the root-registry resolver pointer above: a
  retained reservation resolver or former-resource pointer is not a resolver
  selected by a current registration. This intentionally narrows ENSv2, which
  stores and returns a reservation resolver until expiry.
  (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L255-L258 @ ens_v2@a971bd64)
  (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L461-L478 @ ens_v2@a971bd64)
  A positively classified ownerless ENSv1 or Basenames registry row is
  different: its event-derived resolver
  binding is eligible for `bound_names` only where that resolver family's
  existing binding-enumeration capability is supported.
  `counts.nodes`, `counts.aliases`, `counts.links`, and `counts.role_holders`
  are total counts, while the corresponding `include=nodes`, `include=aliases`,
  `include=links`, and `include=roles` arrays are deterministic samples of at
  most 100 items. A
  count greater than the returned array length means that sample is truncated;
  omitted binding rows remain available through paginated name-side routes,
  and omitted permission rows remain available through permission routes.
  Complete alias mappings, record links, and per-registration permission rows
  are available through the `/aliases`, `/links`, and `/roles` collections
  below. Binding samples sort by name
  and stable identity, alias samples place current binding aliases before
  current alias-event rows and preserve each group’s stable order, link
  samples sort by numeric record ID and then namehash, and
  role-holder samples sort by address.
  `include=links` serves the record links of a resolver of the ENSv2
  record-ID generation (a proxy whose admitted implementation's manifest
  declares `Linked`; see [manifests](manifests.md#record-id-resolver-generation)).
  Such a resolver stores values per numeric record and binds names to records
  with `Linked(recordId, node, name)`; the latest link per node is current,
  record ID `0` unlinks the node, and a record linked at the empty-name node
  (`namehash("")`, all zeros) is the resolver's default record, which answers
  every node without a link of its own.
  (upstream: .refs/ens_v2/contracts/src/resolver/interfaces/IRecordResolver.sol:L32-L38 @ ens_v2@a971bd64)
  (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L379-L386 @ ens_v2@a971bd64)
  Each item is one node with a non-zero current record: `{record_id, namehash,
  default, namespace?, name?, display_name?, link_event}`. `record_id` is the
  decimal record ID as a string; `default` is `true` for the empty-name node;
  `name` and `display_name` are present only when bigname knows a name surface
  for that namehash — a resolver may link a node bigname has never observed as
  a name, which is then served by `namehash` alone; `link_event` is
  `{block_number, timestamp, transaction_hash, log_index}` of the current
  `Linked` observation. Nodes sharing a `record_id` share one record's values,
  so grouping items by `record_id` yields the names each record serves;
  `counts.linked_records` is the number of distinct records with at least one
  linked node. On a node-keyed resolver — a direct `public_resolver_v2` or
  ENSv1-mirror declaration, or any ENSv1 or Basenames resolver — the section
  is unsupported with reason `record_links_not_applicable` and contributes no
  counts; on a resolver whose enumeration is unsupported it carries that
  resolver's enumeration reason. Example item:

  ```json
  {
    "record_id": "7",
    "namehash": "0x5f9c…",
    "default": false,
    "namespace": "ens",
    "name": "shared.eth",
    "display_name": "shared.eth",
    "link_event": {
      "block_number": 11710004,
      "timestamp": "2026-09-16T10:04:12Z",
      "transaction_hash": "0xlink…",
      "log_index": 1
    }
  }
  ```
  `include=roles` items are `{address, registration_count, permission_count,
  powers, grant_event?}`. `registration_count` is the number of distinct registrations with
  resolver-scoped permission rows for the role address. `permission_count` is
  the number of those permission rows, not the number of powers expanded from
  them; a row granting multiple powers counts once. The former embedded
  `registration_ids` list is omitted because it was itself unbounded, and
  permission rows remain queryable through `GET /v1/permissions` using the
  returned addresses. `grant_event`, when present, is the provenance of the
  event that granted the role: `{block_number, timestamp, transaction_hash,
  log_index}` of the earliest canonical `permission`-type event, among the
  events recorded as provenance on the holder's resolver-scoped permission
  rows, whose subject is the holder (ties broken by `log_index`). It is read
  from current permission rows and normalized events, not from the overview
  summary, so it is omitted — never `null` — when the holder has no
  resolver-scoped permission row, when the row's provenance names no
  permission event for that subject, or when the granting event is no longer
  canonical. Example item:

  ```json
  {
    "address": "0x0000000000000000000000000000000000000abc",
    "registration_count": 1,
    "permission_count": 1,
    "powers": ["set_records", "set_resolver"],
    "grant_event": {
      "block_number": 150,
      "timestamp": "2023-11-14T22:15:50Z",
      "transaction_hash": "0xgrant150tx",
      "log_index": 3
    }
  }
  ```

  The resolver arrays are not independently pageable. This
  bounded-sample contract is
  the consumer-visible resolver-overview shape change delivered with
  [issue #401](https://github.com/ensdomains/bigname/issues/401); clients must
  not interpret an included array as exhaustive when its total count is
  larger.
  `include=aliases` exposes binding rows as `{namespace, name, display_name,
  namehash}` and resolver alias rows as `{namespace, from_name, to_name,
  from_display_name?, to_display_name?, state, resolver: {chain_id, address},
  to_registration_id?}`. `to_name` is `null` when the latest alias state is
  `removed` or `unknown`. `include=events` exposes `{count, by_type}` where
  `by_type` aggregates raw resolver event kinds that map to the same friendly
  `type` vocabulary as `GET /v1/events`; raw kinds without a product event type
  remain included in `count` but are excluded from `by_type`.
- Pagination behavior: standard collection pagination applies to the
  nested `bound_names.page` object. The top-level response has no `page`.
- Snapshot behavior: the resolver overview and bound names read
  `bigname_phase` projections from one completed projection-phase generation. A row
  target may precede the selected position when the row was unchanged by later
  incremental publications; it may not be ahead, and a same-height target must
  match the selected hash. The projection-phase generation is revalidated after the
  read, and an invalid target or changed generation returns `409 stale`. A
  publication change during a continuation returns `409 stale` and requires
  restarting without a cursor; a request without a cursor whose publication
  changes during the read returns `409 stale` too and can simply be retried.
- Status semantics: only a request without `at` and with `finality=latest`
  applies the latest served-head Interpret-redo check and returns retryable `409
  stale` while its selected chain is undergoing a redo. Historical `at` reads
  and requests with `finality=safe|finalized` retain their existing generation
  validation without that redo check.
- An otherwise valid current/latest resolver with no overview
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

### `GET /v1/resolvers/{chain_id}/{address}/aliases`, `/links`, and `/roles`

- Tier: product read. These collections make the supported resolver overview
  tables fully pageable without enlarging its bounded previews.
- Parameters: numeric `chain_id`, resolver `address`; `at`, `finality`, `cursor`,
  `page_size` (default 50, maximum 200). Other query parameters are rejected.
- `/aliases` returns the same two row shapes as the overview aliases: current
  alias bindings followed by current active alias-event mappings. The binding
  group sorts by stable name identity; the event group sorts by stable alias
  identity, so equal display names cannot skip or duplicate entries. Counts
  cover both groups. This is a complete enumeration of the supported indexed
  mappings, not a claim to discover arbitrary custom resolver behavior.
- `/links` returns the overview's record-link rows — one per node whose
  current link names a non-zero record, in numeric `record_id` then
  `namehash` order, the default record's empty-name node included — read from
  the latest activated canonical `Linked` observation per node at or below
  the selected height, with names attached from the canonical name surfaces
  at that height. `page.total_count` counts those rows. It is unsupported,
  with the same reason as the overview section, on a node-keyed resolver.
- `/roles` returns one `{address, registration_id, name?, powers, grant_event?,
  record_resource?}` row
  for each current resolver-scoped permission row with at least one power.
  Its `registration_id` follows the same published permission-handle mapping as
  `GET /v1/permissions`, including before a readable name exists. Following that
  handle selects the same grant. Evidence above the captured publication cannot
  change the handle between pages; an unproven lease leaves the resource audit
  handle intact.
  `record_resource` follows the `GET /v1/permissions` contract: on a record-ID
  resolver it names the record a holder's argument-scoped grant is about.
  Grouping rows by `record_resource.hash` therefore lists the records some
  current holder may set — not every resource the resolver has ever named:
  a resource whose setter grants were all revoked, or whose surviving rows
  hold only `link` or `admin_set_*` powers, contributes no hash.
  Rows sort by address and registration ID; multiple registrations for one
  holder remain separate. `name` is present when the registration has a current
  readable name. `grant_event` follows the overview provenance shape,
  but is selected only from that individual registration's permission row.
  Revoked/empty grants, unrelated resolver scopes, and noncanonical resources
  are excluded. `page.total_count` counts these rows, whereas the overview's
  `counts.role_holders` counts distinct addresses.
- Both routes use the normal `{data, page, meta}` envelope. Supported reads
  report an exact `page.total_count`, including zero, from the same database
  statement and predicates as the page. Unsupported enumeration returns an
  empty page with `total_count: null`, explicit unsupported completeness, and
  a reason; it never claims that a resolver has no mappings or holders.
- Pagination is bound to the resolver, chain, collection, selected snapshot,
  and completed projection generation. A cursor automatically selects its
  snapshot when `at` is omitted. A different route or explicit snapshot is
  `400 invalid_input`; a generation no longer available is `409 stale` and the
  client must restart. These routes read current projections, so they do not
  synthesize historical permission/binding tables. A same-height rebuild also
  invalidates prior cursors. The generation is checked again after reading. A
  publication change during a continuation returns `409 stale` and requires
  restarting without a cursor; a request without a cursor whose publication
  changes during the read returns `409 stale` too and can simply be retried.
- Alias and link events come from activated canonical normalized events
  bounded to the selected height; bindings and permissions come from current
  projections using their existing canonical-lineage predicates. Resolver classification
  and enumeration support remain the authority for whether either collection
  can make an indexed completeness claim. No manifest coverage is widened.

### `GET /v1/registries/{chain_id}/{address}`

- Method/path: `GET /v1/registries/{chain_id}/{address}`
- Tier: product read.
- Purpose: one ENSv2 registry contract by numeric `chain_id` and contract
  `address`: the name it serves, its parent registry, when it was created, how
  many labels it holds, and the names that point at it.
- Request parameters: path `chain_id`, `address`; query `include=counts`,
  `at`, `finality`, and `cursor`/`page_size` for the nested `referenced_by`
  collection.
- Response shape: `data` is `{chain_id, address, name, parent_registry,
  created_block_number, created_at, created_transaction_hash, created_basis,
  counts, referenced_by}`. `address` is the lowercase hex form. A registry is
  known when it announced itself with the ENSv2 `RegistryCreated` event, when
  any ENSv2 `SubregistryUpdated` has ever pointed a name at it, or when an
  ENSv2 manifest declares it as a `root_registry` or `registry` contract; the
  ENSv1 and Basenames registries are not registries in this sense.
  `name` is `{name, display_name, namespace, namehash}` for the earliest name
  whose current subregistry pointer targets this contract, and `null` when no
  current pointer does (the root registry, or a registry that was announced or
  declared but never linked). `parent_registry` is `{chain_id, address}` of
  the registry that emitted that pointer, and `null` whenever `name` is
  `null`. The creation fields describe the contract's first observation and
  `created_basis` says which event defines it: `registry_created` is the
  contract's own `RegistryCreated` announcement; `subregistry_pointer` is the
  earliest `SubregistryUpdated` that pointed a name at it, used when the
  contract never announced itself; `declared` is a manifest-declared registry
  with no observed creation, whose `created_block_number` is its configured
  start block, `created_at` is that block's timestamp when the block is known,
  and `created_transaction_hash` is `null`. Declaration evidence applies only
  within its inclusive configured block interval; finite retirement preserves
  earlier reads, while a retracted declaration with no retained interval does not.
  Replacing a manifest's current declaration children does not erase that evidence:
  finite retired intervals use the source manifest's retained active
  `SourceManifestUpdated` payloads recorded during that admission to establish the
  registry role. The retained declaration identity and address must match; later re-admission neither changes
  an older interval's role nor fills the gap between admissions.
  `counts.labels` is the exact
  number of labels the registry currently holds (the rows of the labels route
  below); for a current selection it is `0` when `name` is `null`. `counts.events` is present only with
  `include=counts` and counts the product-visible events emitted by the
  contract — the same rows `GET /v1/events?contract_address=` serves — because
  it reads every event of the contract rather than a projected total.
  `counts.roles`, also present with `include=counts`, is the exact number of
  observed nonzero declared role assignments across the registry's root and
  label resources. One account on two resources counts twice; several role bits
  on one account/resource count once. Counts fold canonical `EACRolesChanged`
  observations through the selected block, remove revoked assignments and
  exclude older observed resource versions for the same label. These are
  declared assignments, not a claim about currently exercisable permissions.
  This preserves the reference indexer's assignment-versus-holder distinction.
  (upstream: .refs/zigens/src/api/resolvers/admin.zig:L1150 @ zigens@77d106e9)
  (upstream: .refs/zigens/src/storage/roles.zig:L171 @ zigens@77d106e9)
  (upstream: .refs/zigens/src/storage/roles.zig:L505 @ zigens@77d106e9)
  `referenced_by` is a nested `{data, page}` collection of every name whose
  current subregistry pointer targets this contract, each `{name,
  display_name, namespace, namehash}`, sorted by display name. It usually
  holds exactly the served `name`; it is empty when `name` is `null`.
- Pagination behavior: standard collection pagination applies to the nested
  `referenced_by.page` object; its cursor binds the chain and registry. The
  top-level response has no `page`. Continuations bind both the selected block
  and the current publication; a publication change returns `409 stale` and
  requires restarting without a cursor. A request without a cursor whose
  publication changes during the read returns `409 stale` too and can simply be
  retried.
- Snapshot behavior: the route selects the chain's served position like the
  resolver overview and reports `meta.as_of` and `meta.as_of_token`. The
  creation, pointer, and event-count evidence is bounded to that position, so
  an `at` or `finality` selector shows the registry as it stood then.
  `counts.labels` reads the current child collection and is `null` for a
  historical selection that differs from the current published position.
  Current reads revalidate their publication before returning.
- Status semantics: an unknown registry returns `404 not_found` at every
  selector, because the bounded read proves absence at the selected position.
  Malformed `chain_id` or `address` and an `include` value other than `counts`
  return `400 invalid_input`.
- Replaces (v1): none; new in F1.

### `GET /v1/registries/{chain_id}/{address}/labels`

- Method/path: `GET /v1/registries/{chain_id}/{address}/labels`
- Tier: product read.
- Purpose: the labels one ENSv2 registry currently holds.
- Request parameters: path `chain_id`, `address`; query `include=counts`,
  `cursor`, `page_size`, and optional `finality=latest`. `at` and historical
  `finality` values are rejected by the shared latest-state collection rule.
- Response shape: `data` is an array of rows using the `GET
  /v1/names/{name}/subnames` shape, including `subregistry` and the
  `include=counts` `subname_count`, plus `role_holder_count` with `include=counts`.
  The latter is the exact number of distinct accounts with a nonzero declared
  assignment on the label's current registration resource in this registry;
  root roles, resolver roles, older resource versions and revoked assignments
  are excluded. Several role bits on one account count once. A label without
  any observed matching assignment has a count of zero. The total describes
  indexed declared assignments, not inherited effective permissions.
  (upstream: .refs/zigens/src/api/builders/impl.zig:L2042 @ zigens@77d106e9)
  (upstream: .refs/zigens/src/storage/roles.zig:L567 @ zigens@77d106e9)
  A label is a direct subname of the name
  the registry serves whose ENSv2 registration the registry itself emitted;
  a child of that name registered by another contract is not a label of this
  registry. A registry that serves no name — the root registry, or one never
  linked from a name — holds no servable labels here and returns an empty
  page: the child collection is projected under a parent name, and the root
  name has no child projection.
- Pagination behavior: standard collection pagination by `display_name`
  ascending. `page.total_count` is the exact label count, the same number as
  `counts.labels` on the registry route. The cursor binds the chain and
  registry.
- Snapshot behavior: rows and totals come from one revalidated current
  publication, reported in `meta.as_of`. Cursors bind that publication; if it
  changes, return `409 stale` and restart pagination without the cursor. A first
  page whose publication changes during the read returns `409 stale` too and can
  simply be retried.
- Status semantics: an unknown registry returns `404 not_found`; a known
  registry with no labels returns `200` with empty `data`. Malformed
  `chain_id`, `address`, `include`, or cursor values return `400 invalid_input`.
- Replaces (v1): none; new in F1.

### `GET /v1/namespaces/{namespace}`

- Method/path: `GET /v1/namespaces/{namespace}`
- Tier: product read.
- Purpose: namespace metadata and supported-capability summary in product
  vocabulary.
- Request parameters: path `namespace`.
- Response shape: `data` is `{namespace, capabilities, networks}`.
  `capabilities` is a product-facing object keyed by capability name; each
  value is `{completeness, unsupported_reason?, chains?}` using the common
  completeness vocabulary. `networks` is an array of `{network, chain_id?}`
  entries when the namespace has public chain mappings. Control-plane metadata
  omits `meta.as_of` and `meta.as_of_token`. Under the Sepolia deployment
  profile, ENS `name_profile` completeness is `partial`: the ENSv2 registrar
  declaration is supported while the admitted ENSv1 registrar declaration is
  shadow because registrar-controller label coverage is absent.
- Capabilities from manifest flags: `subnames`, `name_profile`, and
  `name_history` aggregate the active manifests' capability flags (`full` when
  every declaring manifest is supported, `partial` when some are, otherwise
  `unsupported` with `unsupported_reason=not_supported_for_namespace`). They
  carry no `chains` object.
- Verified capabilities per chain: `verified_records` and
  `verified_primary_name` describe what this deployment's verified routes will
  execute, decided per declared network and reported under `chains`, keyed by
  the numeric chain id (`"1"`, `"11155111"`, `"8453"`). A chain entry is
  `{completeness: full}` when the lookup route table has an execution
  entrypoint for the namespace on that chain (ENS: Ethereum Mainnet or Sepolia;
  Basenames: Base, executing through the Mainnet L1 Resolver), an active or,
  where the route admits it, shadow manifest declares that entrypoint with a
  `verified_resolution` flag the route accepts (ENS accepts `shadow`; Basenames
  requires `supported` on manifest version 2), for `verified_primary_name` the
  same chain also has an active `ens_v1_registry_l1` manifest, and
  `BIGNAME_API_CHAIN_RPC_URLS` names a provider for the execution chain.
  Otherwise the entry is `unsupported` with one of
  `not_supported_for_namespace` (the capability never applies, such as
  `verified_primary_name` on Basenames), `not_supported_for_chain` (no route
  entrypoint for that chain), `execution_entrypoint_not_declared` (no usable
  execution manifest, or no registry manifest for primary names), or
  `execution_provider_not_configured` (the operator has not configured the
  provider). The top-level `completeness` is `full` when every chain is `full`,
  `partial` when some are, and `unsupported` when none is; an `unsupported`
  top level repeats the chains' shared reason, `not_supported_for_chain` when
  the per-chain reasons differ, and `not_supported_for_namespace` when the
  namespace declares no network. Per-name support classes on the routes
  themselves (topology class, authority arm) still apply; this is
  deployment-level support. Example under the Sepolia profile with a Sepolia
  provider configured:

  ```json
  "verified_primary_name": {
    "completeness": "full",
    "chains": { "11155111": { "completeness": "full" } }
  }
  ```
- Pagination behavior: none.
- Status semantics: unsupported public namespaces return `404 not_found`.
- Replaces (v1): `GET /v1/namespaces/{namespace}`. Operational namespace
  internals move to the diagnostics namespace route documented below.

## Tier 3: Diagnostics

Diagnostics are the only routes that may carry pipeline vocabulary. Product
route vocabulary restrictions do not apply to the diagnostic payloads below.

Diagnostic snapshot rules:

- `/v1/diagnostics/names/{name}/coverage`,
  `/v1/diagnostics/names/{name}/binding`,
  `/v1/diagnostics/names/{name}/authority`,
  and `/v1/diagnostics/names/{name}/records` accept `at` and `finality` and
  carry `meta.as_of`/`meta.as_of_token` because they explain one selected
  snapshot.
- `/v1/diagnostics/events` follows the shared latest-state collection rule: it
  omits snapshot metadata and rejects `at` and historical `finality`.
- `/v1/diagnostics/namespaces/{namespace}/manifests` omits `meta.as_of` and
  `meta.as_of_token`; it is control-plane metadata.

`GET /v1/diagnostics/names/{name}/execution` is removed. The persisted-explain
capability it served is retired with the C2 cutover, not deferred to a later
slice: the execution traces, steps, and cache outcomes it read no longer exist
and no replacement route is planned. Verified resolution now runs per request,
so there is no persisted artifact to explain. See
[`execution.md`](execution.md#removed-legacy-artifacts).

### `GET /v1/diagnostics/names/{name}/coverage`

- Method/path: `GET /v1/diagnostics/names/{name}/coverage`
- Tier: diagnostics.
- Purpose: full coverage taxonomy.
- Request parameters: path `name`; query `namespace`, `at`, `finality`.
- Response shape: `data` includes `exhaustiveness`, `enumeration_basis`,
  `source_classes_considered`, and `unsupported_reason` detail.
- Pagination behavior: none.
- Status semantics: missing names return `404 not_found`; unsupported coverage
  classes return diagnostic detail rather than product simplification.
- Replaces (v1): `GET /v1/coverage/{namespace}/{name}`.

### `GET /v1/diagnostics/names/{name}/binding`

- Method/path: `GET /v1/diagnostics/names/{name}/binding`
- Tier: diagnostics.
- Purpose: surface-binding explain.
- Request parameters: path `name`; query `namespace`, `at`, `finality`.
- Response shape: `data` includes binding ids, binding kind, and anchors.
- Pagination behavior: none.
- Status semantics: missing names return `404 not_found`.
- Replaces (v1): `GET /v1/explain/names/{namespace}/{name}/surface-binding`.

### `GET /v1/diagnostics/names/{name}/authority`

- Method/path: `GET /v1/diagnostics/names/{name}/authority`
- Tier: diagnostics.
- Purpose: authority/control explain.
- Request parameters: path `name`; query `namespace`, `at`, `finality`.
- Response shape: `data` includes token lineage, control vectors, and
  permission lineage.
- Pagination behavior: none.
- Status semantics: missing names return `404 not_found`.
- Replaces (v1): `GET /v1/explain/names/{namespace}/{name}/authority-control`.

### `GET /v1/diagnostics/names/{name}/records`

- Method/path: `GET /v1/diagnostics/names/{name}/records`
- Tier: diagnostics.
- Purpose: record inventory and cache internals.
- Request parameters: path `name`; query `namespace`, `at`, `finality`, and
  optional `keys`. `keys` uses the same record-key grammar as
  `/v1/names/{name}/records`: `addr:<coin_type>`, `text:<key>`, `avatar`, and
  `contenthash`.
- Response shape: `data` is
  `{record_inventory, record_cache, value_sources, comparison,
  comparison_explicit_gaps?}`.
  `record_inventory` is
  `{record_version_boundary, enumeration_basis, selectors, explicit_gaps,
  unsupported_families, last_change}` using the existing diagnostic selector
  row fields `record_key`, `record_family`, `selector_key`, and `cacheable`.
  An `unsupported_families` item for `abi` says that ABI record values are
  not served as records; it does not mean ABI content types are unavailable,
  which `include=inventory` on the product routes reports on its own terms.
  `record_cache` is `{record_version_boundary, entries}` where each entry is
  `{record_key, record_family, selector_key, status, value?,
  unsupported_reason?, failure_reason?}`. `value_sources` summarizes the
  indexed or verified origin per key. `comparison` is keyed by `record_key` and
  carries side-by-side `{indexed, verified}` record answers for the former
  `mode=both` workflow. Without `keys`, `comparison` defaults to the first 16
  inventory-derived supported record keys in deterministic order. The indexed
  `record_inventory` and `record_cache` sections remain complete, including
  retained audit state that product name routes omit when no current
  registration exists. When more
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
  `record_cache.entries` is an internal projected representation, not the
  product-route answer envelope. A retained contenthash entry uses
  `status="success"` with
  `value={"encoding":"hex","bytes":"0x..."}`; a retained address entry uses
  `status="success"` with scalar `value="0x..."`. A cleared contenthash or
  address entry uses `status="not_found"` and omits `value`. Normalized fields
  such as `contenthash_hex` and `address_bytes_hex` are not product
  record-answer fields; they can appear inside the complete normalized
  `after_state` returned by the raw event diagnostics route below.
- Pagination behavior: none.
- Status semantics: missing names return `404 not_found`.
- Replaces (v1): record inventory/cache diagnostics formerly embedded in
  `GET /v1/profiles/names/{name}` and
  `GET /v1/names/{namespace}/{name}/records`, including the former
  `mode=both` comparison.

### `GET /v1/diagnostics/namespaces/{namespace}/manifests`

- Method/path: `GET /v1/diagnostics/namespaces/{namespace}/manifests`
- Tier: diagnostics.
- Purpose: active manifest versions, source families, deployment epochs, and
  capability flags.
- Request parameters: path `namespace`.
- Response shape: `data` is the active manifest summary in diagnostics
  vocabulary.
- Pagination behavior: none.
- Status semantics: unsupported public namespaces return `404 not_found`.
- Replaces (v1): `GET /v1/manifests/{namespace}`.

### `GET /v1/diagnostics/events`

- Method/path: `GET /v1/diagnostics/events`
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
  canonicality_state, before_state?, after_state?, provenance, coverage}`. The
  slice-1 diagnostics extension adds `consumer_visibility`,
  `migration_correlation_ids`, and `migration_associations?`; each
  `migration_associations` entry is
  `{migration_correlation_ids, correlation_kind, consumer_visibility}`. A
  correlation-dependent normalized row reports its marker in the top-level
  fields. An independently admitted ordinary row reports top-level
  `consumer_visibility=activated` and an empty ID set; its separate candidate or
  activated correlation relationships appear only in `migration_associations`.
  `migration_associations` is raw diagnostic evidence. The route is
  canonical-only: a normalized-event row is returned only while its own
  `canonicality_state` and the `chain_lineage` row for its block are both
  readable, so an event on a block that head publication has orphaned is absent
  from the response even before an Interpret redo deletes it. The lookup then
  attaches every association with the same `event_identity` and applies no
  lineage predicate of its own; because every `event_identity` embeds
  fork-distinct evidence, those associations sit on the returned event's own,
  still-readable block, and a retained losing-fork association whose event a
  redo deleted attaches to no event. The top-level `canonicality_state`
  describes the returned row. Each attached association reports only the
  `consumer_visibility` Interpret stamped when it last derived the row; the
  association's own stored `canonicality_state` is not part of the response.
  Association presence, or a `candidate` visibility, is evidence of a
  derivation, not an assertion of a current or activated correlation.
  When interpretation links one V1 registry resolver log to both the registry
  resource retained for reads and a distinct control resource, diagnostics
  returns both normalized rows and permits cursors anchored to either row;
  product event and name-history routes return that on-chain log once through
  the control-resource row. Without a distinct control resource, the sole
  registry-resource row remains product-visible.
  (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L89-L94 @ ens_v1@91c966f)
  When `address` is present, diagnostics derives its name/resource anchor set
  from both activated and candidate address-relation evidence. Candidate
  evidence never contributes anchors to `/v1/events` or product history routes.
  A behavior-preserving full re-walk may assign a different numeric
  `normalized_event_id` to a pre-existing row while its `event_identity` and
  pre-existing semantic fields remain stable; the numeric ID change and the
  candidate fields are explicit diagnostic-only deltas. The intentional
  #348/#529 interpreter changes are the documented exception above:
  `RecordChanged` and
  `RecordVersionChanged` can gain `logical_name_id`, keep `resource_id=null`,
  update `raw_fact_ref.interpreter_state_key`, and rethread `before_state` on
  the same resolver event identity.
- Pagination behavior: standard collection pagination.
- Snapshot behavior: diagnostic event rows come from current state, but their
  `migration_associations` are the raw lineage evidence described above, not
  assertions of current correlation. The response omits `meta.as_of` and
  `meta.as_of_token`, and its cursor carries no snapshot validity claim. True
  as-of/finality row-bounding is deferred to the revision-bound storage
  follow-up.
- Status semantics: no matching rows returns `200` with empty `data`.
- Replaces (v1): `view=full` on `GET /v1/history/names/{namespace}/{name}`,
  `GET /v1/history/resources/{resource_id}`, and
  `GET /v1/history/addresses/{address}`.
