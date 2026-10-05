# Consumer Capabilities

This document maps the consumer-facing capabilities served by the bigname API.
Wire format and route details live in [`api-v1.md`](api-v1.md) and
[`api-v1-routes.md`](api-v1-routes.md).

## Served route sets

| Set | Routes | Intended use |
| --- | --- | --- |
| Lookup | `POST /v1/lookup`, `GET /v1/status` | Batched name/address lookup and indexing readiness. |
| Product reads | `/v1/names/*`, `/v1/addresses/*`, `/v1/permissions`, `/v1/search`, `/v1/events`, `/v1/resolvers/*`, `/v1/namespaces/*` | Name, record, address, permission, event, resolver, and namespace reads. |
| Diagnostics | `/v1/diagnostics/*` | Coverage, binding, authority, record, manifest, and event inspection. |
| Operator health | `GET /healthz` | API process, opaque running-database-instance identity, and phase-runner heartbeat readiness. This is not a product route. |

The v1 REST surface has been removed. In particular,
`POST /v1/identity:lookup` no longer serves the native identity capability.
`POST /v1/lookup` owns batched forward and reverse lookup with the v2 envelope;
it does not preserve the deleted v1 DTOs. The GraphQL compatibility surface
(`POST /graphql`) has also been removed; `/graphql` answers like any unknown route.

All top-level v2 collections use the standard `page` object. Latest-state
collections do not claim a frozen snapshot; point-in-time behavior is limited
to the routes and selectors documented in [`api-v1-routes.md`](api-v1-routes.md).

## Capability mapping

| Capability | Route owner | Notes |
| --- | --- | --- |
| Batched forward and reverse lookup | `POST /v1/lookup` | `profile=feed` is the field-budgeted path, carrying identity, `status`, expiry and grace (`expires_at`, `expires_at_reason`, `grace_ends_at`, `ens_v1`); `profile=detail` returns the documented full record shape, including the [grouped `records`](api-v1-routes.md#grouped-name-profile-records) of name detail (each category's key list beside its value map, plus the observed ABI content types, read against the same published inventory row and reported as `abi_observations_stale` if Project replaced that row mid-request), so a caller holding many names reads their record keys and values in one request instead of one records read per name. |
| [Resolver profile](glossary.md#resolver-profile) replay for the [ENSv1→ENSv2 migration](glossary.md#ensv1ensv2-migration) | `POST /v1/lookup` with `profile=detail` | Serves the resolver-profile key read that the ENS manager's ENSv1→ENSv2 migration flow needs before it replays a name's resolver profile: coin types in `records.seen_addresses`, text keys in `records.seen_texts`, whether a content hash or forward name was written in `records.seen_singletons` (with their values in `records.contenthash` and `records.name` when known), and the ABI content types in `records.seen_abis`, for up to 1,000 names per request. The ABI list names content types whose writes the index observed on the selected resolver storage; the caller still reads each ABI's bytes on chain and drops an empty answer, because a removal emits the same event as a set (upstream: .refs/ens_v1/contracts/resolvers/profiles/ABIResolver.sol:L10-L26 @ ens_v1@91c966f). An omitted `seen_abis` with `abi_unsupported_reason` means the index cannot list them for that name, and the caller must not treat it as a resolver profile without ABI records. ABI records remain outside the record-key grammar, and ABI bytes are not served. |
| Indexing readiness | `GET /v1/status` | Per-chain projection progress, stored head, indexing-process liveness, network-head readiness, and required Sepolia completed-Ingest state plus [verification-level evidence](glossary.md#verification-level). |
| Exact name profile | `GET /v1/names/{name}` | Indexed or verified name and record fields, plus [expiry-effective](glossary.md#expiry-effective-namewrapper-fuse-word) ENSv1 NameWrapper lifecycle and fuse data when backed, and the NameWrapper entry's own expiry whenever the name has an entry, lapsed included, subject to the route's source rules. |
| Resolver records | `GET /v1/names/{name}/records` | Per-key record answers for the requested `keys` or, when `keys` is omitted, for the inventory-derived default key set (at most 200 keys), plus inventory metadata. The grouped `records` object is on name detail and `profile=detail` lookup, not this route. |
| Direct subnames | `GET /v1/names/{name}/subnames` | Latest-state direct-subname collection. |
| Names by expiry | `GET /v1/names` | One namespace's current names whose registration expiry falls in a bounded window or 1–32 disjoint repeated `expires_window=after..before` windows, released registrations included, for expiry sweeps and renewal notices. Repeated-window rows carry their original input's zero-based `expires_window_index`; one globally sorted page shares a snapshot and has at most 200 rows. Each row carries the `authority` its name detail serves; `authority=` keeps the listed authorities and `parent=eth` keeps the `<label>.eth` names, excluding deeper subnames. Each continuation reads the current publication using a position-only cursor, and `page.total_count` is null. See [route rules](api-v1-routes.md#get-v1names). |
| Name history | `GET /v1/names/{name}/history` | Name, registration, or combined history scope. With `include=child_registrations` the same paged collection also holds the registrations of the name's direct children, in one order under one cursor, each row marked with `subject`; see [direct child registrations](glossary.md#direct-child-registration). This serves a portal timeline that lists child registrations inside the parent's history, which today merges a second, unpaged indexer query for the parent's subdomains client side. The option lists every grant row of every direct child, released children and registries the parent has since unlinked included; it refuses `eth` and `base.eth`, and it does not list ENSv1 or Basenames registry subnames, which have no registration row. |
| Names by address | `GET /v1/addresses/{address}/names` | Owner, manager, and ENSv2 role-holder relations with optional expansions; `owner` is the token holder of a name with a token and otherwise its registry owner (see the [naming dictionary](api-v1.md#naming-dictionary)). `parent=<name>` keeps the names one label below it, so `relation=owner&parent=eth&dedupe=registration&include=total_count` gives in `page.total_count` the `.eth` registrations the address holds, without the subnames `owner` also lists; it covers the registrations the route lists, which miss a `.eth` name with no [name surface](glossary.md#surface-name-surface) granted by `registerOnly` onto another address's registry record and every Basenames registry child without a name row ([route docs](api-v1-routes.md#get-v1addressesaddressnames)). Inline role summaries allow 1,000 total grant rows; overflow returns 422. Each row exposes `permission_resource_id` for existing cursor-paginated permissions reads, including when the include is omitted. It is the handle `GET /v1/permissions?registration_id=` resolves to the row's permission resource: the same value as name detail's `registration_id` while the name retains a current registration identity, including unsupported name coverage, so for a wrapped `.eth` name it is the BaseRegistrar lease and not the NameWrapper resource; otherwise it is the resource itself. (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L240-L305 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L390-L414 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L118-L152 @ ens_v1@91c966f) |
| Former names by address | `GET /v1/addresses/{address}/names?relation=former_owner` | Released names whose `lapsed_registration.owner` is the address, for renewal reminders; a new registration removes the name from this list, and a name still in its grace period is listed under `owner` instead. This historical-holder relation stands alone, is excluded from `any`, and asserts no current owner, manager, or permission. Optional expiry bounds filter the ended registration; only `sort=expires_at` and name deduplication are supported. Authority, migration, name-prefix, coin-type, and expansion filters are rejected; `parent` applies. Each continuation reads the current publication using a position-only cursor, and `page.total_count` is null. It is not supported by address history or lookup. See [address-name route rules](api-v1-routes.md#get-v1addressesaddressnames); membership does not establish that a name can still be renewed. |
| Names resolving to an address | `GET /v1/addresses/{address}/names?relation=resolves_to` | The names whose current `addr:<coin_type>` resolver record holds the address, one coin type per read (default `60`). With `coin_type=evm` one read covers every EVM coin type (`60`, and `2147483648` through `4294967295`) (upstream: .refs/ens_v1/contracts/utils/ENSIP19.sol:L9-L38 @ ens_v1@91c966f), so an address page can list names set only for another chain, such as Base, without knowing the chain in advance. Each row is one name with `resolutions`, the coin types whose record matched, which a client can render as chain badges; `2147483648` is the ENSIP-19 default record (upstream: .refs/ens_v1/contracts/utils/ENSIP19.sol:L10 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L68-L85 @ ens_v1@91c966f). The list shows matches only, not every coin type the resolver has records for, and it is paginated like the other address-name reads with `total_count` null. A row carries at most 100 matched coin types; when a returned row matched more, the request returns 422 and the address is read one decimal coin type at a time instead. Non-EVM coin types, and legacy SLIP-44 coin types of EVM-compatible chains such as `61`, are read one coin type at a time. See the [known divergence](upstream.md#resolves-to-matched-coin-types) from the subgraph's `resolvedAddress` (upstream: .refs/ens_subgraph/schema.graphql:L16-L17 @ ens_subgraph@723f1b6) (upstream: .refs/ens_subgraph/src/resolver.ts:L45-L48 @ ens_subgraph@723f1b6) (upstream: .refs/ens_subgraph/src/ensRegistry.ts:L194-L199 @ ens_subgraph@723f1b6) (upstream: .refs/ens_subgraph/src/resolver.ts:L219-L222 @ ens_subgraph@723f1b6) and `coinTypes` (upstream: .refs/ens_subgraph/schema.graphql:L294-L295 @ ens_subgraph@723f1b6) (upstream: .refs/ens_subgraph/src/resolver.ts:L59-L79 @ ens_subgraph@723f1b6). |
| Primary name | `GET /v1/addresses/{address}/primary-name` | Indexed tuples and verified ENS coin-type 60 lookup as documented. |
| Address history | `GET /v1/addresses/{address}/history` | Latest-state address-anchored event history. Each page admits and selects its rows on one publication; continuations remain position-based history walks. Pages return `total_count: null` by default; `include=total_count` requests an exact total and may cost substantially more than the page. |
| Permission holders | `GET /v1/permissions` | Known current direct permission rows plus effective ENSv1 and Basenames registry operators that apply to each resource, and the ENSv2 registry operators a token's current owner approved, with that owner's token roles while the entry has not expired. A `name` or `registration_id` read of an ENSv2 registration also lists the registry's root holders; bigname serves no rows for an expired registration (see [permission reads](api-v1-routes.md#get-v1permissions)), and the root `renew` holders who can revive it are on the `registry` read. (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L350-L352 @ ens_v2_sepolia_20261001@07e55a05) (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L243-L258 @ ens_v2_sepolia_20261001@07e55a05) (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L643-L654 @ ens_v2_sepolia_20261001@07e55a05) `registry=<chain_id>:<address>` lists the current holders of an ENSv2 registry's [root resource](glossary.md#registry-root-resource), as `root` rows naming the registry; the holders of a registry admitted by discovery rather than declared in a manifest, and the rows of its registrations, are reported as partial (`ens_v2_registry_operators`). Registry `ApprovalForAll` is served for `address`, `name`, and `registration_id` filters and role-summary expansion. ENSv1 NameWrapper holders, operators, and per-token delegates are direct rows. Surfaces not yet listed are named in `meta.unlisted_permission_surfaces` (`ens_v2_registry_operators`, `registrar_approvals`, `resolver_approvals`, `wrapper_parent_control`) beside `unsupported_reason=permissions_partially_listed`, so coverage stays request-relative partial even for zero rows; the list shrinks as later parts of issue #605 add these surfaces. A registration of a manifest-declared ENSv2 registry reports only `resolver_approvals`; it has no BaseRegistrar token, so it never reports `registrar_approvals`. (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L622-L636 @ ens_v2_sepolia_20261001@07e55a05) An empty name-filter result reports `permission_support_unknown` when the name is missing or unrecognized, its current name is marked unsupported, or its current name is not bound to a registration resource. A wrapped `.eth` name's `registration_id` is its BaseRegistrar lease, and a permissions read by that `registration_id` returns the rows of the NameWrapper resource that currently controls the name. The NameWrapper resource of a wrapped `.eth` name is not a registration, so a read by it returns an empty page without completeness metadata; a wrapped subname has no lease and is still read by its NameWrapper resource. See [registration identity of wrapped names](api-v1.md#registration-identity-of-wrapped-names). (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L108-L118 @ ens_v1@91c966f) (upstream: .refs/basenames/src/L2/Registry.sol:L155-L158 @ basenames@1809bbc) (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L42-L50 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L78-L103 @ ens_v1@91c966f) Returned current wrapper registrations still carry [expiry-effective](glossary.md#expiry-effective-namewrapper-fuse-word) lifecycle and fuse data when backed. (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L240-L305 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L390-L414 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L118-L152 @ ens_v1@91c966f) |
| Search | `GET /v1/search` | Name search only; no registration, pricing, or availability workflow. |
| Events | `GET /v1/events` | Product event collection with included/excluded types, raw product kinds, and one exact record key (including applicable reset rows), all applied before paging. These filters also work on name and address history. Contract-address queries can request an exact filtered row count with `include=total_count`; the default remains null. |
| Resolver overview | `GET /v1/resolvers/{chain_id}/{address}` | Resolver metadata and mirror declaration, and separately paginated complete record-link (ENSv2 record-ID resolvers: which nodes share which record, the default record included (upstream: .refs/ens_v2/contracts/src/resolver/interfaces/IRecordResolver.sol:L32-L38 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L379-L386 @ ens_v2@a971bd64)), per-registration role, and record-shaped bound-name collections, including [expiry-effective](glossary.md#expiry-effective-namewrapper-fuse-word) ENSv1 NameWrapper metadata when backed. |
| Namespace metadata | `GET /v1/namespaces/{namespace}` | Product-facing namespace and capability metadata. |
| Registry overview and labels | `GET /v1/registries/{chain_id}/{address}` and `/labels` | Current labels and exact declared registry assignment/per-label distinct holder counts with `include=counts`. Historical overview label totals are null; declared assignment counts do not imply complete effective-permission coverage. |
| Pipeline diagnostics | `/v1/diagnostics/*` | Explicit diagnostic tier, separate from product reads. |

The [record-ID resolver generation](architecture.md) supplies declared records
and resolver permissions through the existing routes and response shapes once
its manifest and end-to-end implementation are admitted. Record sharing, link
replacement, zero-link default selection, and empty values are part of that
capability. Ownership-only ingestion does not establish this capability, and a
source pin or successful decode alone is not replacement evidence.

## Direct PublicResolverV2 record support

For an owned local chain or the [official Sepolia deployment](sepolia-deployment.md), the exact `public_resolver_v2` declaration described in
[`manifests.md`](manifests.md#direct-publicresolverv2-declarations-on-an-owned-local-chain)
permits the existing record reads to use canonical address, text, and contenthash
observations plus node record-version boundaries, and the ABI content-type
inventory to use its `ABIChanged` observations. Attribution for those record and
inventory reads requires the current ENSv2 pointer, matching namespace, node, and
exact resolver emitter. Separately, a reverse claim uses the declared resolver's
`NameChanged` observations when it is the reverse node's current registry
resolver: the claim is matched by namespace, chain, reverse node, and that exact
resolver, and does not depend on the record-inventory classification.
A supported record classification does not prove exhaustive selector history,
resolver binding enumeration or permission-holder enumeration;
coverage remains limited to the retained observations and admitted capabilities.
No new REST route, schema, or record-ID interpretation is implied.

Empty address bytes, empty text, and empty contenthash remain explicit write
observations. Twenty zero address bytes remain distinct from empty bytes in the
stored observation; public reads retain their existing decode rules. The
inherited setters store values under `recordVersions[node]`, and `clearRecords`
increments that version and emits `VersionChanged`.
(upstream: .refs/ens_v1_publicresolver_5141a2a/contracts/resolvers/profiles/AddrResolver.sol:L47-L65 @ ens_v1_publicresolver_5141a2a@5141a2a)
(upstream: .refs/ens_v1_publicresolver_5141a2a/contracts/resolvers/profiles/TextResolver.sol:L15-L21 @ ens_v1_publicresolver_5141a2a@5141a2a)
(upstream: .refs/ens_v1_publicresolver_5141a2a/contracts/resolvers/profiles/ContentHashResolver.sol:L14-L19 @ ens_v1_publicresolver_5141a2a@5141a2a)
(upstream: .refs/ens_v1_publicresolver_5141a2a/contracts/resolvers/ResolverBase.sol:L20-L22 @ ens_v1_publicresolver_5141a2a@5141a2a)

Later writes contribute only within the current version; old-version values do
not return after a reset. Contract source establishes reachability, not runtime
acceptance of these reads or transactions. The source support alone does not establish public-chain deployment
provenance; the official Sepolia declaration uses its pinned deployment artifact. Zero pointers, undeclared/custom resolver addresses,
and unsupported roles preserve their explicit unsupported behavior. Existing
ENSv1 resolver support and PermissionedResolver proxy classification stay intact.

## Resolver address read modes

The records route and the `primary_address` of exact-name detail and of name
results in batch lookup share one indexed ENSIP-19 behavior. Projected exact entries remain event-derived.
When the selected resolver has the manifest-authorized
[resolver read feature](glossary.md#resolver-read-feature), an eligible EVM
coin-type request whose exact entry is empty or missing reads the projected
default entry instead. The records route identifies per-key derived results in
`records[key].meta`. Exact-name detail and batch lookup do not synthesize it
into grouped `records.addresses`, which holds exact observed writes only (a
cleared one as `null`); their indexed `primary_address` carries the derived
value. Verified exact-name detail serves each getter's own answer, so a getter
that returns its fallback puts it in both `records.addresses` and
`primary_address`. Derived values use the requested getter's verified decode:
coin type `60` treats a 20-byte zero default as `not_found`, while EVM-range
multicoin selectors retain that non-empty byte value. Exact stored records are
not normalized by this rule. Completeness remains request-relative. ENSIP-19 defines the
coin-type-to-chain eligibility rule, and the admitted resolver getter performs
the default-entry fallback only when that rule returns a positive chain ID
`(upstream: .refs/ens_v1/contracts/utils/ENSIP19.sol:L9-L38 @ ens_v1@91c966f)`
`(upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L36-L40 @ ens_v1@91c966f)`
`(upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L68-L85 @ ens_v1@91c966f)`.
The two official Sepolia resolvers that carry this fallback get it from different code.
`PermissionedResolver` inherits it from `AbstractRecordResolver`
(upstream: .refs/ens_v2_sepolia_20260916/contracts/src/resolver/PermissionedResolver.sol:L80-L83 @ ens_v2_sepolia_20260916@366de741)
(upstream: .refs/ens_v2_sepolia_20260916/contracts/src/resolver/AbstractRecordResolver.sol:L169-L178 @ ens_v2_sepolia_20260916@366de741).
`PublicResolverV2` does not inherit `AbstractRecordResolver`; it composes the ENSv1 `AddrResolver`
profile, and the profile source in its deployment compiler input carries the same fallback (the
cited build-info line holds that whole source file)
(upstream: .refs/ens_v2_sepolia_20260916/contracts/src/resolver/PublicResolverV2.sol:L23-L35 @ ens_v2_sepolia_20260916@366de741)
(upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/PublicResolverV2.json:L1272 @ ens_v2_sepolia_20261001@07e55a05)
(upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/build-info/solc-0_8_25-b30e6dc9a03b37f6a0b89af5d02a73d3993944f7.json:L853 @ ens_v2_sepolia_20261001@07e55a05).

| Address read | Indexed | Auto | Verified |
| --- | --- | --- | --- |
| Exact entry | Exact value | Exact value | Chain value |
| Exact 20-byte zero `addr:60` behind an admitted ENSv1 pointer or Basenames registry pointer | Exact `not_found` | Exact `not_found` | Chain `not_found` |
| Same exact ENSv1 zero entry on a flagged resolver plus a successful nonzero `addr:2147483648` | Exact `not_found` | Exact `not_found`; no provider call | Chain `not_found`; agreement |
| Same exact Basenames registry-pointer entry plus a default entry (admitted resolver is unflagged) | Exact `not_found` | Exact `not_found` | Chain `not_found` |
| Eligible EVM coin type, flagged resolver, default entry present | Derived value with per-key metadata | Derived value; no provider call | Chain value |
| Coin type 60, flagged resolver, default entry is 20 zero bytes | Derived `not_found` with per-key metadata | Derived `not_found`; no provider call | Chain `not_found` |
| Eligible EVM coin type, flagged resolver, default source authoritatively absent | Derived `not_found` with per-key metadata | Derived `not_found`; no provider call | Chain result |
| Default source unavailable or inventory non-authoritative | Explicit `unsupported` | Request-scoped verified fallback | Chain result |
| Ineligible coin type or unflagged resolver generation | Exact-key behavior; no derivation | Existing exact-key fallback policy | Chain result |

The auto column's exact-answer rule has one exception: for an Ethereum Mainnet or Sepolia
ENS name whose projected exact resolver is null and whose ordinary direct row
admits [Universal Resolver ancestor
discovery](glossary.md#universal-resolver-ancestor-discovery), all requested
keys execute through verified lookup. Retained exact inventory predates the
resolver-clear boundary and does not satisfy auto for that route. The selected
execution manifest must still admit the name's authority arm; discovery does not
bypass deployment-profile or topology restrictions.

The verified column is additionally scoped by [authority
arm](glossary.md#authority-epoch): a bound ENS name of either arm with a
non-null exact resolver carries a direct topology, and the verified read
executes only when the deployment profile's `ens_execution` manifest lists the
name's selected arm in `verified_authority_arms` (`manifests.md` §
`verified_authority_arms`). Mainnet admits `ens_v1` only; an unlisted arm reports
`exact_name_authority_not_verifiable` under `source=verified` (and its indexed
answer under `source=auto`). The official `sepolia` profile admits both arms.

The flagged deployments are the current ENS PublicResolver on mainnet, the
current Sepolia PublicResolver at `0xE99638b40E4Fff0129D56f03b55b6bbC4BBE49b5`,
and the official Sepolia ENSv2 `PermissionedResolver` implementation, plus its directly declared `PublicResolverV2`. The
admitted Basenames address is the legacy resolver and remains unflagged; its
vendored coin-type getter reads exact storage, while the fallback-bearing
upgradeable resolver proxy is not admitted in this change.
`(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L20-L31 @ ens_v1@91c966f)`
`(upstream: .refs/ens_app_v3/src/constants/resolverAddressData.ts:L151-L166 @ ens_app_v3@7175858)`
`(upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/PermissionedResolverImpl.json:L2 @ ens_v2@a971bd64)`
`(upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/PermissionedResolverImpl.json:L2398 @ ens_v2@a971bd64)`
`(upstream: .refs/basenames/test/Fork/BaseMainnetConstants.sol:L9-L14 @ basenames@1809bbc)`
`(upstream: .refs/basenames/lib/ens-contracts/contracts/resolvers/profiles/AddrResolver.sol:L35-L61 @ basenames@1809bbc)`

Verified answers never claim whether the on-chain resolver used exact storage,
default storage, or another execution path, so they omit derived metadata.

## ENSv1→ENSv2 mixed-history ownership

The replacement contract for exact-name and direct-subname current reads is
the per-name current-authority rule in
[`architecture.md`](architecture.md#ensv1ensv2-current-authority). Under that
rule, the chain first selects one
[authority epoch](glossary.md#authority-epoch), and every current field is then
selected inside that epoch. A migrated name keeps both eras in history. While
its ENSv2 registration is current, registration, control, resolver, expiry,
address relations, and permissions come only from its ENSv2 resource, and
retained ENSv1 facts remain history and provenance that do not make the current
read unsupported. After an ENSv2 release or expiry the name stays with ENSv2
as a [released v2 authority](glossary.md#released-v2-authority) tombstone,
whatever ENSv1 holds; only a later ENSv2 reservation hands it back to ENSv1,
and only while that reservation is live. Ending the reservation restores the
released ENSv2 selection. Slice 2C applies this rule to the
exact-name projection, the name-detail response, and per-result batch-lookup
records. Slice 2D makes the address-name, permission, search, primary-name, and
address-history collections consume that selected current registration, but
they acquire no row-local coverage status or unsupported-reason vocabulary;
callers inspect the exact-name or lookup result for coverage. An explicit
`registration_id` permission query may inspect a superseded ENSv1 registration
as historical/audit data. Every permission row
carries `authority_context`. The
[`current_for_name` context](glossary.md#current-for-name-authority-context)
means a `name` filter selected
the row's current registration for that requested name. A row admitted without
a `name` filter, including an explicit-`registration_id` or address-filtered
resource read, is `resource_audit` and makes no current-name claim; an optional
display name does not change that classification. Rows carrying the
[`resource_audit` context](glossary.md#resource-audit-context) remain queryable.
The marker changes only how that permission
response may be interpreted; the per-name ownership rule independently decides
which registration contributes current authority, address relations, and role
summaries. A superseded ENSv1 registration is therefore never selected, while a
current registration queried by resource can still contribute in a separate
name-scoped view.

An exact-name read is supported when the name's selected authority carries no
refusal. For ENSv2 that means a current registration in an admitted registry:
the root registry, the declared ETH registry, or a registry discovery admits.
No `ETHRegistrar` event, activated ENSv1→ENSv2 migration or positive child-registration
proof is required for support, and none is fabricated; registrar events keep
feeding name history and renewal expiry. An activated `MigrationApplied`
boundary still decides authority where it applies, as described below. Resolver
feature admission is unchanged: support for the name grants no resolver read
feature.

The final activation re-derives a [complete
group](glossary.md#complete-group) through
the production interpreter and records `MigrationApplied` as an activated
authority boundary. In the recorded [issue #822](https://github.com/ensdomains/bigname/issues/822)
baseline, registrar-token `unwrapped` groups stopped at predecessor resolution.
The adapter correction reconciles complete existing-token transactions before
ENSv1 state folding, as specified in [storage](storage.md); runtime acceptance
through Project remains unproven. Every normalized effect whose existence depends on the per-name
[migration correlation group](glossary.md#migration-correlation-group) carries
the completed group's visibility. The `migration_candidate_*_effects` tables
remain candidate-only diagnostic source records and are never Project input.
Refused and incomplete groups remain
candidate and are excluded from Project staging and product event/history reads.
An [independently admitted event](glossary.md#independently-admitted-event)
remains byte-for-byte activated
and only its diagnostic correlation association changes visibility. After its
[physical Interpret batch](glossary.md#batch-grid) commits in an ordinary walk,
the event is product-visible. An active or failed redo stays fenced from serving
until completion, even when an earlier redo batch committed. A batch-level failure
such as [issue #822](https://github.com/ensdomains/bigname/issues/822) rolls the
event back when it shares the failing batch, and later events remain unavailable
while Interpret cannot advance past the failing ENSv1→ENSv2 migration block. The shared production activation function performs the arm-scoped binding
transition already enforced by Interpret and enables the completed group's
dependent rows. Project consumes the validated
transition through its activated
`MigrationApplied` artifact without re-correlating raw ENSv1→ENSv2 migration
evidence. It
replaces that blanket refusal with the per-name exact-name rule. It
also activates non-boundary correlation groups; those groups never perform a
binding transition or change an authority epoch.

Slice 3B selects direct-subname ownership per child, replacing the previous
child recency tie-break rather than layering authority on top of it: recency now
orders only the current relation within the one selected arm. Parent reachability
first removes an ENSv1 relation below a parent on the `unwrapped`,
`unlocked_wrapped`, or `emancipated_child` path. A parent on the
`locked_wrapped` or `locked_child` path retains it only for a
[migratable child](glossary.md#migratable-child). Once that child migrates or
otherwise obtains a current ENSv2
registration, the published relation is the ENSv2 one and the retained ENSv1
relation is residue. A released ENSv2 child is a
[released v2 authority](glossary.md#released-v2-authority) tombstone and
publishes no relation on either arm. An entry in the parent's
[migration registry](glossary.md#migration-registry-wrapperregistry), released or
not, also makes the child non-migratable for good, so a released child of a
locked parent publishes no relation while its ENSv1 wrapper binding is open, and
the child's composed name agrees that it is released under ENSv2. Any other child follows
the chain ([ADR 0007](adrs/0007-follow-the-chain-ens-authority.md)): a current
ENSv2 registration selects its ENSv2 relation, a live ENSv1 registration
selects its ENSv1 relation, and a child with no open binding follows its
history. Event
recency never picks the arm. Only a pair whose child has no selected authority
at all and whose two arms disagree is omitted; it is neither an ambiguous
product row nor a publication failure. A current child registration in the
admitted migration registry selects ENSv2 like any other ENSv2 registration;
no activated boundary for the child or its parent is needed. Names with facts on both protocol eras
are expected on Sepolia because the runtime admits evidence from both; each
selects an arm per name under the same rule and is no longer refused. Names that
were identity-only with `independent_ens_deployments_overlap` (Sepolia) or
`conflicting_current_ens_authority` (Mainnet) now select ENSv2 when their ENSv2
registration is current and ENSv1 otherwise. A name whose selected arm is
ENSv2 and carries no refusal is served from its ENSv2 registration without a
further registrar, ENSv1→ENSv2 migration or child-registration qualification. The
ENS root, `eth`, `reverse`, and `addr.reverse` follow the same rule as every
other name. Complete
direct-child groups now supply production input
to the activated-boundary branch; a refused or unmigrated child reaches ENSv2
authority only through a current ENSv2 registration.

Project does not fail a publication over dual-current state. On both configured
ENS [deployment profiles](glossary.md#deployment-profile) (Mainnet and
Sepolia), bindings that remain current on both arms after a proven activated
boundary resolve through the name's selected arm, and a child publishes only
the relation its own selected arm states. Because an ENSv1 relation can survive
below an unmigrated parent or a locked path, both arms stating a relation for
one pair can be expected residue rather than an anomaly; the residue is not
served. A child registration in a locked parent's migration registry, without a
migration, is permanent entry history there
(upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L293-L307 @ ens_v2@a971bd64),
so parent reachability filters that ENSv1 relation first. Earlier releases
aborted such publications with `dual_current_exact_name_authority` or
`dual_current_child_authority` and recorded them in an append-only
`project_generation_failures` audit; the family publisher has neither
assertion, and the removal schema-migration
`20260929160000_remove_served_projections.sql` dropped that table. The connected
wrapped and locked scenarios in
[PR #852](https://github.com/ensdomains/bigname/pull/852) establish coherent
Interpret-to-Project publication.

## ENSv1→ENSv2 delivery slices

Registry-only ENSv1 and Basenames names with [getter-visible owner](glossary.md#getter-visible-owner) zero form one
cohesive read capability. Exact-name detail is supported and unregistered;
indexed records are supported when the retained [serving resource](glossary.md#serving-resource) has inventory;
verified and auto records follow the ordinary lookup capability; direct
subnames include a read-only row only while a current nonzero event-linked
resolver exists; and resolver `bound_names` remains subject to the resolver
family's existing binding-enumeration capability. Registration/control fields,
address-name relations, and owner-derived permissions stay absent. An ENSv2 TLD
whose root-registry token is reserved, or whose registration is not observed,
reads the same way from its
[root-registry resolver pointer](glossary.md#root-registry-resolver-pointer)
while staying `current_authority_not_projected`. When the
latest nonzero registry resolver selection predates the [name surface](glossary.md#surface-name-surface), the event that first makes the surface active
links it to the retained serving resource without
requiring a repeated selection; a latest zero-address selection remains a
clear.

For the fallback registry, a current-registry `NewOwner` or `Transfer` creates
the current record and ends any resolver pointer inherited from the old
registry. That [registry fallback handoff](glossary.md#registry-fallback-handoff) is retained across replay even for a same-owner
`Transfer`; linked zero-resolver events retract an inherited pointer that had
already become readable from every linked registry, registrar, or wrapper
resource. An old-registry `Transfer` does not affect a resolver
selected from the current registry.
(upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L18-L24 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L60-L68 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L75-L82 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L48-L54 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L150-L172 @ ens_v1@91c966f)

Each slice includes its behavior tests and fixture provenance. Counts are
estimated hand-written production files; test fixtures, test-only harness
files, and docs are not included. Rows before “Final activation” describe the
contract at each historical delivery boundary; that final row supersedes their
statements that complete migration groups remain candidate-only.

| Slice | Coherent capability | Estimated production files |
| --- | --- | ---: |
| 1. Schema vocabulary, candidate ENSv1→ENSv2 intake, and replay with no product-visible change | Extend the closed schema-v2 event/derivation vocabulary through a reviewed in-place schema upgrade; admit fixed ENSv1→ENSv2 migration contracts; ratify [migration-registry](glossary.md#migration-registry-wrapperregistry) discovery; keep the independently admitted `registry_announcement` indexability edge ordinary and traversable by the watch plan while attaching candidate correlation provenance; interpret only controller-mediated second-level correlation-dependent identity, topology, role, registration, renewal, and normalized effects as candidate while leaving independently derivable existing-family output ordinary; exclude candidate groups and association/effect tables from Project staging and product event/history reads; defer every ENSv1→ENSv2 migration-driven `SurfaceBinding` transition; and add production provider-trusted Verify support plus declared-level guard fixtures for `ethereum-sepolia`. Child-migration derivation through a parent `WrapperRegistry` landed in slice 3A below as candidate-only output; publication of child authority landed in slice 3B below, while activating a child ENSv1→ENSv2 migration boundary remains deferred. Restart, full-replay, and live-follow fixtures prove later proxy facts remain retained without changing product behavior. | At least 22 (3 manifest TOML, up to 11 adapter/manifest Rust files, 2 schema contract/check files, at least 1 reviewed versioned schema-migration file, 1 phase-runner Verify module, and up to 4 Project/API/storage visibility modules) |
| 2A. Explicit migration authority transition and arm-scoped ordinary bindings | Add a required `authority_arm` to every binding and closure draft; scope ordinary close, predecessor, and successor behavior to chain, exact logical name, and arm; preserve coexisting ENSv1 and ENSv2 bindings; represent the exact-name cross-arm transition explicitly; and exercise its locked zero/one/multiple predecessor behavior through a code-only activated test seam. Production remains candidate-only and Project behavior does not change. | 3 production modules plus one reviewed schema-migration file |
| 2B. Graveyard, reservation, and renewal semantics | Classify Graveyard cleanup and production reservation seeding without reading cleanup registrations as user leases; establish the remaining renewal rules from deployment evidence. | To be scoped |
| 2C. Exact-name current authority | Select one authority epoch by following the chain, keeping a validated activated transition only as migration history, then publish every exact-name field from only that epoch. Name detail exposes the selected exact-name result or the [deployment-profile](glossary.md#deployment-profile)-specific unsupported reason; candidate events remain inert. The resolver route's `bound_names` listing inherits this selection because it reads the same exact-name selection — a name is listed only under its selected resolver, and rows classified `current_authority_not_projected` are omitted, per the resolver-route contract in [`api-v1-routes.md`](api-v1-routes.md). Batch lookup results carry the same selection in 2C: a name-keyed or reverse lookup result exposes the selected exact-name outcome or the minimal unsupported record shape, per the lookup contract in [`api-v1-routes.md`](api-v1-routes.md). | To be scoped |
| 2D. Authority fanout across product collections | Address-name membership and role summaries, name-filtered permission selection, search membership, primary-name forward verification, and address-derived product-history anchors all consume the exact-name authority slice 2C selects ([current-authority fanout](glossary.md#current-authority-fanout)); no collection performs an ENSv1-versus-ENSv2 ranking of its own. Explicit registration or resource reads remain audit views, and per-result exact-name classification in batch lookup stays 2C-owned. A collection that carries no row-local unsupported vocabulary omits a name whose exact-name authority is unsupported instead of inventing a row-local status. | 5 |
| 2E. Post-rollback generation-failure audit | Enforce the reconciled dual-current invariant on both configured ENS deployment profiles (Mainnet and Sepolia) and persist the rolled-back generation failure in a separate append-only diagnostic transaction. | To be scoped |
| 3A. Direct-child correlation | Derive the deferred child-migration shapes that reach no migration controller, where the already-migrated parent's own [migration registry](glossary.md#migration-registry-wrapperregistry) registers the child into itself through the self-call that definition cites; admit the registry a locked child receives from its parent registry so admitted depth is unbounded; derive the child's ENSv1 predecessor from the parent registry's own migration evidence and the registered labelhash rather than inheriting the `.eth` second-level rule, under the separate `wrapper_backed_child_control` anchor defined at [child migration boundary](glossary.md#child-migration-boundary), selected against the child's ENSv1 cleanup rather than the registration; admit both cleanup shapes that definition cites — the `locked_child` path, whose wrapper token is parked in the Graveyard, and the `emancipated_child` path, whose node is unwrapped into it — each only with that ENSv1 predecessor cleanup present, earlier in the registration's own transaction; and reject the clobbered registration, the unmigrated child, factory-only evidence, incomplete parent discovery, and any self-claim lacking ENSv1 predecessor cleanup as non-boundaries, `MigrationHelper` participation being unobservable for the reason cited there and so never a correlation key at all. Correlation reuses `authority_transition`; every child boundary and effect is candidate-only, so no child state, projection, or product row changes — though an admitted child registry does widen Project's delete-and-rebuild scope — and activating a child transition remains an explicit refusal until slice 3B. | 4 |
| 3B. Children publication invariant | Stage the parent-child relation each authority arm states, first filtering ENSv1 relations by the parent's activated ENSv1→ENSv2 migration path: unwrapped, unlocked-wrapped, and emancipated-child parents retain none, while locked-wrapped and locked-child parents retain only [migratable children](glossary.md#migratable-child). Then publish the arm the child's own staged authority selects, so recency orders only within that arm; a released ENSv2 child publishes no ENSv2 relation and publishes its ENSv1 relation only when its own selected arm is ENSv1 and reachability kept it, and only a pair with no selected authority at all whose surviving arms disagree is omitted as unsupported rather than ranked. The ordered child assertion fails an ENS [projection generation](glossary.md#projection-generation) with `dual_current_child_authority` only when a child with an activated ENSv1→ENSv2 migration keeps a post-epoch ENSv1 relation that survives that parent filter; positive registration in a locked parent's migration registry is itself disqualifying entry history, so it is filtered before the assertion. | 4 (children projection builder, Project integrity assertion, child transition writer, redo reopen) plus one reviewed schema-migration file for the failure-kind vocabulary |
| Final activation. Production [complete groups](glossary.md#complete-group) | Run the already-proven activation function after all batch correlation paths finish; activate the authority paths that pass predecessor resolution and complete non-boundary normalized rows while retaining candidate-only diagnostic effect records; reconcile complete registrar-token `unwrapped` transactions before the ENSv1 state fold while retaining their recorded issue #822 coverage status pending runtime acceptance; preserve named refusals, ordinary events, exact predecessor selection, and Sepolia's then-current refusal of ordinary no-proof overlap (later retired by [ADR 0007](adrs/0007-follow-the-chain-ens-authority.md)); rotate the [interpreter content hash](glossary.md#interpreter-content-hash) and require the full Interpret→Project walk before publication. Coverage is enumerated in [`migration-activation-coverage.md`](migration-activation-coverage.md). | 4 adapter production files, one of which deletes the superseded helper; no schema, manifest, API, or Project vocabulary change |

Issues [#348](https://github.com/ensdomains/bigname/issues/348) and
[#529](https://github.com/ensdomains/bigname/issues/529) ship together at one
[interpreter content hash](glossary.md#interpreter-content-hash)
[re-derivation boundary](glossary.md#re-derivation-boundary), before the
combined slice/PR-#391 boundary. Their allowed product delta arises from late
ENSv2 resolver `RecordChanged` and `RecordVersionChanged` rows for a retained
canonical [name surface](glossary.md#surface-name-surface): `event_identity`
stays fixed, `logical_name_id` becomes non-null, `resource_id` stays null,
`raw_fact_ref.interpreter_state_key`
changes with the attribution, and `before_state` may rethread onto the
logical-name/resource-null state stream. Issue #348 retains the surface from
registry/root evidence. Those rows may newly enter
name-filtered diagnostics and product history. An ended resource whose latest
retained `ResolverChanged` pointer names the emitting resolver may also receive
a different rebuildable record inventory; the released or
expired name must still have no current binding or resource, and its name and
record reads must not expose that inventory. The boundary invalidates the
continuation contract for outstanding collection cursors; consumers restart
from the first page. Acceptance verifies the declared normalized-event and
inventory-row deltas, proves the ended name remains unbound with no served
record inventory, verifies both surface-retention triggers, and verifies fresh
complete pages, fields, membership, and
cursor continuation under the new publication. Any other product difference
blocks that publication. The later combined-boundary gate uses the
post-#348/#529 publication as its behavior-preserving baseline.

Slices 1, 2A, 2B, and 2C are separately reviewed and separately merged implementation
PRs, but deploy together at the same planned [re-derivation
boundary](glossary.md#re-derivation-boundary), which also carries
[PR #391](https://github.com/ensdomains/bigname/pull/391). The boundary
uses one [interpreter content hash](glossary.md#interpreter-content-hash), one
full source re-walk, and one Project
publication decision for `ethereum-sepolia`. Other chains
retain independent publication decisions. There is no production
interval serving candidate-only data on the `ethereum-sepolia` ENSv1→ENSv2
target.
Candidate-versus-activated state remains a replay/test-surface distinction, and
the acceptance comparison below runs in the test environment against the
boundary fixture corpus. The ordinary registry-announcement edge remains a
watch-plan input, so this one-boundary plan has no ingest hole.

Slice 1 has a mandatory full-re-walk acceptance comparison against the
pre-admission Project publication at a fixed readable chain head. This comparison
isolates slice 1: its control and candidate test runs hold every other
shared-boundary input constant, including PR #391's topology serializer. It is
not a comparison between the actual pre-boundary production publication and the
activated Project publication deployed after the shared boundary. It proves identical
product-visible row membership
and every DTO field of the name, subname and address-name reads, both
permission projections and `/v1/permissions`,
resolver and record reads, primary-name and search reads, `/v1/events`, and
name- and address-history reads.
The comparison covers ordered pages, page membership, every REST
DTO field, summary/count fields, `has_more`, and point responses. Before the
test-only slice-1 re-walk, `/v1/diagnostics/events` reads a page and saves its
`next_cursor`. After the full Interpret and Project re-walk publishes, that
pre-rewalk cursor is submitted to the post-rewalk test publication. Product
history cursors hold positions rather than normalized-event row IDs and follow
the [history walk](glossary.md#history-walk) rule in
[api-v1.md](api-v1.md#cursors-and-pagination).
`/v1/diagnostics/events` must accept its old cursor and continue from the same
stable normalized-event anchor, but its remaining rows and fields may include
the expected new candidate diagnostics. A pre-existing diagnostic row's numeric
`normalized_event_id` may change, while its `event_identity` and pre-existing
semantic fields remain stable apart from those allowed candidate additions.
Fresh post-rewalk diagnostic cursors are tested
separately and must continue normally.
Implementations may preserve numeric `normalized_event_id` values or resolve an
old token through stable `event_identity` plus its stored sort tuple; freshly
issued cursor bytes may differ. Raw facts, candidate
normalized events,
diagnostic event associations and identity/discovery effects, manifest
metadata, internal provenance, cursor-embedded row identities, and content
hashes are expected to change; product behavior is not.
The comparison runs over the complete planned re-walk, so a unit fixture that
filters only `ens_v2_migration_l1` cannot satisfy this gate.

A separate shared-boundary integration gate exercises the final combined
slice-1, slice-2A, slice-2B, slice-2C, and PR-#391 artifact. It inspects the actual widened watch
plan, performs the boundary's mandatory historical fetch and full re-walk, and
compares the published DTOs, pages, summaries, and cursor continuation with the
pre-boundary Project publication. PR #391's exact allowed wire delta is that an
existing Basenames `transport.contract_address` becomes lowercase and remains
`0x`-prefixed; no other PR-#391 product DTO field may change. The other allowed
differences are the slice-2C authority and event/history
activation contracted here, and the planned diagnostic, manifest, provenance,
and content-hash changes. Product cursors issued before this combined boundary
must remain valid, although their non-snapshot remaining rows and fields may
reflect those explicit activated deltas. Any other difference or cursor
rejection blocks the `ethereum-sepolia` publication decision.

The production Verify phase validates `ethereum-sepolia`'s durable ingested extent through its finalized marker. A distinct [verification-only](glossary.md#source-role) dRPC earns `cross_checked`; without one, the target-covering intake cursor earns `quick_synced` and is never selected as its own reference. The persisted
intake cursor must match its key, kind, seed basis, and start block and cover that
finalized marker. Project publishes before Verify in the
pipeline sequence, but that publication remains unready and traffic-drained
until Verify succeeds for the target. Acceptance fixtures prove
that distinct intake and verification-only endpoints produce `cross_checked`,
that the verification-only source receives no Ingest/Live requests or cursor,
and that equal intake/reference endpoints fail before cursor initialization, provider construction or access, raw-fact writes, or [redo-marker](glossary.md#redo-marker-scope) publication.
Base fixtures must keep Coinbase and dRPC intake-capable, use only an optional
distinct verification-only dRPC for `cross_checked` through the ingest seam,
and otherwise fall back to the target-covering intake dRPC's `quick_synced`.
Ethereum Mainnet fixtures must likewise reserve `node_checked` for a distinct
verification-only reth and fall back to its intake reth's `quick_synced`; a
`both` source can never earn either independent level. Normal and
manifest-widening Ingest redo must receive every intake-capable source and no
verification-only source, while role-aware Verify and `all` redo must enforce
the complete intake-capable key set before publication or provider access.
Standalone Interpret and Project redo require the complete intake-capable source descriptor set so persisted cursor identities can prove the range, but they perform no ingest-provider I/O. In `all` redo, Interpret receives only intake-capable descriptors after Ingest enforces the complete cursor-key set.
An `--phase all` fixture must prove its Ingest and Interpret contexts receive
only the complete intake-capable set while its Verify context still receives
the optional verification-only reference; the reference must not leak into
intake or cause Interpret cursor rejection.
Endpoint-conflict and provider-construction errors must name source keys without exposing either resolved endpoint. A retained pre-#411 fixture must also prove that a role
change which alters the intake-capable cursor-key set is rejected while raw
facts remain, until the owner-approved reset and full re-walk occur.
Five-field descriptors must remain accepted for normal run and redo, with the
omitted role defaulting to `both`. Each known level—`quick_synced`,
`cross_checked`, and `node_checked`—must satisfy the API's `quick_synced`
readiness floor, while an unknown stored level must fail closed. Completed
pre-part-2 `cross_checked` or `node_checked` evidence must be revalidated and
downgraded to `quick_synced` when the current role configuration cannot earn the
stronger level; it must neither halt revalidation nor preserve the stale claim,
and it must not call a reference provider. Adding an independent source must
not automatically upgrade a completed `quick_synced` extent; only the required
from-zero walk or an explicit full-extent Verify redo may establish the stronger
level. For every accepted production chain, persistence and completed-state
validation must derive the maximum reportable level from the same role-aware
verification plan, without a second chain allowlist or fail-open fallback. The fixtures
will continue to reject a stale or mismatched
intake cursor and prevent Live before Verify succeeds.
Omitting, disabling, or replacing Verify with a no-op is not an acceptable
readiness gate.

The boundary fixture places a migration-created `RegistryCreated` at block N,
restarts Ingest and Interpret, and then emits a registry, role, registration,
renewal, or topology fact from that proxy in a later transaction or block. Full
historical replay and live-follow variants both prove the ordinary announcement
admission keeps the proxy watched, the later raw fact is retained, its
correlation-dependent augmentation is interpreted as candidate, and any output
independently derivable under `ens_v2_registry_l1` remains ordinary and matches
the control test run. The fixture also asserts that the persisted edge appears
in the generated watch plan after restart before either the retained-raw-log
announcement preload or the same-window announcement query adds the proxy, so
neither intake path can mask a missing edge. The product comparison above
remains unchanged. A
same-transaction ordering test alone cannot satisfy slice 1.

Catalog-derived slice-1 fixtures preserve each decoded expiry instead of
reconstructing a fixed premigration delta. They also require ENSv1 registry
resolver- or TTL-clear events only when the cleared value actually changed:
`setRecord` delegates to a helper that compares both stored values before it
emits either event. (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L39 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L40 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L179 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L181 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L184 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L186 @ ens_v1@91c966f)

Slice 1 still fits the approximate production-file budget, but its inertness
contract requires narrow Project staging and API/storage history-selection
plumbing in addition to adapters, manifests, and fixtures. It also requires a
reviewed in-place schema-migration: `MigrationApplied`,
`ContractDiscovered`, `ens_v2_migration`, and the correlation-scoped visibility
provenance are outside the current closed schema-v2 contract. That requirement
is the stop condition for implementation in this change. An empty-schema
replacement is not an alternative for this boundary because it cannot preserve
outstanding cursors whose event identities currently include sequence-assigned
manifest IDs. Slice 1 must also add the reviewed `ethereum-sepolia` production
Verify path and readiness fixtures; the target cannot become ready or serve
traffic by omitting or bypassing that phase. Slices 1 and 2 remain
separately reviewed capabilities but share the deployment boundary above; slice
3 remains a later consumer capability.

## Replacement boundary

The `/v1` route set is the current API contract. This document records local
route ownership only; it does not claim that an external application has
changed its call sites. The checked-in Caddy configuration admits `/v1` reads,
and `POST /v1/lookup` (#315); see
[`production.md`](production.md#public-edge).

### ENSv2 token identifiers

Name detail (indexed and verified), detail lookup in both directions, and resolver overview
bound names agree on `token_id`: the decimal ERC-1155 ID recorded for the selected current
ENSv2 registration at its publication, including role-triggered token regeneration. A reservation,
released/unregistered or identity-only unsupported name, or a name lacking eligible token evidence,
omits the field. No bare labelhash or permission-resource word is substituted. `registration_id`
remains the existing resource UUID through regeneration; a new registration lifecycle may select
a new resource. Feed, name lists and current permission DTOs retain their existing omission.
History `include=data` exposes proven event-position token IDs on ENSv2 registration, transfer
and non-root registry permission rows. It also exposes event-local canonical keys, retained
payment/referrer details and explicit ERC-1155 operators, including uniquely corresponding
registrar payments on existing registration rows. See [history payloads](api-v1-routes.md#history-event-payloads-includedata-includeraw)
for exact evidence rules, registered/linked copies of one charge, and historical/source coverage
limits. Current name state never rewrites these history values.
The construction and separate token/EAC versions are defined by the pinned registry.
(upstream: .refs/ens_v2_sepolia_20261001/contracts/src/utils/LibLabel.sol:L7 @ ens_v2_sepolia_20261001@07e55a05)
(upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L578 @ ens_v2_sepolia_20261001@07e55a05)
(upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L678 @ ens_v2_sepolia_20261001@07e55a05)
