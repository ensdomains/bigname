# API v1

> **Prefix note (#315):** the public route prefix is `/v1`, and this file is
> named after it. Where the text below says `v2`, it means the contract
> generation accepted in ADR 0006, and `v1` in "Replaces" columns means the
> older REST surface that #315 deleted. No `/v2` prefix is served.

Development-time contract for the API surface accepted in
[ADR 0006](adrs/0006-api-v2-product-surface.md). Per-route reference lives in
[`api-v1-routes.md`](api-v1-routes.md). The OpenAPI 3.1 reference is generated
from the machine-marked tables in these two documents and served at
`GET /openapi.json`.

## Contract Principles

`v2` is designed around three rules:

1. **One vocabulary.** Every domain concept has exactly one wire name, drawn
   from common ENS/blockchain usage, defined in the naming dictionary below,
   and used identically on every route.
2. **One envelope.** Every route returns `data`, plus `page` on collections,
   plus `meta`. Field budgets may subset fields but never rename, retype, or
   restructure them.
3. **Three tiers.** Lookup primitives, product reads, and diagnostics are
   separate route families. The route path decides the tier; a query parameter
   never switches a route into another tier.

## Versioning

The binary serves this contract under `/v1`; the old v1 REST surface was
deleted before this contract took over the prefix (#315), and no `/v2` prefix
is served. The production edge admits `/v1` reads and `POST /v1/lookup`; see
[`production.md`](production.md#public-edge) for the edge policy. The former
`POST /graphql` compatibility surface has been removed and answers like any
unknown route.

## Naming Dictionary

Normative one-name-per-concept dictionary from ADR 0006, extended with the
step-3-gate vocabulary needed by the route schemas:

| `v2` name | Meaning | Replaces (`v1`) |
| --- | --- | --- |
| `name` | the ENSIP-15 normalized name string, except on routes that document an explicit [non-name form](glossary.md#non-name-form) for a label bigname cannot state as a name — today only `GET /v1/names/{name}/subnames` | `normalized_name`, `logical_name_id` (derivable as `namespace:name`) |
| `display_name` | display form of the name | `canonical_display_name` |
| `namespace` | public namespace slug used to resolve a name or filter a route, such as `ens` or `basenames` | `namespace` path segment/query usage (unchanged; now echoed consistently) |
| `namehash` | ENS namehash hex string | `namehash` (unchanged) |
| `token_id` | decimal-string token id for tokenized registrations/names | `token_id` (unchanged; now defined consistently) |
| `owner` | who holds the name: the token holder of a name that has a token (an ENSv1 BaseRegistrar lease, a NameWrapper token, including a wrapped subname's, or an ENSv2 registry token), otherwise the registry owner of its node (an unwrapped subname with only a registry record, a registry child with no name row). On a wrapped name it is the NameWrapper token holder, not the NameWrapper contract, and on an unwrapped `.eth` second-level name the BaseRegistrar token holder, not the registry controller. After an ENSv1 `.eth` token transfer without `reclaim`, `owner` is the new holder while `manager` stays the previous registry owner until the holder calls `reclaim` (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L171-L174 @ ens_v1@91c966f); being or becoming that registry owner adds no history under `relation=owner`, while history an address gained by holding the token or by owning a name with no token stays there; omitted on a released name, on an expired emancipated or locked wrapped name, whose owner NameWrapper clears (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L843-L851 @ ens_v1@91c966f), and on a record the admitted Graveyard holds, even while a NameWrapper token of that cleared subname survives (see [upstream divergences](upstream.md)) | `token_holder`, `owner`, `owner_address`, `registry_owner`, `registrant` (removed in v0.3.0; its value is now `owner`) |
| `manager` | the account that can change the name's registry record (see [Manager](#manager)) | `effective_controller`, `manager_address` |
| `relation` | address-to-name relation filter: one or more of the authority relations `owner` (the address is the name's `owner`), `manager` (the address is the name's `manager`), and, on address names and address history, `role_holder` (the address holds an ENSv2 registry role on the name's current registration; not the manager) (comma-separated set); `any` = all authority relations supported on that route; or, on its own, the resolver-record relation `resolves_to` (names whose current `addr:<coin_type>` record resolves to the address, coin type from `coin_type`, default `60`, or every EVM coin type with `coin_type=evm`), or, on its own and on `GET /v1/addresses/{address}/names` only, `former_owner` (released names whose ended registration the address last held; see [lapsed registration](#lapsed-registration)). `resolves_to` and `former_owner` are not part of `any` and cannot be combined with another relation. `registrant` and `former_registrant` were removed in v0.3.0 and are rejected like any unknown value | four divergent relation/role enums incl. `owned`/`managed`/`both` (partner `BOTH` = `owner,manager`); ensjs `resolvedAddress`; `registrant`; `former_registrant` |
| `relations` | address-to-name relations that matched a row, using `owner`, `manager`, `role_holder`, `resolves_to`, and `former_owner` values | `relation_facets`, role-specific match arrays |
| `lapsed_registration.owner` | on a released name: the `owner` its registration had when it ended (see [lapsed registration](#lapsed-registration)) | `lapsed_registration.registrant` |
| `resolution` | on a `resolves_to` row read for one decimal coin type only: `{coin_type, record_key}`, the coin type asked about and the resolver record key that answered it (`addr:<coin_type>`, or `addr:2147483648` when the ENSIP-19 default EVM address answered (upstream: .refs/ens_v1/contracts/utils/ENSIP19.sol:L10 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L68-L85 @ ens_v1@91c966f)) | subgraph `resolver.coinTypes` |
| `resolutions` | on a `resolves_to` row read with `coin_type=evm` only: `[{coin_type, record_key}]`, one entry per EVM coin type (`60`, or `2147483648` through `4294967295`, the set ENSIP-19 treats as EVM (upstream: .refs/ens_v1/contracts/utils/ENSIP19.sol:L9-L38 @ ens_v1@91c966f)) whose stored resolver record matched the address, ascending by coin type, each with the stored record key that matched. The ENSIP-19 default record appears once, as coin type `2147483648` (upstream: .refs/ens_v1/contracts/utils/ENSIP19.sol:L10 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L68-L85 @ ens_v1@91c966f). A row carries at most 100 entries; a row that matched more returns `422 unsupported` for the whole request; read one decimal `coin_type` at a time instead. It lists matches only, not every coin type the resolver has records for; see the [known divergence](upstream.md#resolves-to-matched-coin-types) | subgraph `resolver.coinTypes`, which lists every coin type the resolver has observed whatever its value (upstream: .refs/ens_subgraph/schema.graphql:L294-L295 @ ens_subgraph@723f1b6) (upstream: .refs/ens_subgraph/src/resolver.ts:L59-L79 @ ens_subgraph@723f1b6) |
| `expires_at` | expiry as a decimal string of Unix seconds: for a `.eth` second-level name with a live ENSv2 entry, that entry's expiry once the chain is past the [Universal Resolver cutover](glossary.md#universal-resolver-cutover), whichever arm holds authority (see [Expiry and grace](#expiry-and-grace)); otherwise the registrar lease for registrar-backed names; for a wrapped ENSv1 name with no registrar lease (a wrapped subname) the NameWrapper entry's expiry, which is the only expiry the chain holds for it (zero means the parent set none). Finite values retain every digit, including after year 9999 or above JavaScript’s safe integer range. In a registration context, a classified absent expiry is `null` with `expires_at_reason`; see [Timestamp format and absent expiry](#timestamp-format-and-absent-expiry) | `expiry_date`, `expiration` (unix), `expiry` |
| `expires_at_reason` | present exactly when a registration’s `expires_at` is `null`: `no_expiry`, `not_set`, or `released`; omitted for finite expiry and for a row with no registration context | new in v2 |
| `grace_ends_at` | when the renewal grace of `expires_at` ends, as a decimal string of Unix seconds: `expires_at` plus 90 days for an ENSv1 `.eth` lease or a Basenames name, plus 28 days (the ENSv2 `.eth` registrar's grace period) for a `.eth` name that serves an ENSv2 expiry, and equal to `expires_at` for a name with no registrar grace, such as a subname; finite exactly when `expires_at` is finite, and `null` alongside a classified null expiry, whose `expires_at_reason` also explains the absent grace deadline (see [Expiry and grace](#expiry-and-grace)) | new in v2 |
| `unresolvable_reason` | on name detail and lookup: why a name resolves to nothing although its authority records a resolver. `no_live_ens_v2_entry`: the chain is past the [Universal Resolver cutover](glossary.md#universal-resolver-cutover), ENSv1 decides the name, and neither it nor its `.eth` second-level ancestor has a live ENSv2 entry. The resolver and records are then withheld (see [Expiry and grace](#expiry-and-grace)) | new in v2 |
| `registered_at` | start of the current registration, as a decimal string of Unix seconds. A registration is one continuous holding of the name: renewals keep its start, and so does the ENSv1→ENSv2 migration, so a migrated `.eth` second-level name serves its ENSv1 lease's registration time, not `migrated_at`, and a `.eth` name that premigration reserved serves its ENSv1 lease's registration time before and after the [Universal Resolver cutover](glossary.md#universal-resolver-cutover). Only a release (an ENSv1 lease running out past its grace period, or an ENSv2 entry unregistered or expired) followed by a new registration starts a new one. A name with no ENSv1 registrar lease, such as a subname, has no registration time before ENSv2, so its migration's ENSv2 registration starts one | `registration_date` |
| `created_at` | first observation of the name, as a decimal string of Unix seconds; served only for a name with a name row, so a registry child listed without one omits it | `created_at` (now defined and distinguished from `registered_at`) |
| `registration_status` | registration/control lifecycle label: `active`, `wrapped`, `registered`, `released`, or `unregistered` | `ControlVector.status`, role-summary `status` |
| `ens_v1` | on name-shaped rows (name detail, resolver `bound_names`, lookup `profile=detail` and `profile=feed`, address names, subnames, registry labels, `GET /v1/names` and search): what only ENSv1 holds about the name, `{expires_at, wrapper_state?, wrapper_fuses?}`. Present exactly while the name's `authority` is `ens_v1` or `ens_v0` and omitted otherwise, including on every `ens_v2` row; subname, registry-label, `GET /v1/names` and search rows follow the `authority` they carry, and lookup `profile=feed` records follow the `authority` of the detail record for the same name, which they do not carry | new in v2; replaces the earlier top-level `wrapper_state` and `wrapper_fuses` |
| `ens_v1.expires_at` | the name's ENSv1 BaseRegistrar lease expiry as a decimal string of Unix seconds (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L96-L98 @ ens_v1@91c966f), or `null` when the name has no lease, as for every name below a `.eth` second-level name, including a wrapped subname whose only expiry is its NameWrapper entry's. Before the [Universal Resolver cutover](glossary.md#universal-resolver-cutover) it equals the top-level `expires_at` of a `.eth` second-level name; from the cutover a name with a live ENSv2 entry serves that entry's expiry at the top level and keeps the lease date here (see [Expiry and grace](#expiry-and-grace)). A lease has no sentinel expiry, so there is no reason field: `null` only means no lease. Values are exact up to `9223372036854775807` (`i64::MAX`); a lease expiry above it is served as `"9223372036854775807"`. The admitted Sepolia testnet premigration registrar takes a caller-chosen registration duration, so such a lease is reachable there (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/testnet/TestnetV1PremigrationRegistrar.sol:L161-L165 @ ens_v2_sepolia_20260916@366de741) (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/testnet/TestnetV1PremigrationRegistrar.sol:L214-L217 @ ens_v2_sepolia_20260916@366de741). There is no grace field: below `9223372036854775807` the lease's grace deadline is `ens_v1.expires_at` plus 90 days (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L17 @ ens_v1@91c966f). A saturated `"9223372036854775807"` no longer carries the lease's own expiry, so its grace deadline cannot be recovered from it. Premigration's 62-day continuity bonus makes that deadline equal the top-level `grace_ends_at` when the name is reserved (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/testnet/TestnetV1PremigrationRegistrar.sol:L38-L42 @ ens_v2_sepolia_20260916@366de741), but the alignment is not guaranteed: a reservation can be extended without the BaseRegistrar, for example by the admitted BatchRegistrar, and then the two deadlines differ (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registrar/BatchRegistrar.sol:L66-L70 @ ens_v2_sepolia_20260916@366de741) | new in v2 |
| `wrapper_state` | inside `ens_v1`, and on permission rows and wrapper `restrictions`: bigname's current ENSv1 NameWrapper lifecycle value: [`wrapped`](glossary.md#wrapped-namewrapper-state), [`emancipated`](glossary.md#emancipated-namewrapper-state), or [`locked`](glossary.md#locked-namewrapper-state); omitted when the current name is not in one of those states | raw NameWrapper fuse bitmap |
| `wrapper_fuses` | inside `ens_v1`, and on permission rows and wrapper `restrictions`: typed summary of the current [expiry-effective NameWrapper fuse word](glossary.md#expiry-effective-namewrapper-fuse-word); present exactly when `wrapper_state` is present | raw NameWrapper fuse bitmap |
| `fuses` | uint32 fuse word nested in `wrapper_fuses`; it is zero after wrapper expiry even though normalized events retain their expiry-unadjusted interpreted word | raw NameWrapper fuse bitmap |
| `restrictions` | the [resource restrictions](glossary.md#resource-restrictions) of a registration, constraints that bind the registration itself rather than any one account: `{registration_id, kind, ...}` where `kind` is `ens_v1_wrapper` (with `wrapper_state`, `wrapper_fuses`, `wrapper_expires_at`, and conditional `wrapper_expires_at_reason`) or `ens_v2_registry` (with `locked_roles`); see [resource restrictions](#resource-restrictions) | new in v2 |
| `wrapper_expires_at` | decimal string of Unix seconds for the NameWrapper entry expiry, or `null` with `wrapper_expires_at_reason` (`no_expiry` or `not_set`) inside an `ens_v1_wrapper` `restrictions` object; for a wrapped `.eth` second-level name it is the registrar expiry plus the 90-day grace period NameWrapper stores, so it is later than that name's `expires_at` | `expiry` on NameWrapper events |
| `locked_roles` | inside an `ens_v2_registry` `restrictions` object: the token-scoped registry roles whose assignment can no longer change because no account holds the corresponding admin role on the registration or its registry root | new in v2 |
| `authority` | the registry generation that owns the node, which is where the chain reads the row's current registration and control fields from: `ens_v2`, `ens_v1`, or `ens_v0`. It does not name a registrar: `ens_v0` has none. `ens_v2` and `ens_v1` name the selected [authority epoch](glossary.md#authority-epoch) arm. `ens_v0` is an ENSv1 name whose registry record is still read from the 2017 ENS registry, because the current ENSv1 registry has no ownership record for the node yet (upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L18-L46 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L150-L157 @ ens_v1@91c966f) ([registry generation](glossary.md#registry-generation)); it becomes `ens_v1` at the node's first current-registry `NewOwner` or `Transfer`, the only writes that create that record (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L60-L84 @ ens_v1@91c966f). `ens_v0` changes no other field: registration, control, `registration_id` and permission handles are derived as for `ens_v1`. A node with no registrar lease that an ENSv1 registry `NewOwner` created (a `setSubnodeOwner` or `setSubnodeRecord` child) carries the authority of the registry that holds its record, `ens_v1`, or `ens_v0` while only the 2017 registry does, with `registration_status` `unregistered`; that includes a child with no [name surface](glossary.md#surface-name-surface), which address names and subnames list from its registry record. `GET /v1/names` and search rows carry the `authority` their name detail serves. Subname and registry-label rows carry `authority` too: a child with its own name record serves that record's authority, and a registry-only child with none serves its registry's, `ens_v1` when the current ENSv1 registry holds its record and `ens_v0` while only the 2017 registry does; it is omitted for a Basenames child and for a child whose registry owner is zero or unknown. Omitted when the projection selected no ENSv1/ENSv2 arm (Basenames names have no era split), on an ownerless ENSv1 or Basenames registry row, whose owner the registry reports as zero, and on an `unsupported` name detail object, which carries no registration fields. In the current ENSv1 fallback registry a zero-owner write is stored as the registry's own address and read back as zero (upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L48-L55 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L123-L131 @ ens_v1@91c966f). The ownerless omission applies only to a supported, unregistered row with no selected binding: a zero registry owner does not remove `authority` supplied by a retained registrar binding | new in v2; the zigens `Domain.protocol` / `isMigrated` concept |
| `migrated_at` | decimal string of Unix seconds for the block time of the name's latest activated `MigrationApplied` [migration boundary](glossary.md#migration-boundary), the ENSv1→ENSv2 [migration authority transition](glossary.md#migration-authority-transition) kept as history. It records when the name migrated, not why it has its current authority: the current ENSv2 registration decides that as it would for any name. For a name with an ENSv1 registrar lease the migration does not start a new registration, so `registered_at` keeps that lease's registration time; a migrated name without one, such as a subname, starts its registration at the ENSv2 grant. Present only with `authority=ens_v2`, so a migrated name whose ENSv2 registration was later released keeps it (it stays released under ENSv2). A later ENSv2 reservation hands the name to ENSv1 only while the reservation is live, and the field is omitted only during that ENSv1 selection; it returns when the reservation ends and the released ENSv2 tombstone returns. It is also omitted for a name registered directly in ENSv2 and one on `ens_v1` or `ens_v0` | new in v2 |
| `primary_name` | primary name selected or claimed for an address/coin tuple | `claimed_primary_name`, `verified_primary_name` when surfaced as the selected name |
| `primary_address` | primary/default address value for a name | `primary_address` (unchanged) |
| `is_primary` | whether an address-name row is the selected primary answer for that address/coin tuple | `is_primary` (unchanged) |
| `records` | on name detail and lookup `profile=detail`: the grouped resolver records `{seen_addresses, addresses, seen_texts, texts, seen_abis, abis, seen_singletons, contenthash, name}`, each `seen_*` key list beside its value map ([grouped records](api-v1-routes.md#grouped-name-profile-records)); on `GET /v1/names/{name}/records`, the route-local per-key answers | flat `addresses`, `text_records`, `content_hash`, and the lookup `inventory` container |
| `addresses` | inside grouped `records`: coin-type-to-address map, string keys | `coin_addresses`, `coin_type_addresses` |
| `address` | EVM address used as a subject, filter, or single-address answer | `account`, `subject`, single-address fields named `address` |
| `coin_type` | ENS/SLIP-44 coin type number. As a request parameter of `GET /v1/addresses/{address}/names?relation=resolves_to` it also accepts the literal `evm`, which selects every EVM coin type | `coin_type` (unchanged; now used consistently for reverse and record lookups) |
| `texts` | inside grouped `records`: text-key-to-value map | `text_records` |
| `contenthash` | inside grouped `records`: contenthash value | `content_hash` |
| `resolver` | `{chain_id, address}` | `resolver_address`, `current_resolver`, declared resolver summaries |
| `subregistry` | `{chain_id, address}` of the ENSv2 registry a name's current subregistry pointer targets; omitted when there is none | `SubregistryChanged` after-state `subregistry` |
| `parent_registry` | `{chain_id, address}` of the registry that emitted the pointer to a registry; `null` for the root registry | `SubregistryChanged` emitter |
| `contract_address` | event filter for the contract that emitted an event's source log | `emitting_address` |
| `chain_id` | numeric EVM chain id (`1`, `8453`); string-keyed in maps | string chain ids (`"ethereum-mainnet"`), position slot keys |
| `network` | display slug (`ethereum`, `base`) | `network` (unchanged, display-only) |
| `id` (event row) | opaque 64-character identity of one event row, identical for the same event on `/v1/events`, name history, and address history and across pages; a merge key for consumers combining feeds, not a durable reference (it may change at a re-derivation boundary) | `event_identity`, `normalized_event_id` |
| `registration_id` | the one opaque stable handle for a registration lifecycle; for a `.eth` second-level name it is always the BaseRegistrar lease, wrapped or not (see [registration identity of wrapped names](#registration-identity-of-wrapped-names)) | `resource_id`, `resource_hex`, `resource`, `token_lineage_id`, `surface_binding_id` |
| `input` | caller-supplied lookup input echoed in a result | `input` (unchanged; now specified as result echo, not a parallel DTO family) |
| `normalization` | name-normalization result for an input | `corrected_input_normalization`, `unnormalizable_input` status detail |
| `finality` | `latest`, `safe`, `finalized` (JSON-RPC block-tag vocabulary) | `consistency` = `head`/`safe`/`finalized` |
| `source` | answer origin `indexed` or `verified` (the records route adds request value `auto`) | `mode` = `declared`/`verified`/`both`/`auto`; `declared_state`/`verified_state` |
| `as_of` | readable per-chain `{block_number, block_hash, timestamp}`, keyed by `chain_id` | `chain_positions` (and the `execution_checkpoint` pseudo-slot is diagnostics-only) |
| `as_of_completeness` | per-chain positions suppressed from `as_of`, keyed by `chain_id`, with `{completeness, unsupported_reason}` | inferring request coverage from whichever rows happened to be returned |
| `as_of_token` | opaque URL-safe snapshot token for replaying the exact served positions with `at` | reconstructing `at` from `chain_positions` |
| `at` | snapshot selector parameter for routes that support point-in-time reads | `chain_positions` query parameter and timestamp-specific ad hoc selectors |
| `include` | route-documented query expansion allowlist. `POST /v1/lookup` has no expansion parameter; nonempty body `include` is rejected, while the parser tolerates an empty string as omission. One value selects rows instead of expanding them: name history's `child_registrations` adds direct child registration rows | comma-separated expansion flags, `meta` knobs, and route-specific include flags |
| `sort` | route-documented sort field | `sort` (unchanged; allowed fields are now route-documented) |
| `order` | sort direction, `asc` or `desc`; history collections default to `desc` (newest first) and treat `asc` as the exact reverse | `order` (unchanged) |
| `scope` (history) | `name`, `registration`, `both` | `surface`, `resource`, `both` |
| `subject` (history) | on name history with `include=child_registrations` only: the row's relation to the requested name, `name` for the name's own rows and `child` for a [direct child registration](api-v1-routes.md#direct-child-registrations-includechild_registrations) | comparing a row's `name` with the requested name |
| `grant_scope` | the protocol scope of a permission row: `root`, `registry`, `registration`, `resolver`, `record_manager`, or [`account`](glossary.md#account-permission-scope) | permission-row `scope` (renamed so history `scope` and permission scope are two names for two concepts) |
| `grant_relation` | optional explicit [grant relation](glossary.md#grant-relation); `operator` identifies a registry-wide approval, while direct permission rows omit the field | new in v2 |
| `verification` | typed checked-answer summary for claimed-vs-verified answers | `verified_state`, `verified_primary_name` section wrappers |
| `status` | one result vocabulary: `ok`, `not_found`, `invalid_name`, `mismatch`, `unsupported`, `stale`, `failed` | `ResultStatus`, `IdentityStatus`, `NameRecordStatus`, `unnormalizable_input` (folds into `invalid_name`); `mismatch` kept for verification results |
| `unsupported_reason` | reason code or short reason string required with `status=unsupported` | `coverage.unsupported_reason`, route-specific unsupported details |
| `failure_reason` | reason code or short reason string for `failed`, `stale`, `not_found`, or `mismatch` details | route-specific failure detail fields |
| `completeness` | `full`, `partial`, `unsupported` | `coverage.status` on product routes (full taxonomy moves to diagnostics) |
| `powers` | effective permission powers, drawn from the [permission powers vocabulary](#permission-powers-vocabulary); storage `resource_control` is exposed as `registration_control`; `registry_control` is passed through from an effective registry-operator account row; ENSv2 registry `was_reserved` is a non-authorizing history marker retained here so marker-only transitions remain visible (upstream: .refs/ens_v2/contracts/src/registry/libraries/RegistryRolesLib.sol:L47-L48 @ ens_v2@a971bd64) | `effective_powers` |
| `unlisted_permission_surfaces` | on permission reads, the sorted codes of permission surfaces whose holders the rows do not list: `ens_v2_registry_operators`, `registrar_approvals`, `resolver_approvals`, `wrapper_parent_control`; omitted when nothing is unlisted or support is unknown | new in v2 |
| `unsupported_fields` | fields or expansions that could not be served or proved for a response item | `unsupported_filters`, coverage-derived unsupported field lists |
| `keys` | comma-separated resolver record-key allowlist | `records` query parameter, selector token lists in record diagnostics |
| `page` | pagination object on top-level collections, per-input lookup results, and the resolver overview `bound_names` nested collection | pagination sections with divergent field subsets |
| `cursor` | opaque request cursor for the current page | `cursor` (unchanged; now opaque and versioned) |
| `next_cursor` | opaque cursor for the next page, or `null` | `next_cursor` (unchanged) |
| `page_size` | requested or served page size | `page_size` (unchanged) |
| `total_count` | nullable total item count when cheap or explicitly requested | `total_count` (unchanged; now nullable and budgeted) |
| `has_more` | whether another page is available | `has_more` (unchanged) |
| `meta` | response metadata object for snapshot, completeness, unsupported, and source details | `provenance`, `coverage`, `chain_positions`, `consistency`, `last_updated` top-level peers |
| `subname_count` | count of direct subnames when requested | `subname_count` (unchanged; now the only count name for child rows) |
| `record_count` | count of known record keys when requested | `record_count` (unchanged) |
| `permission_resource_id` | the handle that selects an address-name row's permission rows on the permissions route's `registration_id` filter: the name's `registration_id` while it retains a current registration identity, including unsupported name coverage (for a wrapped `.eth` name the BaseRegistrar lease, although the rows live on the NameWrapper resource), otherwise the permission authority resource UUID itself | new in v2 |
| `role_summary` | grouped permission powers for dashboard-style name rows | `role_summary` (unchanged; rewritten to dictionary field names inside) |
| `authority_context` | required permission-row marker from the [per-name ownership rule](consumer-capabilities.md#ensv1ensv2-mixed-history-ownership); [`current_for_name`](glossary.md#current-for-name-authority-context) means a `name` filter selected the current registration, while [`resource_audit`](glossary.md#resource-audit-context) makes no current-name claim | new in v2 |
| `capabilities` | product-facing summary of supported namespace capabilities; `verified_records` and `verified_primary_name` carry a `chains` object keyed by numeric chain id with per-chain `{completeness, unsupported_reason?}` | capability flag summaries when exposed to product routes |
| `type` | product event category label; as a history filter, one label or a comma-separated set | `event_kind`, compact event `type` aliases |
| `by_type` | map of product event `type` values to counts | event summary `by_kind` maps keyed by raw event kind |
| `block_number` | EVM block number | block-number fields inside chain-position objects |
| `block_hash` | EVM block hash | block-hash fields inside chain-position objects |
| `timestamp` | decimal string of Unix seconds for an event or block timestamp | event timestamps and chain-position timestamps |
| `transaction_hash` | EVM transaction hash | `transaction_hash` (unchanged) |
| `log_index` | EVM log index within a transaction | `log_index` (unchanged) |
| `from_block` | inclusive lower block-number filter | `from_block` (unchanged) |
| `to_block` | inclusive upper block-number filter | `to_block` (unchanged) |
| `from_timestamp` | inclusive lower Unix-seconds or RFC 3339 bound on history collections, resolved per chain to the first readable lineage block at or after it | new in v2 |
| `to_timestamp` | inclusive upper Unix-seconds or RFC 3339 bound on history collections, resolved per chain to the last readable lineage block at or before it | new in v2 |
| `expires_after` | inclusive lower `expires_at` bound on `GET /v1/names` or `GET /v1/addresses/{address}/names?relation=former_owner` (Unix seconds or RFC 3339) | `expires_after` (new) |
| `expires_before` | exclusive upper `expires_at` bound on `GET /v1/names` or `GET /v1/addresses/{address}/names?relation=former_owner` (Unix seconds or RFC 3339) | `expires_before` (new) |
| `parent` (query) | `GET /v1/names` and `GET /v1/addresses/{address}/names` filter: only names exactly one label below the given name, matched on the normalized name spelling rather than on the node or registry topology, so a bracketed labelhash label matches only names stored with that bracketed spelling, except the labelhashes of `eth` and `base`, which normalization spells as text. `parent=eth` selects the `.eth` second-level names, the registrar-governed set on both sides of the [Universal Resolver cutover](glossary.md#universal-resolver-cutover): the ENSv1 BaseRegistrar leases labels one level below `eth` (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L147-L150 @ ens_v1@91c966f) and the ENSv2 `.eth` registrar registers labels in the `eth` registry (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registrar/ETHRegistrar.sol:L151-L158 @ ens_v2_sepolia_20260916@366de741), so the set holds ENSv1 leases and ENSv2 registrations alike and excludes every subname, wrapped or not. `parent=base.eth` with `namespace=basenames` selects the Basenames second-level names | new in v2 |
| `data` | envelope root payload, and the `include=data` event-row payload when nested inside an event row (see [history event payloads](api-v1-routes.md#history-event-payloads-includedata-includeraw)) | compact event payload objects |
| `kind` | raw storage event kind on an event row, exposed only behind the explicit `include=raw` opt-in (never part of `include=data`); the one pipeline term the product tier carries, for explorer and diagnostic use | `event_kind` |
| `contract_address` | lower-cased emitting contract of an event row, exposed only with `include=data`; `null` for state-derived rows | `emitting_address` |
| `resolver` (query) | `/v1/events` filter naming one resolver contract as `<chain_id>:<address>` (numeric chain id, case-insensitive address); matches rows the contract emitted plus `resolver` pointer rows naming it | new in v2 |
| `mirror` | on the resolver overview, present only for a declared [ENSv1 mirror resolver](glossary.md#ensv1-mirror-resolver-ensv1_mirror_resolver): `{kind: "ensv1_registry", registry: {chain_id, address}}`, the ENSv1 registry whose resolvers answer for names bound to this resolver | `declared_summary.classification.mirror`, `ensv1_mirror_resolver` |
| `links` | the resolver `/links` collection: the nodes an ENSv2 record-ID resolver currently binds to a non-zero record, one item per node as `{record_id, namehash, default, namespace?, name?, display_name?, link_event}`; nodes sharing a `record_id` share one record's values, and `default: true` marks the empty-name node whose record answers every unlinked name (upstream: .refs/ens_v2/contracts/src/resolver/interfaces/IRecordResolver.sol:L32-L38 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L379-L386 @ ens_v2@a971bd64) | `ResolverRecordLinked` normalized events, `project_resolver_link` |
| `record_id` | the decimal ID of a record on an ENSv2 record-ID resolver, as a string | `resolver_record_id` |
| `link_event` | on resolver link items: `{block_number, timestamp, transaction_hash, log_index}` of the current `Linked` observation for that node | link `chain_position` |
| `record_resource` | on `GET /v1/permissions` rows and resolver `/roles` rows for a grant on an ENSv2 record-ID resolver: the setter argument the granted resource stands for, decoded — `{kind: "address", hash, coin_type}`, `{kind: "text" \| "data", hash, key \| key_bytes}` (`key` for valid, NUL-free, non-blank UTF-8; `key_bytes` hex otherwise), `{kind: "abi", hash, content_type}`, `{kind: "interface", hash, interface_id}`, or `{kind: "argument", hash, selectors}` when one argument authorizes several setters; only readings whose setter the row's `powers` hold. `coin_type` and `content_type` are numbers; an argument beyond 64 bits is served as a decimal string under `coin_type_decimal` / `content_type_decimal` | permission-event `selector`, `scope_detail.resource_selector` |
| `grant_event` | on resolver `/roles` rows: `{block_number, timestamp, transaction_hash, log_index}` of the earliest permission event that granted the row; omitted when unresolvable | permission-row `provenance.normalized_event_ids` |

An admitted controller-free ENSv1 numeric registration can retain its resource and token lifecycle
before its plaintext name is known. Such normalized events have no logical-name attachment.
Once admitted name evidence and the current ENSv1 authority establish a name binding, normal
name reads can use that registrar owner and expiry, and `owner` reports the registry owner proven
equal to it at disclosure until a later name-attached registry transfer replaces it. Earlier resource-only events remain unchanged;
name evidence alone does not reactivate a dormant registrar authority. See [storage semantics](storage.md)
for the numeric registration and restoration rules. This does not widen route or history coverage.
(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L130-L168 @ ens_v1@91c966f)

For a registrar lease first identified by a later readable observation, registration time remains the original numeric grant time. Compact product history omits only snapshots with both `state_derived=true` and `registrar_surface_snapshot=true`, before pagination and cursor validation. Diagnostics retains the marked snapshot at its later readable trigger; original resource-only history and all unmarked events remain unchanged. See [storage semantics](storage.md).

`GET /v1/permissions` and `GET /v1/addresses/{address}/names?include=role_summary`
read current permission rows and per-resource permission summaries. Canonical
identity checks exclude rows from an orphaned chain lineage. These routes capture
the project publication for each request and read the page, its counts and
permission summaries on one read-only `REPEATABLE READ` database snapshot of it.
Rows use the binding selected by Project. A later Interpret binding closure does
not change the published collection's membership or count; the next Project
publication installs the replacement or removal. Canonicality checks still apply.
A publication that lands between admission and the first read returns
`409 stale` asking to retry with the same cursor; a publication during the read
does not affect the page. Publication changes between pages do not invalidate
the cursor.
The base address-name collection remains available without the expansion.

An approved ENSv1 or Basenames registry `ApprovalForAll` row is effective for a
resource when its chain, emitter-derived registry contract address, and owner
match the resource's current
[registry-owner binding](glossary.md#registry-owner-binding). The read uses the
binding projected under the [registry-owner binding
rule](projections.md#permissions); it does not derive applicability again from
events. The matched row is returned with `grant_relation=operator`,
`grant_scope={"kind":"account","detail":{"chain_id":...,"authority_kind":"registry","authority_contract":...,"owner":...}}`,
and `powers=["registry_control"]`. Direct rows keep their existing wire shape
and omit `grant_relation`. `include=lineage` emits only the bare `lineage.grant={"kind":"event"}` marker; the binding evidence remains internal.
(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L17-L21 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L112-L118 @ ens_v1@91c966f)
(upstream: .refs/basenames/src/L2/Registry.sol:L46-L52 @ basenames@1809bbc)
(upstream: .refs/basenames/src/L2/Registry.sol:L148-L158 @ basenames@1809bbc)

The effective relation joins the account-approval family's `authority_contract`
to the composed registry-owner binding's `registry_contract`. The account row
also retains `authority_contract_instance_id` as admitted-instance evidence.
These agree because one admitted address on one chain maps to one contract
instance across manifest epochs, while a different watched address creates a
different registry generation. A prior generation's approval therefore stops
applying as soon as the current binding names another registry contract.
Revoked (`approved=false`) rows remain replayable projection state but are
served as absence.

Permission-backed v2 reads also classify the served resources from the typed
projection-owned per-resource permission summary, and report the permission
surfaces whose holders the rows do not list. When any surface is unlisted the
response carries `meta.completeness=partial`,
`meta.unsupported_reason=permissions_partially_listed`, and
`meta.unlisted_permission_surfaces`, a sorted list of short stable codes:

<!-- openapi:enum UnlistedPermissionSurface -->
| Code | Surface not listed |
| --- | --- |
| `ens_v2_registry_operators` | operators that the name's owner approved on the ENSv2 registry with `setApprovalForAll` |
| `registrar_approvals` | BaseRegistrar ERC-721 per-token and operator approvals |
| `resolver_approvals` | resolver operator approvals and per-name delegates |
| `wrapper_parent_control` | the parent name's control over a wrapped subname that is not emancipated |

The list shrinks as later parts of issue #605 add these surfaces; consumers
should read the list rather than infer gaps from the reason. A registrar- or
registry-held (unwrapped) registration reports
`["registrar_approvals","resolver_approvals"]`: registry `ApprovalForAll`
operators are rows, while registrar ERC-721 approvals and resolver approvals
and delegates are not. (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L108-L118 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L42-L50 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L78-L103 @ ens_v1@91c966f) An ENSv1 NameWrapper
registration reports `["resolver_approvals","wrapper_parent_control"]`: its token holder,
the owner-wide operators that holder approved, and its per-token approved
delegate are projected rows, while the parent name's control over a
non-emancipated wrapped subname and resolver operator/delegate approvals are
not enumerated as rows. The NameWrapper contract's `Ownable` owner is a
deployment-wide administrator (it sets the upgrade contract and metadata
service), not a per-registration permission, and is never a row.
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L565-L589 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L78-L103 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L162 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L186 @ ens_v1@91c966f)
An ENSv2 registry registration reports
`["ens_v2_registry_operators","resolver_approvals"]`. Its direct role holders
are rows. The ENSv2 registry also gives the owner's roles to every operator the
owner approved with `setApprovalForAll`, and those operators are not rows.
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L575-L592 @ ens_v2@a971bd64)
An ENSv2 registration has no BaseRegistrar token, so it never reports
`registrar_approvals`. It reports `resolver_approvals` because the ENSv2
`PublicResolverV2` authorizes the owner's operators and per-name delegates, and
those are not rows either.
(upstream: .refs/ens_v2/contracts/src/resolver/PublicResolverV2.sol:L51-L59 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/src/resolver/PublicResolverV2.sol:L174-L184 @ ens_v2@a971bd64)
A set of registrations reports the sorted union of its members' lists. A
summary that independently proves full coverage contributes nothing, and a
resource-bound read of it omits all three fields. Missing or indeterminate
support instead uses `meta.unsupported_reason=permission_support_unknown`
without a list and takes precedence.

A supplied `name` that is missing or unrecognized, whose current name is marked
unsupported, or that resolves to a current name not bound to a registration
resource returns an empty result relative to that request with `meta.completeness=partial` and
`meta.unsupported_reason=permission_support_unknown`. This establishes only
that the API could not select a supported current registration; it does not
establish that the name has no permission rows. A supplied current name paired
with an explicitly different `registration_id` is a supported empty
intersection. That zero-row result uses the explicitly requested registration's
support classification under the resource-bound rule—including the
wrapper list or `permission_support_unknown` when applicable—and
does not claim complete permission coverage. A `registration_id` outside an
explicit `namespace` instead returns an empty page without resource
restrictions or permission support metadata.

An address-only permissions read always reports all four codes,
including when it returns zero rows or its current page contains no wrapper
or ENSv2 resource, because returned registrations cannot establish the
request's full permission set.
For `include=role_summary`, any non-full resource summary makes the overall
address-name response `partial`, lists `role_summary` in
`meta.unsupported_fields`, and reports the same reason and surface list. Projected
permission rows remain visible, but an empty or populated expansion is not
authoritative when that metadata is present. A page containing both wrapper
and non-wrapper summaries reports the union of their lists; missing or
unrecognized summary metadata still takes precedence.

These classifications are request-relative. `/v1/permissions` continues to
serve known permission rows that apply to each resource, but those rows and the
derived role summaries are not authoritative enumerations while the coverage
described above remains partial. Zero returned rows therefore
do not prove that no account can mutate the selected name or registration.
NameWrapper holders, operators, and per-token delegates are projected rows.
Registrar ERC-721 approvals, resolver approvals and delegates, and parent
control of non-emancipated wrapped subnames remain unsupported. ENSv2 registry
operator approval also remains outside this slice.
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L575-L592 @ ens_v2@a971bd64)

`wrapper_fuses` has one stable shape inside the `ens_v1` object of name-shaped
rows (name detail, resolver `bound_names`, lookup detail, address names,
subnames, registry labels, `GET /v1/names` and search) and on permission rows:

```json
{
  "fuses": 196609,
  "cannot_unwrap": true,
  "cannot_burn_fuses": false,
  "cannot_transfer": false,
  "cannot_set_resolver": false,
  "cannot_set_ttl": false,
  "cannot_create_subdomain": false,
  "cannot_approve": false,
  "parent_cannot_control": true,
  "is_dot_eth": true,
  "can_extend_expiry": false
}
```

The booleans name the ten fuse bits declared by NameWrapper; mask constants and
the zero sentinel are not fuse booleans.
(upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L10 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L24 @ ens_v1@91c966f)
The word and booleans use the served block timestamp: when wrapper expiry is
earlier, `fuses` and every boolean are cleared. An expired plain wrapped name
keeps `wrapper_state="wrapped"` with the cleared summary; expired emancipated
and locked names expose neither wrapper field because NameWrapper also clears
their owner.
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L843 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L856 @ ens_v1@91c966f)
Each returned item either has both `wrapper_state` and `wrapper_fuses` or has
neither. Collection completeness remains request-relative: metadata on returned
permission rows does not make zero-row permission enumeration complete, so the
`meta.completeness` and unsupported-reason rules above still apply.

During `.eth` registrar grace, bigname keeps the existing approve-only policy
interpretation for projected wrapper holder and operator powers: it removes
owner modification and transfer powers except `approve` and `approve_wrapper`,
then still applies `CANNOT_APPROVE`. Upstream's `canModifyName` rejects
owner/operator modification during grace, while per-token `approve` routes
through the ERC-1155-fuse owner/operator authorization path rather than that
helper. This approve exception is bigname policy, not an upstream lifecycle
state. The approved delegate's `extend_subname_expiry` is removed in grace as
well, because `canExtendSubnames` applies the same grace check.
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L214 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L222 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L228-L238 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L37 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L47 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L127 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L135 @ ens_v1@91c966f)

### Resource restrictions

`restrictions` describes the [resource restrictions](glossary.md#resource-restrictions)
of a registration: constraints that bind the registration itself rather than
any one account. `GET /v1/permissions` returns it once at the envelope's
top level for a resource-bound read (`name` or `registration_id`), and
`GET /v1/addresses/{address}/names?include=role_summary` returns it on each row
next to `role_summary`. It is omitted when the registration has no
resource-level constraint model (ENSv1 registrar- and registry-held names,
Basenames), when a NameWrapper position has expired with a cleared owner, and
once the wrapped token is burnt or unwrapped: the block is served only while the
newest NameWrapper lifecycle evidence on the registration is the mint or a
holder grant, so `NameUnwrapped` removes it and so does the bare ERC-1155 burn
of the un-admitted `upgrade()` path, which emits no `NameUnwrapped`. An explicit
`registration_id` read of such a token returns no wrapper block, matching
`ens_v1.wrapper_state` on the name, which appears only for a current NameWrapper
registration.
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L483-L509 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1022-L1031 @ ens_v1@91c966f)
Name-shaped rows keep `ens_v1.wrapper_state` and `ens_v1.wrapper_fuses`;
`restrictions` adds to them and does not replace them.

```json
{
  "registration_id": "…",
  "kind": "ens_v1_wrapper",
  "wrapper_state": "locked",
  "wrapper_fuses": { "fuses": 196609, "cannot_unwrap": true, "…": "…" },
  "wrapper_expires_at": "1803859200"
}
```

- `kind=ens_v1_wrapper`: `wrapper_state` and `wrapper_fuses` use the same
  atomic, [expiry-effective](glossary.md#expiry-effective-namewrapper-fuse-word)
  contract as `ens_v1` on name detail; a burnt fuse removes the matching power from every
  holder, operator, and delegate row of the registration. `wrapper_expires_at`
  is the NameWrapper entry expiry as a decimal string of Unix seconds. Its
  contract-specific maximum is `null` with
  `wrapper_expires_at_reason: "no_expiry"`; a zero with no expiry set is `null`
  with reason `not_set`.
  Finite values omit the reason. For a wrapped `.eth` second-level name it is
  the registrar expiry plus `GRACE_PERIOD`, and the holder's modification
  powers already stop at the earlier grace boundary.
  (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L268-L277 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1082-L1089 @ ens_v1@91c966f)
- `kind=ens_v2_registry`: `locked_roles` lists the token-scoped registry roles
  `unregister`, `renew`, `set_subregistry`, `set_resolver`, and `transfer`
  whose assignment can no longer change. A role is granted or revoked on a
  registration only by an account whose admin roles, held on that registration
  or on the registry root, cover it; on a registration the settable roles are
  the held admin roles shifted down to their regular counterparts, so an admin
  role cannot be re-granted once every holder has revoked it, and
  `ROLE_CAN_TRANSFER_ADMIN`, checked only on the token owner, has no lower
  role at all. `locked_roles` is therefore the set of those roles whose admin
  counterpart (`can_transfer_admin` for `transfer`) no current permission row on
  the registration or its root carries; whether the role itself is still held
  is read from the rows. An empty list means every one of them can still change.
  (upstream: .refs/ens_v2/contracts/src/access-control/EnhancedAccessControl.sol:L418-L424 @ ens_v2@a971bd64)
  (upstream: .refs/ens_v2/contracts/src/access-control/EnhancedAccessControl.sol:L453-L455 @ ens_v2@a971bd64)
  (upstream: .refs/ens_v2/contracts/src/access-control/libraries/EACBaseRolesLib.sol:L31-L34 @ ens_v2@a971bd64)
  (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L560-L572 @ ens_v2@a971bd64)
  (upstream: .refs/ens_v2/contracts/src/registry/libraries/RegistryRolesLib.sol:L24-L45 @ ens_v2@a971bd64)

### Permission powers vocabulary

`powers` (on `GET /v1/permissions` rows, `include=role_summary` grants, and
lineage objects) is a list of snake_case names drawn from four producers. The
table is the complete vocabulary the code can serve; a test
(`documented_powers_vocabulary_matches_code` in
`apps/api/src/v2/permission_values.rs`) fails when this table and the producing
source files disagree. Names are listed once even where two producers share
them.

- **ENSv1 and Basenames projected control** (adapters
  `crates/adapters/src/schema_v2/protocol/v1/*`, grant family
  `crates/project/src/families/permissions.rs`). Registrar- and registry-held
  ENSv1 and Basenames names receive only these two powers. Storage spells the
  first `resource_control`; the API renames it `registration_control`.
- **ENSv1 and Basenames effective registry operators** (adapter
  `crates/adapters/src/schema_v2/protocol/standard_approvals.rs`, storage
  effective-permission readers). A registry `ApprovalForAll` operator row
  carries `registry_control` alone, applied at read time to the registrations
  the approving account currently owns in that registry.
  (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L108-L118 @ ens_v1@91c966f)
- **ENSv1 NameWrapper fuse vocabulary** (read-time mask in
  `crates/storage/src/families/control/permissions/grants.rs`). The permission
  reader recognises these names and removes each one from a wrapped name's effective powers when
  the corresponding NameWrapper fuse is burnt, evaluated with the
  [expiry-effective](glossary.md#expiry-effective-namewrapper-fuse-word) fuse
  word. The NameWrapper interpreter
  (`crates/adapters/src/schema_v2/protocol/v1/wrapper/permissions.rs`, constant
  `WRAPPER_HOLDER_POWERS`) grants the token holder `registration_control`,
  `set_resolver`, `set_ttl`, `create_subnames`, `transfer`, `unwrap`,
  `burn_fuses`, `approve`, `extend_subname_expiry`, and `extend_expiry` on the
  registration and `resolver_control` on the linked resolver, because the
  holder and any owner-wide operator pass `canModifyName`, the ERC-1155-fuse
  approve and transfer checks, and `canExtendSubnames`; Project copies the
  holder's masked set to each operator, and the per-token approved delegate
  receives `extend_subname_expiry` alone, the only check that consults
  `getApproved`. Two of the holder powers are gated on a fuse being burnt rather
  than unburnt: `burn_fuses` requires `PARENT_CANNOT_CONTROL`, because
  `_canFusesBeBurned` rejects every owner-controlled burn until the parent has
  emancipated the name and the holder cannot burn that parent-controlled bit
  itself, and `extend_expiry` requires `CAN_EXTEND_EXPIRY`.
  The alias spellings stay recognised by the mask but no interpreter emits them.
  (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L10-L16 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L214-L238 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L421-L437 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L443-L470 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1058-L1068 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L37-L47 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L137-L150 @ ens_v1@91c966f)
- **ENSv2 role bitmaps** (adapters
  `crates/adapters/src/schema_v2/protocol/permissions.rs` and
  `v2_record_resolver/permissions.rs`). `EACRolesChanged` bitmaps are decoded
  bit by bit; each name is the pinned upstream `ROLE_<NAME>` constant in
  lower snake case, and `admin_<name>` is `ROLE_<NAME>_ADMIN`, the same bit
  shifted by 128. Unknown bits are omitted rather than surfaced under invented
  names, and so are bits 28 and 156 of the 2026-06-29 resolver generation,
  whose role is no longer interpreted ([upstream](upstream.md)).
  (upstream: .refs/ens_v2/contracts/src/registry/libraries/RegistryRolesLib.sol:L7-L63 @ ens_v2@a971bd64)
  (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/libraries/PermissionedResolverLib.sol:L7-L64 @ ens_v2_sepolia_20260629@ccaeb58)
  (upstream: .refs/ens_v2_sepolia_20260903/contracts/src/resolver/libraries/PermissionedResolverLib.sol:L10 @ ens_v2_sepolia_20260903@5da83f6a)

<!-- powers-vocabulary:start -->
<!-- openapi:enum PermissionPower -->
| Power | Producer | On-chain role or condition |
| --- | --- | --- |
| `registration_control` | ENSv1/Basenames control | Storage `resource_control`. Held by the account that controls the registration's authority object: the registrar token owner (`RegistrationGranted`, registrar `Transfer`), the registry owner of a registry-only resource (`NewOwner`/`Transfer`), or the NameWrapper token holder (`TokenControlTransferred`). Masked away while the wrapper is `locked`. |
| `resolver_control` | ENSv1/Basenames control | Held by the same account, scoped to the registration's current nonzero resolver (`grant_scope.kind = resolver`); revoked and re-granted on `ResolverChanged` and `RegistrationGranted`. Masked by `CANNOT_SET_RESOLVER` (8). |
| `registry_control` | ENSv1/Basenames registry operator | Held by an operator the registry owner approved through registry `ApprovalForAll`, on rows with `grant_relation = operator` and `grant_scope.kind = account`; applies only while the approving account owns the registration in that registry and the approval stands. |
| `set_resolver` | ENSv2 registry; wrapper mask | Registry `ROLE_SET_RESOLVER` (bit 24). On a wrapped ENSv1 name the mask removes it under `CANNOT_SET_RESOLVER` (8). |
| `set_ttl` | wrapper mask | Removed under `CANNOT_SET_TTL` (16). |
| `create_subnames` | wrapper mask | Removed under `CANNOT_CREATE_SUBDOMAIN` (32). |
| `create_subdomain` | wrapper mask | Alias of `create_subnames`; removed under `CANNOT_CREATE_SUBDOMAIN` (32). |
| `transfer` | wrapper mask | Removed under `CANNOT_TRANSFER` (4). |
| `transfer_name` | wrapper mask | Alias of `transfer`; removed under `CANNOT_TRANSFER` (4). |
| `unwrap` | wrapper mask | Removed under `CANNOT_UNWRAP` (1). |
| `burn_fuses` | wrapper mask | Removed under `CANNOT_BURN_FUSES` (2) and whenever `PARENT_CANNOT_CONTROL` (65536) is not burnt: `setFuses` routes through `_canFusesBeBurned`, which rejects any owner-controlled burn unless both `PARENT_CANNOT_CONTROL` and `CANNOT_UNWRAP` are set, and the holder cannot burn the parent-controlled bit, so on a `wrapped` name every non-zero `setFuses` reverts; once the parent has burnt `PARENT_CANNOT_CONTROL` the holder may burn `CANNOT_UNWRAP` together with other fuses. |
| `approve` | wrapper mask | Removed under `CANNOT_APPROVE` (64); retained during `.eth` registrar grace (policy, see above). |
| `approve_wrapper` | wrapper mask | Removed under `CANNOT_APPROVE` (64); retained during `.eth` registrar grace (policy, see above). |
| `extend_subname_expiry` | ENSv1 NameWrapper | Held by the holder, each operator, and the per-token approved delegate: may extend a wrapped subname's expiry up to the parent's own expiry (`canExtendSubnames`). No fuse removes it; the `.eth` registrar grace boundary does. |
| `extend_expiry` | ENSv1 NameWrapper; wrapper mask | Held by the holder and each operator only while `CAN_EXTEND_EXPIRY` (262144) is burnt: `extendExpiry` lets the name's own controller (`canModifyName`) extend the name's expiry up to the parent's expiry. Removed while the fuse is unburnt and during `.eth` registrar grace. |
| `registrar` | ENSv2 registry | `ROLE_REGISTRAR` (bit 0): may register names. |
| `register_reserved` | ENSv2 registry | `ROLE_REGISTER_RESERVED` (bit 4). |
| `set_parent` | ENSv2 registry | `ROLE_SET_PARENT` (bit 8). |
| `unregister` | ENSv2 registry | `ROLE_UNREGISTER` (bit 12). |
| `renew` | ENSv2 registry | `ROLE_RENEW` (bit 16). |
| `set_subregistry` | ENSv2 registry | `ROLE_SET_SUBREGISTRY` (bit 20). |
| `was_reserved` | ENSv2 registry | `ROLE_WAS_RESERVED` (bit 32): a token-only, non-revocable history marker that the name was registered through `ROLE_REGISTER_RESERVED`. It authorizes nothing; it is retained so a marker-only `EACRolesChanged` stays visible. |
| `set_uri` | ENSv2 registry | `ROLE_SET_URI` (bit 36). |
| `can_name` | ENSv2 registry; ENSv2 resolvers | `ROLE_CAN_NAME` (bit 120). |
| `upgrade` | ENSv2 registry; ENSv2 resolvers | `ROLE_UPGRADE` (bit 124). |
| `can_transfer_admin` | ENSv2 registry | `ROLE_CAN_TRANSFER_ADMIN` (bit 156, `(1 << 28) << 128`). |
| `admin_registrar` | ENSv2 registry | `ROLE_REGISTRAR_ADMIN` (bit 128). |
| `admin_register_reserved` | ENSv2 registry | `ROLE_REGISTER_RESERVED_ADMIN` (bit 132). |
| `admin_set_parent` | ENSv2 registry | `ROLE_SET_PARENT_ADMIN` (bit 136). |
| `admin_unregister` | ENSv2 registry | `ROLE_UNREGISTER_ADMIN` (bit 140). |
| `admin_renew` | ENSv2 registry | `ROLE_RENEW_ADMIN` (bit 144). |
| `admin_set_subregistry` | ENSv2 registry | `ROLE_SET_SUBREGISTRY_ADMIN` (bit 148). |
| `admin_set_resolver` | ENSv2 registry | `ROLE_SET_RESOLVER_ADMIN` (bit 152). |
| `admin_set_uri` | ENSv2 registry | `ROLE_SET_URI_ADMIN` (bit 164). |
| `admin_can_name` | ENSv2 registry; ENSv2 resolvers | `ROLE_CAN_NAME_ADMIN` (bit 248). |
| `admin_upgrade` | ENSv2 registry; ENSv2 resolvers | `ROLE_UPGRADE_ADMIN` (bit 252). |
| `set_addr` | ENSv2 resolvers | `ROLE_SET_ADDR` (bit 0 of the resolver bitmap). |
| `set_text` | ENSv2 resolvers | `ROLE_SET_TEXT` (bit 4). |
| `set_contenthash` | ENSv2 resolvers | `ROLE_SET_CONTENTHASH` (bit 8). |
| `set_pubkey` | ENSv2 resolver (20260629) | `ROLE_SET_PUBKEY` (bit 12). |
| `set_abi` | ENSv2 resolvers | `ROLE_SET_ABI` (bit 16; bit 12 on the record resolver). |
| `set_interface` | ENSv2 resolvers | `ROLE_SET_INTERFACE` (bit 20; bit 16 on the record resolver). |
| `set_name` | ENSv2 resolvers | `ROLE_SET_NAME` (bit 24; bit 20 on the record resolver). |
| `clear_records` | ENSv2 resolver (20260629) | `ROLE_CLEAR` (bit 32): may clear a name's records. |
| `set_data` | ENSv2 resolvers | `ROLE_SET_DATA` (bit 36; bit 24 on the record resolver). |
| `link` | ENSv2 record resolver | `ROLE_LINK` (bit 28). |
| `admin_set_addr` | ENSv2 resolvers | `ROLE_SET_ADDR_ADMIN` (bit 128). |
| `admin_set_text` | ENSv2 resolvers | `ROLE_SET_TEXT_ADMIN` (bit 132). |
| `admin_set_contenthash` | ENSv2 resolvers | `ROLE_SET_CONTENTHASH_ADMIN` (bit 136). |
| `admin_set_pubkey` | ENSv2 resolver (20260629) | `ROLE_SET_PUBKEY_ADMIN` (bit 140). |
| `admin_set_abi` | ENSv2 resolvers | `ROLE_SET_ABI_ADMIN` (bit 144; bit 140 on the record resolver). |
| `admin_set_interface` | ENSv2 resolvers | `ROLE_SET_INTERFACE_ADMIN` (bit 148; bit 144 on the record resolver). |
| `admin_set_name` | ENSv2 resolvers | `ROLE_SET_NAME_ADMIN` (bit 152; bit 148 on the record resolver). |
| `admin_clear_records` | ENSv2 resolver (20260629) | `ROLE_CLEAR_ADMIN` (bit 160). |
| `admin_set_data` | ENSv2 resolvers | `ROLE_SET_DATA_ADMIN` (bit 164; bit 152 on the record resolver). |
| `admin_link` | ENSv2 record resolver | `ROLE_LINK_ADMIN` (bit 156). |
<!-- powers-vocabulary:end -->

`set_records` is not in this vocabulary: it appears only in bigname's own API
test fixtures for `record_manager` grants, and no interpreter emits it. A
consumer that saw it came from a fixture, not from chain data. Names ending in
`_resource` or containing `resource_` other than `resource_control` are storage
vocabulary that the API refuses to serve.

Rules:

- Every public timestamp is a decimal string of Unix seconds, including lookup, history, metadata, diagnostics, status, and health responses. Clock times are floored to whole seconds; finite expiry and grace values retain every digit. See [Timestamp format and absent expiry](#timestamp-format-and-absent-expiry).
- JSON map keys are strings (`"60"`, `"8453"`); `chain_id` as an object field
  is a JSON number.
- `token_id` stays a decimal string.
- Pipeline vocabulary (`projection`, `sidecar`, `manifest`, `normalized event`,
  `raw fact`, table names) must not appear in product-route field names, enum
  values, or error messages. The documented exception is the `kind` string on
  `include=raw` event rows, which carries the raw storage event kind behind an
  explicit opt-in.

## Envelope

One success shape applies to every route:

```json
{
  "data": {},
  "page": {
    "cursor": null,
    "next_cursor": "opaque-token",
    "page_size": 50,
    "total_count": 123,
    "has_more": true
  },
  "meta": {
    "as_of": {
      "1": {
        "block_number": 19000000,
        "block_hash": "0x...",
        "timestamp": "1781049600"
      }
    },
    "as_of_completeness": {
      "8453": {
        "completeness": "unsupported",
        "unsupported_reason": "temporarily_unavailable"
      }
    },
    "as_of_token": "opaque-token",
    "completeness": "partial",
    "unsupported_fields": ["role_summary"],
    "unsupported_reason": "not_supported_for_namespace",
    "source": "indexed"
  }
}
```

Rules:

- `data` is an object on single-resource routes and an array on collections.
- Top-level `page` appears on collection routes only. Per-input pagination on
  `POST /v1/lookup` and the nested resolver-overview `bound_names` collection
  use the same object inside their containing result/object.
- `total_count` is nullable. Reverse address results from `POST /v1/lookup`
  populate it by counting the same readable current name/address rows used by
  the page query when the requested relation set maps directly to a stored role
  group. Relation sets that require post-filtering retain `total_count=null`;
  use the address-name GET collection for an exact single-relation count.
  Address-name ownership collections return the exact count of their filtered,
  deduplicated entries before the cursor.
  Anchored history collections (name history, address history, and
  `/v1/events` with a `name`, `registration_id`, `address`, or `resolver`
  anchor) populate
  it with a capped count over the page's exact filters: exact up to 10,000
  product-visible rows and `null` beyond by default; `include=total_count`
  requests an exact uncapped count on anchored history. On name history with
  `include=child_registrations` the count covers the name's rows and its direct
  child registration rows together, each event once. It is always `null` for unanchored event
  reads. `GET /v1/names/{name}/subnames` populates it with the parent's direct
  readable subname count, applying the same optional prefix and expiry filters
  as the page before its cursor.
  The registry overview's nested `referenced_by` page populates it only with
  `include=counts`.
  Other routes populate it only where a precomputed count makes it
  cheap or where they explicitly document `include=total_count`; they must not
  otherwise run unconditional full counts on the request path.
- `meta` is always present. Single-resource routes that read chain-derived state
  include `meta.as_of` and `meta.as_of_token` when they can attribute at least
  one served snapshot-pinned chain position. Product name, subname, ownership,
  and permission collections disclose `meta.as_of` and omit `meta.as_of_token`
  because old publications are not retained for collection replay. Their
  [current-state list cursors](glossary.md#current-state-list-cursor) hold a
  position that each continuation reads from the current publication. Each page
  is read on one database snapshot of the publication `meta.as_of` reports; a
  publication that lands before the page's first read asks to retry with the
  same cursor (see [current-state list cursors](#current-state-list-cursors)). History collections (`/v1/events`, name history, and address
  history) disclose in `meta.as_of` the publication captured when the request
  was admitted and do not bind cursors to it; see [Cursors And Pagination](#cursors-and-pagination).
  `/v1/search` reports request-scoped `meta.as_of` as
  staleness attribution without a publication-bound cursor. Diagnostic event
  collections retain their separately documented latest-state behavior.
  Control-plane routes (`/v1/status`, `/v1/namespaces/{namespace}`) omit both.
  Verified name and record responses keep the same metadata shape as their
  indexed peers. The authoritative position identifies the projection snapshot
  admitted for the lookup. For a cross-chain path, the auxiliary position is
  the canonical execution position retained by that projected row, even when it
  is older than the newest `chain_heads` marker for that chain. The lookup
  engine returns both positions, and `meta.as_of`/`meta.as_of_token` expose
  those actual lookup positions rather than implying execution at the newer
  marker. The engine independently requires a live
  [family publication](glossary.md#family-marker) within the
  [publication lag tolerance](glossary.md#publication-lag-tolerance) of the stored head before executing, and that publication is the
  authoritative position: calls on its chain run at its block. After the live calls it revalidates the
  exact project generation, projected name topology, selected manifest
  declarations, and canonical positions. A concurrent replacement returns the
  existing `409 stale` response and performs no ledger mutation. `meta.as_of` is
  human-readable staleness attribution on routes
  that provide it. `meta.as_of_token` is opaque and is the value to pass to
  `at` when a route supports snapshot replay. `meta.completeness`,
  `meta.unsupported_fields`, and `meta.unsupported_reason` appear only when the
  read is not clean. `meta.source` appears when the route supports `source`.
- Public reverse requests to `POST /v1/lookup` and all `GET /v1/search`
  requests disclose the chain scope selected by the request. A public reverse
  request or search without an explicit `namespace` accounts for the chains of
  every active public namespace; an explicit search namespace accounts only
  for that namespace's chains. Name-only lookup retains its inferred or
  explicit namespace scope. A chain with a readable position appears under
  `meta.as_of`. A chain suppressed by the deployment-readiness check instead
  appears under `meta.as_of_completeness` as
  `{completeness:"unsupported", unsupported_reason:"temporarily_unavailable"}`.
  The two maps have disjoint keys, and their union is exactly the request's
  chain scope; returned rows never reduce that denominator. The sibling map is
  omitted when no in-scope chain is suppressed. A suppressed chain is not added
  to `meta.as_of_token` solely for disclosure. In a mixed lookup batch, the
  token can still contain that chain when another input actually uses its
  snapshot position; the suppression entry takes precedence over a
  human-readable `meta.as_of` entry because some requested data was withheld.
  That precedence is the general rule for every route that emits these maps:
  whenever one chain is both readable in one part of the request's scope and
  suppressed in another, the suppression entry wins and the chain is removed
  from `meta.as_of`.
- `meta.unsupported_fields` names response-level sections or expansions the
  route could not serve. Record-level `unsupported_fields` names data fields
  the index could not prove for that record. One unsupported field is not
  duplicated at both levels in one response.
- There is no `meta` query parameter and no stripped envelope variant.
- There are no `declared_state`/`verified_state` parallel trees and no `both`
  mode.

## Field Budgets

`include` is a route-documented expansion allowlist. It may add documented
sections or route-documented expensive metadata. No route supports
`include=total_count` unless that route's parameter list says so.

`profile=feed` on `POST /v1/lookup` is a field budget over the same record
shape used by `profile=detail`. Feed returns fewer fields; every feed field has
the same name, type and value as its detail counterpart. Beyond identity,
`chain_id`, `network`, `status`, `subregistry` on name results, reverse
`is_primary`/`relations` and `resolution` on `resolves_to` rows, feed carries `expires_at`,
`expires_at_reason`, `grace_ends_at` and `ens_v1`, so a consumer can render
expiry and grace without a second request; the other registration, resolver
and record fields are detail-only. Feed does not change reverse
lookup pagination semantics: `cursor`, `page_size`, `next_cursor`, and
`has_more` mean the same thing as detail.

Flat record optional fields are omitted when there is no backed value. Routes
do not serialize permanently-null placeholders for optionals such as `manager`.
Known-empty key lists and maps inside a detail record's grouped `records`
serialize as `[]` and `{}`; omission of `records` means the name serves no
resolver records from the served source.
Rows classified as `registration_status=unregistered`, including ownerless
ENSv2 reservations, have no current registration, so product name detail and
batch lookup always omit `registration_id`. Resolver and record fields are also
omitted, the records route exposes no resolver, record values, or audit-only
inventory (without `keys` its `records` is `{}`), and resolver `bound_names` omits the row unless it carries an
event-linked registry resolver pointer (a
[serving resource](glossary.md#serving-resource)): an ownerless ENSv1 or
Basenames registry row whose current registry resolver pointer is retained, or
an ENSv2 TLD whose current
[root-registry resolver pointer](glossary.md#root-registry-resolver-pointer)
survives while the TLD's authority is not projected. That pointer permits
resolver and record reads without acquiring registration identity or control;
the TLD row keeps `current_authority_not_projected` wherever a route reports
its coverage. Indexed records require retained, supported inventory;
routes with source selection let verified and auto records follow the ordinary
lookup capability, and resolver `bound_names` remains subject to the resolver
family's binding-enumeration capability.
See [registration status](#status-vocabulary) for the upstream basis.
The ENSv2 rule is an intentional product
narrowing: ENSv2 stores a nonzero resolver supplied for an ownerless
reservation and returns it until expiry. A root-registry TLD reservation is the
one exception: its pointer is served as a
[root-registry resolver pointer](glossary.md#root-registry-resolver-pointer). (upstream:
.refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L255-L258 @
ens_v2@a971bd64) (upstream:
.refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L461-L478 @
ens_v2@a971bd64)

## Tiers

### Tier 1: Lookup Primitives

Lookup primitives serve the partner latency path and current indexing status:

- `POST /v1/lookup`
- `GET /v1/status`

The lookup route uses the common record shape and in-band per-result statuses.
`GET /v1/status` is the only route with the ops status vocabulary
`ready`, `degraded`, `stale`. It reads the chain set from
`bigname_phase.chain_heads` and `bigname_phase.chain_phase_state`. The stored
head and finality fields come from `chain_heads`; indexed progress is the
`project` phase's most recent completed publication. Readiness also uses that
phase's lifecycle state and redo marker, the Interpret `redo_in_progress`
marker, and newest per-chain heartbeat in
`service_heartbeats`. A phase row that startup settled while its chain was
unconfigured is not eligible for `ready` until genuine phase completion or
completed-state revalidation clears that marker. It reports `degraded` unless
a stronger `stale` condition applies, such as a genuinely failed phase or an
expired heartbeat. Ethereum Sepolia readiness requires its `ingest`
phase to remain `completed` and its
[verification](glossary.md#verification-level) phase to be `completed` with
a known level at or above the `quick_synced` floor: `quick_synced`, `cross_checked`, and `node_checked` qualify, while an unknown stored level fails closed.
A failed Ingest or Verify, or an ordinary completed Verify without that
evidence, maps to `stale`. An idle, running, paused, or missing Ingest or Verify
maps to `degraded`. An expired runner heartbeat remains `stale` while either
required phase is incomplete. Chains without this requirement omit those
Ingest and Verify evidence checks. A
failed Project or expired heartbeat maps to `stale`; an active Interpret redo
or a paused, redoing, or missing-heartbeat Project maps
to `degraded`. A running Project with a completed publication remains eligible
for `ready` when its block and time lag are within the configured thresholds,
its interpreter content hash matches this API build, and a same-height
publication has the stored head's exact block hash. These checks read the
[family marker](glossary.md#family-marker), which must also be `live`, on
readable lineage, and at most the
[publication lag tolerance](glossary.md#publication-lag-tolerance) behind the
stored head (one block by default; see below). The marker
supplies `indexed_block` and its timestamp, including the lags computed from
them. Project lifecycle state and redo flags still come from the phase rows.
While an Interpret or Project redo is in progress, `lag_blocks` and
`lag_seconds` are `null`, because lag is unknown during a redo.

A generation mismatch or
running without a completed publication is `degraded`. The schema-v2 project phase has no
invalidation queue or dead-letter table, so the retained response fields map
to `pending_invalidation_count=0`,
`pending_invalidation_count_capped=false`, and `dead_letter_count=0`.
Cached network-head comparison evidence is unchanged. Provider refresh runs
asynchronously under a timeout and cache TTL, so the route never waits for a
provider. A failed latest refresh degrades readiness immediately while keeping
the last successful head comparison visible as cached evidence.

### Tier 2: Product Reads

Product routes serve app and public read workflows. They must use only product
vocabulary in field names, enum values, and error messages. Product routes may
expose simplified `completeness`, `unsupported_fields`, and per-item `status`,
but they must not expose pipeline internals.

The product-route denylist includes pipeline terms such as `projection`,
`sidecar`, `manifest`, `normalized event`, `raw fact`, storage table names,
`logical_name_id`, `resource_id`, `token_lineage_id`,
`surface_binding_id`, `binding_kind`, `normalized_event_id`,
`raw_fact_refs`, `manifest_versions`, `derivation_kind`,
`exhaustiveness`, `enumeration_basis`, `source_classes_considered`, and the
`execution_checkpoint` pseudo-chain slot. If a product capability needs that
detail, it belongs on a diagnostics route instead.

`GET /v1/names/{name}?source=verified` and
`GET /v1/names/{name}/records` with a verified source execute through the
schema-v2 lookup engine on every request. Response fields and per-record status
meaning stay unchanged, but there is no reusable outcome, durable execution
trace, or execution-cache readback. The engine refuses a name whose selected
[authority arm](glossary.md#authority-epoch) is outside the
`verified_authority_arms` the selected `ens_execution` manifest declares
(`manifests.md` § `verified_authority_arms`); the routes report that refusal as
`exact_name_authority_not_verifiable`, distinct from the
`verified_records_not_supported` a row without an admitted topology reports. API requests, including diagnostics and verified fallback, are read-only.
They never create, refresh, or clear the diagnostic
[resolution divergence ledger](glossary.md#resolution-divergence-ledger).
After provider calls, verification rechecks the captured publication, canonical
positions, and manifest authority in a fresh read-only snapshot; concurrent
changes keep their existing stale rejection. For cross-chain resolution, the
selected product snapshot must admit
the current authoritative position and include the execution chain, while the
canonical projected row supplies the exact hash-pinned execution position. The
response metadata reports that actual position, which may be older than the
generic auxiliary checkpoint initially selected by the route, but never newer;
a position at the same height must have the same block hash. A newer or
same-height incompatible position makes the verified answer stale before any
provider call or ledger write. The current
lookup engine does not replay historical `at`, `safe`, or `finalized`
authoritative execution: if the selected product snapshot does not admit the
engine's authoritative position, the captured family publication, the verified section is
`stale` rather than being executed at a different authoritative position.
Provider connect, DNS, TLS, connection-reset, and other transport failures
abort a verified name or record request with `500 internal_error`; they are not
reported as selector-local stale answers, and `source=auto` does not return a
partial blend after such a failure.
Explicit record `keys` and the inventory-derived default verified selector set
are each limited to 200 keys. An oversized server-derived set returns `422
unsupported` before provider execution; the compact records caller can narrow
the request with `keys`. For the verified flat name-profile, the limit applies
before its synthetic `addr:60` request is added, so a 200-selector inventory
may issue 201 provider keys when the primary-address selector was absent; more
than 200 inventory-derived selectors still returns the same error.

`GET /v1/addresses/{address}/primary-name` keeps its documented `answers` and
typed `verification` shapes. Indexed answers read the reverse-claim families
at their publication. The request's `source` parameter narrows the answer list.
A successful stored raw claim is normalized for the indexed product name even when its raw
spelling was not already normalized. The verified producer is a fresh ENS/60
lookup whose reverse and forward calls execute at the block of the Ethereum
[family publication](glossary.md#family-marker) it captures, which may trail
the stored head by the
[publication lag tolerance](glossary.md#publication-lag-tolerance); the
`ens_execution` authority arms the forward gate admits are read from the
manifest selected at that block. It applies the raw-claim
normalization gate before forward resolution and persists neither a legacy
execution outcome nor a divergence row. When `source` is omitted, the route
returns the indexed and verified answers together only if the selected family
publication, its position and generation, matches that lookup's position before
verified execution and remains unchanged after reading the indexed tuple from
that source; otherwise the whole
request returns `409 stale` instead of assigning answers from different
positions to one `meta.as_of`. The indexed answer depends only on the projected
tuple: a live reverse claim or live lookup failure changes only the verified
answer. Other verified primary-name tuples are explicit `unsupported`; indexed
answers remain available where their projection supports the requested tuple.
Provider transport failures abort this route with `500 internal_error` rather
than producing a verified answer entry with `status=stale`.
The post-call guard also revalidates the selected Ethereum publication generation and the
selected ENS manifest declarations: the ENS registry, the Universal Resolver,
and, when the profile declares one, the `default.reverse` registrar. A
concurrent replacement of any of them, including the `default.reverse`
registrar declaration, returns `409 stale` and no verified answer.

Verified lookup captures a live family publication before provider execution.
The post-call guard compares its block identity, interpreter content hash and
marker sequence, alongside the stored head it was admitted against and the
manifest declarations. A
new publication or same-height republish returns `409 stale`. An Interpret or
Project redo whose range overlaps that publication also refuses the lookup.
The ledger transaction holds shared locks on both phase rows and the family
marker through its commit, so an overlapping redo cannot start between the
check and the write. An unrelated phase-row update alone does not change the
publication generation. Routes combining indexed and verified answers also
require both answers to fit the reported `meta.as_of` position. Verified record
and primary-name calls on the publication's chain execute at the publication's
block, which is the position `meta.as_of` reports, so a publication trailing the stored head within
the publication lag tolerance serves both answers. A selection that is not the
publication, such as an older `safe` or `finalized` position or an `at` before
or after the publication's block, still reports the verified section `stale`.

Indexed snapshot selection uses the [family marker](glossary.md#family-marker).
It must be `live`, carry this build's interpreter content hash, sit on readable
lineage and trail the stored head by at most the publication lag tolerance:
`BIGNAME_API_PUBLICATION_LAG_TOLERANCE_BLOCKS`, one block by default, never
negative, and one count for every chain. A `bootstrap_pending` marker means a
rebuild is still populating the families, and reads return `409 stale`. When
the publication trails the requested `head`, `safe` or `finalized` position
within that tolerance, the route reports the publication in `meta.as_of`. A
publication further behind, ahead of the stored head, from another interpreter
generation or on an orphaned fork is unavailable. A tolerance above the `/v1/status` thresholds
means status can report `stale` while reads are still served.

The API captures and rechecks the marker's `sequence` around indexed reads.
Collection cursors carry no publication generation. Each current-state page
captures its own publication and checks it again on its read snapshot before
the first read; later pages read the then-current rows after the cursor
position. See [current-state list cursors](#current-state-list-cursors)
for explicit `at` pins and legacy cursor compatibility.

Collection expiry filters, including `include_expired=false`, use the published
block's timestamp on every page; a multi-chain scope uses the earliest selected
publication timestamp. Lookup composes name topology and indexed comparisons
in one repeatable-read snapshot. Its guarded writer checks that captured
publication and the actual execution manifests in the same transaction that
writes or clears a divergence.

These routes read [composed name rows](glossary.md#composed-name-row). Name detail (`GET /v1/names/{name}` and the name
diagnostics), `GET /v1/search`, the expiring listing of `GET /v1/names` and a
resolver's bound names (`GET /v1/resolvers/{chain_id}/{address}`) serve them
whole. The records and address routes read their own rows from the families
too: the record inventory of `GET /v1/names/{name}/records` (default keys,
indexed answers and `include=inventory`, read at the publication only, so an
`at` below it answers `409 stale`), the same inventory for name detail's
grouped `records` (`GET /v1/names/{name}`, both sources) and for
`GET /v1/diagnostics/names/{name}/records`, the address-name relations of
`GET /v1/addresses/{address}/names`, recomputed at read from the address index
and the composed names, its `relation=resolves_to` pages (both the exact coin
type and `coin_type=evm`) from the node-keyed and record-ID inverse address indexes, the record counts of
`include=counts`, and the indexed primary-name claim of
`GET /v1/addresses/{address}/primary-name`. Batch lookup identity records,
address relations, inventory readback, and verified lookup inputs use those same
family publications. Reverse address pages and their exact counts in `POST /v1/lookup`
also read the address index and primary claims from the families. Candidate keys
are sought in bounded batches before composing names and applying relation masks.
Primary claims, page membership, counts, and returned inventories share one
repeatable-read snapshot over the route's selected authority chains. Exact counts
visit all matching candidates; page-only relation scans stop at the page limit
and overflow row. Primary-first ordering, role ranking, filters, and cursors are
unchanged. History and event routes retain their event sources and join composed
name rows: `GET /v1/names/{name}/history` (whether the name exists),
`GET /v1/events`, `GET /v1/diagnostics/events` and
`GET /v1/addresses/{address}/history` (each event's name). Each composed read sees one committed family
block, so a row never mixes two blocks. A composed row describes the publication
and has no older position of its own, so an `at` below the publication answers
`409 stale` with "requested snapshot is not available for name". While a family rebuild is
in flight (the marker is not `live`, carries another build's hash, or sits on a
block a reorg orphaned) no composed row is served: a route whose fence has not
already refused answers `409 stale` with "requested snapshot is not available
for" its resource, including when the rebuild starts after the fence passed. A
composed read that finds no name to compose (a name with no surface, an empty
bound-name or expiring walk) still reads its chains' markers, so a rebuild
answers `409 stale` rather than `404` or an empty page, and the expiring listing
composes only names of the requested namespace, so another namespace's rebuild
does not refuse it.

Composed names also refuse a publication while Interpret or Project has a redo
whose range overlaps it. Interpret's normalization-flag recompute can change a
surface's visibility before the required Project replay publishes the new name
state; finishing Interpret alone does not make the old publication readable.
The composed reader and the collection's generation check on its read snapshot
both enforce this rule. Diagnostic reads that do not compose published names
keep their existing snapshot selection. Event diagnostics still return their audit rows
when the name publication is unavailable, omitting the optional name attachment.
Resolver bound-name pages require the selected family publication even when the
page is empty, so an older `at` answers
`409 stale` rather than reporting current absence as historical absence.

`GET /v1/permissions` and the resolver routes also serve
their own rows from the families. The permission rows, the registry operator
rows and each registration's authority context and restrictions are built at
read from the grants, approvals and registry bindings the families keep, masked
at the published block's time. Permission pages seek and compose bounded permission-key
batches after the cursor, applying namespace membership before checking those
resources' publications. Address `include=role_summary` uses the same bounded reader
with its existing 1000-row inline-expansion limit. The resolver overview (`GET
/v1/resolvers/{chain_id}/{address}`, including whether it lists bound names)
and whether its `/links` and `/roles` collections are supported come
from the families' resolver classification, and those collections list the
families' rows, with the names each row joins read from composed rows. History
attribution through a resolver's classification reads the same classification.
Its bounded pointer walk checks the relevant chains' classification publications
in the same database snapshot; a partial rebuild returns `409 stale`, while a
completed newer publication does not invalidate the bounded history walk. This
classification check does not add a redo gate to raw audit diagnostics.
A role holder's `grant_event` is the earliest permission event of that holder
at that resolver scope, as before. Grants on a registration whose row is not
readable are not listed; no other request-time lineage check applies, since a
dropped block's grants leave the families when that block is undone. These routes answer
`409 stale` while the families are not servable, as above.

After an ENSv1 registry `Transfer` to
the zero address leaves a name's registry node ownerless
(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L60-L68 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L48-L55 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L123-L131 @ ens_v1@91c966f),
the composed name becomes unregistered but remains projected and can be listed
by `GET /v1/search`. Composed rows carry the complete declared resolution topology
(`declared_summary.topology`): wildcard sources, direct and ownerless
ENS, and admitted Basenames cross-chain transport. Basenames retains its
execution-manifest admission and the Ethereum position selected at the Base
publication's timestamp.

`GET /v1/names/{name}/subnames` (with and without
`include=counts`), `GET /v1/registries/{chain_id}/{address}/labels` and the
registry's `counts.labels` read the child edge families, with each child's arm, serving resource, zero-owner
transfer, registration status and times from the stored [name
summary](glossary.md#name-summary), evaluated against the family marker's
block. Every per-name child count (`subname_count` under `include=counts`, and
a name's subname count) is an exact count over the same relation. The parent
and each child's registration come from composed rows. Each child read sees one committed family
block, and with no servable marker (a rebuild in flight, or another build's
hash) it answers `409 stale` like the composed reads, never an empty list.

Indexed lookup names, record inventories, address-name relations, resolver
overviews and bound names all use the selected family publication. Composition
has no historical per-name row to admit below that publication. Current reads
capture its generation; current-state collections, the resolver and registry
overviews (including the registry's `counts.labels`) and name detail's
`include=counts` then read on one database snapshot that is checked against
that generation before the first read. History routes retain their documented
event windows and audit semantics.

### Tier 3: Diagnostics

Diagnostics are the only public routes that may carry pipeline vocabulary.
They expose coverage taxonomy, binding and authority explanations, record
inventory and indexed-value internals, active manifests, and raw
normalized-event rows.
The diagnostics records route drives the same read-only verified lookup engine.
It does not create, refresh, or clear diagnostic observations.

## Parameters

Common parameter rules:

| Parameter | Applies to | Values |
| --- | --- | --- |
| `at` | Tier-2 single-resource snapshot reads: names, records, and resolver overview; diagnostics exact-name snapshot/explain routes. Top-level collection routes recognize `at` only to return the temporary latest-state limitation error. Lookup, status, primary-name, and namespace metadata do not accept it. | Decimal Unix seconds, an RFC 3339 timestamp, or the URL-safe opaque snapshot token from `meta.as_of_token` |
| `finality` | Single-resource snapshot reads and diagnostics exact-name snapshot/explain routes accept `latest` (default), `safe`, and `finalized`. Top-level collection routes accept only omitted or explicit `latest`. Lookup, status, primary-name, and namespace metadata do not accept it. | `latest` (default), `safe`, `finalized` where supported |
| `source` | names, records, primary-name | names and records use `indexed` (default) or `verified`; the records route also accepts `auto`; primary-name omits `source` to return all supported source answers and may use `indexed` or `verified` to request a subset |
| `namespace` | name-inferred, address-anchored, and collection routes | explicit override or filter |
| `include` | route-documented expansions | per-route allowlist |
| `sort`, `order` | paginated routes that declare a sort set; history collections accept `order` alone over their fixed chain-position sort | route-documented field set plus `asc`/`desc` |
| `resolver` | `/v1/events` | `<chain_id>:<address>` resolver contract; anchors the read, suppresses the `ens` namespace default, and is bound by cursors |
| `type`, `from_timestamp`, `to_timestamp` | name history, address history, `/v1/events` | friendly event type or comma-separated set; inclusive Unix-seconds or RFC 3339 bounds resolved to lineage block ranges (see [history collection filters](api-v1-routes.md#history-collection-filters)) |
| `expires_after`, `expires_before` | `GET /v1/names`; `GET /v1/addresses/{address}/names?relation=former_owner` | Unix-seconds or RFC 3339 window over `expires_at`; `expires_after` inclusive, `expires_before` exclusive. At least one is required on `/v1/names`; both are optional for `former_owner` |
| `include_expired` | `GET /v1/names/{name}/subnames` | `true` (default) lists released and past-expiry children; `false` omits them |
| `cursor`, `page_size` | every paginated route | opaque cursor; default 50, max 200 |

For a cross-namespace read with no explicit `namespace`, the API accounts for
every recognized public namespace with active
[source manifests](manifests.md). It separately determines which of those
namespaces may serve rows: their active manifests must have a completed
projection publication at the current head of the namespace's authority chain
in the selected deployment. A namespace may not serve rows while its selected
authority chain has Interpret
`redo_in_progress=true`, regardless of redo mode. An Interpret redo rewrites
previously served identity history batch by batch, so a page read during the
redo can be incomplete even while Project still reports its prior completed
head.
Bare search and public reverse lookup filter current rows and counts to exactly
the eligible namespaces, and public reverse lookup builds its snapshot scope
from the same authority chains. After reading a bare search page, the API reloads the active
manifest declarations, selected authority chain heads, and completed projection
publication generations captured during derivation, and confirms that no
selected authority chain began an Interpret redo. Any change returns the
existing retryable `409 conflict` instead of serving a response assembled
across deployment states.
Public reverse lookup reloads its captured active manifest declarations before
the route's existing head and projection-publication check, including the same
Interpret redo check: a manifest change returns `409 conflict`, while a redo,
head, or publication change returns the existing retryable `409 stale`. A redo
that begins after derivation therefore never exposes a partial page through
either route.
Explicit-namespace search captures its request-scope metadata before reading
the page and reloads it afterward. A head, completed publication generation, or
readiness change returns the same retryable `409 conflict` instead of
attributing the page to a position selected after the rows were read.
Publication becoming ready between admission reads is such a readiness change,
not evidence of an Interpret redo.
Their namespace-omitted cursors bind that derived namespace list and fail closed if it
changes. Search with an explicit recognized `namespace` bypasses public
namespace derivation. It still requires the selected namespace's
`redo_in_progress` value to be false and returns `409 stale` while an Interpret
redo is active. Name-only lookup likewise keeps its existing name snapshot
selection and does not derive the public set; only address inputs invoke public
reverse derivation.
The chains accounted for by `meta.as_of` and `meta.as_of_completeness` are
selected by the namespace parameter and input kinds, not by the namespaces
eligible to serve rows or the rows returned. An explicit namespace does not
account for another public namespace's chains. A bare cross-namespace read
returns `409 conflict` when no public namespace may serve rows; when at least
one may serve rows, every other in-scope chain is disclosed through
`meta.as_of_completeness`.

Unknown or undocumented query parameters are rejected with `400 invalid_input`
on every `v2` route. As a documented temporary exception, latest-state
collection routes recognize `at`, `finality=safe`, and `finality=finalized` so
they can return the limitation errors defined below instead of implying
snapshot support.
Snapshot-pinned reads require the ADR 0003 slice-3 snapshot-service enabler;
ADR 0006 rollout step 3 includes that read-layer work.

### Name inputs

A name input is the `{name}` path of `GET /v1/names/{name}`, its `/records`,
`/history`, and `/subnames` routes and the exact-name diagnostics routes; a
`POST /v1/lookup` name input; and the `name` filter of `GET /v1/events`,
`GET /v1/diagnostics/events`, and `GET /v1/permissions`. It is normalized with
bigname's ENSIP-15 normalizer (`ENS_NORMALIZER_VERSION` in
`crates/domain/src/normalization.rs`) before reading, with one exception: a label spelled `[`, 64 lowercase hex digits, `]`
is a labelhash, not label text. It stands for the label whose labelhash those
digits are, and is the [placeholder](glossary.md#non-name-form) that the
subnames route serves for a label bigname cannot state. The normalizer rejects
`[` and `]` (its `rejects_square_brackets` test pins this), so no label it
accepts has this form. The other labels are normalized one at a time. The node
is the namehash with the given labelhash used for that label. A bracketed label
whose labelhash is that of `eth` or `base` is read as that label, so
`alice.[<labelhash of base>].eth` infers the `basenames` namespace as
`alice.base.eth` does.

The bracketed spelling is another way to name the node, not a separate name:
every route reads the node exactly as it reads the node's plain spelling, with
the same snapshot selection, cursor binding, and `404 not_found` or
`409 stale` outcomes. Responses that come from the node's name row serve that
row's `name` and `display_name`, so when bigname knows the label the response
uses the label, not the brackets. Name history rows carry that name too; a
history continuation, which does not require the row, serves the bracketed
spelling when the node no longer has one. A node that registry events created
without a label-bearing event (an ENSv1 or Basenames registry child with no
[name surface](glossary.md#surface-name-surface)) has no name row yet. Its
placeholder is listed on its parent's subnames page but does not address a
row. Uppercase hex digits, a `0x` prefix, or any digit count other than 64 are
rejected: the name routes return `400 invalid_input`, and lookup returns an
in-band `invalid_name`. The octal-escape non-name form is never accepted as a
name input.

### Manager

`manager` is the account that can change the name's registry record, such as
its resolver, which `owner` does not always say. It answers the same question
as the `manager` address relation, and the two agree except where noted
below. A name with no
NameWrapper state (an unwrapped ENSv1 name, a Basenames name or an ENSv2 name)
serves the registry owner of its node. On an unwrapped `.eth` second-level
name that is the controller, which can differ from `owner`, the BaseRegistrar
token holder; the holder can set the controller with the registrar's `reclaim`
(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L171-L174 @ ens_v1@91c966f).
A wrapped name's registry owner is the NameWrapper contract
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L372 @ ens_v1@91c966f),
and the NameWrapper lets its token holder, or an operator the holder
approved, change the record
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L202-L205 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L666-L669 @ ens_v1@91c966f),
whatever the name's `ens_v1.wrapper_state` (`wrapped`, `emancipated` or
`locked`)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L214-L222 @ ens_v1@91c966f);
a burned fuse can still forbid a particular change. A wrapped name therefore
serves the NameWrapper token holder, its `owner`, except while a wrapped `.eth`
second-level name is inside its registrar grace period, when NameWrapper
refuses the holder
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1082-L1089 @ ens_v1@91c966f):
`manager` is omitted then and the `manager` relation does not list the name.
Bigname checks whether the name is in that grace period when it publishes the
name and again when the grace period starts, so reads need no clock; the grace
state itself is not served, and clients can place the window with `expires_at`
and `grace_ends_at`.
`manager` is omitted wherever the address it copies is omitted, such as on a
released name, and on a wrapped name whose NameWrapper state is unknown (its
fuses or expiry were never observed) or lapsed, where the `manager` relation
does not list it either. A registry child with no name row serves its registry owner,
except a child that a NameWrapper or registrar event named only under a label
failing ENSIP-15 normalization: bigname cannot tell whether NameWrapper holds
it for a token holder, so, as with its `ens_v1` lifecycle fields, it omits
`manager` rather than serve the NameWrapper contract. The `manager` relation
still lists that child for its registry owner (TYR-148).

One known gap remains. A parent owner can reassign a wrapped child's registry
record with the registry's `setSubnodeOwner`, which emits no `NameUnwrapped`
(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L75-L84 @ ens_v1@91c966f),
after which NameWrapper no longer treats the child as wrapped
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1076-L1079 @ ens_v1@91c966f);
bigname keeps its wrapper state, so the field serves the old token holder while
the `manager` relation follows the new registry owner (TYR-147). It is listed
under [known divergences](upstream.md#known-divergences).

## Status Vocabulary

`unregistered` describes the absence of current registration or control; it
does not assert that resolver data is absent. A supported row may therefore have
`registration_status=unregistered` when it is
an ownerless ENSv1 or Basenames registry row whose current registry resolver
pointer is retained (a [serving resource](glossary.md#serving-resource)). It
serves indexed records when retained inventory exists; routes with source
selection can also serve verified records under the ordinary lookup capability.
An ENSv2 TLD whose root-registry token is reserved or whose registration is
not projected serves the same way from its current
[root-registry resolver pointer](glossary.md#root-registry-resolver-pointer)
while it stays `current_authority_not_projected` and
`registration_status=unregistered`: the root registry stores the pointer per
token and returns it while the label is unexpired, and bigname serves that
pointer without inventing the TLD's registration or authority.
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L150-L155 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L255-L258 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L476-L478 @ ens_v2@a971bd64)
The current ENSv1 registry and the Basenames registry emit the supplied owner
from `setOwner`.
(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L63-L68 @ ens_v1@91c966f)
(upstream: .refs/basenames/src/L2/Registry.sol:L100-L103 @ basenames@1809bbc)
Both map registry self-ownership to zero in `owner()`.
(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L123-L131 @ ens_v1@91c966f)
(upstream: .refs/basenames/src/L2/Registry.sol:L165-L170 @ basenames@1809bbc)
In both registries, the resolver write and read use the resolver field,
separately from the owner field.
(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L7-L10 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L89-L94 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L137-L140 @ ens_v1@91c966f)
(upstream: .refs/basenames/src/L2/Registry.sol:L16-L22 @ basenames@1809bbc)
(upstream: .refs/basenames/src/L2/Registry.sol:L132-L134 @ basenames@1809bbc)
(upstream: .refs/basenames/src/L2/Registry.sol:L178-L180 @ basenames@1809bbc)
The internal reason
`current_authority_not_projected` is reserved for authority selection that is
unresolved or unsupported, not for a registry event stream that positively
proves current authority is absent.

One result-status vocabulary is used everywhere except the `/v1/status` ops
route:

<!-- openapi:enum Status -->
| Value | Meaning |
| --- | --- |
| `ok` | The requested answer is served. |
| `not_found` | No answer is present. |
| `invalid_name` | Lookup input cannot be normalized as a name. |
| `mismatch` | Verification produced a different answer. |
| `unsupported` | The requested answer is not supported. |
| `stale` | The selected position is not currently available. |
| `failed` | Execution failed for this answer. |

Rules:

- `unsupported_reason` is required when `status=unsupported`.
- `mirrored_resolver_not_projected` is the projected inventory reason for a
  name whose current ENSv2 resolver is a declared
  [ENSv1 mirror resolver](glossary.md#ensv1-mirror-resolver-ensv1_mirror_resolver)
  while the ENSv1 resolver the mirror's registry walk selects for the name (the
  exact node's, else the nearest ancestor's) is not projected, or is an
  ancestor's resolver, which Project never derives through; it crosses the
  serving boundary unchanged wherever a route exposes the inventory reason.
- A read over a projected row keys `unsupported` on that row's own coverage
  status, not on a list of known reasons: an unsupported row serves
  `status=unsupported` even when it names no reason or names a reason the build
  does not recognize. Exceptions are per-route and named there, such as the
  name-detail and batch-lookup partial serve for
  `current_authority_not_projected` in
  [`api-v1-routes.md`](api-v1-routes.md).
- When an unsupported projected row names a reason that this build does not
  recognize and that cannot cross the serving boundary as public vocabulary,
  the public `unsupported_reason` is `unsupported_reason_unrecognized`.
- `failure_reason` is permitted on `failed`, `stale`, `not_found`, and
  `mismatch`.
- `mismatch` is the verification state where a claimed answer verifies to a
  different value.
- `completeness` is `full`, `partial`, or `unsupported`.
- Empty arrays and empty maps mean known-empty, not unknown.

### Resolver record answers and values

A keyed resolver record answer contains `status`. It contains `value` only
when `status` is `ok`; `unsupported_reason`, `failure_reason`, and `meta` are
present only where the route contract permits them. An indexed keyed answer is
taken from a record inventory only while that inventory's coverage is
authoritative (`full` or `projected` with no `unsupported_reason`); an
`unsupported` inventory row yields `status=unsupported` with the row's reason
and no `value` for every key, whatever entries the projection retained
([api-v1-routes.md](api-v1-routes.md#get-v1namesnamerecords)).

For a successful `contenthash` answer, `value` is a lowercase,
`0x`-prefixed hex string containing the bytes returned by the resolver. The API
does not decode those bytes into a URI or media-type-specific representation.
For a successful `addr:<coin_type>` answer, both `<coin_type>` and the selector
are decimal strings and `value` is a lowercase, `0x`-prefixed hex string. For
multicoin records, that string contains the resolver-returned native binary
address bytes without chain-specific textual re-encoding.

ENSv1 and Basenames store the supplied contenthash and address byte payloads
verbatim and emit the same bytes. Their address setters encode
`setAddr(node,address)` as a 20-byte coin-type-60 value; the coin-type setter emits and stores
that payload. The Basenames legacy getter returns the zero address for an empty payload and routes
a nonempty payload through a conversion helper that requires exactly 20 bytes.
(upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L22-L24 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L47-L70 @ ens_v1@91c966f)
(upstream: .refs/basenames/src/L2/resolver/AddrResolver.sol:L43-L66 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/resolver/AddrResolver.sol:L76-L82 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/resolver/AddrResolver.sol:L108-L110 @ basenames@1809bbc)
(upstream: .refs/basenames/src/L2/resolver/AddrResolver.sol:L116-L121 @ basenames@1809bbc)
Contenthash reads, and address reads when no default-address fallback applies,
return the stored bytes. An empty payload is therefore the stored value after a
clear, and those reads return the same empty bytes.
(upstream: .refs/ens_v1/contracts/resolvers/profiles/ContentHashResolver.sol:L14-L28 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L47-L85 @ ens_v1@91c966f)
(upstream: .refs/basenames/src/L2/resolver/ContentHashResolver.sol:L32-L43 @ basenames@1809bbc)
(upstream: .refs/basenames/src/L2/resolver/AddrResolver.sol:L57-L99 @ basenames@1809bbc)
Bigname represents that zero-length exact stored contenthash or address answer
as `{"status":"not_found"}` and omits `value`. It also represents an exact
ENSv1 or Basenames `addr:60` value of exactly 20 zero bytes as `not_found` and omits `value`. This
includes an `AddressChanged(node,60,...)` payload of 20 zero bytes and a retained legacy-only normalized
`AddrChanged(node,address(0))` behind an ENSv1 registry, registrar, or wrapper resolver pointer,
or a Basenames registry resolver pointer. Other origins, types, nonempty lengths, and nonzero values
retain their stored values. Raw facts and normalized events remain unchanged; Project classifies the
row.

A records route may then apply a documented derived-record rule, such as the
ENSIP-19 default-address rule; the keyed answer on `GET /v1/names/{name}/records`
and the convenience field on name detail and lookup follow it.
A selected exact zero20 `addr:60` remains `not_found` in indexed, auto, and
verified reads even when a nonzero default exists. No value or default-derived
convenience field is returned. Empty or missing eligible exact records retain
fallback only when the resolver's declared read feature permits it. The admitted
Basenames resolver has no default fallback. The internal observation marker does not
change public response fields or grant authority to incomplete inventory.
(upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L36-L40 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L81-L84 @ ens_v1@91c966f)
(upstream: .refs/basenames/lib/ens-contracts/contracts/resolvers/profiles/AddrResolver.sol:L57-L62 @ basenames@1809bbc)

On `GET /v1/names/{name}` and `POST /v1/lookup` with `profile=detail`, the
grouped `records.addresses` map uses the same scalar hex string for each decimal
coin type, and `records.contenthash` uses the same contenthash scalar string.
A cleared exact value, including a zero-address `addr:60` clear, is `null`
there. Indexed `records.addresses` holds exact observed writes only and never
synthesizes the ENSIP-19 derived default, which indexed detail serves only as
`primary_address`. Verified detail serves the getter's answer for each key it
read, so a getter that returns its fallback puts that answer in both
`records.addresses` and `primary_address`.
`GET /v1/names/{name}/records` has no grouped object: its per-key `records`
answers are its only value shape. Diagnostics and
Project use the internal status `success` for a retained value; product routes
publish that status as `ok`.

The exact-name detail route, resolver-records route, and `profile=detail` lookup
flatten both projected `{encoding,bytes}`
address values and projected scalar address values to the same scalar hex
string. The supported internal shapes therefore do not create a second public
multicoin-address shape.

## Finality And Snapshots

`finality` values are `latest`, `safe`, and `finalized`. Snapshot selection is
uniform across single-resource snapshot-read routes. Each such chain-derived
response carries `meta.as_of`, keyed by stringified `chain_id`, and
`meta.as_of_token`, an opaque token that can round-trip as `at` to pin exact
per-chain positions. Tokens must cover every required slot in the target
route's snapshot scope and must not carry extra slots outside that scope.
For lookup and search responses that account for chains selected by the
request, the target route's served snapshot scope can be narrower than that
chain scope; every additional in-scope chain is reported under
`meta.as_of_completeness` and is not added to the token.

Public chain-position timestamps are decimal strings of whole Unix seconds.
Snapshot inputs accept decimal Unix seconds or RFC 3339; stored projection
positions and opaque snapshot tokens keep RFC 3339 instants. RFC 3339 values
may use `Z` or a numeric UTC offset (`+HH:MM` or `-HH:MM`) and may carry one
to nine fractional-second digits; readers normalize the instant to UTC before
comparison without losing that precision. Because `+` is decoded as a space in query strings,
clients must percent-encode it as `%2B` in an `at=` query value. For example,
`at=2025-06-15T17:37:42%2B02:30` selects the same instant as
`at=2025-06-15T15:07:42Z`. Different accepted spellings of the same instant do
not make a projection stale.

Public `meta.as_of.*.timestamp` values use whole Unix seconds. Opaque snapshot
tokens keep their internal UTC `Z` serialization and non-zero fractional
seconds, as do cursor positions where needed. Public formatting does not
change snapshot identity, filter boundaries, or keyset continuation.

The API selects current `latest`, `safe`, and `finalized` positions from
`bigname_phase.chain_heads` and obtains their timestamps from readable
`bigname_phase.chain_lineage`. Every selection is available only when a live
[family marker](glossary.md#family-marker) carries the API's compiled
interpreter content hash on readable lineage at most the publication lag
tolerance behind the latest head (see the publication-lag rule above); the
Project phase may be running meanwhile. Timestamp `at` selection and opaque-token replay
still choose historical positions: every supplied or resolved position must
exist in `bigname_phase.chain_lineage` and satisfy the requested finality
floor, and an authoritative cross-chain selection bounds auxiliary positions
by its timestamp. The current project-publication check also applies to those
historical selectors.
A token or timestamp that selects a block absent from readable phase lineage
returns `409 conflict`.

API startup discovers the status chain set from the union of
`bigname_phase.chain_heads` and `bigname_phase.chain_phase_state`. `/v1/status` uses
those same relations for its chain set and reads stored head/finality positions
from `chain_heads`, the indexed block from the
[family marker](glossary.md#family-marker), Project lifecycle and redo state
from the `project` row in `chain_phase_state`, and both timestamps from the
matching readable `chain_lineage` rows.

Latest-only collections (names, subnames, address names, permissions, search,
registry labels and product history) page over current data. They omit
`meta.as_of_token`, because old publications are not retained for replay
through `at`. Resolver collections and registry references retain their
route-specific selectors and position tokens. Current-state collections report in `meta.as_of` the publication
each page read, since their cursors hold only a position; history
collections report the publication captured when the request was admitted,
as a [history walk](glossary.md#history-walk) whose cursor holds only a
position; search reports request-scoped `meta.as_of` for staleness
attribution. No collection cursor claims a snapshot bound that `at` could
replay. A history cursor carries no publication token, and the token of a
history cursor issued before the walk rule is ignored rather than treated as
a validity condition. For the latest-only collections, omitted `finality` and
explicit `finality=latest` are accepted.
An `at` selector returns `400 invalid_input` with
`at is not supported because collection routes read latest state`.
`finality=safe` and `finality=finalized` return `400 invalid_input` with
`finality must be latest because collection routes read latest state`.

A cursor does not preserve historical rows or freeze ordering. Supporting a
historical collection selection would require retained historical data; it is
not implied by continuation or by the request's publication metadata.

`POST /v1/lookup` is a current-state read. It does not accept `at` or
`finality`; when a served head is available, its `meta.as_of` and
`meta.as_of_token` record the served positions for staleness attribution and
shadow-diff correlation. Lookup rejects partial scoped heads instead of
emitting a token that cannot replay on a compatible snapshot-read route. Each
returned forward or reverse phase row must have a projection target at or before
the selected position; a target at the same height must have the same hash. The
selected `chain_heads` rows and completed schema-v2 projection generations must
remain unchanged across the read. An ahead, same-height wrong-hash, or
publication-generation mismatch returns `409 stale`.
Name-only and exact-scope lookup, explicit-namespace search, and resolver reads
without `at` at `finality=latest` apply the same `redo_in_progress` readiness
check and return `409 stale`. Historical resolver reads selected with `at` and
resolver reads at `finality=safe|finalized` retain their existing generation
validation without that redo check.

`GET /v1/addresses/{address}/primary-name` is also a current-state read. It
does not accept `at` or `finality`; when a served head is available, its
`meta.as_of` and `meta.as_of_token` record the served positions for staleness
attribution and shadow-diff correlation. For an ENS/60 verified answer, both
metadata fields identify the captured family publication's Ethereum position
that pins the fresh lookup. An omitted-source ENS/60 response fences the
indexed claim to that same schema-v2 position and project publication generation across the
verified and indexed reads and returns `409 stale` if either changes.
There is no persisted trace or verified-outcome cache. Indexed-only Basenames
responses remain Base-scoped; Basenames verified primary-name lookup is
currently unsupported.

The `chain_positions` query parameter from `v1` does not exist in `v2`.

## Cursors And Pagination

Cursors are opaque and versioned. They are not bound to the route path string,
so route evolution does not invalidate outstanding cursors. Top-level
collection cursors bind the collection anchor, namespace, filters, and sort.
Current-state collection cursors, including nested resolver `bound_names` and
registry `referenced_by`, hold positions and bind no publication; see
[current-state list cursors](#current-state-list-cursors). History collection cursors bind no snapshot or
publication, and the publication token of a history cursor issued before the
walk rule is ignored. A bare search cursor uses the request's derived namespace
set as its namespace anchor and fails closed if that set has changed. Cursors
preserve keyset position across requests without claiming that the mutable
dataset is frozen. The per-input lookup cursor contract remains separately documented.

History collections (`/v1/events`, name history including
`include=child_registrations`, and address history) are
[history walks](glossary.md#history-walk), not snapshots.
A history cursor holds the position of the last row it returned in the history
order: block number, chain, block hash, transaction index, log index, and the
row's `event_identity` as the final tiebreaker. It carries no publication token
and no evaluation time. A continuation reads whatever is published when it runs
and returns the rows after that position; the row the cursor came from need not
still exist. A later page can therefore include rows published after the first
page, a row can move or disappear after an Interpret redo, and `total_count`
can change between pages. `meta.as_of` is the publication captured when the page was admitted: the
page's rows are bounded at it, but some inputs are read from current state
rather than from that publication (the resolver classification that decides
whether a pointer attributes writes, and the address relation kinds listed
under [history collection filters](api-v1-routes.md#history-collection-filters)),
so not every field of a page belongs to that one publication. Publication
changes do not expire a position cursor, and a publication that lands while a
page is being read does not refuse that page either. That page is still capped at the publication
it reports, so no row above it can appear; but when the new publication
rewrites rows at or below it, for example after a reorg, the page can mix rows
from before and after that rewrite. Because `event_identity`
is only the final tiebreaker, a re-derivation that changes the identities of
events sharing one log position can skip or repeat a row at that position,
which the same walk rule covers. A history cursor returns `400 invalid_input`
when it is malformed or replayed against a different query, and that check
comes before publication admission. A history cursor issued before this rule
names its last row instead of carrying its position: its publication token is
ignored and it resumes from that row's position. A history read can still
return `409 stale` for three distinct reasons: a requested namespace has no
publication yet ("not available; retry after indexing is ready"), an Interpret
redo is active or ran during the read, or a cursor issued before this rule
names a row that no longer exists. The first two are temporary: retry the same
request with the same cursor. Only the third requires restarting without the
cursor, and it happens once. A parameter that pins a history walk to
one block may be added later; it is not part of this contract.

The `/v1/events`, name-history, and address-history collections use a
collection-wide `redo_in_progress` check. An active Interpret redo on any chain
returns retryable `409 stale` for all three collections, regardless of the
requested namespace or name. Events validates the request and cursor binding
before its first check, while address history also validates its namespace
first. Name history validates the request and cursor binding, then captures the
check before parent lookup. A missing parent returns `404
not_found` only when no redo is active and the captured generations are
unchanged; otherwise it returns `409 stale`. Each route checks before deriving
identity and event anchors written by Interpret and checks again inside the
repeatable-read page transaction. Events and address history then resolve
display names and revalidate the captured redo state before returning data;
name history has no post-transaction display-name read. With
`include=child_registrations`, a child row's name comes from the child's name
surface, read inside the same page transaction. An active redo at any
check, or a redo
that began between them, returns `409 stale` instead of exposing a partially
reconstructed normalized-event range. That refusal is a retry, not an expiry:
once the redo finishes, the same cursor continues. Product
event-type filtering precedes keyset pagination, so page rows and continuation
metadata describe only product-visible events. A cursor's position orders the
walk whether or not the row it came from is product-visible or matches an
explicit `type`, so the continuation starts at the next product-visible row
after that position.
History cursors also encode the direction in their sort token and the
canonical `type` set and timestamp bounds in their filters, so `order=asc`,
`type`, `from_timestamp`, and `to_timestamp` each fail closed when a cursor is
replayed against a different query. Name history cursors also record
`include=child_registrations`; see the [history collection
filters](api-v1-routes.md#history-collection-filters). Cursor bytes remain
unstable.

A full Interpret and Project re-walk does not invalidate a history cursor: the
cursor holds a position, not a normalized-event row ID, and continues under the
[history walk](glossary.md#history-walk) rule above. The diagnostic-events
route must accept its pre-re-walk cursor and continue from the same stable
normalized-event anchor, but its remaining diagnostic rows and fields may
reflect newly admitted candidate data. A pre-existing diagnostic row's numeric
`normalized_event_id` may change, while its `event_identity` and pre-existing
semantic fields remain stable apart from those allowed candidate additions.
Implementations may preserve numeric
normalized-event IDs or resolve an old token through stable `event_identity`
plus its stored sort tuple; these are alternative storage strategies. Freshly
issued cursor bytes may differ. The boundary acceptance gate exercises the
diagnostic continuation contract, then separately verifies fresh post-re-walk
diagnostic cursors.

A re-walk leaves product rows unchanged only when the declared
[re-derivation boundary](glossary.md#re-derivation-boundary) preserves product
semantics. The intentional
[#348](https://github.com/ensdomains/bigname/issues/348) and
[#529](https://github.com/ensdomains/bigname/issues/529) interpreter changes keep
an ENSv2 resolver `RecordChanged.event_identity` or
`RecordVersionChanged.event_identity`, changes `logical_name_id` from null to
the retained canonical [name surface](glossary.md#surface-name-surface), keeps
`resource_id` null, and updates the attribution embedded in
`raw_fact_ref.interpreter_state_key`. Its `before_state` may also become the
preceding `after_state` from the logical-name/resource-null state stream that
the event now joins. Issue #348 retains the surface from registry/root
evidence. The event may therefore newly enter name-filtered
diagnostics and product history. A cursor issued before that change has no
continuation guarantee and may be rejected. Consumers must discard
pre-#348/#529 cursors and restart from the first page; fresh post-publication cursors
continue normally.

A `record` row may also come from a node-keyed resolver observation that carries
no logical name or resource of its own, such as an exact direct
`public_resolver_v2` write. Project attributes that observation to a registration
through its selected resolver pointer; `registration` and `both` scope name history
derive the same attribution from the pointer evidence at or below the read's
published block, so history lists the writes the name's records serve, while
`name` scope does not because the observation has no surface link. The row's
`registration_id` stays null. The attribution spans every resolver pointer the
registration has selected, so a later switch or clear leaves the earlier writes
attributed. `GET /v1/events?registration_id=...` lists such
a write only when Project attributed it to that registration's own records or
to the records of a NameWrapper resource whose `NameWrapped` row recorded that
lease; a write attributed only to another registration of the same name is not
part of the read, its `total_count`, or its cursor anchors.

The [#613](https://github.com/ensdomains/bigname/issues/613) interpreter change
keeps the original [pre-surface](glossary.md#pre-surface) ENSv1 registry `ResolverChanged` row unchanged,
then adds a name- and resource-linked, [state-derived](glossary.md#state-derived-normalized-event) `ResolverChanged` when the
first active [name surface](glossary.md#surface-name-surface) is learned. Product
events or name history may therefore gain one historical resolver row, while
diagnostics may gain each linked resource copy. When current-registry ownership
ends old-registry fallback, its resource-specific resolver-clear copies represent
one ownership-log/node transition. Product history selects the lexically first
stable event identity among the activated copies matching the request and its
canonicality filters. Selection happens before pagination and is shared by
counts, summaries, and cursor validation. A resource-only request therefore
retains its matching clear even when another resource has the globally first
copy; a sole matching clear is never suppressed. All normalized copies remain
available to diagnostics, projection, and replay.
(upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L18-L24 @ ens_v1@91c966f)
A cursor issued before this
change has no continuation guarantee and may be rejected. Consumers must
discard pre-#613 cursors and restart from the first page; fresh post-publication
cursors continue normally.

If an ended resource still
retains a resolver pointer to the emitter, its rebuildable record-inventory
projection may change too. The
resource-less late event does not restore the composed name's `resource_id`, so name
and record reads for the released or expired name continue to expose no current
record inventory.

An ENSv2 registration that lapses by path expiry is served like one released by
`unregister`, which ends the entry at the current block
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L196-L216 @ ens_v2@a971bd64):
`GET /v1/names/{name}` keeps answering with `registration_status`
`released`, the registration identity, timestamps and the lapsed `expires_at`,
without a current owner, resolver or records, and `GET /v1/names/{name}/history`
keeps serving the name's history. An ENSv1 lease that lapses past grace with no
revived custody, which is how a wrapped `.eth` name lapses and how a `.eth` name
lapses after its registrar token was transferred without `reclaim`, is served the
same way as a [released v1 authority](glossary.md#released-v1-authority):
`registration_status` `released` with the registration identity, timestamps and
the lapsed lease's own `expires_at`, no current `owner`, `manager`, resolver or
records, and its history intact. Only a name that never had a
readable surface answers `404 not_found`.

### Current-state list cursors

A [current-state list cursor](glossary.md#current-state-list-cursor), the
cursor of every current-state product collection (search, names, subnames,
address names, permissions, registry labels, resolver links/roles,
and nested resolver `bound_names` and registry `referenced_by`), holds the list's
sort and filters, the sort position of the last row it returned, and, when the
request pinned `at`, that `at` token. Only resolver and registry overviews and
resolver links/roles accept `at`. It holds no publication,
generation, or evaluation time. A continuation reads whatever is published when
it runs and returns the rows that sort after that position:

- The row the cursor came from need not still exist or still sort where it did;
  the page starts after the position either way.
- Pages of one walk can read different publications, and `meta.as_of` reports
  the one each page read. A row whose sort key changed between pages can be
  returned again or not at all: a renewal moves a name's `expires_at` in
  `GET /v1/names`, a re-registration removes a `former_owner` row, and a resolver change moves a name into or out of
  `bound_names`. Rows published after the first page can appear on later
  pages.
- A position after the last row returns `200` with empty `data`,
  `has_more: false`, and `next_cursor: null`.
- A cursor that does not decode, comes from another list, carries different
  filters or sort, or carries a field this contract does not write returns
  `400 invalid_input` with `cursor must be a valid pagination cursor`; restart
  without the cursor when correcting the request is insufficient. That includes
  the publication token, evaluation time, or
  resolver generation that `GET /v1/names` and `bound_names` cursors carried
  before this contract, and the snapshot field of `GET /v1/search` cursors
  issued before July 2026, so such a cursor is refused once. Resolver
  links/roles likewise reject their old publication/generation layout.
  Subnames, address names, permissions, registry labels and `referenced_by`
  accept their previous position layout after validating sort, filters and
  anchors, ignoring only the old publication/evaluation fields. Address names
  also ignore the old `registry_children` digest. Old permissions cursors did
  not distinguish a name's inferred registration from an explicit registration
  filter, and old registry-reference cursors did not distinguish automatically
  selected `at` from an explicit pin. These stored selectors must be supplied
  and match on continuation, or the legacy cursor returns `400 invalid_input`
  once. Restarting without it issues an unpinned position cursor for an unpinned
  request. New name-only permissions cursors follow the current registration.
- With `at`, the continuation must send the same `at`, or it returns
  `400 invalid_input`. Resolver collections read current projections, so after
  a later block is published their pinned continuation returns `409 stale`.
  Registry references keep their documented historical pointer reads at that
  selected position. The pin is a chain position, not a publication generation:
  a same-block rebuild does not invalidate the cursor. See each route's
  historical availability and finality constraints.
- Every one of these collections except `GET /v1/search` reads a page on one
  read-only `REPEATABLE READ` database snapshot of the publication captured at
  admission, which `meta.as_of` reports. A publication that lands between
  admission and the page's first read returns `409 stale` with a message asking
  to retry, and retrying with the same cursor then continues; a publication
  during the read does not affect the page. A change to the public namespace
  manifests during the read returns the same retryable `409 stale`.
  `GET /v1/search` keeps its
  documented request-scope recheck, which returns `409 conflict` for a head,
  publication, or readiness change and `409 stale` when an Interpret redo is
  involved (see [`GET /v1/search`](api-v1-routes.md#get-v1search)).

### Timestamp format and absent expiry

Every public timestamp keeps its existing field name and is a decimal string
of Unix seconds, for example `"1803965433"`. This includes registration,
creation, migration and release dates, `timestamp` on events and chain
positions, resolver grant timestamps, `network_head_observed_at`, and health
`started_at` / `heartbeat_at`. Clock times are floored to whole seconds. Expiry
and grace are exact integers, including finite values beyond year 9999,
`2^53 - 1`, and `i64::MAX`; `"18446744073709551614"` remains finite. Consumers
must use an exact integer representation when comparing large values.

The output format changes together across all routes, with no parallel date
fields or optional UTC output. Timestamp query inputs (`at`,
`from_timestamp`, `to_timestamp`, `expires_after`, `expires_before`) accept
decimal Unix seconds and RFC 3339. Decimal input is seconds, never guessed to
be milliseconds. RFC 3339 offsets and fractional seconds retain their input
precision for selection and filtering. Equivalent spellings select the same
instant and bind the same cursor filter. Clock selectors outside their
supported instant range return `400 invalid_input`; a decimal clock value
is not treated as an opaque token. Expiry bounds compare exact numeric seconds
and can select finite expiry beyond the calendar range.

A registration with a classified absent expiry serves `expires_at: null` and
`expires_at_reason`:

<!-- openapi:enum ExpiryReason -->
| Reason | Meaning |
| --- | --- |
| `no_expiry` | A contract-specific maximum sentinel treated as having no expiry, such as the NameWrapper maximum or the declared ENSv2 root entries. |
| `not_set` | The NameWrapper entry has zero expiry: no expiry has been set. |
| `released` | A non-expiry release ended the registration without a retained renewal deadline, including an explicit ENSv2 unregister. |

Its `grace_ends_at` is also `null`; the same `expires_at_reason` explains both
fields. A finite expiry omits the reason. A row without a registration context
omits these fields; missing or malformed evidence is not a no-expiry sentinel.
Inside wrapper `restrictions`, `wrapper_expires_at` follows the same finite
string / classified-null rule with its own sibling
`wrapper_expires_at_reason` (`no_expiry` or `not_set`). This does not change the
conditions under which the restrictions object itself is present.

Null expiry is outside every expiry window. Collections that sort by expiry
compare finite values numerically and treat a null as smaller than every finite
value, so nulls come first ascending and last descending; every nullable list
sort key follows the same rule. A registration that expired naturally retains its finite expiry;
an explicit unregister is distinct from expiry.

Classification follows the contract and retained registration state, not a
global zero or maximum check. NameWrapper defines a maximum and caps child
expiry to its parent; ENSv2 registration accepts `uint64` and can use a zero
argument to inherit the live reservation's expiry. ENSv2 compares the retained
integer expiry against block time; the null/reason shape is Bigname's public
presentation, not a Solidity timestamp type.
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L57 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L68-L75 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L978-L990 @ ens_v1@91c966f)
(upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registry/PermissionedRegistry.sol:L206-L219 @ ens_v2_sepolia_20260916@366de741)
(upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registry/PermissionedRegistry.sol:L473-L479 @ ens_v2_sepolia_20260916@366de741)
(upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registry/PermissionedRegistry.sol:L663-L665 @ ens_v2_sepolia_20260916@366de741)

### Expiry and grace

A `.eth` second-level name can hold an ENSv1 BaseRegistrar lease and an ENSv2
`eth` registry entry at once: premigration reserves every live ENSv1 name in
ENSv2 with the lease's expiry plus 62 days, the ENSv1 grace of 90 days less the
ENSv2 grace of 28, so that both renewal deadlines fall on the same second.
Which expiry a name serves follows where resolution starts on chain. Before the
[Universal Resolver cutover](glossary.md#universal-resolver-cutover) clients
resolve through ENSv1, so `expires_at` is the ENSv1 lease's expiry and
`grace_ends_at` adds the 90-day ENSv1 grace, reservation or not. From the
cutover a name with a live ENSv2 entry (a reservation or registration that has
not been released or passed its expiry) serves that entry's expiry and adds
the 28-day ENSv2 grace, even while ENSv1 still decides who owns it: the
reservation defers ownership to ENSv1 ([ADR 0007](adrs/0007-follow-the-chain-ens-authority.md)),
not its expiry. A name ENSv2 decides always serves its ENSv2 expiry and grace.
While ENSv1 decides the name, the lease's own date stays readable as
`ens_v1.expires_at`.
(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L96-L98 @ ens_v1@91c966f)
(upstream: .refs/ens_v2_sepolia_20260916/contracts/src/testnet/TestnetV1PremigrationRegistrar.sol:L38-L42 @ ens_v2_sepolia_20260916@366de741)
(upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registrar/ETHRegistrar.sol:L43 @ ens_v2_sepolia_20260916@366de741)
(upstream: .refs/ens_v2_sepolia_20261001/contracts/script/deploy-constants.ts:L187 @ ens_v2_sepolia_20261001@07e55a05)
(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L17 @ ens_v1@91c966f)

`registration_status` stays the state at the indexed head: a name past
`expires_at` but inside its grace keeps its status, and an app shows it as
expired when `now` is past `expires_at` and renewable until `grace_ends_at`.
`GET /v1/names` windows and sorts on the same `expires_at`.
`expires_at` and its grace are derived from integral Unix seconds, including
quoted integer values in retained input.
Adding grace preserves the exact finite deadline even beyond year 9999 or the
signed-integer range; both expiry fields remain decimal strings.

From the cutover the Universal Resolver reads only ENSv2 registries, so a `.eth`
name that ENSv1 decides with no live ENSv2 entry, and every name below it,
resolve to nothing: the deployment registers `eth` without a resolver. Such a
name keeps its owner, registration and expiry, serves no `resolver` or
`records`, reports `unresolvable_reason: "no_live_ens_v2_entry"` on name detail
and lookup, and does not match `relation=resolves_to`. Verified name detail
applies the same withholding. A live reservation still resolves, through `ENSV1Resolver`, which reads the
ENSv1 registry. Before the cutover, and on Mainnet, which has none, this rule
does not apply ([known divergence](upstream.md#ensv1-authority-without-an-ensv2-entry)).
(upstream: .refs/ens_v2_sepolia_20260916/contracts/src/universalResolver/UniversalResolverV2.sol:L55-L63 @ ens_v2_sepolia_20260916@366de741)
(upstream: .refs/ens_v2_sepolia_20261001/contracts/deploy/01_ETHRegistry.ts:L39-L51 @ ens_v2_sepolia_20261001@07e55a05)
(upstream: .refs/ens_v2_sepolia_20260916/contracts/src/resolver/ENSV1Resolver.sol:L40-L43 @ ens_v2_sepolia_20260916@366de741)

### Lapsed registration

A released name carries `lapsed_registration` when it is an ENSv1 lease that lapsed
past its grace, or an ENSv2 registration released by `RegistryPathExpired` or
`LabelUnregistered`. An ENSv1 lease whose registry record the release leaves in place
carries it too: the name keeps that record as its registry custody, with no `owner` or
`manager`, and the block names the lease's last holder. Other release causes, such as a
registration displaced during token regeneration, carry no block and do not enter
`relation=former_owner`.
Only those two kinds of registration lapse: a subname with no registrar lease,
wrapped or not, never carries the block. A `.eth` name inside its registrar
grace period has not lapsed; it keeps its current `owner` and still lists under
`relation=owner`.
The block identifies the holder when the registration ended. It is separate
so that nobody reads it as current state:

```json
{
  "name": "example.eth",
  "registration_status": "released",
  "registration_id": "…",
  "expires_at": "1714521600",
  "lapsed_registration": {
    "owner": "0x…",
    "held_through": "wrapper",
    "released_at": "1722297612",
    "release_kind": "expired"
  }
}
```

`owner` is the `owner` the name had when its registration ended, the holder the
lease had when it was released. For a wrapped
name that is the NameWrapper token owner at the release, never the NameWrapper
contract, which only holds the BaseRegistrar token on the owner's behalf. For an
unwrapped name, including one whose token was transferred without `reclaim`, it is
the BaseRegistrar token owner at the release. For an ENSv2 registration it is
the registry token's holder when the registration ended: the holder recorded
by the latest grant, transfer or release on its registry entry. `held_through`
is `registrar` or `wrapper`, the contract the lapsed lease was held through, or
`registry` for an ENSv2 registration, and is omitted for any other value; the top-level `authority` field is a different
thing and names the `ens_v0`, `ens_v1` or `ens_v2` side. `released_at` is the time of the
block at which Bigname recorded the release. That is the first block whose
timestamp is after `expires_at` plus the 90-day grace period for an ENSv1 lease, so it is always
later than `expires_at` plus 90 days and never equal to it. For an ENSv2
registration it is the block that recorded the unregister, or the first block
at or past its expiry. `release_kind` is `expired` for a lapsed ENSv1 lease and
an ENSv2 registration past its expiry, which keeps its `expires_at`; the `.eth`
registrar still permits renewal during its grace while the registry remembers its last
owner. `unregistered` identifies an explicit ENSv2 unregister, which burns the
token, serves `expires_at: null`, `expires_at_reason: "released"` and
`grace_ends_at: null`, and cannot be renewed. Other fields are omitted when unknown.
(upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registrar/ETHRegistrar.sol:L270-L292 @ ens_v2_sepolia_20260916@366de741)
(upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registry/PermissionedRegistry.sol:L224-L235 @ ens_v2_sepolia_20260916@366de741)
(upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registry/PermissionedRegistry.sol:L353-L362 @ ens_v2_sepolia_20260916@366de741) The top-level `owner` and `manager` stay
absent.
(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L17 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L101-L104 @ ens_v1@91c966f)
The block appears on `GET /v1/names/{name}`, detail-profile
`POST /v1/lookup` rows and `GET /v1/names` rows, only while the name is
released; a re-registration removes it. It is never an input to the authority
relations, permissions or counts: the lapsed holder lists the name under
`GET /v1/addresses/{address}/names` only with `relation=former_owner`. A name whose
NameWrapper expiry alone has passed while its registrar lease is live is not
released and carries no block; in that state the NameWrapper reports no owner,
so the name serves no current `owner` and no `owner` relation until a
renewal through the NameWrapper restores it.
(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L100-L103 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L843-L856 @ ens_v1@91c966f)

### Registration identity of wrapped names

For a `.eth` second-level name, `registration_id` always identifies the
BaseRegistrar lease of the current registration: while the name is unwrapped,
after it is wrapped in a later transaction, and when it was wrapped in its
registration transaction. Wrapping, unwrapping and re-wrapping inside one lease
keep the same `registration_id`; a new lease after a lapse gets a new one.
`registration_status = "wrapped"`, `ens_v1.wrapper_state` and
`ens_v1.wrapper_fuses` report the wrapper. Names with no registrar lease keep the identity they had: a
wrapped subname is identified by its NameWrapper resource, and ENSv2 and
Basenames registrations by their own registration resource.

Exact-name detail, batch lookup, `GET /v1/permissions`, permission
`restrictions`, and registration-scoped history (`GET /v1/names/{name}/history`
and `GET /v1/events?registration_id=...`) all use this one handle. NameWrapper
events of a wrapped `.eth` name report the lease as their `registration_id`, and
a `registration_id` read of the lease returns them together with the
registrar's own rows. A NameWrapper resource that wraps or wrapped a lease is never a public
registration handle: `GET /v1/events?registration_id=` with it selects nothing,
`GET /v1/permissions?registration_id=` with it answers `200` with empty `data`
whether or not `address` is also given, and `GET /v1/permissions` pairing the
name with it is the proven-empty selection described above. A wrapped subname
has no lease, so its NameWrapper resource is its `registration_id` and selects
its permissions as before. `GET /v1/permissions?registration_id=<lease>` returns
the rows of the NameWrapper resource that currently controls the name. The lease
is matched to its name through the name's current `registration_id`, not through
the `NameWrapped` link, so this holds for a name registered through the
NameWrapper as well. On every page of a read bound to the lease, by `name` or by
`registration_id`, `restrictions.registration_id` is the lease, including an
empty page produced by an `address` with no grant.

#### Known gap: a name registered through the NameWrapper where the controller event grants the lease

Today's Mainnet manifest creates a `.eth` registration from the registrar
controller's `NameRegistered` event. When a name is registered through the
NameWrapper, that event comes after `NameWrapped` in the registration
transaction, so the wrap records no lease. For such a name the routes do not
yet agree on one handle:

- `GET /v1/names/{name}`, `POST /v1/lookup` and `GET /v1/permissions` serve the
  BaseRegistrar lease as `registration_id`, and permissions accept only the
  lease: `GET /v1/permissions?registration_id=<lease>` selects the NameWrapper
  resource's rows for this shape too.
- History still uses the NameWrapper resource as that name's handle.
  `GET /v1/names/{name}/history` and `GET /v1/events` report the NameWrapper
  resource as the `registration_id` of the name's NameWrapper events, and
  `GET /v1/events?registration_id=<lease>` returns the registrar's own rows
  without the NameWrapper events. History cannot tell this name from a wrapped
  subname, whose wrap also records no lease.

The gap closes when registrations come from the BaseRegistrar's own events. The
lease then exists before `NameWrapped`, every wrap records it, and history
follows the recorded lease. The registration identity change described here and
that manifest change must be deployed together. A name wrapped in a transaction
after its registration is not affected, because its wrap records the lease.

This is a client-visible change. Earlier releases served the NameWrapper
resource as the `registration_id` of a wrapped `.eth` name. A client that stored
a `registration_id` for a wrapped `.eth` name must read the name again and
replace the stored value; the old value no longer selects history or
permissions.
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L240-L278 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L110-L168 @ ens_v1@91c966f)

Every collection uses `cursor`, `next_cursor`, `page_size`, nullable
`total_count`, and `has_more`. Default `page_size` is 50; maximum is 200.
For reverse address inputs to `POST /v1/lookup` whose relation set maps directly
to a stored role group, `total_count` is the exact distinct-name count from the
same current joins and readability filters as the page, and `has_more` compares
against that live count on the one-row first-page path. Relation sets that need
post-filtering retain `total_count=null`; feed and detail profiles use the same
count and pagination semantics.
For a name with multiple relation rows, a readable row admits the name and the
returned `is_primary` is computed from the current name and primary-name claim
even when a different primary-matching relation row is unreadable. The retired
v1 page/sidecar pair disagreed on this case; the v2 live page and count joins
share the same eligibility rule.
Reverse address results from `POST /v1/lookup` additionally require the
projection rows behind a name to be [readable](glossary.md#readable--read-safe)
*and* supported. Both the name row and the address-relation row that admits it
must carry a supported support status; a name whose current name row or whose
matching relation row is unsupported is absent from both the page and
`total_count` rather than listed with an unsupported reason. Reverse lookup
results therefore answer which supported names an address holds. This
deliberately narrows earlier behavior, which listed unsupported names and left
the caller to read the reason; per-name unsupported detail now lives on the
name-shaped routes and diagnostics, which read the row directly.
`GET /v1/addresses/{address}/names` is the exception: it lists an unsupported
row when the matching current address relation is provable but other coverage
for that name is unsupported. Once the [per-name ownership
rule](consumer-capabilities.md#ensv1ensv2-mixed-history-ownership) is activated,
address relations come only from a name's selected current binding, so a
name with no selected binding, such as a `current_authority_not_projected`
row, has no current address relation and is structurally absent from this collection.
Listed unsupported rows do not carry a per-row reason; read the reason from the
name-shaped routes or diagnostics for the name in question.

## Error Model

Error envelope:

```json
{
  "error": {
    "code": "unsupported",
    "message": "the requested route option is not supported",
    "details": {}
  }
}
```

Uniform mapping:

<!-- openapi:enum ErrorCode -->
| Code | HTTP | Meaning |
| --- | --- | --- |
| `invalid_input` | 400 | malformed input, unnormalizable path name, bad parameter combination |
| `not_found` | 404 | single-resource GET with no answer |
| `unsupported` | 422 | the route cannot produce its contract for this input |
| `stale` | 409 | coherent selector not yet served for the selected snapshot |
| `conflict` | 409 | selector cannot form one canonical snapshot |
| `request_timeout` | 408 | the whole request exceeded the configured deadline |
| `rate_limited` | 429 | the enabled client limit, keyed by an IPv4 address or IPv6 `/64`, rejected a route that can trigger verified execution |
| `overloaded` | 503 | the process-wide, health-specific, or verified-execution in-flight ceiling was exhausted |
| `internal_error` | 500 | unexpected failure |

Rules:

- `unsupported` is `422`.
- Verified record-resolution failures surface as `status: "failed"` on the
  affected section with `failure_reason`, or as `stale` when the RPC provider
  cannot serve the selected block. Provider response timeouts for that path use
  the existing in-band execution-failure behavior; they are not whole-request
  `408` responses. Provider connect-phase timeouts and other transport failures
  during verified record resolution return whole-request `500 internal_error`;
  no execution outcome is cached for any v2 lookup. ENS/60 primary-name
  verification uses the same transport
  split with its CCIP-Read gateway leg: configured provider or gateway response
  timeouts, and the shared CCIP-Read gateway budget running out in whatever
  phase the in-flight gateway request was in, remain in-band failures for that
  response, while provider or gateway
  connect-phase timeouts, DNS failures, TLS failures, connection resets, and
  other transport failures within a request's own timeouts return
  whole-request `500 internal_error`. Neither
  result is persisted by the v2 serving path.
- Every route has a whole-request deadline. `/healthz` and `/v1/status` retain
  that deadline as their final backstop. `/healthz` bypasses
  the process-wide concurrency limiter and load shedding, uses a reserved
  one-connection database pool with a two-second check limit, and has a small
  independent health ceiling. HTTP-concurrency saturation and request-pool
  exhaustion therefore do not queue the probe; a failed or timed-out readiness
  connection reports the database as unreachable. The status routes retain
  global admission because their aggregate database query is not a liveness
  probe. A successful `/healthz` database check also returns a one-way identity
  token scoped to the currently running PostgreSQL postmaster, database OID,
  and server listener used by the connection, without exposing their raw
  values. The token changes when PostgreSQL restarts or the connection reaches
  a different listener. Alternate paths to the same postmaster, such as a Unix
  socket and TCP or different listen addresses, can therefore produce different
  tokens. It is populated only when bounded probes of the serving and
  reserved-readiness pools identify the same token.
- The verified-execution rate limit, when enabled, and all in-flight ceilings
  reject work before it waits for execution capacity. The rate-limit key is an
  IPv4 address or IPv6 `/64`; `/healthz` passes only through the health-specific
  ceiling. `GET /v1/names/{name}/records?source=auto` with omitted, empty, or
  whitespace-only `keys` (an indexed read of the inventory-derived default key
  set) and `GET /v1/addresses/{address}/primary-name?source=indexed` are
  indexed reads and do not enter verified-execution admission.
- Single-resource GETs return `404 not_found` when no answer exists.
- Collections return `200` with empty `data`.
- Batch lookup results carry in-band `status` per input; a batch never returns
  `404` for one missing input.
- The primary-name route is the documented exception to single-resource `404`:
  a valid `{address, coin_type, namespace}` tuple with no claim or an
  unsupported/mismatched verification returns `200` with in-band `status`.
- Error messages must not name internal storage or pipeline components.

## Table Conventions

The OpenAPI 3.1 document for the `/v1` surface is generated from Markdown
tables in this file and in [`api-v1-routes.md`](api-v1-routes.md). The prose
stays the contract of record. The tables state the part of it a machine can
check: object fields, their types and presence, enum values, and each
operation's parameters and responses. A table is read by the generator only
when a marker comment sits directly above it. Every other table, paragraph and
code block is ignored, so generation cannot tell whether a sentence beside a
table is still true; review keeps the two in step.

### Where things live

Objects and the presence conditions table live in this file, objects under
[Objects](#objects). Named enums live in this file, either under
[Enums](#enums) or in place where a section already lists the vocabulary (the
[status vocabulary](#status-vocabulary), the [error codes](#error-model) and
the unlisted permission surfaces). Operations and the response headers table
live in [`api-v1-routes.md`](api-v1-routes.md), in the section of the route
they describe. The generator reads no other file.

### Markers

A marker is an HTML comment on a line of its own, starting in the first
column, and the header row of its table is the next line. Markers and tables
inside list items, block quotes or fenced code blocks are not read.

| Marker | File | Table that follows |
| --- | --- | --- |
| `<!-- openapi:object Name -->` | `api-v1.md`, under Objects | fields table of object `Name` |
| `<!-- openapi:enum Name -->` | `api-v1.md` | enum table of enum `Name` |
| `<!-- openapi:conditions -->` | `api-v1.md`, under Objects | the presence conditions table; exactly one |
| `<!-- openapi:headers -->` | `api-v1-routes.md` | the response headers table; exactly one |
| `<!-- openapi:parameters METHOD /path -->` | `api-v1-routes.md` | parameters table of one operation |
| `<!-- openapi:responses METHOD /path -->` | `api-v1-routes.md` | responses table of one operation |

Object and enum names are ASCII identifiers that start with an uppercase
letter and continue with letters and digits, such as `NameRecord`. Condition
names are lowercase ASCII letters and digits, with words joined by
underscores, such as `counts_requested` or `released_ens_v1`. An operation is
named by its method and path: the method is `GET` or `POST`, and the path is
written exactly as the router declares it, with `{segment}` placeholders, for
example `GET /v1/names/{name}`. The marker, not the heading above it, names
the operation, so one route section can hold several operations.

In table cells, backticks mark literal wire text: field and parameter names,
header names, enum values, default values and error codes. Names of objects,
enums and conditions are written bare. Cells are never empty. A cell that
has nothing to say holds the bare word `none`, which cannot be confused with a
literal because literals are always in backticks. A literal `|` inside a cell
is written `\|`.

### Fields tables

An object is a level-3 heading under Objects whose text is the object's name,
then optional description paragraphs, then an optional composition line, then
the marker and the table. The description paragraphs become the object's
schema description.

The fields table has exactly these columns, in this order:

| Column | Meaning |
| --- | --- |
| `Field` | The wire name of the field, in backticks. |
| `Type` | A type expression from the grammar below. |
| `Presence` | A presence expression from the vocabulary below. |
| `Description` | What the field means. It becomes the property description. |

Every object is closed: the generator emits `additionalProperties: false`, so
a field that is not in the table is a contract violation. Fields that are
genuinely open-ended use a map type instead. Properties keep table order, and
the fields whose presence is `always` are the schema's `required` list.

Composition is written as the line `Extends Parent.`, for example
`Extends Envelope.`, directly above the marker, with blank lines allowed in
between. The object then has every field of the named parent, in the parent's order, followed by its own fields. The
generator writes out that complete property set in one schema rather than
combining schemas with `allOf`, because a closed parent cannot be widened by
combination. A field name that appears in both the parent and the child is
rejected, and so is a cycle of extends lines.

### Type grammar

A type expression is one of the productions below. Scalars are the JSON
types; every enum value is a string.

```text
type        = "nullable " value-type | value-type
value-type  = "string" | "integer" | "integer [" bound ", " bound "]"
            | "boolean" | "json"
            | "array of " type | "array [" size ", " size "] of " type
            | "map of string to " type
            | "object " Name
            | "enum " Name
            | "enum " literal { ", " literal }
            | "one of " alternative { ", " alternative }
alternative = "string" | "integer" | "boolean" | "object " Name
literal     = "`" wire-text "`"
bound       = decimal integer, optionally negative, without leading zeros
size        = nonnegative decimal integer without leading zeros
```

An inline enum and a one of consume the rest of the cell, so either may appear
last in an array or map type but nothing may follow it. `nullable` may prefix
any value type once; `nullable nullable` does not parse. Integer bounds must
be ordered from minimum to maximum. They constrain defaults and executable
examples as well as the generated schema. Use them for explicit contract
limits, rather than inferring limits from a wire field’s implementation type.
Array size bounds use the same inclusive minimum/maximum ordering and emit
`minItems`/`maxItems`. A deployment-configurable limit, such as the lookup
batch limit, stays in the description because the checked artifact must also
cover deployments configured above or below that default.

| Production | Example | Meaning | Generated schema |
| --- | --- | --- | --- |
| string | `string` | A JSON string. Formats such as RFC 3339 or `0x` hex are stated in the description. | `{"type": "string"}` |
| json | `json` | Any JSON value, only for a deliberately extensible leaf such as error details. Success response objects remain closed. | `{}` |
| integer | `integer` | A JSON number with no fractional part. | `{"type": "integer"}` |
| bounded integer | `integer [1, 200]` | An integer from the inclusive minimum through the inclusive maximum. | `{"type": "integer", "minimum": 1, "maximum": 200}` |
| boolean | `boolean` | `true` or `false`. | `{"type": "boolean"}` |
| nullable | `nullable string` | The value may be JSON `null`. This is about the value, not about whether the key is present. | `{"type": ["string", "null"]}`; for an object, enum or one of, `anyOf` of that schema and `{"type": "null"}` |
| array of | `array of string` | A JSON array whose items all have the inner type. | `{"type": "array", "items": {"type": "string"}}` |
| bounded array | `array [0, 200] of string` | An array containing zero through 200 items of the inner type. | `{"type": "array", "minItems": 0, "maxItems": 200, "items": {"type": "string"}}` |
| map of string to | `map of string to object AsOf` | A JSON object used as a dictionary: any key, each value of the inner type. The description says what the keys are. | `{"type": "object", "additionalProperties": {"$ref": "#/components/schemas/AsOf"}}` |
| object | `object ContractRef` | A named object from Objects. | `{"$ref": "#/components/schemas/ContractRef"}` |
| named enum | `enum Status` | A named enum from an enum table. | `{"$ref": "#/components/schemas/Status"}` |
| inline enum | ``enum `registrar`, `wrapper` `` | One of the listed strings. Use it for a vocabulary that only one field has. | `{"type": "string", "enum": ["registrar", "wrapper"]}` |
| one of | `one of object LookupNameInput, object LookupAddressInput` | Exactly one of the listed alternatives. | `{"oneOf": [{"$ref": "..."}, {"$ref": "..."}]}` |

A one of lists at least two alternatives, and they must be mutually
exclusive, so that JSON Schema's `oneOf` accepts every valid value. Scalar
alternatives must have different JSON types. A scalar and an object are
exclusive by type. Two object alternatives are exclusive when one has an
`always` field the other does not declare, or when both declare an `always`
field whose types are single-value inline enums with different values. The
scalar alternatives cover retained history record values alongside their
closed structured forms; they do not admit arbitrary JSON.

An open vocabulary, such as `unsupported_reason`, is typed `string` and not an
enum; its description names the values it can carry today. A closed
vocabulary is an enum.

### Presence vocabulary

The presence cell says whether the key is in the JSON object. It never says
anything about `null`: a key that is present with the value `null` is present,
and only a nullable type allows that value.

| Presence | Meaning | Generated |
| --- | --- | --- |
| `always` | The key is in every instance of the object. | listed in `required` |
| `optional` | The key may be absent. Nothing more is promised. | not required |
| `when name` | The key is present exactly when condition `name` holds, and absent otherwise. | not required; `"x-presence": "when name"` |
| `only when name` | The key is absent unless condition `name` holds. It may still be absent when the condition holds. | not required; `"x-presence": "only when name"` |

For example, `subname_count` on `NameRecord` has presence
`when name_counts_requested`, and `record_count` has `only when name_counts_requested`,
because a name with no current record inventory has no record count even when
counts were requested.

A condition is defined once, in the conditions table under Objects, and
referenced by its bare name after `when` or `only when`. The conditions table
has exactly two columns, `Condition` and `Holds when`. `Condition` holds the
bare name. `Holds when` says in prose when the condition holds; it may refer to
the request, to other fields of the same object by their backticked names, or
to the served state. The generator appends the condition's text to the
description of every field that uses it and emits the `x-presence` extension,
so payload tests can check a `when` field in both directions and an
`only when` field in one.

### Enum tables

An enum table follows an `openapi:enum` marker. Its first column holds exactly
one backticked value per row, and its header can be anything, so a table that
already lists a vocabulary can be marked in place. Any further columns are
prose; the generator joins them into that value's description and emits the
values in table order. The enum named `ErrorCode` must also have an `HTTP`
column, which the generator uses to check responses tables.

### Parameters tables

Every operation has exactly one parameters table, including an operation that
takes no parameters, whose table has a header and no rows. The table lists
every parameter the operation accepts; any other query parameter is rejected
with `400 invalid_input`, as [Parameters](#parameters) states.

| Column | Meaning |
| --- | --- |
| `Parameter` | The name as sent, in backticks. A header parameter uses its usual spelling, such as `If-None-Match`. A request body is the literal `body`. |
| `In` | `path`, `query`, `header` or `body`. |
| `Type` | A type expression. A query or header parameter is a string, integer (optionally bounded), boolean, enum, or an array of one of those. A body is `object Name`. |
| `Required` | `yes` or `no`. A path parameter is always `yes`. |
| `Default` | The value the server applies when the parameter is omitted, as a backticked literal, or `none`. A default that depends on other input, such as a namespace inferred from the name, is `none`, and the description says how it is chosen. |
| `Description` | What the parameter does, including values the type does not rule out but the route rejects. |

A query parameter of type `array of X` or `array [min, max] of X` is one
comma-separated value, such as
`include=counts,role_summary`; the generator emits `style: form` and
`explode: false`. Sending the key twice is not part of the contract. Every
`{segment}` in the path has one `path` row with the same name, and every
`path` row has a segment. At most one row is `body`, and only on a `POST`
operation; it becomes the operation's JSON request body.

### Responses tables

Every operation has exactly one responses table, with exactly these columns:

| Column | Meaning |
| --- | --- |
| `Status` | The HTTP status code, three digits. |
| `Body` | `object Name` for a JSON body, or `none` for a response without a body. |
| `Code` | On an error row, the backticked `ErrorCode` value the body carries. On a `2xx` or `304` row, `none`. |
| `Headers` | The backticked names of response headers the response can carry, separated by commas, or `none`. Each must be defined in the response headers table, which says when it appears. |
| `When` | In prose, when the route answers this way. |

There is one row per status and code, so `409 stale` and `409 conflict` are
two rows. Rows with the same status must have the same body; the generator
merges them into one OpenAPI response whose description lists each code with
its `When` text and whose schema constrains `error.code` to those codes while
retaining the closed `ErrorEnvelope`. An error row's code must map to its status in the `HTTP`
column of `ErrorCode`, and its body is `object ErrorEnvelope`. Every operation
has a `2xx` row, and every `2xx` body is an object that extends `Envelope`.

The response headers table follows the `openapi:headers` marker and has the
columns `Header`, `Type` and `Description`, one row per header.

### Operation metadata

The generator derives the rest of each operation. The `operationId` is the
lowercase method followed by the path segments with braces removed and hyphens
turned into underscores, joined by underscores: `GET /v1/names/{name}` becomes
`get_v1_names_name`. The single tag is the first path segment after `/v1`,
such as `names`. The description is a link to the operation's section in
[`api-v1-routes.md`](api-v1-routes.md). No summary is generated. Relative
links in any description are resolved against the documentation base URL the
generator is given.

When serving the document, the API pins generated Bigname documentation links
to its full hexadecimal build commit SHA. Builds whose SHA is unknown or is
not a full commit identifier retain the generator's `main` links. Custom
documentation hosts, existing pinned links and example values are preserved.

### Examples

A fenced code block tagged `json` is an illustration. It may elide values with
`…` or `0x...` and is never read. A fenced block whose info string is
`json openapi-example Name` is an executable example: complete JSON that the
generator validates against object `Name` and publishes as that schema's
example. Generation fails when it does not validate.

### What the generator rejects

Generation fails, and writes nothing, when any of these holds:

- a marker is malformed, of an unknown kind, or not followed on the next line
  by a table header;
- a table's columns differ from the ones given here for its kind, a row has
  the wrong number of cells, or a cell is empty;
- a type, presence, required or default cell does not parse, or a default is
  not a valid value of its type;
- a name is defined twice, or an object, enum or condition is defined and
  never referenced, or referenced and never defined;
- an object marker is outside Objects or not under a level-3 heading whose
  text is the object's name;
- a field name repeats within an object's complete property set, an extends
  line names an unknown object, or extends lines form a cycle;
- a one of has fewer than two alternatives, or its alternatives are not
  scalar types or named objects, or are not mutually exclusive;
- an enum table repeats a value or has a value that is not in backticks, or
  `ErrorCode` lacks its `HTTP` column;
- an operation has no parameters table or no responses table, or more than
  one of either;
- path parameters and path segments do not match, a `body` row appears on a
  `GET` operation or more than once, a path parameter is not required, or a
  required parameter has a default;
- a responses table has two rows with the same status and code, rows with the
  same status and different bodies, no `2xx` row, a `2xx` body that does not
  extend `Envelope`, an error row whose code does not map to its status, or a
  header that the headers table does not define.

Generation checks the tables against each other. Whether the tables match the
router and the served JSON is checked by tests over the generated document,
not by the generator.

## Objects

The tables enumerate closed response and request objects. Route prose carries
semantic combinations and the presence conditions that JSON Schema alone cannot enforce.

### Envelope

Every success response carries metadata, even when the metadata object is empty.

<!-- openapi:object Envelope -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `meta` | object Meta | always | Response metadata; present even when empty. |

### Page

<!-- openapi:object Page -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `cursor` | nullable string | always | Cursor supplied for this page, or null for the first page. |
| `next_cursor` | nullable string | always | Continuation cursor, or null when there is no next page. |
| `page_size` | integer | always | Requested maximum number of rows. |
| `total_count` | nullable integer | always | Exact count where supported and requested; null when unavailable or above the route count cap. |
| `has_more` | boolean | always | Whether another page exists. |

### Meta

<!-- openapi:object Meta -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `as_of` | map of string to object AsOf | optional | Readable per-chain positions keyed by decimal chain ID. |
| `as_of_completeness` | map of string to object AsOfCompleteness | optional | Reasons for request-scope chain positions suppressed from `as_of`; the key sets are disjoint. |
| `as_of_token` | string | optional | Opaque selector for replaying the served positions on routes that support `at`. |
| `completeness` | enum Completeness | optional | How completely the response can answer the requested capability. |
| `unsupported_fields` | array of string | optional | Names of fields or sections this answer could not serve. |
| `unsupported_reason` | string | optional | Open product reason vocabulary explaining an unsupported answer. |
| `unlisted_permission_surfaces` | array of enum UnlistedPermissionSurface | optional | Sorted permission surfaces whose holders the response does not enumerate. |
| `source` | enum Source | optional | Answer origin. |

### AsOf

<!-- openapi:object AsOf -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `block_number` | integer | always | EVM block number; nullable only where the table type permits an unknown block position. |
| `block_hash` | string | always | EVM block hash. |
| `timestamp` | string | always | Decimal string of Unix seconds; see [timestamp format and absent expiry](#timestamp-format-and-absent-expiry). |

### AsOfCompleteness

<!-- openapi:object AsOfCompleteness -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `completeness` | enum Completeness | always | How completely the response can answer the requested capability. |
| `unsupported_reason` | string | always | Open product reason vocabulary explaining an unsupported answer. |

### ContractRef

<!-- openapi:object ContractRef -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `chain_id` | integer | always | Numeric EVM chain ID. |
| `address` | string | always | EVM address in hexadecimal form. |

### WrapperFuses

Typed [expiry-effective NameWrapper fuse word](glossary.md#expiry-effective-namewrapper-fuse-word); each boolean reports whether the corresponding fuse is burnt.

<!-- openapi:object WrapperFuses -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `fuses` | integer | always | Expiry-effective uint32 fuse word. |
| `cannot_unwrap` | boolean | always | True when CANNOT_UNWRAP is burnt in the effective fuse word. |
| `cannot_burn_fuses` | boolean | always | True when CANNOT_BURN_FUSES is burnt in the effective fuse word. |
| `cannot_transfer` | boolean | always | True when CANNOT_TRANSFER is burnt in the effective fuse word. |
| `cannot_set_resolver` | boolean | always | True when CANNOT_SET_RESOLVER is burnt in the effective fuse word. |
| `cannot_set_ttl` | boolean | always | True when CANNOT_SET_TTL is burnt in the effective fuse word. |
| `cannot_create_subdomain` | boolean | always | True when CANNOT_CREATE_SUBDOMAIN is burnt in the effective fuse word. |
| `cannot_approve` | boolean | always | True when CANNOT_APPROVE is burnt in the effective fuse word. |
| `parent_cannot_control` | boolean | always | True when PARENT_CANNOT_CONTROL is burnt in the effective fuse word. |
| `is_dot_eth` | boolean | always | True when IS_DOT_ETH is burnt in the effective fuse word. |
| `can_extend_expiry` | boolean | always | True when CAN_EXTEND_EXPIRY is burnt in the effective fuse word. |

### EnsV1

What only ENSv1 holds about a name while ENSv1 decides it: the BaseRegistrar lease date and the NameWrapper position. Name-shaped rows carry it as `ens_v1` exactly while the name's authority is `ens_v1` or `ens_v0`; see the [naming dictionary](#naming-dictionary).

<!-- openapi:object EnsV1 -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `expires_at` | nullable string | when ens_v1_lifecycle | BaseRegistrar lease expiry as a decimal Unix-second string, exact up to `9223372036854775807` and served as that value above it, or null when the name has no lease, such as any subname or an ENSv1 registry child with no name row that no NameWrapper or registrar event named. The exception is a registry child with no name row that a NameWrapper or registrar event named only under a label that fails ENSIP-15 normalization: it omits this field and the wrapper fields (presence condition `ens_v1_lifecycle`), because its lease and NameWrapper state are projected without a name row. After the Universal Resolver cutover the top-level `expires_at` of a name with a live ENSv2 entry is that entry's expiry instead. Below that cap the lease's grace deadline is this value plus 90 days; a capped value does not give the deadline. |
| `wrapper_state` | enum WrapperState | when wrapper_backed | Current [NameWrapper lifecycle](#naming-dictionary) value. |
| `wrapper_fuses` | object WrapperFuses | when wrapper_backed | Typed [expiry-effective NameWrapper fuse word](glossary.md#expiry-effective-namewrapper-fuse-word). |

### NameRecord

Flat name-detail object, also used by resolver bound names. An identity-only unsupported record omits registration fields. A verified unsupported record may retain registration fields, but all unsupported name-level records omit counts and subregistry.

<!-- openapi:object NameRecord -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `registration_id` | string | only when registration_held | Opaque registration lifecycle handle; permission rows use the published permission-handle mapping. |
| `token_id` | string | optional | Decimal-string token identifier. |
| `owner` | string | optional | Who holds the name (see Naming Dictionary): the token holder of a name with a token (a BaseRegistrar lease, a NameWrapper token or an ENSv2 registry token), otherwise the registry owner of its node; omitted on released names and on expired emancipated or locked wrapped names. |
| `manager` | string | optional | Account that can change the name's registry record (see Manager): the registry owner of a name with no NameWrapper state and the token holder, the owner, of a wrapped name in any wrapper state; omitted while a wrapped `.eth` second-level name is in its registrar grace period, wherever the address it copies is omitted, and on a registry child whose NameWrapper state is unknown. |
| `registered_at` | string | optional | Start of the current registration, which renewals keep, and the ENSv1→ENSv2 migration of a name with an ENSv1 registrar lease (a `.eth` second-level name); a migrated name without one, such as a subname, starts its registration at its ENSv2 grant; decimal string of Unix seconds; see [timestamp format and absent expiry](#timestamp-format-and-absent-expiry). |
| `created_at` | string | optional | Decimal string of Unix seconds; see [timestamp format and absent expiry](#timestamp-format-and-absent-expiry). |
| `expires_at` | nullable string | optional | Decimal Unix-second string for a finite deadline, or null for a classified absent expiry; see [timestamp format and absent expiry](#timestamp-format-and-absent-expiry). |
| `expires_at_reason` | enum ExpiryReason | when null_expiry | Reason for a classified null expiry; absent for finite expiry and absent registration context. |
| `grace_ends_at` | nullable string | optional | Decimal Unix-second string for a finite deadline, or null for a classified absent expiry; see [timestamp format and absent expiry](#timestamp-format-and-absent-expiry). |
| `registration_status` | enum RegistrationStatus | when full_record | Current registration and control lifecycle label. |
| `authority` | enum Authority | optional | Selected authority arm; omitted when none is selected or for an ownerless registry row without a retained registrar binding. |
| `ens_v1` | object EnsV1 | when ens_v1_authority | What only ENSv1 holds about the name; present exactly while its authority is `ens_v1` or `ens_v0`. |
| `lapsed_registration` | object LapsedRegistration | optional | Last holder and release cause for a supported lapsed registration; absent for other release causes and for non-released names. |
| `migrated_at` | string | when migration_proven | Decimal string of Unix seconds; see [timestamp format and absent expiry](#timestamp-format-and-absent-expiry). |
| `name` | string | always | ENSIP-15 normalized name. |
| `display_name` | string | always | Display form of the name. |
| `namespace` | string | always | Resolved public namespace slug. |
| `namehash` | string | always | Hexadecimal ENS namehash. |
| `resolver` | object ContractRef | optional | Resolver contract for this answer. |
| `unresolvable_reason` | string | optional | Why the retained resolver cannot resolve this name; currently `no_live_ens_v2_entry`. |
| `subregistry` | object ContractRef | only when supported_name | Current subregistry pointer. Absent on every status=unsupported record, including verified unsupported records that retain registration fields. |
| `records` | object RecordGroups | optional | Grouped resolver keys and known values when the name may serve resolver records and an inventory or verified read supplies them. |
| `primary_name` | string | optional | Selected primary name when known. |
| `primary_address` | string | optional | Primary address when the read can serve it. |
| `chain_id` | integer | optional | Numeric EVM chain ID. |
| `network` | string | when full_record | Display network slug. |
| `subname_count` | integer | when name_counts_requested | Direct readable subname count, only with the counts expansion. |
| `record_count` | integer | only when name_counts_requested | Known record-selector count, only with counts requested and a current inventory. |
| `status` | enum Status | always | Result status; the route defines which outcomes are possible. |
| `unsupported_reason` | string | when status_unsupported | Open product reason vocabulary explaining an unsupported answer. |
| `failure_reason` | string | only when failure_status | Open product reason vocabulary explaining a failed, stale, missing or mismatched answer. |
| `unsupported_fields` | array of string | optional | Names of fields or sections this answer could not serve. |

### LapsedRegistration

<!-- openapi:object LapsedRegistration -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `owner` | string | optional | The name's owner when the registration ended; this is historical information, not the current owner. |
| `held_through` | enum LapsedHeldThrough | optional | Contract through which the ended registration was held. |
| `released_at` | string | optional | Decimal string of Unix seconds; see [timestamp format and absent expiry](#timestamp-format-and-absent-expiry). |
| `release_kind` | enum LapsedReleaseKind | optional | Whether the registration expired or was explicitly unregistered. |

### LookupRequest

<!-- openapi:object LookupRequest -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `profile` | enum LookupProfile | optional | Field budget: feed or detail; omission defaults to detail. |
| `namespace` | string | optional | Optional override: ens or basenames; auto, public or omission infer the public namespace set. |
| `inputs` | array of one of object LookupNameInput, object LookupAddressInput | always | One name or address input per result, preserving caller order. Batch limit defaults to 1000 and is deployment configurable. |

### LookupNameInput

<!-- openapi:object LookupNameInput -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `id` | string | optional | Optional caller correlation ID, echoed without synthesis. |
| `name` | string | always | Caller-supplied name; normalization failure is an in-band invalid_name result. |

### LookupAddressInput

<!-- openapi:object LookupAddressInput -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `id` | string | optional | Optional caller correlation ID, echoed without synthesis. |
| `address` | string | always | EVM address in hexadecimal form. |
| `coin_type` | integer | optional | Numeric coin type, default 60; no evm literal on this route. |
| `relation` | string | optional | Comma-separated owner, manager or any, or resolves_to alone. Omission asks for the selected primary name. role_holder and former_owner are not supported here. |
| `page_size` | integer [1, 200] | optional | Reverse result page size from 1 through 200; default 50. |
| `cursor` | string | optional | Per-input reverse continuation token, omitted when none was supplied. |

### LookupResult

<!-- openapi:object LookupResult -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `input` | object LookupResultInput | always | Caller input echoed with normalized reverse relation selection. |
| `kind` | enum LookupKind | always | Discriminator for this object. |
| `status` | enum Status | always | Result status; the route defines which outcomes are possible. |
| `unsupported_reason` | string | when status_unsupported | Open product reason vocabulary explaining an unsupported answer. |
| `failure_reason` | string | only when failure_status | Open product reason vocabulary explaining a failed, stale, missing or mismatched answer. |
| `normalization` | object NormalizationInfo | optional | Name normalization result, only when normalization changed the input or failed. |
| `record` | object LookupRecord | optional | One name result when an indexed answer exists. |
| `records` | array of object LookupRecord | optional | Address-result rows, empty when no name matches. |
| `page` | object Page | optional | Per-input pagination for reverse collections, never top-level batch pagination. |

### LookupResultInput

<!-- openapi:object LookupResultInput -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `id` | string | optional | Optional caller correlation ID, echoed without synthesis. |
| `name` | string | optional | Original caller-supplied name, before normalization. |
| `address` | string | optional | EVM address in hexadecimal form. |
| `coin_type` | integer | optional | Numeric ENS/SLIP-44 coin type. |
| `relation` | string | optional | Normalized comma-separated reverse relation set; any expands to owner,manager. |
| `page_size` | integer | optional | Requested maximum number of rows. |
| `cursor` | string | optional | Per-input reverse continuation token, omitted when none was supplied. |

### NormalizationInfo

<!-- openapi:object NormalizationInfo -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `changed` | boolean | always | Whether normalization changed the input spelling. |
| `input_name` | string | always | Original input name. |
| `reason` | enum `case_normalized`, `invalid_normalized_name` | always | Normalization outcome reason. |

### LookupRecord

Shared lookup feed/detail record. Feed records carry identity, `chain_id`, `network`, status, `subregistry` on name results, reverse `is_primary`/`relations`, `resolution` on `resolves_to` rows, `expires_at`, `expires_at_reason`, `grace_ends_at` and `ens_v1` with the detail record's values; detail adds the other registration fields and the resolver and grouped record fields; reverse records additionally carry matching relations and primary-name information. Feed records omit owner, manager and the other registration fields.

<!-- openapi:object LookupRecord -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `name` | string | always | ENSIP-15 normalized name. |
| `display_name` | string | always | Display form of the name. |
| `namespace` | string | always | Resolved public namespace slug. |
| `namehash` | string | always | Hexadecimal ENS namehash. |
| `registration_id` | string | optional | Opaque registration lifecycle handle; permission rows use the published permission-handle mapping. |
| `token_id` | string | optional | Decimal-string token identifier. |
| `owner` | string | optional | Who holds the name (see Naming Dictionary): the token holder of a name with a token (a BaseRegistrar lease, a NameWrapper token or an ENSv2 registry token), otherwise the registry owner of its node; omitted on released names and on expired emancipated or locked wrapped names. |
| `manager` | string | optional | Account that can change the name's registry record (see Manager): the registry owner of a name with no NameWrapper state and the token holder, the owner, of a wrapped name in any wrapper state; omitted while a wrapped `.eth` second-level name is in its registrar grace period, wherever the address it copies is omitted, and on a registry child whose NameWrapper state is unknown. |
| `registered_at` | string | optional | Start of the current registration, which renewals keep, and the ENSv1→ENSv2 migration of a name with an ENSv1 registrar lease (a `.eth` second-level name); a migrated name without one, such as a subname, starts its registration at its ENSv2 grant; decimal string of Unix seconds; see [timestamp format and absent expiry](#timestamp-format-and-absent-expiry). |
| `created_at` | string | optional | Decimal string of Unix seconds; see [timestamp format and absent expiry](#timestamp-format-and-absent-expiry). |
| `expires_at` | nullable string | optional | Decimal Unix-second string for a finite deadline, or null for a classified absent expiry; see [timestamp format and absent expiry](#timestamp-format-and-absent-expiry). |
| `expires_at_reason` | enum ExpiryReason | when null_expiry | Reason for a classified null expiry; absent for finite expiry and absent registration context. |
| `grace_ends_at` | nullable string | optional | Decimal Unix-second string for a finite deadline, or null for a classified absent expiry; see [timestamp format and absent expiry](#timestamp-format-and-absent-expiry). |
| `registration_status` | enum RegistrationStatus | optional | Current registration and control lifecycle label. |
| `lapsed_registration` | object LapsedRegistration | optional | Last holder and release cause for a supported lapsed registration; absent for other release causes and for non-released names. |
| `resolver` | object ContractRef | optional | Resolver contract for this answer. |
| `unresolvable_reason` | string | optional | Why the retained resolver cannot resolve this name; currently `no_live_ens_v2_entry`. |
| `subregistry` | object ContractRef | only when supported_name | Current subregistry pointer. Absent on every status=unsupported record, including verified unsupported records that retain registration fields. |
| `records` | object RecordGroups | optional | Grouped records on detail results when a current inventory is available; absent on feed results. |
| `primary_name` | string | optional | Selected primary name when known. |
| `primary_address` | string | optional | Primary address when the read can serve it. |
| `chain_id` | integer | optional | Numeric EVM chain ID. |
| `network` | string | optional | Display network slug. |
| `is_primary` | boolean | optional | Whether this name is the selected primary answer for the requested address and coin type. |
| `relations` | array of enum Relation | optional | Address-to-name relations that matched the row. |
| `resolution` | object AddressNameResolution | optional | Single-coin resolver match; present on a decimal-coin `resolves_to` result. |
| `authority` | enum Authority | optional | Selected authority arm; omitted when none is selected or for an ownerless registry row without a retained registrar binding. |
| `ens_v1` | object EnsV1 | when ens_v1_authority | What only ENSv1 holds about the name, on `profile=detail` and `profile=feed` records alike; present exactly while its authority is `ens_v1` or `ens_v0`, which a feed record does not carry. |
| `migrated_at` | string | optional | Decimal string of Unix seconds; see [timestamp format and absent expiry](#timestamp-format-and-absent-expiry). |
| `status` | enum Status | always | Result status; the route defines which outcomes are possible. |
| `unsupported_reason` | string | when status_unsupported | Open product reason vocabulary explaining an unsupported answer. |
| `failure_reason` | string | only when failure_status | Open product reason vocabulary explaining a failed, stale, missing or mismatched answer. |
| `unsupported_fields` | array of string | optional | Names of fields or sections this answer could not serve. |

### StatusData

<!-- openapi:object StatusData -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `status` | enum OpsStatus | always | Result status; the route defines which outcomes are possible. |
| `pending_invalidation_count` | integer | always | Always zero under the current phase architecture. |
| `pending_invalidation_count_capped` | boolean | always | Always false under the current phase architecture. |
| `dead_letter_count` | integer | always | Always zero under the current phase architecture. |
| `chains` | map of string to object ChainStatus | always | Per-chain readiness, keyed by decimal chain ID. |

### ChainStatus

<!-- openapi:object ChainStatus -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `latest_block` | nullable integer | always | Latest block. |
| `indexed_block` | nullable integer | always | Indexed block. |
| `safe_block` | nullable integer | always | Safe block. |
| `finalized_block` | nullable integer | always | Finalized block. |
| `lag_blocks` | nullable integer | always | Nonnegative indexing lag, or null when evidence is missing or a redo is active. |
| `lag_seconds` | nullable integer | always | Nonnegative indexing lag, or null when evidence is missing or a redo is active. |
| `network_block` | nullable integer | always | Network block. |
| `network_head_observed_at` | nullable string | always | Decimal string of Unix seconds; see [timestamp format and absent expiry](#timestamp-format-and-absent-expiry). |
| `network_head_age_seconds` | nullable integer | always | Network head age seconds. |
| `network_head_status` | enum NetworkHeadStatus | always | Network head status. |
| `ingestion_lag_blocks` | nullable integer | always | Ingestion lag blocks. |
| `ingestion_lag_seconds` | nullable integer | always | Ingestion lag seconds. |
| `status` | enum OpsStatus | always | Result status; the route defines which outcomes are possible. |

### SearchName

Current name summary used by search and the namespace expiry list. The expiry list may include lapsed_registration; search omits it.

<!-- openapi:object SearchName -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `name` | string | always | ENSIP-15 normalized name. |
| `display_name` | string | always | Display form of the name. |
| `namespace` | string | always | Resolved public namespace slug. |
| `namehash` | string | always | Hexadecimal ENS namehash. |
| `owner` | string | optional | Who holds the name (see Naming Dictionary): the token holder of a name with a token (a BaseRegistrar lease, a NameWrapper token or an ENSv2 registry token), otherwise the registry owner of its node; omitted on released names and on expired emancipated or locked wrapped names. |
| `manager` | string | optional | Account that can change the name's registry record (see Manager): the registry owner of a name with no NameWrapper state and the token holder, the owner, of a wrapped name in any wrapper state; omitted while a wrapped `.eth` second-level name is in its registrar grace period, wherever the address it copies is omitted, and on a registry child whose NameWrapper state is unknown. |
| `registration_status` | enum RegistrationStatus | always | Current registration and control lifecycle label. |
| `registered_at` | string | optional | Start of the current registration, which renewals keep, and the ENSv1→ENSv2 migration of a name with an ENSv1 registrar lease (a `.eth` second-level name); a migrated name without one, such as a subname, starts its registration at its ENSv2 grant; decimal string of Unix seconds; see [timestamp format and absent expiry](#timestamp-format-and-absent-expiry). |
| `created_at` | string | optional | Decimal string of Unix seconds; see [timestamp format and absent expiry](#timestamp-format-and-absent-expiry). |
| `expires_at` | nullable string | optional | Decimal Unix-second string for a finite deadline, or null for a classified absent expiry; see [timestamp format and absent expiry](#timestamp-format-and-absent-expiry). |
| `expires_at_reason` | enum ExpiryReason | when null_expiry | Reason for a classified null expiry; absent for finite expiry and absent registration context. |
| `grace_ends_at` | nullable string | optional | Decimal Unix-second string for a finite deadline, or null for a classified absent expiry; see [timestamp format and absent expiry](#timestamp-format-and-absent-expiry). |
| `authority` | enum Authority | optional | Registry generation that owns the node, as the name's detail serves it; omitted when none is selected or for an ownerless registry row without a retained registrar binding. |
| `ens_v1` | object EnsV1 | when ens_v1_authority | What only ENSv1 holds about the name; present exactly while the row's `authority` is `ens_v1` or `ens_v0`. |
| `lapsed_registration` | object LapsedRegistration | optional | Last holder and release cause for a supported lapsed registration; absent for other release causes and for non-released names. |

### Subname

<!-- openapi:object Subname -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `name` | string | always | Normalized name; the subnames and registry-label routes also permit the documented non-name forms. |
| `display_name` | string | always | Display form of the name. |
| `namespace` | string | always | Resolved public namespace slug. |
| `namehash` | string | always | Hexadecimal ENS namehash. |
| `labelhash` | string | optional | Hexadecimal labelhash when the readable label is not known. |
| `owner` | string | optional | Who holds the name (see Naming Dictionary): the token holder of a name with a token (a BaseRegistrar lease, a NameWrapper token or an ENSv2 registry token), otherwise the registry owner of its node; omitted on released names and on expired emancipated or locked wrapped names. |
| `manager` | string | optional | Account that can change the name's registry record (see Manager): the registry owner of a name with no NameWrapper state and the token holder, the owner, of a wrapped name in any wrapper state; omitted while a wrapped `.eth` second-level name is in its registrar grace period, wherever the address it copies is omitted, and on a registry child whose NameWrapper state is unknown. |
| `registration_status` | enum RegistrationStatus | always | Current registration and control lifecycle label. |
| `registered_at` | string | optional | Start of the current registration, which renewals keep, and the ENSv1→ENSv2 migration of a name with an ENSv1 registrar lease (a `.eth` second-level name); a migrated name without one, such as a subname, starts its registration at its ENSv2 grant; decimal string of Unix seconds; see [timestamp format and absent expiry](#timestamp-format-and-absent-expiry). |
| `created_at` | string | optional | Decimal string of Unix seconds; see [timestamp format and absent expiry](#timestamp-format-and-absent-expiry). |
| `expires_at` | nullable string | optional | Decimal Unix-second string for a finite deadline, or null for a classified absent expiry; see [timestamp format and absent expiry](#timestamp-format-and-absent-expiry). |
| `expires_at_reason` | enum ExpiryReason | when null_expiry | Reason for a classified null expiry; absent for finite expiry and absent registration context. |
| `grace_ends_at` | nullable string | optional | Decimal Unix-second string for a finite deadline, or null for a classified absent expiry; see [timestamp format and absent expiry](#timestamp-format-and-absent-expiry). |
| `authority` | enum Authority | optional | Registry generation that owns the node, as the child's name row serves it, or for an ENSv1 registry child with no name row its registry's, `ens_v1` or `ens_v0`; omitted when none applies or for an ownerless registry row without a retained registrar binding. |
| `ens_v1` | object EnsV1 | when ens_v1_authority | What only ENSv1 holds about the name; present exactly while the row's `authority` is `ens_v1` or `ens_v0`; a child with no name row holds no lease, so its `expires_at` is null. |
| `subregistry` | object ContractRef | optional | Current subregistry pointer, omitted when no current pointer is known. |
| `subname_count` | integer | optional | Direct readable subname count, only with the counts expansion. |

### AddressName

<!-- openapi:object AddressName -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `name` | string | always | Normalized name; the subnames and registry-label routes also permit the documented non-name forms. |
| `display_name` | string | always | Display form of the name. |
| `namespace` | string | always | Resolved public namespace slug. |
| `namehash` | string | always | Hexadecimal ENS namehash. |
| `permission_resource_id` | string | optional | Opaque handle for requesting the selected registration's permissions. |
| `owner` | string | optional | Who holds the name (see Naming Dictionary): the token holder of a name with a token (a BaseRegistrar lease, a NameWrapper token or an ENSv2 registry token), otherwise the registry owner of its node; omitted on released names and on expired emancipated or locked wrapped names. |
| `manager` | string | optional | Account that can change the name's registry record (see Manager): the registry owner of a name with no NameWrapper state and the token holder, the owner, of a wrapped name in any wrapper state; omitted while a wrapped `.eth` second-level name is in its registrar grace period, wherever the address it copies is omitted, and on a registry child whose NameWrapper state is unknown. |
| `registration_status` | enum RegistrationStatus | always | Current registration and control lifecycle label. |
| `registered_at` | string | optional | Start of the current registration, which renewals keep, and the ENSv1→ENSv2 migration of a name with an ENSv1 registrar lease (a `.eth` second-level name); a migrated name without one, such as a subname, starts its registration at its ENSv2 grant; decimal string of Unix seconds; see [timestamp format and absent expiry](#timestamp-format-and-absent-expiry). |
| `created_at` | string | optional | Decimal string of Unix seconds; see [timestamp format and absent expiry](#timestamp-format-and-absent-expiry). |
| `expires_at` | nullable string | optional | Decimal Unix-second string for a finite deadline, or null for a classified absent expiry; see [timestamp format and absent expiry](#timestamp-format-and-absent-expiry). |
| `expires_at_reason` | enum ExpiryReason | when null_expiry | Reason for a classified null expiry; absent for finite expiry and absent registration context. |
| `grace_ends_at` | nullable string | optional | Decimal Unix-second string for a finite deadline, or null for a classified absent expiry; see [timestamp format and absent expiry](#timestamp-format-and-absent-expiry). |
| `authority` | enum Authority | optional | Registry generation that owns the node: the selected authority arm, or for an ENSv1 registry child with no name row its registry's, `ens_v1` or `ens_v0`; omitted when none applies or for an ownerless registry row without a retained registrar binding. |
| `ens_v1` | object EnsV1 | when ens_v1_authority | What only ENSv1 holds about the name; present exactly while its authority is `ens_v1` or `ens_v0`. |
| `migrated_at` | string | optional | Decimal string of Unix seconds; see [timestamp format and absent expiry](#timestamp-format-and-absent-expiry). |
| `relations` | array of enum Relation | always | Address-to-name relations that matched the row. |
| `is_primary` | boolean | always | Whether this name is the selected primary answer for the requested address and coin type. |
| `resolution` | object AddressNameResolution | optional | Single-coin resolver match; present on a decimal-coin `resolves_to` result. |
| `resolutions` | array of object AddressNameResolution | optional | Resolver matches for `coin_type=evm`, ascending by coin type; at most 100 per row. |
| `lapsed_registration` | object LapsedRegistration | optional | Last holder and release cause for a supported lapsed registration; absent for other release causes and for non-released names. |
| `subname_count` | integer | optional | Direct readable subname count, only with the counts expansion. |
| `record_count` | integer | optional | Known record-selector count, only with counts requested and a current inventory. |
| `role_summary` | array of object AddressNameRoleSummary | optional | Per-address grants requested with `include=role_summary`. |
| `restrictions` | one of object WrapperRestrictions, object RegistryRestrictions | optional | [Resource restrictions](glossary.md#resource-restrictions) of the selected registration when that model applies. |

### AddressNameRoleSummary

<!-- openapi:object AddressNameRoleSummary -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `address` | string | always | EVM address in hexadecimal form. |
| `grants` | array of object AddressNameGrant | always | Grants. |

### AddressNameGrant

<!-- openapi:object AddressNameGrant -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `grant_relation` | enum GrantRelation | optional | `operator` for an effective account approval; direct grants omit it. |
| `grant_scope` | object GrantScope | always | Scope in which these powers apply. |
| `powers` | array of enum PermissionPower | always | Product permission powers; see [permission powers vocabulary](#permission-powers-vocabulary). |

### NameRecords

<!-- openapi:object NameRecords -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `namespace` | string | always | Resolved public namespace slug. |
| `resolver` | nullable object ContractRef | always | Resolver contract for this answer. |
| `records` | map of string to object RecordAnswer | always | Resolver records or reverse result rows in the route-specific shape. |
| `inventory` | object RecordInventory | optional | Optional record inventory requested with `include=inventory`. |

### RecordAnswer

<!-- openapi:object RecordAnswer -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `status` | enum Status | always | Result status; the route defines which outcomes are possible. |
| `value` | string | only when record_ok | Successful record value as a string; text is decoded text and binary record families use hex. |
| `unsupported_reason` | string | when status_unsupported | Open product reason vocabulary explaining an unsupported answer. |
| `failure_reason` | string | only when failure_status | Open product reason vocabulary explaining a failed, stale, missing or mismatched answer. |
| `meta` | object RecordAnswerMeta | optional | Response metadata; present even when empty. |

### RecordAnswerMeta

<!-- openapi:object RecordAnswerMeta -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `basis` | enum `derived` | always | The answer was derived from a stored resolver read rule. |
| `rule` | enum ResolverReadFeature | always | Rule. |
| `source_record_key` | string | always | Source record key. |

### RecordInventory

<!-- openapi:object RecordInventory -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `known_keys` | array of string | always | Record keys observed in the selected inventory. |
| `unset_keys` | array of string | always | Keys whose absence is authoritatively retained; currently empty for phase inventory reads. |
| `unsupported_keys` | array of string | always | Requested or inventoried keys that the inventory cannot answer. |
| `abi_content_types` | nullable array of string | always | Known decimal ABI content types, or null with abi_unsupported_reason when unavailable. |
| `abi_unsupported_reason` | string | optional | Why ABI content types cannot be enumerated; present instead of their key list. |

### PrimaryName

<!-- openapi:object PrimaryName -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `address` | string | always | EVM address in hexadecimal form. |
| `coin_type` | integer | always | Numeric ENS/SLIP-44 coin type. |
| `namespace` | string | always | Resolved public namespace slug. |
| `answers` | array of object PrimaryNameAnswer | always | Answers ordered by indexed, then verified source. |
| `verification` | object PrimaryNameVerification | optional | Cross-source verification, absent from indexed-only reads. |

### PrimaryNameAnswer

<!-- openapi:object PrimaryNameAnswer -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `source` | enum Source | always | Answer origin. |
| `status` | enum Status | always | Result status; the route defines which outcomes are possible. |
| `name` | string | optional | Normalized name; the subnames and registry-label routes also permit the documented non-name forms. |
| `raw_claim_name` | string | optional | Stored reverse claim before product normalization when it differs from the served name. |
| `unsupported_reason` | string | when status_unsupported | Open product reason vocabulary explaining an unsupported answer. |
| `failure_reason` | string | only when failure_status | Open product reason vocabulary explaining a failed, stale, missing or mismatched answer. |

### PrimaryNameVerification

<!-- openapi:object PrimaryNameVerification -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `status` | enum Status | always | Result status; the route defines which outcomes are possible. |
| `name` | string | optional | Normalized name; the subnames and registry-label routes also permit the documented non-name forms. |
| `unsupported_reason` | string | when status_unsupported | Open product reason vocabulary explaining an unsupported answer. |
| `failure_reason` | string | only when failure_status | Open product reason vocabulary explaining a failed, stale, missing or mismatched answer. |

### RegistryName

<!-- openapi:object RegistryName -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `name` | string | always | Normalized name; the subnames and registry-label routes also permit the documented non-name forms. |
| `display_name` | string | always | Display form of the name. |
| `namespace` | string | always | Resolved public namespace slug. |
| `namehash` | string | always | Hexadecimal ENS namehash. |

### RegistryCounts

<!-- openapi:object RegistryCounts -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `labels` | nullable integer | always | Exact current label count, or null for a historical selection where a current label count is not meaningful. |
| `roles` | integer | optional | Count of observed nonzero declared role assignments; only with include=counts. |
| `events` | integer | optional | Count of product-visible events emitted by the registry; only with include=counts. |

### ReferencedBy

<!-- openapi:object ReferencedBy -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `data` | array of object RegistryName | always | Data. |
| `page` | object Page | always | Standard collection pagination metadata. |

### RegistryOverview

<!-- openapi:object RegistryOverview -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `chain_id` | integer | always | Numeric EVM chain ID. |
| `address` | string | always | EVM address in hexadecimal form. |
| `name` | nullable object RegistryName | always | Earliest current name that points to this registry, or null when no current pointer names it. |
| `parent_registry` | nullable object ContractRef | always | Registry that emitted the selected name pointer, or null when name is null. |
| `created_block_number` | nullable integer | always | Block of the first registry observation, or the declaration start block; null when unknown. |
| `created_at` | nullable string | always | Decimal string of Unix seconds; see [timestamp format and absent expiry](#timestamp-format-and-absent-expiry). |
| `created_transaction_hash` | nullable string | always | Creation or first-pointer transaction; null for a declaration or missing transaction evidence. |
| `created_basis` | enum `registry_created`, `subregistry_pointer`, `declared` | always | Evidence used for the registry creation fields. |
| `counts` | object RegistryCounts | always | Current label count and optional role/event totals. |
| `referenced_by` | object ReferencedBy | always | Nested page of current names pointing to this registry. |

### ResolverOverview

<!-- openapi:object ResolverOverview -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `chain_id` | integer | always | Numeric EVM chain ID. |
| `address` | string | always | EVM address in hexadecimal form. |
| `mirror` | object ResolverMirror | optional | Registry followed by an admitted mirror resolver; absent for other resolver kinds. |
| `bound_names` | object BoundNames | always | Nested page of names whose current serving resource selects this resolver. |

### ResolverMirror

<!-- openapi:object ResolverMirror -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `kind` | enum `ensv1_registry` | always | Discriminator for this object. |
| `registry` | object ContractRef | always | Registry followed by this mirror resolver. |

### BoundNames

<!-- openapi:object BoundNames -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `data` | array of object NameRecord | always | Data. |
| `page` | object Page | always | Standard collection pagination metadata. |

### Namespace

<!-- openapi:object Namespace -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `namespace` | string | always | Resolved public namespace slug. |
| `capabilities` | map of string to object NamespaceCapability | always | Capability names mapped to completeness and per-chain support. |
| `networks` | array of object NamespaceNetwork | always | Networks. |

### NamespaceCapability

<!-- openapi:object NamespaceCapability -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `completeness` | enum Completeness | always | How completely the response can answer the requested capability. |
| `unsupported_reason` | string | optional | Open product reason vocabulary explaining an unsupported answer. |
| `chains` | map of string to object NamespaceChainCapability | optional | Per-chain capability results, keyed by decimal chain ID; absent when no per-chain split applies. |

### NamespaceChainCapability

<!-- openapi:object NamespaceChainCapability -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `completeness` | enum Completeness | always | How completely the response can answer the requested capability. |
| `unsupported_reason` | string | optional | Open product reason vocabulary explaining an unsupported answer. |

### NamespaceNetwork

<!-- openapi:object NamespaceNetwork -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `network` | string | always | Display network slug. |
| `chain_id` | integer | optional | Numeric EVM chain ID. |

### PermissionRow

Extends AddressNameGrant.

<!-- openapi:object PermissionRow -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `address` | string | always | EVM address in hexadecimal form. |
| `registration_id` | string | always | Opaque registration lifecycle handle; permission rows use the published permission-handle mapping. |
| `record_resource` | object RecordResource | optional | Record selector described by the grant; only current setter powers contribute. |
| `name` | string | optional | Normalized name; the subnames and registry-label routes also permit the documented non-name forms. |
| `authority_context` | enum AuthorityContext | always | Whether the row is current for the named registration selection or an audit by registration handle. |
| `wrapper_state` | enum WrapperState | optional | Current [NameWrapper lifecycle](#naming-dictionary) value. |
| `wrapper_fuses` | object WrapperFuses | optional | Typed [expiry-effective NameWrapper fuse word](glossary.md#expiry-effective-namewrapper-fuse-word). |
| `lineage` | object PermissionLineage | optional | Lineage. |

### PermissionLineage

<!-- openapi:object PermissionLineage -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `grant` | object LineageItem | always | Allowlisted evidence describing the grant. |
| `revocation` | object LineageItem | optional | Allowlisted evidence describing a retained revocation when present. |
| `inheritance_path` | array of object LineageItem | optional | Allowlisted grant inheritance steps; absent for an empty path. |
| `transfer_behavior` | one of string, object LineageItem | optional | Retained transfer rule string (currently replace_on_authority_change or cleared_on_transfer_unless_cannot_approve), or an allowlisted lineage object. An object with no surviving fields is omitted. |

### Event

<!-- openapi:object Event -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `id` | string | always | Opaque 64-character event identity; identical for the same event across the product history routes. |
| `type` | enum HistoryEventType | always | Type. |
| `name` | string | optional | Normalized name; the subnames and registry-label routes also permit the documented non-name forms. |
| `namespace` | string | always | Resolved public namespace slug. |
| `registration_id` | nullable string | always | Opaque registration lifecycle handle; permission rows use the published permission-handle mapping. |
| `block_number` | nullable integer | always | EVM block number; nullable only where the table type permits an unknown block position. |
| `timestamp` | nullable string | always | Decimal string of Unix seconds; see [timestamp format and absent expiry](#timestamp-format-and-absent-expiry). |
| `transaction_hash` | nullable string | always | Transaction hash; null or absent, as indicated, for a row without a transaction. |
| `log_index` | nullable integer | always | Log index; null or absent, as indicated, for a row without a log. |
| `kind` | enum HistoryEventKind | when history_raw_requested | Raw event kind, only with include=raw. |
| `contract_address` | nullable string | when history_data_requested | Emitter address, or null for a state-derived row. |
| `data` | object HistoryEventData | when history_data_requested | Friendly retained event payload. |

### HistoryEvent

<!-- openapi:object HistoryEvent -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `id` | string | always | Opaque 64-character event identity; identical for the same event across the product history routes. |
| `type` | enum HistoryEventType | always | Type. |
| `name` | string | always | Normalized name; the subnames and registry-label routes also permit the documented non-name forms. |
| `subject` | enum HistoryRowSubject | when child_registrations_requested | Whether the history row concerns the named parent or one of its direct children. |
| `namespace` | string | always | Resolved public namespace slug. |
| `registration_id` | nullable string | always | Opaque registration lifecycle handle; permission rows use the published permission-handle mapping. |
| `block_number` | nullable integer | always | EVM block number; nullable only where the table type permits an unknown block position. |
| `timestamp` | nullable string | always | Decimal string of Unix seconds; see [timestamp format and absent expiry](#timestamp-format-and-absent-expiry). |
| `transaction_hash` | nullable string | always | Transaction hash; null or absent, as indicated, for a row without a transaction. |
| `log_index` | nullable integer | always | Log index; null or absent, as indicated, for a row without a log. |
| `kind` | enum HistoryEventKind | when history_raw_requested | Raw event kind, only with include=raw. |
| `contract_address` | nullable string | when history_data_requested | Emitter address, or null for a state-derived row. |
| `data` | object HistoryEventData | when history_data_requested | Friendly retained event payload. |

### RegistryLabel

Extends Subname.

<!-- openapi:object RegistryLabel -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `role_holder_count` | integer | optional | Distinct direct role-holder count, present with `include=counts`. |

### RecordGroups

Grouped resolver keys and values shared by name detail and lookup detail; see [grouped name-profile records](api-v1-routes.md#grouped-name-profile-records).

<!-- openapi:object RecordGroups -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `seen_addresses` | array of string | always | Observed canonical decimal coin types. |
| `addresses` | map of string to nullable string | always | Known address values by coin type. Null means cleared; a seen key absent from this map has an unknown value. |
| `seen_texts` | array of string | always | Observed text keys, including keys outside the request selector grammar. |
| `texts` | map of string to nullable string | always | Known text values. Null means cleared; absent means unknown. |
| `seen_abis` | array of string | optional | Observed decimal ABI content types; absent when enumeration is unsupported. |
| `abi_unsupported_reason` | string | optional | Why ABI content types cannot be enumerated; present instead of their key list. |
| `abis` | map of string to nullable string | always | Known ABI values by content type. Currently empty because ABI bytes are not retained. |
| `seen_singletons` | array of enum `contenthash`, `name` | always | Singletons whose write was observed or whose key was verified. |
| `contenthash` | nullable string | optional | Contenthash bytes as a hex string, null when cleared or authoritatively unset, absent when unknown. |
| `name` | nullable string | optional | Forward name record, null when cleared or authoritatively unset, absent when unknown. |

### AddressNameResolution

<!-- openapi:object AddressNameResolution -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `coin_type` | integer | always | Numeric ENS/SLIP-44 coin type. |
| `record_key` | string | always | Public resolver-record key that answered the request. |

### GrantScope

<!-- openapi:object GrantScope -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `kind` | enum `root`, `registry`, `registration`, `resolver`, `record_manager`, `account`, `registrar_controller` | always | Discriminator for this object. |
| `detail` | object GrantScopeDetail | always | Scope-specific fields; empty for root, registry and registration. Registrar-controller is history-only. |

### GrantScopeDetail

Closed union of scope detail fields. Each scope uses exactly the shape listed in [permissions](api-v1-routes.md#get-v1permissions).

<!-- openapi:object GrantScopeDetail -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `resolver` | object ContractRef | optional | Resolver contract for this answer. |
| `chain_id` | integer | optional | Numeric EVM chain ID. |
| `manager` | string | optional | Record-manager address, only for record_manager scope. |
| `authority_kind` | enum `registry`, `registrar`, `wrapper` | optional | Account approval authority kind. |
| `authority_contract` | string | optional | Account approval contract address. |
| `owner` | string | optional | Account whose approval grants the powers. |
| `registrar` | object ContractRef | optional | Registrar contract, only for the history registrar_controller scope. |

### RecordResource

<!-- openapi:object RecordResource -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `kind` | enum `address`, `text`, `data`, `abi`, `interface`, `argument` | always | Discriminator for this object. |
| `hash` | string | always | Hexadecimal hash of the setter argument. |
| `coin_type` | integer | optional | Numeric ENS/SLIP-44 coin type. |
| `coin_type_decimal` | string | optional | Coin type beyond uint64, as exact decimal text; mutually exclusive with coin_type. |
| `key` | string | optional | Printable UTF-8 text/data key. |
| `key_bytes` | string | optional | Raw hex key when it cannot be presented as text; mutually exclusive with key. |
| `content_type` | integer | optional | Numeric ABI content type. |
| `content_type_decimal` | string | optional | ABI content type beyond uint64; mutually exclusive with content_type. |
| `interface_id` | string | optional | Hex interface ID. |
| `selectors` | array of object RecordResource | optional | Current setter interpretations when kind is argument; at least two entries. |

### LineageItem

<!-- openapi:object LineageItem -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `kind` | enum `event`, `permission`, `registration_authority`, `registration_rebound`, `ens_v1_authority`, `resolver_root_fallback`, `registry_root_fallback` | optional | Discriminator for this object. |
| `registration_id` | string | optional | Opaque registration lifecycle handle; permission rows use the published permission-handle mapping. |
| `resolver` | object ContractRef | optional | Resolver contract for this answer. |
| `powers` | array of enum PermissionPower | optional | Product permission powers; see [permission powers vocabulary](#permission-powers-vocabulary). |
| `relation` | enum `holder`, `operator`, `token_approval` | optional | NameWrapper relationship behind the grant or revocation. |

### WrapperRestrictions

<!-- openapi:object WrapperRestrictions -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `kind` | enum `ens_v1_wrapper` | always | Discriminator for this object. |
| `registration_id` | string | always | Opaque registration lifecycle handle; permission rows use the published permission-handle mapping. |
| `wrapper_state` | enum WrapperState | always | Current [NameWrapper lifecycle](#naming-dictionary) value. |
| `wrapper_fuses` | object WrapperFuses | always | Typed [expiry-effective NameWrapper fuse word](glossary.md#expiry-effective-namewrapper-fuse-word). |
| `wrapper_expires_at` | nullable string | optional | Decimal Unix-second string for a finite deadline, or null for a classified absent expiry; see [timestamp format and absent expiry](#timestamp-format-and-absent-expiry). |
| `wrapper_expires_at_reason` | enum `no_expiry`, `not_set` | when null_wrapper_expiry | Reason for a classified null wrapper expiry. |

### RegistryRestrictions

<!-- openapi:object RegistryRestrictions -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `kind` | enum `ens_v2_registry` | always | Discriminator for this object. |
| `registration_id` | string | always | Opaque registration lifecycle handle; permission rows use the published permission-handle mapping. |
| `locked_roles` | array of enum `unregister`, `renew`, `set_subregistry`, `set_resolver`, `transfer` | always | Token-scoped roles whose assignment can no longer change. |

### HexBytes

<!-- openapi:object HexBytes -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `encoding` | enum `hex` | always | Encoding. |
| `bytes` | string | always | Lowercase 0x-prefixed bytes. |

### DeletedRecordValue

<!-- openapi:object DeletedRecordValue -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `deleted` | boolean | always | True for a retained DNS record deletion. |

### DnsZonehashValue

<!-- openapi:object DnsZonehashValue -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `previous` | object HexBytes | always | Previous DNS zone hash. |
| `current` | object HexBytes | always | New DNS zone hash. |

### DataHashValue

<!-- openapi:object DataHashValue -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `indexed_data_hash` | string | always | Hexadecimal hash retained by a data-record event. |

### HistoryEventData

Closed history payload fields. The event type and retained evidence determine which fields appear; an empty object is valid. See [history event payloads](api-v1-routes.md#history-event-payloads-includedata-includeraw).

<!-- openapi:object HistoryEventData -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `registrant` | string | optional | Registrant the registration event named, as the on-chain event carries it. |
| `owner` | string | optional | Owner address the event named. |
| `expires_at` | nullable string | optional | Decimal Unix-second string for a finite deadline, or null for a classified absent expiry; see [timestamp format and absent expiry](#timestamp-format-and-absent-expiry). |
| `expires_at_reason` | enum ExpiryReason | when null_expiry | Reason for a classified null expiry; absent for finite expiry and absent registration context. |
| `resolver` | object ContractRef | optional | Resolver contract for this answer. |
| `subregistry` | object ContractRef | optional | New subregistry pointer; omitted for a cleared pointer. |
| `action_id` | string | optional | Opaque registration action grouping key. |
| `action_role` | enum `registered`, `linked`, `reachable` | optional | Role of this row in its registration action. |
| `fuses` | integer | optional | Retained uint32 NameWrapper fuse word. |
| `from` | string | optional | Previous owner or transfer sender. |
| `to` | string | optional | Transfer recipient. |
| `coin_type` | integer | optional | Numeric ENS/SLIP-44 coin type. |
| `key` | string | optional | Retained record key; history can contain families unavailable on the records read route. |
| `value` | one of string, object HexBytes, object DeletedRecordValue, object DnsZonehashValue, object DataHashValue | optional | Retained record write value. Text and hex strings, raw bytes, DNS changes and data hashes keep their current wire forms. |
| `record_id` | string | optional | Decimal record ID on a record-ID resolver. |
| `node` | string | optional | Lowercase hex node on a node-keyed resolver. |
| `address` | string | optional | Primary-name subject or permission holder. |
| `name` | string | optional | Recorded primary-name value when available. |
| `name_status` | enum `set`, `cleared`, `unknown` | optional | Whether the primary-name event set, cleared or did not retain a name. |
| `grant_scope` | object GrantScope | optional | Scope in which these powers apply. |
| `approved` | boolean | optional | Registrar-controller approval after the change. |
| `powers` | array of enum PermissionPower | optional | Product permission powers; see [permission powers vocabulary](#permission-powers-vocabulary). |
| `added_powers` | array of enum PermissionPower | optional | Powers added, when the log supplies the old set. |
| `removed_powers` | array of enum PermissionPower | optional | Powers removed, when the log supplies the old set. |
| `migration_path` | enum `unwrapped`, `unlocked_wrapped`, `locked_wrapped`, `locked_child`, `emancipated_child` | optional | Migration path retained by the event. |

### LinkEvent

<!-- openapi:object LinkEvent -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `block_number` | integer | always | EVM block number; nullable only where the table type permits an unknown block position. |
| `timestamp` | string | always | Decimal string of Unix seconds; see [timestamp format and absent expiry](#timestamp-format-and-absent-expiry). |
| `transaction_hash` | string | always | Transaction hash; null or absent, as indicated, for a row without a transaction. |
| `log_index` | integer | always | Log index; null or absent, as indicated, for a row without a log. |

### GrantEvent

<!-- openapi:object GrantEvent -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `block_number` | nullable integer | always | EVM block number; nullable only where the table type permits an unknown block position. |
| `timestamp` | string | optional | Decimal string of Unix seconds; see [timestamp format and absent expiry](#timestamp-format-and-absent-expiry). |
| `transaction_hash` | string | optional | Transaction hash; null or absent, as indicated, for a row without a transaction. |
| `log_index` | integer | optional | Log index; null or absent, as indicated, for a row without a log. |

### ResolverLink

<!-- openapi:object ResolverLink -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `record_id` | string | always | Decimal record ID. |
| `namehash` | string | always | Hexadecimal ENS namehash. |
| `default` | boolean | always | True for the empty-name node. |
| `namespace` | string | optional | Resolved public namespace slug. |
| `name` | string | optional | Normalized name; the subnames and registry-label routes also permit the documented non-name forms. |
| `display_name` | string | optional | Display form of the name. |
| `link_event` | object LinkEvent | always | Current link observation. |

### ResolverRole

<!-- openapi:object ResolverRole -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `address` | string | always | EVM address in hexadecimal form. |
| `registration_id` | string | always | Opaque registration lifecycle handle; permission rows use the published permission-handle mapping. |
| `name` | string | optional | Normalized name; the subnames and registry-label routes also permit the documented non-name forms. |
| `powers` | array of enum PermissionPower | always | Product permission powers; see [permission powers vocabulary](#permission-powers-vocabulary). |
| `grant_event` | object GrantEvent | optional | Earliest canonical permission event for the holder in the row provenance. |
| `record_resource` | object RecordResource | optional | Record selector described by the grant; only current setter powers contribute. |

### ErrorEnvelope

<!-- openapi:object ErrorEnvelope -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `error` | object ErrorBody | always | Uniform error body. |

### ErrorBody

<!-- openapi:object ErrorBody -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `code` | enum ErrorCode | always | Stable code whose HTTP mapping is defined in Error Model. |
| `message` | string | always | Human-readable error message. |
| `details` | map of string to json | always | Extensible structured error details. Keys and JSON value types are open; current handlers commonly return an empty object. |

### NameDetailResponse

Success envelope for the corresponding product operation.

Extends Envelope.

<!-- openapi:object NameDetailResponse -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `data` | object NameRecord | always | Requested result data. |

### LookupResponse

Success envelope for the corresponding product operation.

Extends Envelope.

<!-- openapi:object LookupResponse -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `data` | array of object LookupResult | always | Requested result data. |

### StatusResponse

Success envelope for the corresponding product operation.

Extends Envelope.

<!-- openapi:object StatusResponse -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `data` | object StatusData | always | Requested result data. |

### NamesResponse

Success envelope for the corresponding product operation.

Extends Envelope.

<!-- openapi:object NamesResponse -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `data` | array of object SearchName | always | Requested result data. |
| `page` | object Page | always | Standard collection pagination metadata. |

### SearchResponse

Success envelope for the corresponding product operation.

Extends Envelope.

<!-- openapi:object SearchResponse -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `data` | array of object SearchName | always | Requested result data. |
| `page` | object Page | always | Standard collection pagination metadata. |

### SubnamesResponse

Success envelope for the corresponding product operation.

Extends Envelope.

<!-- openapi:object SubnamesResponse -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `data` | array of object Subname | always | Requested result data. |
| `page` | object Page | always | Standard collection pagination metadata. |

### AddressNamesResponse

Success envelope for the corresponding product operation.

Extends Envelope.

<!-- openapi:object AddressNamesResponse -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `data` | array of object AddressName | always | Requested result data. |
| `page` | object Page | always | Standard collection pagination metadata. |

### RecordsResponse

Success envelope for the corresponding product operation.

Extends Envelope.

<!-- openapi:object RecordsResponse -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `data` | object NameRecords | always | Requested result data. |

### PrimaryNameResponse

Success envelope for the corresponding product operation.

Extends Envelope.

<!-- openapi:object PrimaryNameResponse -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `data` | object PrimaryName | always | Requested result data. |

### EventsResponse

Success envelope for the corresponding product operation.

Extends Envelope.

<!-- openapi:object EventsResponse -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `data` | array of object Event | always | Requested result data. |
| `page` | object Page | always | Standard collection pagination metadata. |

### NameHistoryResponse

Success envelope for the corresponding product operation.

Extends Envelope.

<!-- openapi:object NameHistoryResponse -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `data` | array of object HistoryEvent | always | Requested result data. |
| `page` | object Page | always | Standard collection pagination metadata. |

### PermissionsResponse

Success envelope for the corresponding product operation.

Extends Envelope.

<!-- openapi:object PermissionsResponse -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `data` | array of object PermissionRow | always | Requested result data. |
| `page` | object Page | always | Standard collection pagination metadata. |
| `restrictions` | one of object WrapperRestrictions, object RegistryRestrictions | optional | [Resource restrictions](glossary.md#resource-restrictions) of the selected registration when that model applies. |

### ResolverResponse

Success envelope for the corresponding product operation.

Extends Envelope.

<!-- openapi:object ResolverResponse -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `data` | object ResolverOverview | always | Requested result data. |

### ResolverLinksResponse

Success envelope for the corresponding product operation.

Extends Envelope.

<!-- openapi:object ResolverLinksResponse -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `data` | array of object ResolverLink | always | Requested result data. |
| `page` | object Page | always | Standard collection pagination metadata. |

### ResolverRolesResponse

Success envelope for the corresponding product operation.

Extends Envelope.

<!-- openapi:object ResolverRolesResponse -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `data` | array of object ResolverRole | always | Requested result data. |
| `page` | object Page | always | Standard collection pagination metadata. |

### RegistryResponse

Success envelope for the corresponding product operation.

Extends Envelope.

<!-- openapi:object RegistryResponse -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `data` | object RegistryOverview | always | Requested result data. |

### RegistryLabelsResponse

Success envelope for the corresponding product operation.

Extends Envelope.

<!-- openapi:object RegistryLabelsResponse -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `data` | array of object RegistryLabel | always | Requested result data. |
| `page` | object Page | always | Standard collection pagination metadata. |

### NamespaceResponse

Success envelope for the corresponding product operation.

Extends Envelope.

<!-- openapi:object NamespaceResponse -->
| Field | Type | Presence | Description |
| --- | --- | --- | --- |
| `data` | object Namespace | always | Requested result data. |

### Presence conditions

<!-- openapi:conditions -->
| Condition | Holds when |
| --- | --- |
| full_record | The record is not the identity-only unsupported name object. Indexed coverage downgrades produce that identity-only object; a verified lookup failure can retain the registration summary even when its result status is unsupported. |
| supported_name | The name-level record has status other than unsupported. This excludes both identity-only unsupported records and verified unsupported records that retain registration fields. |
| status_unsupported | The object status is unsupported. |
| failure_status | The object status is failed, stale, not_found or mismatch. |
| registration_held | The record is full and its registration_status is not unregistered. |
| wrapper_backed | The name has a current NameWrapper lifecycle value under the expiry-effective fuse rule; wrapper_state and wrapper_fuses appear together. |
| ens_v1_lifecycle | The row is not an ENSv1 registry child with no current name row that a NameWrapper or registrar event named only under a label failing ENSIP-15 normalization. Such a child's lease and NameWrapper state are kept without a name row, so its object carries no lifecycle fields: no expires_at and no wrapper fields. |
| ens_v1_authority | The name's served authority is ens_v1 or ens_v0. The object is absent under ens_v2, with no authority, and on identity-only unsupported records. A lookup feed record follows the authority of the detail record for the same name. |
| migration_proven | The full record selects authority ens_v2 and retains the block time of its latest activated MigrationApplied transition. |
| name_counts_requested | The request to name detail carries include=counts and the record status is not unsupported. Counts are absent on every unsupported name-level record. |
| null_expiry | The object contains expires_at with JSON null. Absent expires_at and finite strings do not satisfy this condition. |
| null_wrapper_expiry | The object contains wrapper_expires_at with JSON null. |
| history_data_requested | The history request includes data in include. |
| history_raw_requested | The history request includes raw in include. |
| child_registrations_requested | The name-history request includes child_registrations in include. |
| record_ok | The per-key record status is ok. |

## Enums

Closed vocabularies used by the object and operation tables. The result status,
error code, expiry reason, permission power, and unlisted permission surface
vocabularies are marked at their existing canonical tables above.

### RegistrationStatus

<!-- openapi:enum RegistrationStatus -->
| Value |
| --- |
| `active` |
| `wrapped` |
| `registered` |
| `released` |
| `unregistered` |

### WrapperState

<!-- openapi:enum WrapperState -->
| Value |
| --- |
| `wrapped` |
| `emancipated` |
| `locked` |

### Authority

<!-- openapi:enum Authority -->
| Value |
| --- |
| `ens_v0` |
| `ens_v1` |
| `ens_v2` |

### Source

<!-- openapi:enum Source -->
| Value |
| --- |
| `indexed` |
| `verified` |

### Completeness

<!-- openapi:enum Completeness -->
| Value |
| --- |
| `full` |
| `partial` |
| `unsupported` |

### Finality

<!-- openapi:enum Finality -->
| Value |
| --- |
| `latest` |
| `safe` |
| `finalized` |

### OpsStatus

<!-- openapi:enum OpsStatus -->
| Value |
| --- |
| `ready` |
| `degraded` |
| `stale` |

### HistoryScope

<!-- openapi:enum HistoryScope -->
| Value |
| --- |
| `name` |
| `registration` |
| `both` |

### AuthorityContext

<!-- openapi:enum AuthorityContext -->
| Value |
| --- |
| `current_for_name` |
| `resource_audit` |

### Relation

<!-- openapi:enum Relation -->
| Value |
| --- |
| `owner` |
| `manager` |
| `role_holder` |
| `resolves_to` |
| `former_owner` |

### AddressNamesDedupe

<!-- openapi:enum AddressNamesDedupe -->
| Value |
| --- |
| `name` |
| `registration` |

### AddressNamesSort

<!-- openapi:enum AddressNamesSort -->
| Value |
| --- |
| `name` |
| `expires_at` |
| `registered_at` |
| `created_at` |

### HistoryEventType

<!-- openapi:enum HistoryEventType -->
| Value |
| --- |
| `registration` |
| `renewal` |
| `release` |
| `expiry` |
| `transfer` |
| `authority` |
| `resolver` |
| `record` |
| `primary_name` |
| `permission` |
| `subregistry` |
| `migration` |

### NameMatch

<!-- openapi:enum NameMatch -->
| Value |
| --- |
| `prefix` |
| `contains` |

### SortOrder

<!-- openapi:enum SortOrder -->
| Value |
| --- |
| `asc` |
| `desc` |

### LookupKind

<!-- openapi:enum LookupKind -->
| Value |
| --- |
| `name` |
| `address` |

### LookupProfile

<!-- openapi:enum LookupProfile -->
| Value |
| --- |
| `feed` |
| `detail` |

### LapsedReleaseKind

<!-- openapi:enum LapsedReleaseKind -->
| Value |
| --- |
| `expired` |
| `unregistered` |

### LapsedHeldThrough

<!-- openapi:enum LapsedHeldThrough -->
| Value |
| --- |
| `registrar` |
| `wrapper` |
| `registry` |

### GrantRelation

<!-- openapi:enum GrantRelation -->
| Value |
| --- |
| `operator` |

### NetworkHeadStatus

<!-- openapi:enum NetworkHeadStatus -->
| Value |
| --- |
| `fresh` |
| `stale` |
| `unavailable` |
| `pending` |
| `unconfigured` |

### ResolverReadFeature

<!-- openapi:enum ResolverReadFeature -->
| Value |
| --- |
| `ensip19_default_address` |
| `ensip10_extended_resolver` |

### HistoryRowSubject

<!-- openapi:enum HistoryRowSubject -->
| Value |
| --- |
| `name` |
| `child` |

### HistoryEventKind

<!-- openapi:enum HistoryEventKind -->
| Value |
| --- |
| `RegistrationGranted` |
| `LabelRegistered` |
| `RegistrationRenewed` |
| `RegistrationReleased` |
| `ExpiryChanged` |
| `TokenControlTransferred` |
| `AuthorityTransferred` |
| `AuthorityEpochChanged` |
| `ResolverChanged` |
| `RecordChanged` |
| `RecordVersionChanged` |
| `ReverseChanged` |
| `PermissionChanged` |
| `PermissionScopeChanged` |
| `RolesChanged` |
| `EACRolesChanged` |
| `SubregistryChanged` |
| `MigrationApplied` |
