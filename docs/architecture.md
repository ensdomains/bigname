# Architecture

bigname is a versioned, replayable indexing and read platform for ENS (v1 and v2) and Basenames. It serves the native `v2` REST contract, a narrow GraphQL compatibility surface, and operator health.

This document defines the model. Wire format lives in [`api-v2.md`](api-v2.md) and [`api-v2-routes.md`](api-v2-routes.md); persistence in [`storage.md`](storage.md); manifests, intake, projections, and execution in their own files. Implementation sequencing and parallel-work boundaries live under [`internal/`](internal/).

## Objectives

For any supported name or address, every answer must be:

- point-in-time
- replayable
- auditable
- explicit about provenance, coverage, finality, and consistency
- safe under chain reorgs and source-graph expansion

REST answers use the v2 `data`/`page`/`meta` envelope and report unsupported,
partial, stale, or failed results explicitly.

## Public namespaces

The public namespaces are exactly `ens` and `basenames`.

- `ens` is a single product that absorbs both ENSv1 and ENSv2 as internal authority epochs.
- `basenames` is separate, covering Basenames-issued `*.base.eth` names on Base.[^bn-readme-l70]
- `base.eth` itself stays under `ens` because upstream treats it as the L1 root domain handled by the Mainnet `L1Resolver`.[^bn-l1resolver-l13][^bn-l1resolver-l154]

Namespace assignment is driven by an internal `NamespaceRegistry` with versioned rules: highest-priority `exact_name`, then `suffix`, then `authority_root`. Initial policy:

- exact `base.eth` → `ens`
- suffix `*.base.eth` → `basenames`
- other supported ENS surfaces → `ens`

Conflicts reject canonical [admission](glossary.md); namespace assignment happens before `logical_name_id` is minted. [Deployment profile](glossary.md) is separate from namespace: deployment profiles select the admitted chain set (mainnet, post-audit Sepolia), not a different namespace product. One runtime answers under one deployment profile at a time.

## Read contract

The served REST families are lookup, status, name, address, permission, search,
event, resolver, namespace, and diagnostic routes under `/v2`. Their parameters,
result vocabulary, snapshot behavior, and pagination rules are defined in
[`api-v2-routes.md`](api-v2-routes.md). The deleted v1 REST shapes are not a
compatibility layer for this contract.

### Subgraph-compatible GraphQL surface

Alongside the REST contract, bigname serves a narrow, deliberately scoped subgraph-compatible read surface at `POST /graphql`. It is **not** general subgraph parity: it implements only `domain`, `domains`, `registrationConnection`, and `domainConnection` over `bigname_phase.name_current`, `bigname_phase.address_names_current`, and `bigname_phase.record_inventory_current` [projections](glossary.md). A root read selects the current ENS chain position from `bigname_phase.chain_heads`, admits unchanged rows whose target is at or before that position, carries the same selection into nested record-inventory fields, and verifies before returning that the matching completed `project` phase row did not change. Rows whose projection support status is `unsupported` are not exposed; an unsupported record inventory maps to the compatibility surface's existing empty record shapes. GraphQL `createdAt` uses a declared registration or history timestamp; when neither exists, it preserves the non-null response field with Unix epoch `0` because the current phase projection has no legacy surface-creation timestamp. The GraphQL surface is a compatibility adapter, not a consumer-replacement declaration.

Manager name inputs have ENS name semantics rather than display-string equality.
`domain(id: ...)` and `DomainFilter.name` normalize a name, compute its
namehash, and match that hash, so `ALICE.eth` resolves the same ENS name as
`alice.eth`. An `id` already shaped as a namehash is matched only within the
`ens` namespace. `name_contains` compares its pattern case-sensitively with
the normalized ENS name, and `orderBy: name` uses byte-wise stored
display-name order. Resolver record fields select the
sole projected inventory for the name's resource without coupling its event
boundary to the later name-publication target. If a resource has multiple
inventory rows and no declared boundary selects exactly one, the operation
errors instead of serving empty records or choosing arbitrarily.

GraphQL availability follows the exact current-head admission rule shared
with v2 lookup. While the `project` phase catches up to a newly stored chain head, an
operation that would return projection rows errors instead of serving the
previous completed publication as a stale view. Connection counts are also
more expensive than a plain `COUNT(*)`: they compute distinct matched rows and
rank their publication targets so the API can validate count admission before
returning the result.

## Identity model

Four identity layers, each with its own continuity rules:

### `logical_name_id`

Stable identity for an on-chain name within a namespace, written as `<namespace>:<namehash>` where `namehash` is the lowercase `0x`-prefixed 32-byte node. It survives backing-resource rotation, token regeneration, lapses, re-registrations, and normalizer-version changes. Raw label text and normalization results are attributes, never identity inputs, under the audit's [normalization-as-a-gate decision](../simplification-audit-20260730.md#normalization-as-a-gate-not-stored-identity).

### `resource_id`

Stable identity for the backing authority object — the [anchor](glossary.md) for permission lineage, control lineage, token lineage, and resolver-scoped permissions. Opaque UUID.

- For ENSv2, `resource_id` maps to the upstream permissioned-registry EAC resource, not the current ERC-1155 token ID. The registry exposes `getResource(anyId)` and `getTokenId(anyId)`, emits `TokenResource(tokenId, resource)` when a label is linked, and emits `TokenRegenerated(oldTokenId, newTokenId)` when role changes burn and mint a replacement token while leaving the resource unchanged.[^v2-iperm-l34][^v2-iperm-l67][^v2-iperm-l72][^v2-events-l69][^v2-pr-l451]
- For ENSv1, `resource_id` is the stable identity for the authority object: registry-only control, registrar-backed registration, or wrapper-backed control. Registry-only authority is scoped to the full node/namehash, not just the leftmost labelhash, so subnames with the same label under different parents never share a registry-only `resource_id`. The same `resource_id` persists across holder, resolver, expiry, grace, fuse, status, and non-divergent controller changes. It rotates when authority moves to a different anchor — the concrete authority object backing the name (direct registry control, a registrar lease, or a wrapper position). Rotation happens on a registry-only ↔ registrar ↔ wrapper move, a live registrar ↔ registry-owner divergence, or a full lapse + re-registration. Exact prior-anchor reuse applies only when that prior anchor becomes authoritative again, including unwrap back to the same registrar lease and registry-side convergence back to the same live unreleased registrar lease. It does not imply that all registry owner / token holder convergence collapses history; post-release returns or different holders / controllers stay on distinct anchors.
- For Basenames, `resource_id` anchors the Base-side authority object even when L1 compatibility transport is involved.[^bn-readme-l69][^bn-readme-l70][^bn-l1resolver-l13]

### `token_lineage_id`

Stable identity for tokenized ownership history. Token IDs can change while the resource is unchanged; the lineage outlives the ID.

- ENSv1: registry-only control has none. A registrar lease or wrapper position mints one. Renewal, transfer, expiry, and grace within the same anchor preserve it. Authority moving to a different tokenized anchor rotates it; returning to the prior tokenized anchor reactivates the prior lineage.
- ENSv2: preserved across `TokenRegenerated`. Update the current token ID attribute and append the normalized event. Resource identity is anchored by upstream `eacVersionId`; tokens are versioned by `tokenVersionId`. Unregister/re-register increments both; regeneration increments only the token version.[^v2-pr-l28][^v2-pr-l203][^v2-pr-l237][^v2-pr-l241][^v2-pr-l242][^v2-pr-l451][^v2-pr-l461][^v2-pr-l542][^v2-pr-l547]

### `contract_instance_id`

Stable identity for registry, registrar, resolver, wrapper, or transport instances. Minted when a manifest-declared or discovery-admitted contract is first added to the canonical source graph. One admitted address on one chain maps to one `contract_instance_id` across all manifest and discovery epochs; re-admission after an inactive gap reuses it with a new active range. A proxy keeps its identity when implementation changes; only a different watched contract address rotates it.

## Name surface model

Two layers separate public names from backing authority:

`NameSurface` is the canonical row per `logical_name_id`. It stores the raw name and labels observed for that namehash when they have a PostgreSQL-safe UTF-8 decoding, their available DNS wire encoding and hash path, the normalizer version used to evaluate them, and explicit visibility/error state. A label that does not byte-equal its normalized form, cannot decode, or cannot be represented as one DNS label keeps a deactivated shadow row. For undecodable labels, the chain-native namehash and byte-valued `label_preimages` are identity truth while the row's unavailable text display inputs remain empty. Normalized label or display text is derived at read time and is not stored as identity. Verbatim labels also live in `label_preimages` and [normalized events](glossary.md).

`SurfaceBinding` records how a public surface binds to a backing [resource](glossary.md) through time — each row is a [surface binding](glossary.md):

- `surface_binding_id`, `logical_name_id`, `resource_id`, `binding_kind`, `active_from`, `active_to`, provenance, [canonicality](glossary.md) state.

Binding kinds: `declared_registry_path`, `linked_subregistry_path`, `resolver_alias_path`, `observed_wildcard_path`, `migration_rebind`, `observed_only`.

Resolver-family normalized events attach `logical_name_id` and `resource_id` only when their node has a materialized active or deactivated-shadow `NameSurface`. Without that row, both identity fields remain null and only `raw_fact_ref.interpreter_state_key` relates successive state for the same record.

A standalone ENSv1 or Basenames registry-owner observation for a node without a materialized `NameSurface` creates the node-scoped direct-registry resource, but it does not independently create a public surface or binding. A registry-owner observation attributed to a live registrar lease, including ownership setup reconciled within the registration transaction, instead remains retained interpreter state without creating a separate direct-registry resource, surface, or binding; that attribution keeps the direct-registry authority dormant. Once a registrar or wrapper observation materializes the surface, retained direct-registry authority may become its fallback. If release of the active registrar lease makes a nonzero retained registry owner authoritative, the release boundary must materialize the registry-anchored resource and open its replacement `SurfaceBinding` in the same interpret batch. The resource and binding use block-boundary provenance because upstream registrar availability is derived by comparing the stored expiry plus the 90-day grace period with `block.timestamp`, rather than by a lease-expiry log (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L17 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L100 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L103 @ ens_v1@91c966f). The retained registry owner survives that registrar release because ENS stores node ownership independently until another registry ownership write replaces it (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L7 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L13 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L170 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L171 @ ens_v1@91c966f). This rule does not widen ENSv2 registry-name topology or emit either side of a parent-child binding for an otherwise unknown registry-only surface.

ENSv1 authority moves (wrap, unwrap, re-registration) carry the identity change in `resource_id` and `token_lineage_id`; ordinary lifecycle stays `declared_registry_path`. A new `SurfaceBinding` row appears only when the bound `resource_id` changes — transfer and expiry within the same anchor do not. Same-transaction reconciliation considers only setup observations whose `source_event == NewOwner` when removing transient controller artifacts; its canonical admitted case is the retired 2019 controller stream and its register/reclaim-shaped ownership setup. Registrar reclaim writes the registry through `setSubnodeOwner`, which emits `NewOwner` (upstream: .refs/ens_subgraph/subgraph.yaml:L145 @ ens_subgraph@723f1b6) (upstream: .refs/ens_subgraph/subgraph.yaml:L148 @ ens_subgraph@723f1b6) (upstream: .refs/ens_subgraph/subgraph.yaml:L162 @ ens_subgraph@723f1b6) (upstream: .refs/ens_subgraph/subgraph.yaml:L165 @ ens_subgraph@723f1b6) (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L172 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L174 @ ens_v1@91c966f). The current controller's resolver path also registers first to `address(this)`, but then calls registry `setRecord`; that ownership write emits `Transfer`, not `NewOwner`, so it never enters this removal branch. This is benign: the general same-transaction reconciliation still attributes the incoming `Transfer` observations to the registration resource (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L294 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L301 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L33 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L39 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L68 @ ens_v1@91c966f). In the wrapped registration path, NameWrapper registers the registrar token and registry ownership to itself while the separate `wrappedOwner` remains the user; the incoming NameWrapper `resource_control` grant therefore belongs to the registrar-backed registration resource even though its subject differs from the registration event's registrant (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L289 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L291 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L297 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L300 @ ens_v1@91c966f). `NameWrapped` is emitted within that wrapper call before the controller emits its later `NameRegistered`, so the later registrar observation records the registrar resource without displacing the active wrapper resource (upstream: .refs/ens_v1/deployments/mainnet/WrappedETHRegistrarController.json:L656 @ ens_v1@91c966f). Basenames writes the final owner directly before its registrar emits `NameRegistered`, so it has no equivalent wrapper-owner split or transient controller-owner epoch (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L423 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L425 @ basenames@1809bbc). When block-scoped replay has preloaded an older registry-only authority for that name, same-transaction registry and resolver observations that establish the incoming registration are attributed to the new registrar resource. Resolver records before the first registry ownership-setup observation retain the predecessor resource. From that first setup through `NameRegistered`, otherwise-unattached or registry-only resolver records belong to the registration resource; records already attached to another materialized resource retain their event-time authority. Pre-registration membership for permission events is decided by revocation semantics, not by comparing a subject with the registrant: revocations, plus matching earlier grants that those revocations close, stay on the preceding registry-only resource so latest-wins permission projection supersedes them. Other incoming grants move to the registration resource. The superseded registry-only resource row is always retained at its first derivation block, whether or not a surviving same-batch row references it. A proven transient registration-controller `NewOwner` observation and that controller's matching self-grant and self-revoke are setup artifacts rather than a separate authority transition. Renewals and wrapper observations retain their event-time authority. This registration-setup rule does not define a registry-only `RegistrationGranted` pre-state contract.

For born-wrapped registrations, registrar authority state tracks the final registry owner separately from the controller event's registrant. On unwrap, NameWrapper transfers the registrar token from itself to the requested registrant, so that registrar transfer closes NameWrapper's grant on the registration resource (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L382 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L391 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L394 @ ens_v1@91c966f).

| Case | Anchor | `resource_id` | `token_lineage_id` |
| --- | --- | --- | --- |
| Registry-only sub.alice.eth | direct registry | one registry-anchored | none |
| Register alice.eth | registrar lease | one registrar-anchored | mint registrar lineage |
| Wrap alice.eth | wrapper-backed | close registrar binding, open wrapper-anchored | mint wrapper lineage |
| Unwrap before lease ends | same registrar lease | reactivate prior registrar | reactivate prior registrar lineage |
| Expiry / grace | unchanged anchor | unchanged | unchanged |
| Registrar release with a retained nonzero registry owner | direct registry fallback | materialize the registry-anchored resource and replacement binding together | none |
| Re-registration after lapse | new registrar lease | mint new | mint new |

This separation captures: one resource under multiple public names, alias-resolved names without direct registry entries, observed wildcard names, and surfaces that rebind across time.

## Normalization and preimage observation

Normalization is version-pinned via `normalizer_version`. The active normalizer is `ensip15@ens-normalize-0.1.1`, backed by the Rust `ens-normalize` crate and its embedded ENSIP-15 data. API input normalization, adapter name-surface admission, reverse-claim claim-name normalization, resolver alias target normalization, DNS-encoded name handling, `namehash`, `labelhashes`, and DNS wire-name derivation all use that one boundary. IDNA/UTS-46 conversion, ASCII lowercasing, trimming, or route-local normalization are not fallback normalizers. Blank or whitespace-only reverse-claim source values are classified as no claim before name normalization; every nonblank reverse-claim source value must pass this ENSIP-15 boundary or surface as `invalid_name`.

The canonical `NameSurface` carries one representative result; alternate spellings persist as immutable preimage observation facts.

`PreimageObserved` facts may come from registrar/registry events with explicit labels, wrapper events with human-readable names, reverse/primary flows that reveal names, and metadata when a manifest allows. Invalid input is never silently coerced into a valid identity.

For ENSv1, resolver `NameChanged(node, name)` strings observed through admitted reverse/primary flows are preimage observations only.[^v1-namechanged-l10][^v1-namechanged-l18][^v1-revreg-l129][^v1-revreg-l130] They can attach already-observed forward-node facts to a human-readable name; they do not synthesize ownership, resolver, or record facts.

For ENSv2, admitted registry, registrar, and resolver name-bearing events produce preimage observations: registry `LabelRegistered`, `LabelReserved`, `ParentUpdated`; registrar `NameRegistered`, `NameRenewed`; resolver `AliasChanged`, `NamedResource`, `NamedTextResource`, `NamedAddrResource`.[^v2-events-l15][^v2-events-l30][^v2-events-l75][^v2-iethreg-l32][^v2-iethreg-l53][^v2-iperm-resolver-l14][^v2-pres-l132][^v2-pres-l142][^v2-pres-l153] These do not write projections or mutate manifest capability state.

## Canonicality, authority, and epochs

- For `ens`, authoritative registration and control come from Ethereum L1. `authority_epoch` is `ens_v1` or `ens_v2` per name and time; it is separate from `resolution_epoch`.
- For `basenames`, authoritative registration and control live on Base.[^bn-readme-l70] The Basenames L1 path is compatibility transport, not a competing authority source.[^bn-readme-l69][^bn-l1resolver-l13]
- Primary names are canonical only when verification succeeds for the requested `coin_type`. Reverse claims alone are insufficient; verification must resolve the claimed name back to the requested address.[^v1-aur-l217][^v1-aur-l226][^v1-aur-l263][^v1-aur-l269]

## Source families

ENS:

- `ens_v1_registry_l1`
- `ens_v1_registrar_l1`
- `ens_v1_wrapper_l1`
- `ens_v1_resolver_l1`
- `ens_v1_reverse_l1`
- `ens_dns_l1`
- `ens_offchain_metadata`
- `ens_v2_root_l1`
- `ens_v2_registry_l1`
- `ens_v2_registrar_l1`
- `ens_v2_resolver_l1`
- `ens_execution`

Basenames:

- `basenames_base_registry`
- `basenames_base_registrar`
- `basenames_base_resolver`
- `basenames_base_primary`
- `basenames_l1_compat`
- `basenames_execution`
- `basenames_offchain` (reserved; not currently admitted)

Shared: `shared_manifests`, `shared_normalization_rules`, `shared_capability_registry`.

Family ownership is fixed:

- `ens_v1_wrapper_l1` owns Mainnet NameWrapper authority, holder facts, direct fuse/expiry observations, wrapper-revealed names, and wrapper-originated resolver/TTL changes.[^v1-namewrapper-deploy][^v1-iname-l27][^v1-iname-l35][^v1-iname-l37][^v1-iname-l38][^v1-nw-l240][^v1-nw-l377][^v1-nw-l637][^v1-nw-l666][^v1-nw-l676]
- `ens_v1_resolver_l1` owns the declared Mainnet PublicResolver address list. The schema-v2 project phase classifies an emitter as supported only when its exact address is in the active manifest; that classification permits projection of retained canonical normalized observations but does not prove complete history, authorization semantics, or event-to-call parity. Unlisted emitters are unsupported.[^v1-publicresolver-deploy][^v1-pres-l5][^v1-pres-l13][^v1-pres-l20][^v1-pres-l66][^v1-pres-l114]
- ENS verified resolution belongs to `ens_execution` at the official Universal Resolver proxy `0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe`,[^ens-docs-univ] not to `ens_v1_registry_l1`. The pinned implementation artifact is recorded under `.refs/`.[^v1-ur-deploy][^v1-ursol-l8] (See [`upstream.md`](upstream.md) for the proxy-vs-implementation divergence.)
- ENS reverse-claim intake belongs to `ens_v1_reverse_l1` at `0xa58E81fe9b61B5c3fE2AFD33CF304c454AbFc7Cb`.[^v1-revreg-deploy][^v1-revreg-l15][^v1-revreg-l19]
- ENSv1 `.eth` registrar label intake belongs to `ens_v1_registrar_l1`. BaseRegistrar is the tokenized authority; legacy, wrapped, and current registrar-controller contracts are admitted within the same family for label-bearing registration and renewal observations.[^subgraph-l145][^subgraph-l170][^subgraph-l226][^v1-ethrc-l116][^v1-ethrc-l133] A renewal from the admitted `wrapped_registrar_controller` additionally derives a wrapper-resource expiry observation in this registrar family because that controller calls `NameWrapper.renew`, which stores registrar expiry plus grace without emitting `ExpiryExtended`. (upstream: .refs/ens_v1/deployments/mainnet/WrappedETHRegistrarController.json:L656 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L318 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L333 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L337 @ ens_v1@91c966f) Label preimage intake is shared storage support rather than a new authority source family: proof-checked on-chain preimage observations, retained name surfaces, and optional rainbow-table imports may resolve labelhashes for projection readability, but they do not create exact-name authority, ownership, resolver, record, or primary-name truth.
- ENSv1 `NewResolver(node, resolver)` changes only the node-to-resolver binding; it creates no resolver contract instance or discovery edge.[^v1-ens-l12][^v1-ensreg-l89][^v1-ensreg-l174] Resolver-local logs come from the manifest-declared match-all resolver signature set, but schema-v2 support classification uses only exact addresses in the active resolver manifest. Code-hash observations are not a classification input. Current record visibility still follows the node's resolver pointer.
- `ENSRegistryOld` is admitted as migration-aware input under `ens_v1_registry_l1`. Old- and current-registry logs are not unioned by latest block: a current-registry `NewOwner` marks a node migrated; later old-registry updates for that node are suppressed except for the root resolver.[^subgraph-l15][^subgraph-l39][^subgraph-l44][^subgraph-ts-l134][^subgraph-ts-l230][^subgraph-ts-l238][^subgraph-ts-l246]
- ENSv2 post-audit Sepolia admits four families: `ens_v2_root_l1` (`RootRegistry`), `ens_v2_registry_l1` (`ETHRegistry` plus discovered `UserRegistry`), `ens_v2_registrar_l1` (`ETHRegistrar`), and `ens_v2_resolver_l1` (discovered or explicitly admitted `PermissionedResolver` instances). `PermissionedResolverImpl` is implementation metadata, not a watched root or contract.[^v2-deploy-root][^v2-deploy-ethreg][^v2-deploy-ethrc][^v2-deploy-pres][^v2-userreg-l15][^v2-ethrc-l30][^v2-ethrc-l151] No other current Sepolia deployment artifact is admitted until a doc-first update.
- ENSv2 exact-name profile support is only promoted — a [capability promotion](glossary.md) — in the post-audit Sepolia deployment profile when `ens_v2_registrar_l1` declares `exact_name_profile = "supported"`. Other deployment profiles or capability states stay unsupported or shadow.
- Basenames mainnet authority splits across `basenames_base_registry` (`registry` at `0xb94704422c2a1e396835a571837aa5ae53285a95`), `basenames_base_registrar` (`registrar` at `0x03c4738ee98ae44591e1a4a4f3cab6641d95dd9a`, with `legacy_registrar_controller` at `0x4cCb0BB02FCABA27e82a56646E81d8c5bC4119a5` and `upgradeable_registrar_controller` proxy at `0xa7d2607c6BD39Ae9521e514026CBB078405Ab322` admitted for label-bearing registration and renewal observations), and `basenames_base_resolver` (`resolver` at `0xC6d566A56A1aFf6508b41f6c90ff131615583BCD`).[^bn-readme-l28][^bn-readme-l29][^bn-readme-l30][^bn-readme-l34][^bn-readme-l37][^bn-registry-l10][^bn-baseregistrar-l15][^bn-registrar-controller-l180][^bn-registrar-controller-l187][^bn-upgradeable-registrar-controller-l191][^bn-upgradeable-registrar-controller-l198][^bn-l2resolver-l22] `basenames_base_primary` uses the ENSv1 Base `L2ReverseRegistrar` at `0x0000000000D8e504002cC26E3Ec46D81971C1664` for declared primary-name value intake at Base coin type `2147492101`; the Basenames `ReverseRegistrar` at `0x79ea96012eea67a83431f1701b3dff7e37f9e282` is not the primary-name value authority.[^v1-l2rev-base-deploy][^v1-l2rev-base-args][^v1-l2rev-event][^v1-l2rev-nameforaddr][^bn-readme-l33][^bn-revreg-l12][^bn-revreg-l150] `basenames_l1_compat` and `basenames_execution` both reference the L1 Resolver at `0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31` for transport and execution respectively.[^bn-readme-l22][^bn-l1resolver-l154][^bn-l1resolver-l173][^bn-l1resolver-l191]
- Basenames `NewResolver` changes only the node-to-resolver binding; it creates no resolver contract instance or discovery edge.[^bn-registry-l19][^bn-registry-l132][^bn-registry-l223] The Base resolver signature set selects resolver-local logs across all emitters, while schema-v2 support classification requires the emitter's exact address in the active `basenames_base_resolver` manifest. Code-hash observations are not a classification input.[^bn-l2resolver-l4][^bn-l2resolver-l16][^bn-l2resolver-l29][^bn-l2resolver-l182][^bn-l2resolver-l193][^bn-l2resolver-l209][^bn-l2resolver-l225]

## Source manifests

Manifests pin each [source family](glossary.md) by version and live under a selected deployment-profile root at `manifests/<profile>/<chain_combo>/<namespace>/<source_family>/<version>.toml`. The shipped runtime default is `manifests/mainnet/`; the Sepolia profile root is `manifests/sepolia/`. One runtime selects exactly one profile root.

Each manifest contains: `manifest_version`, `namespace`, `source_family`, `chain`, `deployment_epoch`, `rollout_status` (`draft` | `shadow` | `active` | `deprecated`), `normalizer_version`, optional `resolver_implementations`, `capability_flags` (`unsupported` | `shadow` | `supported`), `roots`, `contracts`, `discovery_rules`. `resolver_implementations` declares the implementation addresses that canonical ERC-1967 upgrade history may classify for ENSv2; it does not create watch targets. `start_block` is optional inclusive bootstrap metadata; omitted remains unknown in manifest storage. The stabilized Stage B ingest and interpret loaders currently use zero as the effective range-filter fallback for an omitted value. That fallback is a documented port gap, not historical provenance or authority for an unbounded ingest.

Manifest declaration changes are first-class `SourceManifestUpdated` normalized events. Proxy declarations and authored capability fields are part of that source-manifest state; the schema does not mint separate manifest-change event kinds for them.

Rules:

- A contract is indexable when an active manifest declares it, an admitted creation event announces it, or an allowed discovery edge makes it reachable from a canonical root. Announcement admission alone does not confer parent or name authority.
- Re-declaring the same address mints no new instance — it appends a new active range.
- Declared proxy implementations resolve to separate `contract_instance_id` nodes; implementation changes update the proxy/implementation edge, not the proxy identity.
- Capability ownership attaches to the declaring `source_family` only.
- Draft features may sit behind manifest flags without changing the public contract.

Schema, capability ownership detail, and the discovery edge model are in [`manifests.md`](manifests.md).

## Discovery graph

Discovery expands the canonical graph through time-versioned indexability and relationship edges. The schema-v2 baseline constrains `edge_kind` to exactly five values: `resolver`, `subregistry`, `proxy_implementation`, `registry_announcement`, and `migration`. Four of the five have producers; nothing writes `migration`, which is [reserved surface](glossary.md#reserved-surface). (The legacy `public` schema built from `migrations/` never constrained the column, so historical rows there are not bounded by this list.) Each edge stores `edge_id`, `from_contract_instance_id`, `to_contract_instance_id`, `discovered_by`, `edge_kind`, `active_from`, `active_to`, provenance, and canonicality.

ENSv2 mappings:

- `RegistryCreated()` → normalized `RegistryCreated` and a registry-announcement instance admission at the emitting address. The admission does not require a parent link. When that address also has a declaration in an active manifest for the same namespace, interpretation uses the declaring manifest; otherwise it falls back to the announcer's manifest. The registry emits this event during construction. (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L9 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L113 @ ens_v2@ccaeb58)
- `SubregistryUpdated(tokenId, subregistry, sender)` → normalized `SubregistryChanged` and the parent-child reachability edge. It does not decide whether the child registry instance is indexable.[^v2-events-l49][^v2-pr-l131][^v2-pr-l222]
- `ParentUpdated(parent, label, sender)` → normalized `ParentChanged` contract history. Manifest-declared `RootRegistry` and `ETHRegistry` instances are suffix anchors; every registry below those anchors has a registry-name suffix only while both current sides agree: the child's latest claim names `(parent, label)`, and that parent's latest unexpired `SubregistryUpdated` pointer for `label` leads back to the child. Either side changing, clearing, or expiring retracts the binding. A suffix move closes and releases each old logical-name binding, then opens and grants a distinct binding epoch under the new reachable suffix; the underlying registry resource remains the same, and its current resolver and subregistry pointers are restated under the new logical name. `ParentUpdated` does not create parent-child reachability; `SubregistryUpdated` remains its only source. Replay retains both current sides even while an intermediate registry has no reachable name, and later descendant events recheck the complete bidirectional, unexpired ancestor path. The child's `setParent` call writes its parent and label atomically, independently of the parent's subregistry pointer. (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L171 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L175 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L176 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L177 @ ens_v2@ccaeb58) Canonical validation reads the child's current claim and rejects it unless the parent's current pointer leads back to the child. (upstream: .refs/ens_v2/contracts/src/universalResolver/libraries/LibRegistry.sol:L82 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/universalResolver/libraries/LibRegistry.sol:L86 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/universalResolver/libraries/LibRegistry.sol:L87 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/universalResolver/libraries/LibRegistry.sol:L88 @ ens_v2@ccaeb58) Upstream stops that walk only at the supplied `RootRegistry`; treating the manifest-declared `ETHRegistry` as an additional suffix anchor is the documented ENSv2 cutover divergence. (upstream: .refs/ens_v2/contracts/src/universalResolver/libraries/LibRegistry.sol:L78 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/universalResolver/libraries/LibRegistry.sol:L79 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/universalResolver/libraries/LibRegistry.sol:L80 @ ens_v2@ccaeb58) See [`upstream.md` § Known divergences](upstream.md#known-divergences). An expired parent label makes `getSubregistry` return zero at the event timestamp. (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L251 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L253 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L625 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L626 @ ens_v2@ccaeb58)
Registry-name suffix labels are retained verbatim. Raw label text keys the live topology maps, so the current parent pointer and child claim must agree on the same raw label. A raw-distinct label path has a distinct namehash identity; if any label does not byte-equal its ENSIP-15 normalized result, that identity remains a shadow and cannot open a current binding. Thus labels such as `Foo` and `foo` retain distinct preimages and namehash identities, while only the normalization-gate-passing identity can bind.

- `ResolverUpdated(tokenId, resolver, sender)` → updates the resolver edge for the current registry resource. Admitted resolver endpoints belong to `ens_v2_resolver_l1`.[^v2-events-l59][^v2-pr-l141][^v2-pr-l225]

[Watch-plan](glossary.md) expansion includes manifest-declared instances, registry-announcement admissions, resolver admissions, and proxy/implementation targets. Announcement admission is forward-only from the observed event; bootstrap can discover earlier announcements only back to the configured ingest coverage start and therefore assumes that coverage reaches the deployment. A `subregistry` edge changes parent-child topology only; it never admits the child emitter. Match-all signature scopes are manifest event subscriptions rather than address rows.

## Intake architecture

Three intake planes for one selected deployment profile:

- Ethereum L1 chain intake
- Base chain intake
- execution intake (verified reads, CCIP)

Per-profile provider availability: a Base RPC is not required for an Ethereum-only run, and a deployment profile with no Base provider must mark Base intake idle/unavailable rather than failing startup.

Phases per chain:

1. `ingest` — block lineage, selected transactions/receipts/logs, and raw-fact
   persistence
2. `interpret` — schema-v2 identity, discovery, and normalized-event writes
3. `project` — canonical identity and normalized-event input, staged current
   projections, one-transaction publication for the affected scope, and
   canonical-head [hydration](glossary.md#hydration) after publication
4. `verify` — read-only [stored-history verification](glossary.md#stored-history-verification)
   through a finalized boundary; Base compares its Coinbase-loaded range with
   dRPC through the ingest seam and Ethereum compares with local reth through
   the finalized head
5. `live` — continuous provider-head walk, bounded gap fill, chain-head
   publication, and downstream re-derivation after a reorg

Postgres is the hot indexed and replay-focused store. Lineage anchors, selected target logs and their same-transaction sibling replay context, replay-required call snapshots, and compact payload-cache metadata are durable. Legacy public-schema code-hash observations remain available to the old worker but are not schema-v2 project-phase inputs. Large block payloads, non-indexed transaction or receipt bodies, and non-audit raw-log staging rows are evictable cache once their replay contract is satisfied. Empty historical blocks retain only lineage anchors and audit metadata.

The phase runner persists exact per-source and per-phase block-hash cursors.
Historical work is an explicit finite `ingest`, `interpret`, `project`, or
`verify` redo. The old persisted backfill scheduler, coverage frontier, adapter
startup pass, and normalized-event replay driver have been deleted. A newly
admitted source returns to `ingest` for the required range, and `interpret`
cannot advance past the ingested boundary.

### Stage B runtime boundary

The checked-in phase runner contains real `ingest`, `interpret`, `project`,
`verify`, and `live` implementations. Verification reads canonical selected
raw logs and the manifest-derived watch set through a separately credentialed,
SELECT-only database handle. Startup requires that login to be directly
authenticated (the session user and active role must match) and rejects one that has
application-relation write privileges, schema/database creation authority,
elevated role attributes, or another role membership; the verifier never
receives the phase runner's writer pool. The reader and writer connections must
also report the same PostgreSQL system identifier, database OID, and database
name. It compares Base's Coinbase-loaded
range with dRPC through the fixed block `48,428,000` ingest seam and records
`cross_checked`; the later dRPC-ingested suffix does not inherit
that level. It compares Ethereum with local reth and records `node_checked`.
Provider typing prevents a dRPC-backed chain from recording `node_checked`. A
mismatch records its block, field, stored value, and reference value, then stops
only that chain. Normal verification starts at the durable ingest-cursor extent,
not a replacement command-line start, and a resumed scan retains the weaker of
its prior whole-extent level and the current reference's level.
The project phase is the single schema-v2 projection writer and has no claim
queue, dead-letter referee, watermarks, heartbeat threading, or standing
hydration planner. When a hydration RPC is configured, the same project run
refreshes eligible Ethereum legacy reverse-name and text values at the exact
published canonical head after its event-derived projection work. A redo whose
event-derived publication target is behind that head defers hydration until
project catches up. The existing
worker remains only so the API can read the legacy public-schema projections
until the Stage C cutover; it does not write schema-v2 projections.

Current ingest, interpretation, projection, live follow, redo, and rewind
boundaries are described in [`chain-intake.md`](chain-intake.md).

## Immutable facts and rebuildable state

Immutable schema-v2 raw facts: blocks, transactions, receipts, logs, preimage
observations, and selected `eth_call` snapshots. Legacy code-hash observations
remain in the public schema but are not project inputs. For large payloads
the durable fact may be selected replay fields plus optional cache metadata or
a digest, not the full body — compaction can evict non-critical bytes after
replay facts are extracted.

Interpretation output is replay-derived: schema-v2 identity rows, discovery
edges, and normalized events can be replaced by an explicit bounded
`interpret` redo while raw facts remain unchanged. Current name, binding,
authority, control, permissions, resolver, record, primary-name, reverse,
address, history, and coverage projections remain rebuildable by the project
phase from canonical identity and normalized-event input. Canonical-head
hydration values are execution-derived current-state enrichment layered into
`record_inventory_current` and `primary_names_current` only after that
rebuildable event-derived publication; they are never raw facts, identity rows,
or normalized events.
Execution traces and outcomes remain durable execution artifacts.

Every projected row carries provenance pointers, manifest version, canonicality state, and chain-position context.

## Internal domain model

Core objects: `NameSurface`, `SurfaceBinding`, `BackingResource`, `NameClass`, `RegistrationSnapshot`, `AuthoritySnapshot`, `ControlVector`, `PermissionSnapshot`, `ResolutionTopology`, `RecordInventory`, `RecordCache`, `PrimaryNameSnapshot`, `SourceProvenance`, `CoverageSnapshot`, `TokenLineage`, `ExecutionResult`.

`ControlVector` is never a single owner field. It carries `token_holder`, `registrant`, `effective_controller`, `record_manager`, `delegates`, `reverse_manager`, `resolved_address_target`, `status`, `expiry`, `authority_epoch`, `resolution_epoch`.

`Registration.kind`: `lease`, `subname_assignment`, `reservation`, `dns_control`, `offchain_policy`, `observed_only`.

Permissions and control are anchored to `resource_id`, never to surface text. The chain `logical_name_id → SurfaceBinding → resource_id → token_lineage` must remain reconstructible through time.

## Normalized event taxonomy

Identity, preimage, discovery: `PreimageObserved`, `NameClassified`, `SurfaceBound`, `SurfaceUnbound`, `ContractDiscovered`, `MetadataChanged`, `SourceManifestUpdated`.

Registration and authority: `RegistrationReserved`, `RegistrationGranted`, `RegistrarNameRegistered`, `RegistrationRenewed`, `RegistrationReleased`, `ExpiryChanged`, `AuthorityTransferred`, `AuthorityEpochChanged`, `MigrationApplied`, `PricingPolicyChanged`.

Lineage and control: `TokenResourceLinked`, `TokenRegenerated`, `TokenControlTransferred`, `ResolutionEpochChanged`.

Topology and resolution: `ResolverChanged`, `SubregistryChanged`, `ParentChanged`, `AliasChanged`, `WildcardCoverageChanged`, `RecordChanged`, `RecordVersionChanged`, `RecordInventoryObserved`.

Permissions: `PermissionChanged`, `RootPermissionChanged`, `PermissionScopeChanged`.

Reverse and primary: `ReverseChanged`, `PrimaryNameClaimed`, `PrimaryNameVerified`, `PrimaryNameInvalidated`.

Execution and coverage: `VerifiedResolutionObserved`, `VerifiedResolutionInvalidated`, `CoverageChanged`.

ENSv2 mappings:

- `TokenResourceLinked` ← upstream `TokenResource(tokenId, resource)`. The only adapter event linking current token ID to upstream EAC resource.[^v2-iperm-l34][^v2-pr-l216]
- `TokenRegenerated` ← upstream `TokenRegenerated(oldTokenId, newTokenId)`. Preserves `resource_id`, `token_lineage_id`, and active surface binding.[^v2-events-l69][^v2-pr-l451]
- `TokenControlTransferred` ← each positive-value item in upstream ERC-1155 `TransferSingle` or `TransferBatch` when both `from` and `to` are nonzero. A batch item produces its own normalized event. The upstream update changes the current owner only for positive values and uses the zero address for mint and burn, so those lifecycle logs do not become token-control transfers. Both events are present in the deployed `ETHRegistry` and `UserRegistryImpl` ABIs. (upstream: .refs/ens_v2/contracts/deployments/sepolia/ETHRegistry.json:L652 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/deployments/sepolia/ETHRegistry.json:L689 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/deployments/sepolia/UserRegistryImpl.json:L723 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/deployments/sepolia/UserRegistryImpl.json:L760 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/erc1155/ERC1155Singleton.sol:L194 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/erc1155/ERC1155Singleton.sol:L201 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/erc1155/ERC1155Singleton.sol:L208 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/erc1155/ERC1155Singleton.sol:L210 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/erc1155/ERC1155Singleton.sol:L318 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/erc1155/ERC1155Singleton.sol:L333 @ ens_v2@ccaeb58)
- `SubregistryChanged` ← `SubregistryUpdated`; `ParentChanged` ← `ParentUpdated`.[^v2-events-l49][^v2-events-l75]
- `AliasChanged` ← `PermissionedResolver.AliasChanged`; the alias path stores source and destination DNS-encoded names.[^v2-iperm-resolver-l14][^v2-pres-l230]
- `PermissionChanged` and `RootPermissionChanged` ← upstream `EACRolesChanged(resource, account, oldRoleBitmap, newRoleBitmap)`. Root-resource permissions stay distinguishable because EAC root roles are checked separately and satisfy resource-level checks via root fallback; this taxonomy admission covers normalized-event emission and manifest watch admission, while current-state projection consumption is a separate projection contract.[^v2-eac-l19][^v2-eac-l176][^v2-eac-l181] Registry/root sources decode role bitmaps with the post-audit `RegistryRolesLib` vocabulary (`registrar`, `register_reserved`, `set_parent`, `unregister`, `renew`, `set_subregistry`, `set_resolver`, `set_uri`, `can_name`, `upgrade`, `can_transfer_admin`, and the corresponding `admin_` powers). `ROLE_WAS_RESERVED` at bit 32 is a non-power marker retained in the bitmap and omitted from `effective_powers`; unknown bits are likewise omitted rather than surfaced under invented names.[^v2-regroles-l6][^v2-regroles-l9][^v2-regroles-l14][^v2-regroles-l19][^v2-regroles-l24][^v2-regroles-l29][^v2-regroles-l34][^v2-regroles-l39][^v2-regroles-l45][^v2-regroles-l47][^v2-regroles-l50][^v2-regroles-l55][^v2-regroles-l60] Resolver sources decode the resolver vocabulary, including `set_data`, `can_name`, `upgrade`, and their admin powers.[^v2-resroles-l7][^v2-resroles-l51][^v2-resroles-l56][^v2-resroles-l61] `DataChanged` and `NamedDataResource` remain unadmitted even though `set_data` is a named permission power.[^v2-pres-l161][^v2-pres-l437]
- `RegistrarNameRegistered` ← upstream `ETHRegistrar.NameRegistered`; it is registrar-local registration intent and links back to the registry resource when that registry resource has already been observed.[^v2-iethreg-l32]
- `RegistrationRenewed` ← upstream `IETHRenewer.NameRenewed`; the post-audit terminal payment field is `amount`.[^v2-iethreg-l53] Post-audit normalized `after_state` publishes `amount` and retains `base` with the same value as a compatibility alias. When a two-topic renewal admitted by the deprecated pre-audit manifest is explicitly decoded, it retains its historical `base`-only payload shape.[^v2-sepolia-dev-iethreg-l53] Deprecated pre-audit emitter addresses remain outside the active post-audit watch and replay plan. This is an intentional payload-compatibility rule, not a claim that the post-audit upstream field is still named `base`.

Taxonomy reconciliation decisions:

- `RecordDeleted` is not a separate normalized kind for the currently admitted sources. Deletes are represented as `RecordChanged` payloads with deletion metadata, so consumers only need one record-change stream.
- `CommitmentMade` is not admitted in the normalized taxonomy yet. Upstream ENSv2 `ETHRegistrar` emits `CommitmentMade(bytes32 commitment)`, but current manifests and adapters do not consume it, and no current projection depends on commitment history. (upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRegistrar.sol:L19 @ ens_v2@ccaeb58)
- `DelegateRetainedAfterTransfer` is not admitted until a concrete source event and consumer projection are specified. Role changes remain `PermissionChanged`, `RootPermissionChanged`, or `PermissionScopeChanged`; token ownership comes from `TokenControlTransferred` rather than inference from a role-event pattern.
- ERC-1155 `ApprovalForAll` remains unsupported. Operator approval is neither token ownership nor an ENSv2 resource-role grant, and no current projection consumes it. (upstream: .refs/ens_v2/contracts/src/erc1155/ERC1155Singleton.sol:L336 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/erc1155/ERC1155Singleton.sol:L341 @ ens_v2@ccaeb58)

ENSv1 direct wrapper/resolver mappings from admitted NameWrapper and PublicResolver events are `PreimageObserved`, `SurfaceBound`, `SurfaceUnbound`, `AuthorityTransferred`, `ExpiryChanged`, `TokenControlTransferred`, `ResolverChanged`, `PermissionChanged`, `PermissionScopeChanged`, and `RecordChanged`.[^v1-iname-l27][^v1-iname-l31][^v1-iname-l35][^v1-iname-l37][^v1-iname-l38][^v1-nw-l1022][^v1-nw-l1034][^v1-pres-l20][^v1-pres-l51][^v1-pres-l58] The admitted wrapped registrar controller's `NameRenewed` also derives an `ExpiryChanged` for the wrapper resource under `ens_v1_registrar_l1`, as defined above; the source family follows the emitting log while the resource identifies the affected wrapper state. `PermissionScopeChanged` retains the effective fuse bitmap and its derived NameWrapper lifecycle state without inventing a subject grant: unwrapping retains fuse/expiry data, and an unexpired rewrap restores the parent-controlled fuses and larger expiry even though `NameWrapped` emits the supplied arguments. (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L235 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L239 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L242 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L246 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L269 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L276 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L901 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L902 @ ens_v1@91c966f) When a separate compatible holder grant exists, current projections apply the derived state, individual owner-controlled fuse bits, and wrapper expiry to that row. (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L10 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L16 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L843 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L852 @ ens_v1@91c966f)

Every normalized event carries: namespace, `logical_name_id` when applicable, `resource_id` when applicable, source family, manifest version, chain position, raw fact reference, derivation kind, canonicality flag, and before/after state where possible.

Normalized events are schema-v2 interpreter transitions. Interpretation loads
canonical raw facts in chain order and carries the compact prior state needed
across physical batches. A redo is an explicit bounded operation: it prepares
the selected derived range, replays it through the same interpreter, and heals
identities the replay did not reproduce. Preparation restages only identities
whose stored anchor lies inside the redone range, so a redo that starts after
an identity's derivation block cannot move that anchor forward; an identity
the replay re-observes keeps its anchor at its first derivation block, and
only an identity still
orphaned after the replay is re-anchored to the earliest surviving reference
outside the redone range — for a name surface, the earliest surviving
body-carrying `PreimageObserved` observation of that name, and a surface with
no surviving body-carrying observation stays orphaned. The
deleted old-schema storage layer no longer provides general field repair,
payload arbitration, supersession, full-closure proof, or adapter-checkpoint
reuse.

Physical batching is an execution detail, not an input to interpretation.
Identity rows, discovery edges, and normalized events are a pure function of
the canonical raw facts and the declared manifests, discovery rules, and
admissions: a fresh full walk, an incremental follow, and a resumed session
over identical input write identical rows no matter where the 500-block batch
boundaries fall. Three rules keep the written rows batch-independent:

- `before_state` chains over the emitted event stream: a retained event's
  `before_state` is the `after_state` of the previous retained event under the
  same final `raw_fact_ref.interpreter_state_key` (`{}` at stream start),
  seeded from the pre-batch retained state. Events that same-transaction
  reconciliation later drops or re-keys leave no trace in surviving
  stream-chained state: each retained `before_state` is re-derived from the
  surviving stream alone. Interpreter-declared explicit befores (deliberate
  snapshots such as wrapper fuse state or permission grant bodies) are exempt
  from re-threading and are written as computed at emission, with one
  carve-out: same-transaction re-attribution to a registration (the second
  rule) resets the before to `{}`, so the registration's stream starts from
  an empty snapshot. A surviving explicit before may quote in-memory state a
  later-dropped same-transaction event wrote; that snapshot is computed
  identically in every [run shape](glossary.md#run-shape). The block-scoped
  predecessor-epoch
  permission closures keep their computed snapshot.
- Identity attribution is fixed at emission, with one exception:
  same-transaction reconciliation may attribute registry, resolver, and
  permission observations to a registration established later in the same
  transaction, and predecessor-epoch permission closures may be attributed
  when they share the registration's block. Reconciliation never reaches
  across a block boundary — batches never split a block, so the block is the
  atomic unit every [batch grid](glossary.md#batch-grid) loads — so
  predecessor-epoch observations that only
  a later block's registration could identify keep their event-time
  attribution (null `logical_name_id`/`resource_id` where no authority was
  known) in every run shape (fresh, incremental, or
  resumed).
- Resource rows anchor at their first derivation block. A superseded
  registry-only resource emission is retained even when no surviving
  same-batch row references it, so the first-committed identity upsert anchors
  the resource at the same block in every run shape. The single mover is the
  bounded redo's orphan healing above: an identity still orphaned after a
  replay re-anchors to the earliest surviving reference outside the redone
  range.

## Resolution

`Resolution` is one mixed-route envelope with three declared sections and one verified section: `topology`, `record_inventory`, `record_cache`, `verified_queries`.

### `topology`

Fixed declared object:

- `registry_path` — ordered `NameRef` array from the requested surface toward declared registry authority. Never empty when `topology` is supported.
- `subregistry_path` — toward the nearest declared subregistry ancestor. Empty when none participates.
- `resolver_path` — ordered hops; each carries `logical_name_id`, `namespace`, `normalized_name`, `canonical_display_name`, `resource_id`, `chain_id`, `address`, `latest_event_kind`.
- `wildcard` — `{source, matched_labels}`. `null/[]` means wildcard didn't participate.
- `alias` — `{final_target, hops}`. `null/[]` means alias didn't participate.
- `version_boundaries` — `{topology_version_boundary, record_version_boundary}` with `logical_name_id`, `resource_id`, `normalized_event_id`, `event_kind`, `chain_position`.
- `transport` — `{source_chain_id, target_chain_id, contract_address, latest_event_kind}`. All `null` means no transport. For Basenames capability-promotion target paths, `source=base-mainnet, target=ethereum-mainnet` through the L1 Resolver.[^bn-readme-l22][^bn-readme-l28][^bn-readme-l29][^bn-readme-l34][^bn-readme-l69][^bn-readme-l70]

For ENSv2, `alias` is declared topology only when `PermissionedResolver` provides an `AliasChanged` mapping; the resolver resolves aliases by longest suffix and rewrites calldata before [resolver-profile](glossary.md) dispatch.[^v2-iperm-resolver-l14][^v2-pres-l56][^v2-pres-l412][^v2-pres-l650] Wildcard is observed topology — populated only when execution input identifies an ancestor/source resolver and matched labels.[^v2-pres-l38][^v2-pres-l412]

### `record_inventory`

What record space is known to exist. Carries `record_version_boundary`, `enumeration_basis` (`observed_selectors`, `capability_declared_families`, `globally_enumerable`), `selectors`, `explicit_gaps`, `unsupported_families`, `last_change`.

Selectors carry `record_key`, `record_family`, `selector_key`, `cacheable`. `record_key` is the round-trip string `record_family + ":" + selector_key`; `selector_key` is `null` for scalar families and a string otherwise. Numeric selector domains use string `selector_key` so `record_key` stays text.

Inventory is not global enumeration. It defines the stable selector space admitted by the route, including explicit gaps and unsupported families. Version changes invalidate inventory and cache for the prior boundary.

### `record_cache`

Last-known declared values for supported records. Each entry carries `record_key`, `record_family`, `selector_key`, `status`, `value`, `unsupported_reason`. Status uses `success`, `not_found`, `unsupported`. `value` appears only on `success` and uses the family-native JSON shape. `record_version_boundary` matches `record_inventory`'s and `topology.version_boundaries.record_version_boundary`.

Unsupported records remain requestable through verified execution where possible.

### `verified_queries`

Execution-derived answers per requested record selector, reusing `ResultStatus`. Verified queries do not backfill `record_inventory` or `record_cache` in the same response.

Public verified support is narrower than the topology model. ENS supports:

- exact-surface direct path: `resolver_path[0].logical_name_id == route surface`, `wildcard.source=null`, `alias.final_target=null`, all `transport=null`
- exact-surface alias-only non-direct: same but `alias.final_target` non-null with non-empty `hops`
- exact-surface wildcard-derived: `wildcard.source` non-null with non-empty `matched_labels`, `resolver_path[0].logical_name_id == wildcard.source.logical_name_id`, `alias.final_target=null`, `subregistry_path=[]`, `transport=null`

Other ENS classes (non-alias ancestor-selected, linked-subregistry ancestor-selected, transport-assisted, CCIP-participating) return selector-local `unsupported`.

Basenames supports the exact-surface transport-assisted direct path through active `basenames_execution` v2 at the L1 Resolver. Other Basenames verified [path classes](glossary.md) return selector-local `unsupported`.[^bn-readme-l69][^bn-readme-l70][^bn-l1resolver-l154][^bn-l1resolver-l173][^bn-l1resolver-l191]

Legacy execution artifacts remain available to diagnostics and worker code,
but no v1 route serves them. V2 verified name and record routes execute through
the schema-v2 lookup engine without a durable trace or reusable outcome. A
guarded direct live/indexed disagreement may
create or replace an active
[resolution divergence ledger](glossary.md#resolution-divergence-ledger) row;
restored agreement may clear the matching active row.

## Permissions

Permissions are first-class projections and explain views. Track grants by scope (root, registry, resource, resolver, record manager/operator, migration-derived, transport-derived). Each grant records source, revocation source, inheritance path, transfer behavior, scope, and effective powers.

`migration_derived` and `transport_derived` are [reserved surface](glossary.md#reserved-surface): the scope kinds are accepted by the schema and rendered by the API, but no adapter writes either one. `transport_derived` is a remnant of an abandoned cross-chain ENSv2 design in which a name's authority could move between chains. ENSv2 is an Ethereum L1 system and its migration from ENSv1 is single-chain (see [`upstream.md`](upstream.md#known-divergences) § Known divergences for the citations and for the stale upstream comment that says otherwise), so no bigname source family can ever produce a `transport_derived` grant; it is retained only because removing a projection scope kind requires a schema migration. Do not treat it as a supported scope or add exemplars presenting it as expected output; guards that pin the absence of a producer, or that the retained reader still decodes a stored row carrying the value, are the exception.

Public reads expose effective powers directly so callers do not reconstruct
authority from raw role bitmaps. `GET /v2/permissions` is the current
resource-anchored permission collection; name- and address-centric views
summarize or filter the same truth.

For ENSv1 wrapper-backed resources, the current projection publishes no wrapper-holder subject grant derived from fuse state. Fuse changes remain available as `PermissionScopeChanged` history, and any separately observed compatible holder grant is masked by the effective lifecycle state and owner-controlled fuses. A locked name has no broad `resource_control`; individual fuses remove only their matching powers. Once an emancipated or locked position expires, it contributes no wrapper-holder powers because NameWrapper clears the owner and fuse values. (upstream: .refs/ens_v1/contracts/wrapper/README.md:L89 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/README.md:L93 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L843 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L848 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L849 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L852 @ ens_v1@91c966f) A `.eth` second-level name keeps its lifecycle state and token holder through the 90-day registrar grace period, while owner modification, transfer, and effective-controller membership stop at grace start. (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L48 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L218 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L221 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L820 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L825 @ ens_v1@91c966f) Internal projection inputs for a registrar name wrapped after registration can retain stale pre-wrap control facets; public exact-name reads do not publish those facets as effective control and instead return an explicit unsupported control summary for every current wrapper resource.[^v1-iname-l10][^v1-nw-l421][^v1-nw-l427][^v1-nw-l637][^v1-nw-l666][^v1-nw-l676][^v1-nw-l723][^v1-nw-l827][^v1-nw-l1023][^v1-nw-l132] An empty permission result therefore still does not prove complete wrapper-holder enumeration.

For ENSv2, `PermissionedRegistry.getResource(anyId)` keys permissions by upstream resource, so public permissions key by the bigname `resource_id` linked to that resource, not by token ID.[^v2-iperm-l57][^v2-pr-l261][^v2-pr-l351] Resolver-scoped permissions live in the same resource-anchored model with resolver scope metadata; `PermissionedResolver` uses name-, text-key-, and coin-type-specific EAC resources for setters.[^v2-pres-l70][^v2-pres-l159][^v2-pres-l239][^v2-pres-l257][^v2-pres-l282]

Required indexes: by resource, by account, by resolver; permission history by resource and by account.

## Primary and reverse names

The primary-name projection is address- and `coin_type`-centric, not just a
reverse-record projection. The public-schema persistence, cache, fallback, and
provenance rules below describe legacy worker and storage artifacts retained
until slice 3; no v1 API route serves them.

That retained legacy plane persists `claimed_primary_name`,
`verified_primary_name`, `reverse_namespace`, `coin_type`, `resolver`,
provenance, and coverage.

- Both objects use `ResultStatus`. `mismatch` applies to verified only; `execution_failed` also applies to a route-local claimed lookup when its provider fails.
- `claimed_primary_name` is candidate-only; `verified_primary_name` is authoritative only when `success`.
- A raw claim that cannot be normalized surfaces `invalid_name`, not silent drop.
- Verified success additionally requires the untrimmed on-chain claim to byte-equal its ENSIP-15 normalized form. A normalizable claim with a different raw spelling remains a successful claimed candidate, but `verified_primary_name` returns `status=invalid_name` with `failure_reason=claim_not_normalized` instead of resolving the normalized variant.
- Reverse claims alone don't verify — verification must resolve back to the requested address.[^v1-aur-l217][^v1-aur-l226][^v1-aur-l263][^v1-aur-l269]

For ENS, declared claim precedence is reverse-only through `ens_v1_reverse_l1`.[^v1-revreg-deploy][^v1-revreg-l74][^v1-revreg-l83][^v1-revreg-l84] Persisted `claimed_primary_name.name` comes only from the exact requested `primary_names_current(address, coin_type, namespace)` row's declared normalized claim-identity source, including the projection-owned legacy reverse-resolver [hydration](glossary.md) exception documented for configured [event-silent](glossary.md) ENSv1 reverse resolvers. Admitted reverse tuples remain eligible when the hydrated claim normalizes but differs in raw spelling; their row records `claim_name_is_normalized=false`. For current registry resolver edges, resolver-edge-only hydration may persist the exact row only when the untrimmed hydrated name first byte-equals its normalized form and the candidate node hash then forward-confirms through `addr:60` on the ENS Universal Resolver to the recovered address at the same [hash-pinned](glossary.md) checkpoint; the forward check only recovers the address preimage for the reverse node and does not persist verified-primary state.[^v1-revreg-l137][^v1-registry-l137][^v1-nameresolver-l7][^v1-iaddrres-l11][^v1-iur-l44][^v1-iur-l52] The app default tuple (`namespace=ens`, `coin_type=60`) may use a route-local Ethereum Mainnet reverse RPC fallback when that persisted tuple is missing: select the stored head checkpoint, build the `addr.reverse` node, read its ENS registry resolver, call resolver `name(bytes32)` at that block hash, normalize the result, and publish claim provenance as `ens_reverse_rpc` without populating `primary_names_current`.[^v1-registry-deploy][^v1-revreg-l137][^v1-registry-l137][^v1-nameresolver-l7][^v1-nameresolver-l11][^v1-nameresolverimpl-l25] In `mode=verified|both`, that route-local fallback applies the normalization gate before verifying `addr:60` through the ENS Universal Resolver proxy at the same block hash, then persists the complete `verified_primary_name` execution trace and outcome.[^v1-ur-deploy][^v1-iur-l44][^v1-iur-l52] Expiration of a configured provider or CCIP-Read gateway response deadline remains a persisted in-band execution failure. Provider or gateway connect-phase timeouts, DNS failures, TLS failures, connection resets, and other transport failures abort with `409 stale` before persistence so a later read retries. Outside exact-row hydration and that fallback, `claimed_primary_name.name` is never synthesized from manifest presence, resolver identity alone, or verified execution.

For Basenames, declared primary-name value intake is `basenames_base_primary` at the ENSv1 Base `L2ReverseRegistrar` (`0x0000000000D8e504002cC26E3Ec46D81971C1664`), using the `NameForAddrChanged(address,string)` event and Base coin type `2147492101`.[^v1-l2rev-base-deploy][^v1-l2rev-base-args][^v1-l2rev-event][^v1-l2rev-nameforaddr] It does not replace the Base registry/registrar/resolver families for declared truth on exact-name, address-name, or children reads, and it does not use the Basenames `ReverseRegistrar` as the primary-name value source. Verified primary names enter through `basenames_execution` against the L1 Resolver.[^bn-readme-l22][^bn-l1resolver-l13]

V2 reads an indexed claim from `primary_names_current` but obtains ENS/60
verification from a fresh hash-pinned schema-v2 lookup. It writes no legacy
trace or reusable outcome and no divergence row. Provider transport failure
aborts v2 with `500 internal_error`. V2 Basenames primary-name verification is
unsupported; its indexed response remains Base-scoped.

Verified-primary cache identity is `request_type=verified_primary_name` with key `{namespace}:{normalized_address}:{coin_type}`. Materialized results are fenced by the matching `primary_names_current` row. The route-local ENS/60 exception is fenced by that exact row remaining absent and by an exact selected-checkpoint match; its topology and record dependency fields carry the explicit selected checkpoint rather than fabricated projected name/resource identities. Route-local and materialized traces do not satisfy each other's readback fence.

Retained legacy section-local provenance, not currently served:

- `claimed_primary_name.provenance` is exact-tuple declared-only provenance from the requested row, optionally with projection-owned legacy reverse-resolver hydration metadata, or route-local `ens_reverse_rpc` resolver provenance for the ENS/60 on-demand fallback. No `execution_trace_id`.
- `verified_primary_name.provenance` (when present) is `{execution_trace_id, manifest_versions}` for persisted readback and must equal the top-level `execution_trace_id`, including persisted ENS/60 fallback results. The v1 fallback also exposes the selected positions through `chain_positions`.

V2 publishes no execution-trace provenance. Its fresh ENS/60 response uses
snapshot metadata for the actual lookup position.

## Collection semantics

### Exact-name lookup

Resolves a `NameSurface`. Returns normalized identity, current binding, declared summary sections (registration, authority, control, resolver, record inventory, history), provenance, coverage.

Each declared summary section is always present as an object; unprojected sections return an explicit unsupported object rather than disappearing. Exact-name `control` carries `registrant`, `registry_owner`, `latest_event_kind`. Exact-name `resolver` carries `chain_id`, `address`, `latest_event_kind`; `chain_id=null/address=null` means "no declared resolver", not "resolver reads unsupported". Exact-name `history` is two head pointers — `surface_head` and `resource_head` — into the canonical history contract, not embedded rows.

For Basenames, exact-name declared truth comes from the Base authority split (`basenames_base_registry`, `basenames_base_registrar`, `basenames_base_resolver`); claim and transport families don't widen it.[^bn-readme-l69][^bn-readme-l70]

### Address → names

Returns surfaces, not backing resources. Each item carries `logical_name_id`, surface identity, `resource_id`, relation facets (`registrant`, `token_holder`, `effective_controller`), binding kind, provenance, coverage.

`dedupe_by=resource` is grouping-only. Default sort is `display_name_asc`. Exhaustiveness is authoritative only for source classes with enumerable ownership/assignment surfaces; wildcard- and offchain-derived names are never silently treated as exhaustive.

### Address → names with `include=role_summary`

Additive expansion, not a separate route. Adds `role_summary` (one `subjects[*]` entry per distinct current permission subject for the same `resource_id`, with `scope` and `effective_powers`), `subname_count`, `record_count`, `status`, `expiry`. Identity, supported filters, grouping, default sort, cursor, and coverage stay unchanged.

`subname_count` reuses declared-direct-children semantics. `record_count` is the count of distinct stable declared record selectors at the current version boundary.

### Name → children

Default returns declared direct child nodes. ENSv1 registry edges whose parent surface is known remain children even when the child label is unknown; those rows use the bracketed labelhash placeholder rather than minting exact-name surfaces. Optional buckets: linked-subregistry, alias-derived, observed wildcard. `subname_count` in the main name summary means declared direct children only.

### Resource → permissions

The resource-centric collection. One current row per `(resource_id, subject, scope)` key. Subject- or resolver-centric summaries derive from these rows. If a surface rebinds across ENSv1 anchors, reads stay partitioned by `resource_id` rather than stitching predecessors together.

### History

Queryable by `scope=surface|resource|both`. History reads are canonical normalized-event reads, not separate denormalized truth tables. `Address.history` composes address anchor resolution with the same contract.

### Resolver overview

Resolvers are first-class read targets. Sections: bindings, alias mappings, resolver-scoped permissions, role holders, events, counts. Each section is supported only when a projection owns the fan-in. Shared ENSv1 PublicResolver targets do not enumerate current-name fan-in for `bindings`, `aliases`, or event summaries — those return `UnsupportedSummary` with `resolver_binding_enumeration_not_projected`. Exact-name resolver state stays on exact-name routes.

### Explain by exact name

Three thin views over already-projected truth, each scoped to the same exact-name snapshot:

- `surface-binding` — current `SurfaceBinding` plus exact-name history head pointers
- `authority-control` — same `authority` and `control` summaries as the exact-name route
- `coverage` — the projection coverage facts exposed by the v2 name coverage diagnostic

None of these introduces a separate truth system or ledger.

## Coverage and exhaustiveness

Coverage is contractual.

- Exact-name lookup is authoritative for supported source classes. Route-level coverage may still be authoritative when individual declared summary subdocuments are unsupported.
- Address-to-name enumeration is exhaustive only for enumerable source classes.
- Wildcard and offchain name classes are not globally enumerable.
- Record inventory is `best_effort` unless a resolver family enumerates explicitly or there's a source-specific index.
- Child enumeration is authoritative only for declared direct children unless the caller opts into other surface classes.
- V2 primary-name route-level coverage is `partial`, with
  `exhaustiveness=non_enumerable` and
  `enumeration_basis=primary_name_lookup` for ENS/60 fresh verification. Other
  v2 verified tuples are explicit `unsupported`.

Every response carries `coverage.status`, `coverage.exhaustiveness`, `coverage.source_classes_considered`, `coverage.unsupported_reason`, `coverage.enumeration_basis`.

## Verified execution

Default verified entrypoints:

- ENS: `ens_execution` at the official Universal Resolver proxy `0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe`.[^ens-docs-univ][^v1-aur-l90][^v1-aur-l106]
- Basenames: active `basenames_execution` v2 at `0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31` supports only the exact-surface transport-assisted direct path; other Basenames verified path classes stay `unsupported`.[^bn-readme-l22][^bn-l1resolver-l154][^bn-l1resolver-l173][^bn-l1resolver-l191]

The legacy execution engine retained for worker use supports onchain calls,
wildcard resolution, alias-aware execution, nested CCIP-Read, batch/multicall,
proof and verification persistence. Its artifacts carry `ExecutionTrace` and
cache identity, but no v1 API route serves them.

The v2 lookup engine executes afresh at the schema-v2 current readable position.
It has no trace or cache identity. It may compare a direct record answer with
the exact projected record row and perform the guarded divergence-ledger write;
v2 primary-name verification performs no write.

## Reorg, redo, and historical ranges

The phase runner stores competing block lineage per chain. Head publication
marks a displaced readable lineage branch `orphaned` and promotes the selected
branch; interpretation selects raw facts through that lineage rather than
rewriting immutable raw rows. An explicit `interpret` redo replaces derived
identity, discovery, and normalized-event output for its selected range.

The old synchronous reorg-repair tree, normalized-event repair/replay driver,
and its broad orphan-repair sweep have been deleted. The live phase uses the
same head-publication transaction as ingest. That transaction orphans the
displaced suffix, removes cache eligibility for affected rows in
`public.execution_cache_outcomes` while leaving durable traces intact, and
stamps `interpret` and `project` for bounded redo when the orphaned suffix
starts at or below their recorded cursors. The live loop consumes those stamps
in dependency order before advancing downstream work, so projections cannot
silently retain output from the losing fork. Successful `interpret` redo also
stamps `project` for the same replayed suffix; a direct data-repair redo
therefore cascades without a second operator command. Since interpret replaces
normalized events before project runs, project redo retains the incremental
keys cited by current rows when those event IDs disappear, including both
primary-name reverse and claim citations. The winning replay can therefore
retract an event-only losing-fork value even when its stable identity was first
observed before the redo range.
The persisted system marker distinguishes a pending required range from its
active replay. Dependency selection and live gap fill may pass a pending marker;
after replay acquires the phase writer slot, ordinary cross-phase writer
exclusion applies.
Live also checks that a suffix loaded after ancestry discovery still descends
from the selected common ancestor. A provider reorg between those reads is a
retryable snapshot change, not a terminal lineage failure.

The `phase-runner rewind` command is a thin head-publication operation. It takes
the ingest, interpret, project, and live advisory locks so it cannot race a head
publisher or downstream writer, selects an exact stored readable ancestor at or
above the safe head, and invokes the same atomic orphaning, cache invalidation,
and redo-stamping path. The next supervised live cycle fills the winning path,
then runs the required downstream redo.

Historical work is a finite `ingest`, `interpret`, `project`, or `verify` run.
An explicit redo can select one phase or all four in dependency order for one,
several, or every active-manifest chain; it is not a persisted old-schema
backfill job. Live follow starts at the completed ingest handoff and only walks
the current head and a winning-fork gap; it never provides historical coverage.
`--phase recompute-flags` supports bounded flag recomputation. Among otherwise
configured redo requests, only historical `live` redo and unreadable range ends
are rejected before redo state is written. A deployment therefore still needs
complete admitted history for ENSv1, ENSv2, and Basenames source families.
Wildcard and offchain names remain
discovery/observed-answer based rather than exhaustively enumerable.

## Operations

The old indexer metrics and backfill-capacity checks retired with that binary.
API and worker metrics remain available. The live phase records its phase state,
exact block-hash progress, and heartbeat through the shared runner control
plane; dedicated chain-lag and reorg metrics remain deferred.

The phase runner owns the current schema-v2 operator tools. None expose public
API routes:

- `phase-runner redo` and `phase-runner rewind` own finite phase repair and thin head publication, respectively.
- `phase-runner inspect block-canonicality` reads bounded fork labels plus retained fact counts.
- `phase-runner inspect stored-lineage` reads bounded lineage and optional stored header audit fields.
- `phase-runner inspect raw-events` reads bounded raw logs with transaction, receipt, lineage, header-presence, and normalized-event context.

The inspection commands use read-only repeatable-read transactions and retain
orphaned forks in their output with explicit canonicality labels. Drift,
payload-cache, execution-trace, and watch-plan views were cut and have no
schema-v2 phase-runner replacements. Schema-v2 projection maintenance remains
an explicit project-phase normal run or bounded redo; there is no independent
projection replay command.

Live manifest drift / proxy upgrade alerting is a worker-owned operational loop. It does not write `normalized_events`, mutate manifests, rewrite discovery, or expose a public route.

## Constraints

- versioned native public contract from day one
- namespace is first-class and explicit
- public surface identity is distinct from backing resource, token, resolver instance, and reverse namespace identity
- provenance, coverage, and finality are first-class
- resolution is not modeled as event-only
- verified execution is a required subsystem
- permissions are first-class
- source manifests are first-class
- preimage observation is first-class
- projections are disposable and rebuildable
- protocol-specific logic lives in adapters and execution drivers, not in the public contract
- no silent cross-source fallback; every fallback appears in provenance/explain
- no requirement to preserve the ENSv1 indexer API surface

## Implementation shape

Rust modular monolith. PostgreSQL is the hot indexed/replay store for durable replay facts, projections, retained payload metadata, and execution artifacts. Workers handle ingestion, projection, replay, and retained execution work. The API serves v2 projection and lookup reads, GraphQL compatibility reads, health, and diagnostic readback.

Repository layout:

- `apps/api`, `apps/phase-runner`, `apps/worker`
- `crates/domain`, `crates/storage`, `crates/manifests`, `crates/adapters`,
  `crates/ingest`, `crates/interpret`, `crates/execution`,
  `crates/test-support`

## Test matrix

This is a protocol-risk inventory, not a claim that the e2e suite covers every
row. API crate tests own the v2 and GraphQL route behavior. `tests/e2e` is
current contract-to-schema-v2 pipeline evidence: pinned contracts run on
Anvil, the production `phase-runner` ingests or consumes immutable raw facts,
and scenarios assert normalized events, phase state, and projections directly.
The suite does not start the API, so it is not public HTTP evidence.
Its exact runnable, retired, and deferred inventory is maintained in
[`tests/e2e/README.md`](../tests/e2e/README.md) and the expanded
[coverage ledger](internal/e2e-testing-plan.md).

ENSv1 and wrapper: ENSv1-only name, derived wrapped/emancipated/locked state, wrapped expiry/grace edge, expiry-gated fuse-scope history plus incomplete wrapper-holder permission enumeration and public suppression of stale internal control inputs, wrapped owner ≠ registrant, reverse claim vs verified primary mismatch. (upstream: .refs/ens_v1/contracts/wrapper/README.md:L32 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/README.md:L34 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L843 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L852 @ ens_v1@91c966f)

ENSv2: root-scope role grant, delegate retained after transfer, token regeneration without ownership change, shared subregistry creating multiple surfaces for one resource, alias-derived surface with no direct registry entry, subregistry swap replacing a subtree, and unregister/re-register rotating both resource and token lineage.[^v2-pr-l237][^v2-pr-l241][^v2-pr-l242][^v2-pr-l542][^v2-pr-l547]

DNS / wildcard / offchain: imported DNS name, gasless DNS or metadata-discovered name where supported, wildcard-derived subname, CCIP success, CCIP failure, offchain gateway mismatch.

Basenames: NFT-only transfer, management-only transfer, address-resolution change, full transfer, primary-name set/unset, L1 compatibility resolution, current single-address capability.

Operational: reorg across authority events, reorg across verified execution cache, replay determinism from raw facts, replay determinism from normalized events, proxy implementation change, manifest version change.

End-to-end cases validate every schema-v2 layer material to their claim: raw
facts, normalized events, projections, and, once a contract-backed caller
exists, execution output or public API output. The current suite stops before
the API; route behavior remains owned by API crate tests.

## Open decisions

- exact Postgres partitioning strategy
- exact cache invalidation granularity for verified queries
- whether any execution artifacts should move out of inline Postgres
- exact raw-payload cache retention windows and which payload classes are durable
- whether subscriptions ship in the first stable read milestone or after

---

[^ens-docs-univ]: <https://docs.ens.domains/resolvers/universal/> (official Universal Resolver proxy)

[^bn-readme-l8]: (upstream: .refs/basenames/README.md:L8 @ basenames@1809bbc)
[^bn-readme-l14]: (upstream: .refs/basenames/README.md:L14 @ basenames@1809bbc)
[^bn-readme-l22]: (upstream: .refs/basenames/README.md:L22 @ basenames@1809bbc)
[^bn-readme-l28]: (upstream: .refs/basenames/README.md:L28 @ basenames@1809bbc)
[^bn-readme-l29]: (upstream: .refs/basenames/README.md:L29 @ basenames@1809bbc)
[^bn-readme-l30]: (upstream: .refs/basenames/README.md:L30 @ basenames@1809bbc)
[^bn-readme-l33]: (upstream: .refs/basenames/README.md:L33 @ basenames@1809bbc)
[^bn-readme-l34]: (upstream: .refs/basenames/README.md:L34 @ basenames@1809bbc)
[^bn-readme-l37]: (upstream: .refs/basenames/README.md:L37 @ basenames@1809bbc)
[^bn-readme-l69]: (upstream: .refs/basenames/README.md:L69 @ basenames@1809bbc)
[^bn-readme-l70]: (upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc)
[^bn-readme-l71]: (upstream: .refs/basenames/README.md:L71 @ basenames@1809bbc)

[^bn-l1resolver-l13]: (upstream: .refs/basenames/src/L1/L1Resolver.sol:L13 @ basenames@1809bbc)
[^bn-l1resolver-l154]: (upstream: .refs/basenames/src/L1/L1Resolver.sol:L154 @ basenames@1809bbc)
[^bn-l1resolver-l173]: (upstream: .refs/basenames/src/L1/L1Resolver.sol:L173 @ basenames@1809bbc)
[^bn-l1resolver-l191]: (upstream: .refs/basenames/src/L1/L1Resolver.sol:L191 @ basenames@1809bbc)

[^bn-registry-l10]: (upstream: .refs/basenames/src/L2/Registry.sol:L10 @ basenames@1809bbc)
[^bn-registry-l19]: (upstream: .refs/basenames/src/L2/Registry.sol:L19 @ basenames@1809bbc)
[^bn-registry-l132]: (upstream: .refs/basenames/src/L2/Registry.sol:L132 @ basenames@1809bbc)
[^bn-registry-l223]: (upstream: .refs/basenames/src/L2/Registry.sol:L223 @ basenames@1809bbc)
[^bn-baseregistrar-l15]: (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L15 @ basenames@1809bbc)
[^bn-registrar-controller-l180]: (upstream: .refs/basenames/src/L2/RegistrarController.sol:L180 @ basenames@1809bbc)
[^bn-registrar-controller-l187]: (upstream: .refs/basenames/src/L2/RegistrarController.sol:L187 @ basenames@1809bbc)
[^bn-upgradeable-registrar-controller-l191]: (upstream: .refs/basenames/src/L2/UpgradeableRegistrarController.sol:L191 @ basenames@1809bbc)
[^bn-upgradeable-registrar-controller-l198]: (upstream: .refs/basenames/src/L2/UpgradeableRegistrarController.sol:L198 @ basenames@1809bbc)
[^bn-l2resolver-l4]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L4 @ basenames@1809bbc)
[^bn-l2resolver-l16]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L16 @ basenames@1809bbc)
[^bn-l2resolver-l22]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L22 @ basenames@1809bbc)
[^bn-l2resolver-l29]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L29 @ basenames@1809bbc)
[^bn-l2resolver-l182]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L182 @ basenames@1809bbc)
[^bn-l2resolver-l193]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L193 @ basenames@1809bbc)
[^bn-l2resolver-l209]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L209 @ basenames@1809bbc)
[^bn-l2resolver-l225]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L225 @ basenames@1809bbc)
[^bn-revreg-l12]: (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L12 @ basenames@1809bbc)
[^bn-revreg-l150]: (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L150 @ basenames@1809bbc)
[^bn-revreg-l155]: (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L155 @ basenames@1809bbc)
[^bn-revreg-l156]: (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L156 @ basenames@1809bbc)
[^bn-revreg-l157]: (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L157 @ basenames@1809bbc)
[^bn-revreg-l193]: (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L193 @ basenames@1809bbc)
[^bn-constants-l12]: (upstream: .refs/basenames/src/util/Constants.sol:L12 @ basenames@1809bbc)
[^bn-constants-l13]: (upstream: .refs/basenames/src/util/Constants.sol:L13 @ basenames@1809bbc)

[^v1-ens-l12]: (upstream: .refs/ens_v1/contracts/registry/ENS.sol:L12 @ ens_v1@91c966f)
[^v1-ensreg-l89]: (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L89 @ ens_v1@91c966f)
[^v1-ensreg-l174]: (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L174 @ ens_v1@91c966f)

[^v1-iname-l10]: (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L10 @ ens_v1@91c966f)
[^v1-iname-l27]: (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L27 @ ens_v1@91c966f)
[^v1-iname-l31]: (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L31 @ ens_v1@91c966f)
[^v1-iname-l35]: (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L35 @ ens_v1@91c966f)
[^v1-iname-l37]: (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L37 @ ens_v1@91c966f)
[^v1-iname-l38]: (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L38 @ ens_v1@91c966f)

[^v1-namewrapper-deploy]: (upstream: .refs/ens_v1/deployments/mainnet/NameWrapper.json:L2 @ ens_v1@91c966f)
[^v1-publicresolver-deploy]: (upstream: .refs/ens_v1/deployments/mainnet/PublicResolver.json:L2 @ ens_v1@91c966f)
[^v1-revreg-deploy]: (upstream: .refs/ens_v1/deployments/mainnet/ReverseRegistrar.json:L2 @ ens_v1@91c966f)
[^v1-ur-deploy]: (upstream: .refs/ens_v1/deployments/mainnet/UniversalResolver.json:L2 @ ens_v1@91c966f)
[^v1-iur-l44]: (upstream: .refs/ens_v1/contracts/universalResolver/IUniversalResolver.sol:L44 @ ens_v1@91c966f)
[^v1-iur-l52]: (upstream: .refs/ens_v1/contracts/universalResolver/IUniversalResolver.sol:L52 @ ens_v1@91c966f)
[^v1-iaddrres-l11]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/IAddrResolver.sol:L11 @ ens_v1@91c966f)

[^v1-nw-l132]: (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L132 @ ens_v1@91c966f)
[^v1-nw-l240]: (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L240 @ ens_v1@91c966f)
[^v1-nw-l377]: (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L377 @ ens_v1@91c966f)
[^v1-nw-l421]: (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L421 @ ens_v1@91c966f)
[^v1-nw-l427]: (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L427 @ ens_v1@91c966f)
[^v1-nw-l637]: (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L637 @ ens_v1@91c966f)
[^v1-nw-l666]: (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L666 @ ens_v1@91c966f)
[^v1-nw-l676]: (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L676 @ ens_v1@91c966f)
[^v1-nw-l723]: (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L723 @ ens_v1@91c966f)
[^v1-nw-l827]: (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L827 @ ens_v1@91c966f)
[^v1-nw-l1022]: (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1022 @ ens_v1@91c966f)
[^v1-nw-l1023]: (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1023 @ ens_v1@91c966f)
[^v1-nw-l1034]: (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1034 @ ens_v1@91c966f)

[^v1-pres-l5]: (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L5 @ ens_v1@91c966f)
[^v1-pres-l13]: (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L13 @ ens_v1@91c966f)
[^v1-pres-l20]: (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L20 @ ens_v1@91c966f)
[^v1-pres-l51]: (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L51 @ ens_v1@91c966f)
[^v1-pres-l58]: (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L58 @ ens_v1@91c966f)
[^v1-pres-l66]: (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L66 @ ens_v1@91c966f)
[^v1-pres-l114]: (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L114 @ ens_v1@91c966f)

[^v1-namechanged-l10]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/NameResolver.sol:L10 @ ens_v1@91c966f)
[^v1-namechanged-l18]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/NameResolver.sol:L18 @ ens_v1@91c966f)

[^v1-revreg-l15]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L15 @ ens_v1@91c966f)
[^v1-revreg-l19]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L19 @ ens_v1@91c966f)
[^v1-revreg-l74]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L74 @ ens_v1@91c966f)
[^v1-revreg-l83]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L83 @ ens_v1@91c966f)
[^v1-revreg-l84]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L84 @ ens_v1@91c966f)
[^v1-revreg-l129]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L129 @ ens_v1@91c966f)
[^v1-revreg-l130]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L130 @ ens_v1@91c966f)
[^v1-registry-deploy]: (upstream: .refs/ens_v1/deployments/mainnet/ENSRegistry.json:L2 @ ens_v1@91c966f)
[^v1-revreg-l137]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L137 @ ens_v1@91c966f)
[^v1-registry-l137]: (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L137 @ ens_v1@91c966f)
[^v1-nameresolver-l7]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/INameResolver.sol:L7 @ ens_v1@91c966f)
[^v1-nameresolver-l11]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/INameResolver.sol:L11 @ ens_v1@91c966f)
[^v1-nameresolverimpl-l25]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/NameResolver.sol:L25 @ ens_v1@91c966f)
[^v1-l2rev-base-deploy]: (upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L2 @ ens_v1@91c966f)
[^v1-l2rev-base-args]: (upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L391 @ ens_v1@91c966f)
[^v1-l2rev-event]: (upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L98 @ ens_v1@91c966f)
[^v1-l2rev-nameforaddr]: (upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L154 @ ens_v1@91c966f)

[^v1-aur-l90]: (upstream: .refs/ens_v1/contracts/universalResolver/AbstractUniversalResolver.sol:L90 @ ens_v1@91c966f)
[^v1-aur-l106]: (upstream: .refs/ens_v1/contracts/universalResolver/AbstractUniversalResolver.sol:L106 @ ens_v1@91c966f)
[^v1-aur-l217]: (upstream: .refs/ens_v1/contracts/universalResolver/AbstractUniversalResolver.sol:L217 @ ens_v1@91c966f)
[^v1-aur-l226]: (upstream: .refs/ens_v1/contracts/universalResolver/AbstractUniversalResolver.sol:L226 @ ens_v1@91c966f)
[^v1-aur-l263]: (upstream: .refs/ens_v1/contracts/universalResolver/AbstractUniversalResolver.sol:L263 @ ens_v1@91c966f)
[^v1-aur-l269]: (upstream: .refs/ens_v1/contracts/universalResolver/AbstractUniversalResolver.sol:L269 @ ens_v1@91c966f)
[^v1-ursol-l8]: (upstream: .refs/ens_v1/contracts/universalResolver/UniversalResolver.sol:L8 @ ens_v1@91c966f)

[^v1-ethrc-l116]: (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L116 @ ens_v1@91c966f)
[^v1-ethrc-l133]: (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L133 @ ens_v1@91c966f)

[^subgraph-l15]: (upstream: .refs/ens_subgraph/subgraph.yaml:L15 @ ens_subgraph@723f1b6)
[^subgraph-l39]: (upstream: .refs/ens_subgraph/subgraph.yaml:L39 @ ens_subgraph@723f1b6)
[^subgraph-l44]: (upstream: .refs/ens_subgraph/subgraph.yaml:L44 @ ens_subgraph@723f1b6)
[^subgraph-l145]: (upstream: .refs/ens_subgraph/subgraph.yaml:L145 @ ens_subgraph@723f1b6)
[^subgraph-l170]: (upstream: .refs/ens_subgraph/subgraph.yaml:L170 @ ens_subgraph@723f1b6)
[^subgraph-l226]: (upstream: .refs/ens_subgraph/subgraph.yaml:L226 @ ens_subgraph@723f1b6)
[^subgraph-ts-l134]: (upstream: .refs/ens_subgraph/src/ensRegistry.ts:L134 @ ens_subgraph@723f1b6)
[^subgraph-ts-l230]: (upstream: .refs/ens_subgraph/src/ensRegistry.ts:L230 @ ens_subgraph@723f1b6)
[^subgraph-ts-l238]: (upstream: .refs/ens_subgraph/src/ensRegistry.ts:L238 @ ens_subgraph@723f1b6)
[^subgraph-ts-l246]: (upstream: .refs/ens_subgraph/src/ensRegistry.ts:L246 @ ens_subgraph@723f1b6)

[^v2-deploy-root]: (upstream: .refs/ens_v2/contracts/deployments/sepolia/RootRegistry.json:L2 @ ens_v2@ccaeb58)
[^v2-deploy-ethreg]: (upstream: .refs/ens_v2/contracts/deployments/sepolia/ETHRegistry.json:L2 @ ens_v2@ccaeb58)
[^v2-deploy-ethrc]: (upstream: .refs/ens_v2/contracts/deployments/sepolia/ETHRegistrar.json:L2 @ ens_v2@ccaeb58)
[^v2-deploy-pres]: (upstream: .refs/ens_v2/contracts/deployments/sepolia/PermissionedResolverImpl.json:L2 @ ens_v2@ccaeb58)

[^v2-userreg-l15]: (upstream: .refs/ens_v2/contracts/src/registry/UserRegistry.sol:L15 @ ens_v2@ccaeb58)
[^v2-ethrc-l30]: (upstream: .refs/ens_v2/contracts/src/registrar/ETHRegistrar.sol:L25 @ ens_v2@ccaeb58)
[^v2-ethrc-l151]: (upstream: .refs/ens_v2/contracts/src/registrar/ETHRegistrar.sol:L151 @ ens_v2@ccaeb58)

[^v2-iperm-l34]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L38 @ ens_v2@ccaeb58)
[^v2-iperm-l57]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L61 @ ens_v2@ccaeb58)
[^v2-iperm-l67]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L71 @ ens_v2@ccaeb58)
[^v2-iperm-l72]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L76 @ ens_v2@ccaeb58)
[^v2-events-l15]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L18 @ ens_v2@ccaeb58)
[^v2-events-l30]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L33 @ ens_v2@ccaeb58)
[^v2-events-l49]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L56 @ ens_v2@ccaeb58)
[^v2-events-l59]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L66 @ ens_v2@ccaeb58)
[^v2-events-l69]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L82 @ ens_v2@ccaeb58)
[^v2-events-l75]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L88 @ ens_v2@ccaeb58)

[^v2-pr-l28]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L29 @ ens_v2@ccaeb58)
[^v2-pr-l131]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L142 @ ens_v2@ccaeb58)
[^v2-pr-l141]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L150 @ ens_v2@ccaeb58)
[^v2-pr-l151]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L171 @ ens_v2@ccaeb58)
[^v2-pr-l203]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L452 @ ens_v2@ccaeb58)
[^v2-pr-l216]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L464 @ ens_v2@ccaeb58)
[^v2-pr-l222]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L471 @ ens_v2@ccaeb58)
[^v2-pr-l225]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L474 @ ens_v2@ccaeb58)
[^v2-pr-l237]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L201 @ ens_v2@ccaeb58)
[^v2-pr-l241]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L205 @ ens_v2@ccaeb58)
[^v2-pr-l242]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L206 @ ens_v2@ccaeb58)
[^v2-pr-l261]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L233 @ ens_v2@ccaeb58)
[^v2-pr-l351]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L360 @ ens_v2@ccaeb58)
[^v2-pr-l451]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L528 @ ens_v2@ccaeb58)
[^v2-pr-l461]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L537 @ ens_v2@ccaeb58)
[^v2-pr-l542]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L637 @ ens_v2@ccaeb58)
[^v2-pr-l547]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L647 @ ens_v2@ccaeb58)

[^v2-regroles-l6]: (upstream: .refs/ens_v2/contracts/src/registry/libraries/RegistryRolesLib.sol:L6 @ ens_v2@ccaeb58)
[^v2-regroles-l9]: (upstream: .refs/ens_v2/contracts/src/registry/libraries/RegistryRolesLib.sol:L9 @ ens_v2@ccaeb58)
[^v2-regroles-l14]: (upstream: .refs/ens_v2/contracts/src/registry/libraries/RegistryRolesLib.sol:L14 @ ens_v2@ccaeb58)
[^v2-regroles-l19]: (upstream: .refs/ens_v2/contracts/src/registry/libraries/RegistryRolesLib.sol:L19 @ ens_v2@ccaeb58)
[^v2-regroles-l24]: (upstream: .refs/ens_v2/contracts/src/registry/libraries/RegistryRolesLib.sol:L24 @ ens_v2@ccaeb58)
[^v2-regroles-l29]: (upstream: .refs/ens_v2/contracts/src/registry/libraries/RegistryRolesLib.sol:L29 @ ens_v2@ccaeb58)
[^v2-regroles-l34]: (upstream: .refs/ens_v2/contracts/src/registry/libraries/RegistryRolesLib.sol:L34 @ ens_v2@ccaeb58)
[^v2-regroles-l39]: (upstream: .refs/ens_v2/contracts/src/registry/libraries/RegistryRolesLib.sol:L39 @ ens_v2@ccaeb58)
[^v2-regroles-l45]: (upstream: .refs/ens_v2/contracts/src/registry/libraries/RegistryRolesLib.sol:L45 @ ens_v2@ccaeb58)
[^v2-regroles-l47]: (upstream: .refs/ens_v2/contracts/src/registry/libraries/RegistryRolesLib.sol:L47 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/registry/libraries/RegistryRolesLib.sol:L48 @ ens_v2@ccaeb58)
[^v2-regroles-l50]: (upstream: .refs/ens_v2/contracts/src/registry/libraries/RegistryRolesLib.sol:L51 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/registry/libraries/RegistryRolesLib.sol:L53 @ ens_v2@ccaeb58)
[^v2-regroles-l55]: (upstream: .refs/ens_v2/contracts/src/registry/libraries/RegistryRolesLib.sol:L56 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/registry/libraries/RegistryRolesLib.sol:L58 @ ens_v2@ccaeb58)
[^v2-regroles-l60]: (upstream: .refs/ens_v2/contracts/src/registry/libraries/RegistryRolesLib.sol:L61 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/registry/libraries/RegistryRolesLib.sol:L63 @ ens_v2@ccaeb58)

[^v2-iperm-resolver-l14]: (upstream: .refs/ens_v2/contracts/src/resolver/interfaces/IPermissionedResolver.sol:L19 @ ens_v2@ccaeb58)
[^v2-iethreg-l32]: (upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRegistrar.sol:L32 @ ens_v2@ccaeb58)
[^v2-iethreg-l53]: (upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRenewer.sol:L21 @ ens_v2@ccaeb58)
[^v2-sepolia-dev-iethreg-l53]: (upstream: .refs/ens_v2_sepolia_dev/contracts/src/registrar/interfaces/IETHRegistrar.sol:L53 @ ens_v2_sepolia_dev@554c309) (upstream: .refs/ens_v2_sepolia_dev/contracts/src/registrar/interfaces/IETHRegistrar.sol:L54 @ ens_v2_sepolia_dev@554c309) (upstream: .refs/ens_v2_sepolia_dev/contracts/src/registrar/interfaces/IETHRegistrar.sol:L59 @ ens_v2_sepolia_dev@554c309) (upstream: .refs/ens_v2_sepolia_dev/contracts/src/registrar/interfaces/IETHRegistrar.sol:L60 @ ens_v2_sepolia_dev@554c309)

[^v2-resroles-l7]: (upstream: .refs/ens_v2/contracts/src/resolver/libraries/PermissionedResolverLib.sol:L7 @ ens_v2@ccaeb58)
[^v2-resroles-l51]: (upstream: .refs/ens_v2/contracts/src/resolver/libraries/PermissionedResolverLib.sol:L52 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/resolver/libraries/PermissionedResolverLib.sol:L54 @ ens_v2@ccaeb58)
[^v2-resroles-l56]: (upstream: .refs/ens_v2/contracts/src/resolver/libraries/PermissionedResolverLib.sol:L57 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/resolver/libraries/PermissionedResolverLib.sol:L59 @ ens_v2@ccaeb58)
[^v2-resroles-l61]: (upstream: .refs/ens_v2/contracts/src/resolver/libraries/PermissionedResolverLib.sol:L62 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/resolver/libraries/PermissionedResolverLib.sol:L64 @ ens_v2@ccaeb58)

[^v2-pres-l38]: (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L33 @ ens_v2@ccaeb58)
[^v2-pres-l56]: (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L53 @ ens_v2@ccaeb58)
[^v2-pres-l70]: (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L65 @ ens_v2@ccaeb58)
[^v2-pres-l132]: (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L142 @ ens_v2@ccaeb58)
[^v2-pres-l142]: (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L149 @ ens_v2@ccaeb58)
[^v2-pres-l153]: (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L172 @ ens_v2@ccaeb58)
[^v2-pres-l159]: (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L178 @ ens_v2@ccaeb58)
[^v2-pres-l161]: (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L161 @ ens_v2@ccaeb58)
[^v2-pres-l230]: (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L258 @ ens_v2@ccaeb58)
[^v2-pres-l239]: (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L273 @ ens_v2@ccaeb58)
[^v2-pres-l257]: (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L303 @ ens_v2@ccaeb58)
[^v2-pres-l282]: (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L369 @ ens_v2@ccaeb58)
[^v2-pres-l412]: (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L508 @ ens_v2@ccaeb58)
[^v2-pres-l437]: (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L437 @ ens_v2@ccaeb58)
[^v2-pres-l650]: (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L767 @ ens_v2@ccaeb58)

[^v2-eac-l19]: (upstream: .refs/ens_v2/contracts/src/access-control/interfaces/IEnhancedAccessControl.sol:L22 @ ens_v2@ccaeb58)
[^v2-eac-l176]: (upstream: .refs/ens_v2/contracts/src/access-control/EnhancedAccessControl.sol:L180 @ ens_v2@ccaeb58)
[^v2-eac-l181]: (upstream: .refs/ens_v2/contracts/src/access-control/EnhancedAccessControl.sol:L454 @ ens_v2@ccaeb58)
