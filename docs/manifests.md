# Source Manifests

Manifests pin watched contracts, capability ownership, and rollout state per [source family](glossary.md). They are part of the truth model, not deploy-time configuration. The wider model lives in [`architecture.md`](architecture.md); persistence in [`storage.md`](storage.md); intake in [`chain-intake.md`](chain-intake.md); execution in [`execution.md`](execution.md).

## File format and location

Manifests are TOML files at:

```text
manifests/<profile>/<chain_combo>/<namespace>/<source_family>/<version>.toml
```

[Deployment profiles](glossary.md) select a corpus, and chain-combo directories partition the chains inside that corpus:

```text
manifests/mainnet/ethereum/<namespace>/<source_family>/v1.toml
manifests/mainnet/base/<namespace>/<source_family>/v1.toml
manifests/sepolia/ethereum/<namespace>/<source_family>/v1.toml
manifests/sepolia/base/<namespace>/<source_family>/v1.toml
```

One runtime selects exactly one manifest profile root at startup — `manifests/mainnet/` for the shipped mainnet profile, or `manifests/sepolia/` for the ENSv2 Sepolia profile. Deployment-profile selection is not a manifest schema change. A runtime never loads two profile roots into the same canonical corpus, [watch plan](glossary.md), discovery graph, or [projection](glossary.md) set.

Within a selected profile root, the first directory component is the chain combo. It must match the leading component of each manifest `chain` ID: `ethereum-mainnet` lives under `ethereum/`, `base-mainnet` under `base/`, and `ethereum-sepolia` under `ethereum/`.

TOML is chosen for deterministic diffs, hand-editing, and straightforward Rust parsing.

## Required fields

Each manifest contains:

- `manifest_version`
- `namespace`
- `source_family`
- `chain`
- `deployment_epoch`
- `rollout_status` — `draft` | `shadow` | `active` | `deprecated`
- `normalizer_version`
- `resolver_implementations` — optional declared implementation artifacts for
  upgradeable resolver families
- `capability_flags`
- `roots`
- `contracts`
- `discovery_rules`

For one `(namespace, source_family, chain)` tuple in a selected deployment-profile root, at most one manifest version may declare `rollout_status = "active"`. Zero active versions remains valid for a family whose versions are only `draft`, `shadow`, or `deprecated`. Each `(namespace, source_family, chain, deployment_epoch, manifest_version)` tuple may come from only one file; the loader rejects duplicate tuples across repository layouts regardless of rollout status. Within one manifest version, every `[[contracts]].role` must be unique. The loader rejects these violations before repository sync.

The loader also rejects two active manifest versions on the same chain when both declare the same address as roots or both declare it as contracts, their open-ended `start_block` ranges overlap, and either family feeds manifest-declared event data into `PreimageObserved` rows produced directly from block logs. Those families are `ens_v1_registrar_l1`, `basenames_base_registrar`, `ens_v1_wrapper_l1`, `ens_v2_root_l1`, `ens_v2_registry_l1`, `ens_v2_registrar_l1`, and `ens_v2_resolver_l1`. This check does not compare a root declaration with a contract declaration. Two roots or two contracts may still share an address when neither family is in that list; this is why the shared `l1_resolver` declaration in `basenames_l1_compat` and `basenames_execution` is accepted.

Each `[[roots]]` and `[[contracts]]` entry may declare an optional `start_block`.
`start_block` is the inclusive first historical block for that target. Omitted
means unknown, and manifest storage preserves it as null. The stabilized Stage B
ingest and interpret loaders currently use zero as the effective range-filter
fallback for an omitted value. That fallback is a documented port gap, not
historical provenance or authority to run an unbounded ingest; the phase range
must still be admitted explicitly.

For `[[contracts]]`, `proxy_kind` is required. `proxy_kind = "none"` omits
`implementation`. Any non-`none` `proxy_kind` includes `implementation` as the
expected implementation address for that manifest version. Manifest
synchronization stores that expectation as a
[proxy/implementation discovery edge](glossary.md#discovery-graph--discovery-edge).
When no current `Upgraded` observation exists, the manifest-declared edge is the
active baseline. An `Upgraded` observation naming a different implementation
closes that baseline and makes the observed edge current. An observation naming
the same implementation records event-derived evidence alongside the baseline.
Later manifest synchronization preserves either current observed edge instead
of resetting it to the declaration. The declared implementation remains
deployment metadata used to seed the baseline. There is no separate runtime
manifest-drift observation job. Baseline
materialization and handling of upgrade observations are the schema-v2
consumers that keep `proxy_kind` in the manifest schema.

`resolver_implementations` is a list of `{ role, address }` entries with unique
addresses; several implementation generations may share one role. It
does not admit an implementation as a watched emitter. The project phase uses
the list only to classify a discovered ENSv2 resolver proxy after canonical
ERC-1967 `Upgraded` history identifies its current implementation. ENSv1 and
Basenames resolver classification instead requires the resolver address itself
to be an active `[[contracts]]` declaration. Neither path reads or infers a
runtime code hash.

For `[[discovery_rules]]`, the only authorable `admission` value is `reachable_from_root` — the discovered edge is authoritative while its `from_role` endpoint remains reachable from an active manifest root under an allowed rule. Internal labels are storage tags, not authored values: `manifest_declared` is an `admission_basis`, and `manifest_declared_proxy` is the `discovery_source` written alongside it for manifest-declared proxy edges.

`[abi]` is optional. When present, it declares the Solidity ABI fragments that this manifest version authorizes for adapter, execution, or watch-plan use. ABI entries are source-family metadata; they do not by themselves promote public capability support.

### `capability_flags`

Each flag carries a name, a status (`unsupported` | `shadow` | `supported`), and optional notes.

### `chain`

`chain` names the authority chain for that manifest within the selected deployment profile. Mainnet manifests use chain IDs like `ethereum-mainnet` and `base-mainnet`. Sepolia support is additive as a separate manifest profile root and chain-ID set.

### `abi`

ABI entries use Alloy-parseable human-readable Solidity fragments, not handwritten selectors or topic hashes. The loader validates each fragment with Alloy and derives event topic0 values, function selectors, canonical signatures, indexed parameters, inputs, and outputs from the fragment. Authored selector/topic fields are intentionally absent.

`[[abi.events]]` entries contain:

- `name` — must match the parsed event name.
- `fragment` — a human-readable event fragment such as `event ResolverUpdated(uint256 indexed node, address resolver, address sender)`.
- `emitter_roles` — optional `[[contracts]].role` values that may emit the event. An empty list is
  valid only for a documented
  [emitter-role-independent event](glossary.md#emitter-role-independent-event). The only exception
  outside that finite list is `RegistryCreated` in `ens_v2_registry_l1` when the manifest has a
  `registry_announcement` discovery rule; that event may match without an address admission. Other
  events without `emitter_roles` fail manifest validation.
- `normalized_events` — optional normalized event kinds produced from the event.
- `status` — optional `unsupported` | `shadow` | `supported` marker for the ABI entry.
- `notes` — optional reviewer-facing context.

`[[abi.calls]]` entries contain:

- `name` — must match the parsed function name.
- `fragment` — a human-readable function fragment such as `function resolver(bytes32 node) view returns (address)`.
- `target_roles` — optional `[[contracts]].role` values that may be called.
- `status` — optional `unsupported` | `shadow` | `supported` marker for the ABI entry.
- `notes` — optional reviewer-facing context.

ABI fragments should cite upstream in nearby manifest comments or in the public doc section that admits the source family. If an adapter still has an in-code selector or `sol!` definition for a manifest-declared fragment, that code is a compatibility bridge until the adapter consumes the manifest ABI directly.

`normalizer_version` is currently `ensip15@ens-normalize-0.1.1` for all admitted ENS, ENSv2, and Basenames source families. Runtime code treats this as one shared normalization boundary, not a per-source-family choice.

## Example shape

```toml
manifest_version = 1
namespace = "ens"
source_family = "ens_v2_registry_l1"
chain = "ethereum-mainnet"
deployment_epoch = "ens_v2"
rollout_status = "active"
normalizer_version = "ensip15@ens-normalize-0.1.1"

[[roots]]
name = "RootRegistry"
address = "0x0000000000000000000000000000000000000000"
code_hash = "sha256:..."
abi_ref = "abis/ens_v2_root_registry.json"
start_block = 123456

[[contracts]]
role = "registry"
address = "0x0000000000000000000000000000000000000000"
proxy_kind = "none"
start_block = 123456

[[discovery_rules]]
edge_kind = "subregistry"
from_role = "registry"
admission = "reachable_from_root"

[[abi.events]]
name = "ResolverUpdated"
fragment = "event ResolverUpdated(uint256 indexed node, address resolver, address sender)"
emitter_roles = ["registry"]
normalized_events = ["ResolverChanged"]
status = "supported"

[[abi.calls]]
name = "resolver"
fragment = "function resolver(bytes32 node) view returns (address)"
target_roles = ["registry"]
status = "shadow"

[capability_flags]
declared_children = "supported"
```

## Capability ownership

Capability ownership attaches to the declaring `source_family`. It is never implied by another family's presence.

### ENS mainnet

`ens_execution` owns verified resolution at the ENS Universal Resolver proxy `0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe` with `verified_resolution = "shadow"`.[^ens-docs-univ][^v1-ur-deploy][^v1-ursol-l8] The pinned `.refs/` artifact is the implementation/ABI anchor; the lookup entry is the proxy address. The shadow flag records manifest ownership for the execution substrate; public ENS verified-resolution support is gated by the route-level support classes in `docs/api-v2-routes.md` and `docs/execution.md`, not by widening this manifest flag.

The ENS primary-name route does not introduce a second manifest capability. `ens_execution` supplies the manifest selection for the request-scoped, hash-pinned ENS/60 missing-tuple lookup under the same owner manifest, without turning `verified_resolution = "shadow"` into a route-level primary-name support flag. Indexed exact-tuple claim state lives in `bigname_phase.primary_names_current`; provider lookup responses are not persisted as execution outcomes or traces.

`ens_v1_reverse_l1` owns declared reverse-claim intake at the Mainnet `addr.reverse` Reverse Registrar `0xa58E81fe9b61B5c3fE2AFD33CF304c454AbFc7Cb`.[^v1-revreg-deploy][^v1-revreg-l15][^v1-revreg-l19] No dedicated `claimed_primary_name` flag is needed for that indexed claim-state contract.

`ens_v1_registry_l1` owns the current ENS registry at `0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E` with `start_block = 9380380`,[^subgraph-l15] plus `ENSRegistryOld` at `0x314159265dd8dbb310642f98f50c066173c1259b` with `start_block = 3327417` as a migration-aware input.[^subgraph-l39][^subgraph-l44] Old-registry logs do not union with current logs by latest block: a current-registry `NewOwner` marks the node migrated; later old-registry `NewOwner`, `Transfer`, `NewTTL`, and non-root `NewResolver` updates for that node are suppressed.[^subgraph-ts-l134][^subgraph-ts-l230][^subgraph-ts-l238][^subgraph-ts-l246] Root-resolver updates from the old registry are the one frozen exception.[^v1-ensregfb-l40]

`ens_v1_registrar_l1` owns `.eth` BaseRegistrar at `start_block = 9380410`[^subgraph-l122] plus the legacy, wrapped, and current ETHRegistrarController contracts as label-bearing intake (LegacyEthRegistrarController `9380471`,[^subgraph-l145] WrappedETHRegistrarController `16925618`,[^v1-wrapethrc-l640] current ETHRegistrarController `22764821`).[^v1-ethrc-l706] Controllers do not split into a separate source-family owner. The wrapped controller's `NameRenewed` additionally derives a wrapper-resource expiry observation under this family because its renewal calls `NameWrapper.renew`, which stores registrar expiry plus grace without emitting `ExpiryExtended`. (upstream: .refs/ens_v1/deployments/mainnet/WrappedETHRegistrarController.json:L656 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L318 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L333 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L337 @ ens_v1@91c966f)

`ens_v1_wrapper_l1` owns NameWrapper at `0xD4416b13d2b3a9aBae7AcD5D6C2BbDBE25686401` with `start_block = 16925608`,[^v1-namewrapper-deploy] for wrapper authority, direct fuse/expiry observations, wrapper-revealed names, and wrapper-driven registry changes.[^v1-iname-l27][^v1-iname-l35][^v1-iname-l37][^v1-iname-l38]

`ens_v1_resolver_l1` owns ENS Labs PublicResolver-generation profile admission. The seed entry is the latest PublicResolver at `0xF29100983E058B709F3D539b0c765937B804AC15` with `start_block = 22764828`.[^v1-publicresolver-deploy] [Resolver-profile](glossary.md) [admission](glossary.md) is exact-address classification from this declared list. It permits the project phase to publish the canonical normalized observations retained for that resolver; it does not prove exhaustive history or event-to-call parity. Unadmitted resolvers are explicitly unsupported.

Admitted ENS Labs PublicResolver generations on Ethereum Mainnet (first-party app-known data):[^v1-app-resolvers]

| Address | Profile | Limitations |
| --- | --- | --- |
| `0xF29100983E058B709F3D539b0c765937B804AC15` | latest: address, multicoin, default coin-type fallback, name, ABI, text, contenthash, DNS, interface, name-wrapper-aware, VersionableResolver | no pubkey or `DataResolver` |
| `0x231b0Ee14048e9dCcD1d247744d114a4EB5E8E63` | as latest minus default coin-type fallback | no pubkey or `DataResolver` |
| `0x4976fb03C32e5B8cfe2b6cCB31c09Ba78EBaBa41` | address, multicoin, name, ABI, text, contenthash, DNS, interface | no name-wrapper, no fallback, no Versionable, no pubkey/`DataResolver` |
| `0xDaaF96c344f63131acadD0Ea35170E7892d3dfBA` | same as `0x4976...` | same |
| `0x226159d592E2b063810a10Ebf6dcbADA94Ed68b8` | legacy: address, multicoin, name, ABI, text, contenthash, interface | no DNS, no name-wrapper, no fallback, no Versionable, no pubkey/`DataResolver` |
| `0x5FfC014343cd971B7eb70732021E26C35B744cc4` | older legacy: ETH-address, name, ABI, text, interface | no multicoin, contenthash, DNS, name-wrapper, fallback, Versionable, pubkey/`DataResolver` |
| `0x1da022710dF5002339274AaDEe8D58218e9D6AB5` | oldest legacy: ETH-address, name, ABI, interface | no text, contenthash, multicoin, DNS, name-wrapper, fallback, Versionable, pubkey/`DataResolver` |

Older rows do not inherit latest-only behavior. Unsupported interfaces and pending resolver profiles surface explicitly through `coverage`, `UnsupportedSummary`, `resolver_family_pending`, or `resolver_family_unsupported`. They are never reported as absent records.

Address-specific resolver `start_block`s come from ENSNode datasource pins where available: `0x1da0...` `3648359`, `0x5FfC...` `3733668`, `0x2261...` `8659893`, `0x4976...` `9412610`, `0x231b...` `16925619`.[^ensnode-mainnet] `0xDaaF...` has no pinned datasource; it uses the current ENSRegistry epoch `9380380` as a conservative bootstrap basis. The OffchainDNSResolver and ExtendedDNSResolver app-known maps remain deferred — they are not PublicResolver-generation profile admissions.

Registry `NewResolver(node, resolver)` changes only the node-to-resolver binding.[^v1-ens-l12][^v1-ensreg-l89][^v1-ensreg-l174] It does not discover or admit a resolver contract. Resolver-local history is selected across all emitting addresses by the ENS resolver signature set declared in `ens_v1_resolver_l1`; no discovered-address edge is required.[^v1-resolver-signature-abis] A matching event produces a record-history observation for a known ENS node even when that resolver is not the node's current resolver. Current record visibility still follows the node's resolver pointer. Resolver-profile admission remains the separate gate for complete-family coverage, parity, and supported resolver claims.

`PubkeyChanged` is ignored by the current admission model. `DataResolver`-shaped events are unsupported on admitted generations and `pending` on unknown resolver profiles. The generic `resolver_record` fact is an observation bucket; it does not act as a catch-all for unknown families.

### ENSv2 (`sepolia` profile)

The `sepolia` profile admits four ENSv2 families from the post-audit current Sepolia deployment under `manifests/sepolia/ethereum/ens/`, all in `deployment_epoch = "ens_v2_sepolia_post_audit"`:[^v2-deploy-root][^v2-deploy-ethreg][^v2-deploy-ethrc][^v2-deploy-pres]

- `ens_v2_root_l1` — `RootRegistry` at `0x11b5bfbe9078d826b1edbdd1cfc12f5828d9f50c`, `start_block = 11163319`. Tokenized, [resource](glossary.md)-scoped permissioned registry seed for discovery and parent graph state.[^v2-pr-l22][^v2-pr-l28]
- `ens_v2_registry_l1` — `ETHRegistry` at `0x67b728a792e789a8978b30cf1b3b641f19354b43`, `start_block = 11163391`, plus registry instances announced by `RegistryCreated()`. Direct `PermissionedRegistry` construction emits the announcement first; a `UserRegistry` proxy emits it during initialization. It admits the emitting address from that exact log position without requiring a parent link. `UserRegistryImpl` at `0x840fa461059862ea466a711e8c98c8de732061c0` is implementation metadata, not a separate owner. (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L9 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L113 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/registry/UserRegistry.sol:L43 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/registry/UserRegistry.sol:L47 @ ens_v2@ccaeb58)[^v2-userreg-l15]
- `ens_v2_registrar_l1` — `ETHRegistrar` at `0xa4449a0dd2b83007553d9b1d28b583a46a805a30`, `start_block = 11163403`. Admitted registration and renewal lifecycle facts; registered-name resource identity links back to the registry resource.[^v2-ethrc-l49][^v2-ethrc-l173]
- `ens_v2_resolver_l1` — registry-discovered resolver contract instances retain the manifest-configured normalized record and record-version observations. `PermissionedResolver` instances additionally provide alias, named-resource, and resolver-scoped EAC events. Resolver-local projection is supported only when the proxy's latest canonical ERC-1967 `Upgraded` event names an implementation in the active manifest's `resolver_implementations` list. The current declared `PermissionedResolverImpl` is `0x7e4b2d59938930168024201752ee5503df402303`; the contract inherits UUPS upgradeability and its deployment ABI exposes `Upgraded(address)`.[^v2-deploy-pres][^v2-pres-uups][^v2-pres-upgraded] A manifest admission change reclassifies the affected resolver inline during project-phase publication. No code-hash observation participates.

The preceding `ens_v2_sepolia_dev` manifest versions remain checked in as `deprecated` historical records and citation evidence. Their addresses and ranges do not participate in the active post-audit watch or replay plan.

Exact-name profile [capability promotion](glossary.md) is deployment-profile-scoped: only `exact_name_profile = "supported"` on the active `ens_v2_registrar_l1` version in the `sepolia` root promotes `.eth` exact-name declared reads to supported, backed by `ETHRegistry` resource/token state and `ETHRegistrar` lifecycle facts.[^v2-iperm-l22][^v2-events-l15][^v2-iethreg-l32] The capability promotion does not apply to mainnet, another manifest profile, or any runtime that has not selected `manifests/sepolia`. Active rollout, raw preimage observations, resolver admission, or backfill completion promote no other capability.

Upstream events map to normalized adapter output: `TokenResource` → `TokenResourceLinked`, `TokenRegenerated` → `TokenRegenerated`, each positive-value item in `TransferSingle` or `TransferBatch` with nonzero `from` and `to` → `TokenControlTransferred`, `SubregistryUpdated` → `SubregistryChanged`, `ParentUpdated` → `ParentChanged`, `AliasChanged` → `AliasChanged`, `EACRolesChanged` → resource- or resolver-scoped permission events.[^v2-iperm-l34][^v2-events-l49][^v2-events-l69][^v2-events-l75][^v2-iperm-resolver-l14][^v2-eac-l19] The deployed `ETHRegistry` and `UserRegistryImpl` ABIs both contain the transfer events, and upstream changes the stored owner only for a positive value; mint and burn use a zero endpoint and therefore do not become token-control transfers. (upstream: .refs/ens_v2/contracts/deployments/sepolia/ETHRegistry.json:L652 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/deployments/sepolia/ETHRegistry.json:L689 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/deployments/sepolia/UserRegistryImpl.json:L723 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/deployments/sepolia/UserRegistryImpl.json:L760 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/erc1155/ERC1155Singleton.sol:L194 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/erc1155/ERC1155Singleton.sol:L201 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/erc1155/ERC1155Singleton.sol:L208 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/erc1155/ERC1155Singleton.sol:L210 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/erc1155/ERC1155Singleton.sol:L318 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/erc1155/ERC1155Singleton.sol:L333 @ ens_v2@ccaeb58) These are adapter semantics, not manifest schema fields. Role changes remain permission events and are not ownership evidence.

ENSv2 terminal lifecycle events also close interpreter-owned state. `LabelUnregistered` is emitted before upstream expires the entry and has no paired zero-target subregistry or resolver updates, so the ENSv2 interpreter closes the current surface binding and emits terminal discovery observations at that log position. It also emits null `SubregistryChanged` and `ResolverChanged` boundaries for any attached roles so full and incremental projections retire the old topology. (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L201 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L208 @ ens_v2@ccaeb58) A replacement registration or reservation can bump the token version and overwrite the stored subregistry and resolver, while upstream emits follow-up target updates only for nonzero replacements; the adapter therefore closes the prior discovery targets before accepting the successor lifecycle and emits the same null role boundaries. Replacement registration lets the following `TokenResource` close the old surface at the successor start; replacement reservation has no successor resource, so it closes immediately and emits `SurfaceUnbound` as position-specific reorg-repair evidence. (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L452 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L459 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L471 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L474 @ ens_v2@ccaeb58)

`RegistryCreated` is admitted as registry-instance history and discovery input. `URIUpdated`, the `PermissionedResolver` `DataChanged` / `NamedDataResource` pair, and ERC-1155 `ApprovalForAll` remain outside the active normalized behavior.[^v2-events-created][^v2-events-uri][^v2-pres-data] Operator approval is not treated as token ownership or an ENSv2 resource-role grant. (upstream: .refs/ens_v2/contracts/src/erc1155/ERC1155Singleton.sol:L336 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/erc1155/ERC1155Singleton.sol:L341 @ ens_v2@ccaeb58) The separately deployed `ETHRenewerV1` is not an admitted registrar emitter; `NameRenewed` intake remains limited to the admitted `ETHRegistrar` emitter.[^v2-deploy-renewer][^v2-iethrenewer-l21] `PublicResolverV2` is not directly declared by a manifest and is not an admitted resolver profile.[^v2-deploy-public-resolver] Its configured normalized observations may remain stored, but its projection support status stays unsupported unless canonical upgrade history later matches an explicitly declared resolver implementation. Current record visibility remains limited to the current resolver emitter.[^v2-public-resolver-discovery][^v2-public-resolver-version]

All other current Sepolia artifacts — including universal/reverse resolution, wrapper, migration, factory, oracle, batch-registrar, and mock-payment surfaces — remain outside admission until a doc-first update.

### Basenames mainnet

Basenames mainnet admits six families:[^bn-readme-l22][^bn-readme-l28][^bn-readme-l29][^bn-readme-l30][^bn-readme-l33][^bn-readme-l34][^bn-readme-l36][^bn-readme-l37][^bn-readme-l69][^bn-readme-l70]

- `basenames_base_registry` — `registry` at `0xb94704422c2a1e396835a571837aa5ae53285a95` (Base). Per-node owner/resolver/ttl state.[^bn-registry-l10][^bn-registry-l100][^bn-registry-l113][^bn-registry-l132]
- `basenames_base_registrar` — `registrar` at `0x03c4738ee98ae44591e1a4a4f3cab6641d95dd9a` (Base), plus `legacy_registrar_controller` at `0x4cCb0BB02FCABA27e82a56646E81d8c5bC4119a5` and `upgradeable_registrar_controller` proxy at `0xa7d2607c6BD39Ae9521e514026CBB078405Ab322`. Tokenized authority stays with BaseRegistrar; controller contracts are admitted in the same source family for label-bearing registration and renewal observations only.[^bn-baseregistrar-l15][^bn-baseregistrar-l17][^bn-baseregistrar-l237][^bn-baseregistrar-l327][^bn-registrar-controller-l180][^bn-registrar-controller-l187][^bn-upgradeable-registrar-controller-l191][^bn-upgradeable-registrar-controller-l198]
- `basenames_base_resolver` — `resolver` at `0xC6d566A56A1aFf6508b41f6c90ff131615583BCD` (Base). Default `L2Resolver` profile seed.[^bn-l2resolver-l22][^bn-l2resolver-l49][^bn-l2resolver-l52][^bn-l2resolver-l193]
- `basenames_base_primary` — ENSv1 `L2ReverseRegistrar` at `0x0000000000D8e504002cC26E3Ec46D81971C1664` (Base). Declared primary-name value intake only, keyed by `NameForAddrChanged(address,string)` and scoped to Base coin type `2147492101`; the adapter emits both the reverse claim anchor and the accompanying `RecordChanged(name)` claim-name observation from that raw fact. This source family does not admit the Basenames `ReverseRegistrar` at `0x79ea96012eea67a83431f1701b3dff7e37f9e282` as the primary-name value authority; Basenames exact-name, address-name, and children truth still comes from the Base registry/registrar/resolver families.[^v1-l2rev-base-deploy][^v1-l2rev-base-args][^v1-l2rev-event][^v1-l2rev-nameforaddr][^bn-readme-l33][^bn-revreg-l12][^bn-revreg-l150]
- `basenames_l1_compat` — `l1_resolver` at `0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31` (Ethereum). L1 compatibility transport for the `base.eth` domain.[^bn-l1resolver-l13]
- `basenames_execution` — `l1_resolver` at the same Ethereum address with `verified_resolution = "supported"` for the exact-surface transport-assisted direct-path class only. Execution entrypoint that initiates `OffchainLookup` and completes through `resolveWithProof`.[^bn-l1resolver-l154][^bn-l1resolver-l173][^bn-l1resolver-l191]

The L1 Resolver address appears in both `basenames_l1_compat` and `basenames_execution`. Transport ownership stays with `basenames_l1_compat`; execution entrypoint and verified-resolution routing stay with `basenames_execution`.

`basenames_execution` v2 capability-promotes only the [path class](glossary.md) where `resolver_path[0].logical_name_id` equals the route surface, `wildcard.source = null`, `alias.final_target = null`, `subregistry_path = []`, `transport.source_chain_id = "base-mainnet"`, `transport.target_chain_id = "ethereum-mainnet"`, and `transport.contract_address = "0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31"`. Alias-participating, wildcard-derived, linked-subregistry, transport-free, and offchain-gateway classes return selector-local `unsupported`.[^bn-readme-l71]

`basenames_execution` does not admit verified primary-name lookup. The current
verified primary-name product path is limited to ENS coin type `60`.

Basenames registry `NewResolver` updates a node binding but does not discover a contract. Base-side resolver-local events use the `basenames_base_resolver` signature set across all emitting addresses. Resolver-local supported behavior still requires `L2Resolver`-compatible profile admission for the emitted family. This match-all rule does not admit the L1 Resolver or offchain gateways.[^bn-registry-l19][^bn-registry-l223][^bn-l2resolver-l4][^bn-l2resolver-l16][^bn-l2resolver-l29][^bn-l2resolver-l182][^bn-l2resolver-l209][^bn-l2resolver-l225]

`basenames_offchain` is reserved for later gateway admission. It is not part of the current split.

## Contract instance admission and continuity

Manifest loading admits source-graph nodes as `contract_instance_id`s, not raw addresses. Each active `[[roots]]` and `[[contracts]]` entry resolves to one admitted instance.

- `[[roots]]` seed canonical graph and watch-plan expansion; otherwise they follow the same identity rules as `[[contracts]]`.
- Reusing the same address on the same chain across manifest versions, even across an inactive gap, carries forward the existing `contract_instance_id` and appends a new non-overlapping active range.
- Changing a declared address closes the prior active range and admits a new instance, with a new `contract_instance_id` rather than ID reuse. No discovery edge records the succession: the loader writes only `proxy_implementation` edges, and the [`migration` edge kind](glossary.md#migration-edge-migration) the schema permits is [reserved surface](glossary.md#reserved-surface) with no writer. What ties the two instances together is the manifest declaration, not their addresses: successive manifest versions carry the same `(chain_id, declaration_kind, declaration_name, role)` tuple against different declared addresses. Do not try to recover succession from the instances' active ranges — a retired address is closed at the chain head observed when the manifest loaded, while its successor opens at its own declared `start_block`, so the two ranges frequently overlap instead of abutting.
- `proxy_kind = "none"` resolves the declared address directly; `implementation` is omitted.
- `proxy_kind != "none"` requires `implementation`. The proxy and implementation are separate instances linked by a time-ranged proxy/implementation edge.
- Changing only `implementation` keeps the proxy's identity. The implementation instance is reused if its address reappears, otherwise a new one is minted.

Contract addresses persist as time-ranged attributes for raw-fact matching and watch-plan expansion.

### Admission selection for addresses with multiple declared roles

One address may be declared under more than one role in the same manifest. The common case is an
address declared both as a `[[roots]]` entry and under its contract role. For each raw log,
interpretation considers the full set of active declarations at that address; database row order
does not choose the declaration.

When the event produces a discovery edge governed by a role-scoped `discovery_rules` entry, a
candidate carrying that rule's `from_role` outranks candidates with other roles. Selection
otherwise preserves each declaration's role. It clears the selected role only for a checked-in
`(source_family, event)` entry below whose adapter does not consume it. The checked-in pairs are:

- `ens_v1_resolver_l1`: `ABIChanged`, `AddrChanged`, `AddressChanged`, `ContentChanged`,
  `ContenthashChanged`, `DNSRecordChanged`, `DNSRecordDeleted`, `DNSZonehashChanged`, `DataChanged`,
  `InterfaceChanged`, `NameChanged`, `TextChanged`, and `VersionChanged`;
- `basenames_base_resolver`: `AddrChanged`, `AddressChanged`, `NameChanged`, `TextChanged`, and
  `VersionChanged`;
- `ens_v2_resolver_l1`: `AddressChanged`, `AliasChanged`, `ContenthashChanged`, `EACRolesChanged`,
  `NameChanged`, `NamedAddrResource`, `NamedResource`, `NamedTextResource`, `TextChanged`, `Upgraded`,
  and `VersionChanged`.

The canonical typed table is `bigname_manifests::ROLE_INSENSITIVE_EVENTS`; every entry carries a
justification and the adapter file it describes. A manifest that omits `emitter_roles` for any
other event is rejected unless the `ens_v2_registry_l1` `RegistryCreated` exception described
above applies.

Role-scoped events use `[[abi.events]].emitter_roles` to constrain eligible declarations before
selection, so the discovery-rule tie-break does not change their role. Candidates made equivalent
because the pair appears in the list above may collapse to one selection. If distinct-role
candidates remain and the applicable discovery rule cannot choose between them, interpretation
stops with the deterministic `ambiguous admitted adapters` error; it never picks an arbitrary row.

## Discovery admission

A contract is an indexable event source when one of these holds:

- it is declared directly in an active manifest
- it is reachable from an active manifest root through an allowed indexability-producing `discovery_rules` edge; an ENSv2 `subregistry` edge is topology-only and does not satisfy this rule
- it is explicitly allow-listed by a manifest version for a migration epoch
- its creation is announced by an admitted match-all event

Discovery is forward-only from the announcement event. ENSv1 and Basenames registries are manifest-declared singletons; registry owners are leaves and do not create contract instances. A registry `NewOwner` log still records the child-name assignment and its history, including removal through the zero address, but the assigned owner is not admitted as a registry contract. (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L75 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L82 @ ens_v1@91c966f) ENSv1 and Basenames resolver record history is the exception to address admission: their manifest-declared ENS-specific signature sets are matched across all emitters because those resolver generations have no creation announcement. `NewResolver` changes a name's resolver pointer but creates no discovery edge. `VersionChanged` before-state is tracked independently for each `(emitting resolver, node)` pair because each resolver contract stores its own per-node record-version mapping. (upstream: .refs/ens_v1/contracts/resolvers/ResolverBase.sol:L7 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/ResolverBase.sol:L22 @ ens_v1@91c966f) (upstream: .refs/basenames/src/L2/resolver/ResolverBase.sol:L11 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/resolver/ResolverBase.sol:L38 @ basenames@1809bbc)

For ENSv2, `RegistryCreated()` admits the emitting registry with a [`registry_announcement` edge](glossary.md#registry-announcement-edge-registry_announcement) anchored by the active registry manifest. The edge records indexability only. It is not a parent-child edge and does not make the announced registry reachable through any name. Manifest-declared `RootRegistry` and `ETHRegistry` instances seed suffix anchors; an announced registry below them gains a suffix only when its current child-side parent claim and the parent's current unexpired `SubregistryUpdated` pointer agree. Either side breaking retracts that suffix and its name bindings. The additional `ETHRegistry` suffix anchor is recorded in [`upstream.md` § Known divergences](upstream.md#known-divergences). `SubregistryUpdated` remains the only source of registry parent-child reachability. `ResolverUpdated` remains the source of resolver-instance discovery. Address-scoped interpretation begins at the exact `RegistryCreated()` transaction/log position: direct `PermissionedRegistry` construction emits it first, while a `UserRegistry` proxy emits it during initialization. (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L9 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L113 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/registry/UserRegistry.sol:L43 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/registry/UserRegistry.sol:L47 @ ens_v2@ccaeb58)

The active match-all sets widen retained live facts from this change forward. Historical Base resolver events, ENSv2 `RegistryCreated` events, and ERC-1967 `Upgraded` events that predate the widening require the mandatory one-time historical fetch before a derived-state rebuild. That fetch is an ingest operation, not discovery inference.

At cutover, these interpretation changes are applied through a fresh-schema rebuild. The cutover carries raw facts, chain lineage, and label preimages, but it does not carry normalized events, identity rows, or projections from the transitional schema. This change therefore has no supported in-place replay over previously derived ENSv2 rows; the one-time historical fetch completes the raw input before the fresh interpretation run.

Each admitted edge stores `from_contract_instance_id`, `to_contract_instance_id`, source manifest version, edge kind, discovery source, active range, and provenance.

Discovery resolves `(chain, address, point in time)` to endpoint `contract_instance_id`s before storing the edge. Re-admitting an address that was previously admitted on the same chain reuses the prior `contract_instance_id` and appends a new range; replaying the exact same observation reuses its historical edge epoch instead of appending a duplicate. A new ID is minted only for addresses never admitted on that chain. Manifest-declared and discovered proxy/implementation links share the same edge and active-range rules.

Schema-v2 interpretation applies discovery opens and closes in block,
transaction, and log order. Every raw-log discovery edge requires non-negative
transaction and log positions and an observation key. Inserting an earlier
observation caps its predecessor at that position; repeating the same target
reuses or backdates the existing epoch instead of creating an overlapping
active epoch. An `interpret` redo first prepares the selected derived range and
then applies the same ordered writer. It does not use omitted observations as a
complete-source deletion signal or consult the deleted stored-lineage coverage
and adapter-checkpoint machinery. Closed intervals remain historical emitter
admission while their manifest authority is active, but do not expand the
current watch plan.

Schema-v2 manifest synchronization retires manifest-declared address ranges and
updates manifest-declared proxy edges. It does not run a full-source
reconciliation over event-driven edges. An authority change invalidates the
interpret and project phase content hashes; explicit redo then re-derives the
affected discovery rows from retained facts under the new manifest authority.
The Base project phase also consumes Ethereum Mainnet `basenames_execution`
authority for Basenames support and provenance. Changing that family therefore
invalidates the `base-mainnet` project epoch as well, without invalidating its
interpret epoch.

ENSv1 and Basenames owner events do not create discovery contract instances.
Schema-v2 interpretation processes retained `RegistryCreated`,
`SubregistryUpdated`, and `ResolverUpdated` facts through the current
interpreter. The deleted indexer no longer synthesizes
`orphaned_discovery_edge` tombstones; branch replacement is handled through
lineage selection plus an explicit derived-range redo.

## Manifest change propagation

Manifest declaration changes produce the `SourceManifestUpdated` [normalized
event](glossary.md). Its state includes proxy declarations and the staged
authored capability fields, so manifest synchronization does not mint separate
proxy- or capability-change event kinds. The synchronization transaction also
invalidates completed interpret and project phase content hashes for changed
chains. The deleted admission-epoch and full-source reconciliation writers no
longer participate; phase redo applies the new authority to discovery and
projection state.

The legacy resolver-profile authority journal, input queue, and reconciliation tables remain only in immutable migration history. Their Rust APIs and consumers have been deleted. Current manifests and schema-v2 discovery edges are the admission truth; legacy rows do not gate synchronization or interpretation.

ERC-1967 `Upgraded(address)` logs from manifest-declared and event-announced contracts produce `Upgraded` normalized history on the emitting contract. (upstream: .refs/basenames/lib/openzeppelin-contracts/contracts/interfaces/IERC1967.sol:L13 @ basenames@1809bbc) Manifest synchronization does not infer upgrades from code-hash drift and does not synthesize drift-alert normalized events.

The schema-v2 project phase has no code-hash reader. ENSv1 and Basenames use
only exact resolver addresses declared by the active manifest, while ENSv2
uses the latest canonical resolver-family `Upgraded` history and the active
manifest's `resolver_implementations` list. A manifest admission change
reclassifies the affected address inline; there is no journal, queue, code-hash
reader, or legacy resolver-profile view.

## Watch-plan expansion

Watch-plan expansion starts from active manifest roots by `contract_instance_id` and traverses active discovery edges by ID.

- The chain-intake watch target is the address range attached to each active contract instance at the requested time.
- If a manifest target carries `start_block`, the materialized watch range starts at that inclusive block unless a later active-range boundary narrows it.
- If `start_block` is omitted, the historical start is unknown and a finite
  historical ingest must obtain an explicit admitted bound. The current Stage B
  loaders nevertheless use zero as their effective range-filter fallback; that
  implementation gap does not make zero authoritative.
- Legacy watch rows may denormalize address and code-hash state, but their durable explanation path is `manifest root → discovery edge(s) → contract_instance_id`; schema-v2 resolver classification does not read that denormalization.
- Address-only watch state is rebuildable from manifests, instance attributes, and active discovery edges.

The materialized watch plan is derived from active manifests and discovery
edges. No worker watch-plan inspection command remains.

## Capability policy

Capabilities gate behavior, not public-contract existence. An unsupported capability surfaces as `coverage.unsupported_reason` or a typed error. Shadow capabilities admit facts without enabling general reads. Adding a new capability is additive only when it does not change prior semantics.

## Ownership

- Manifest/discovery owners maintain the TOML files.
- Schema-v2 interpretation consumes manifest versions as inputs.
- Lookup uses manifest versions and admitted entrypoints for request-scoped
  provider execution.
- Schema changes require a doc-first update to this file.

---

## Bootstrap `start_block` provenance

Known historical starts cite a pinned upstream source. Targets without a pinned
source omit `start_block`; historical bootstrap skipped them rather than
inventing values. Basenames mainnet families and the ENS Universal Resolver
remain unknown. The current phase-loader fallback described
above does not change that provenance rule.

| Target | `start_block` | Source |
| --- | --- | --- |
| ENSv1 ENSRegistry | `9380380` | [^subgraph-l15] |
| ENSv1 ENSRegistryOld | `3327417` | [^subgraph-l39] |
| ENSv1 BaseRegistrar | `9380410` | [^subgraph-l122] |
| LegacyEthRegistrarController | `9380471` | [^subgraph-l145] |
| WrappedETHRegistrarController | `16925618` | [^v1-wrapethrc-l640] |
| ETHRegistrarController | `22764821` | [^v1-ethrc-l706] |
| ENSv1 NameWrapper | `16925608` | [^v1-namewrapper-deploy] |
| ENSv1 PublicResolver (latest) | `22764828` | [^v1-publicresolver-deploy] |
| ENSv1 ReverseRegistrar | `16925606` | [^v1-revreg-deploy-l379] |
| ENSv2 RootRegistry (post-audit Sepolia) | `11163319` | [^v2-deploy-root] |
| ENSv2 ETHRegistry (post-audit Sepolia) | `11163391` | [^v2-deploy-ethreg] |
| ENSv2 ETHRegistrar (post-audit Sepolia) | `11163403` | [^v2-deploy-ethrc] |

---

[^ens-docs-univ]: <https://docs.ens.domains/resolvers/universal/>
[^v1-app-resolvers]: (upstream: .refs/ens_app_v3/src/constants/resolverAddressData.ts:L32 @ ens_app_v3@7175858)
[^ensnode-mainnet]: (upstream: .refs/ensnode/packages/datasources/src/mainnet.ts:L343 @ ensnode@9b8f590)

[^v1-ens-l12]: (upstream: .refs/ens_v1/contracts/registry/ENS.sol:L12 @ ens_v1@91c966f)
[^v1-ensreg-l89]: (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L89 @ ens_v1@91c966f)
[^v1-ensreg-l174]: (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L174 @ ens_v1@91c966f)
[^v1-ensregfb-l40]: (upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L40 @ ens_v1@91c966f)
[^v1-resolver-signature-abis]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/IABIResolver.sol:L5 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/IAddrResolver.sol:L6 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/IAddressResolver.sol:L6 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/Resolver.sol:L33 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/IContentHashResolver.sol:L5 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/IDNSRecordResolver.sol:L6 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/IDNSRecordResolver.sol:L13 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/IDNSZoneResolver.sol:L6 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/IDataResolver.sol:L7 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/IInterfaceResolver.sol:L5 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/INameResolver.sol:L5 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/ITextResolver.sol:L5 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/IVersionableResolver.sol:L5 @ ens_v1@91c966f) (upstream: .refs/ens_subgraph/subgraph.yaml:L109 @ ens_subgraph@723f1b6)

[^v1-iname-l27]: (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L27 @ ens_v1@91c966f)
[^v1-iname-l35]: (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L35 @ ens_v1@91c966f)
[^v1-iname-l37]: (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L37 @ ens_v1@91c966f)
[^v1-iname-l38]: (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L38 @ ens_v1@91c966f)

[^v1-namewrapper-deploy]: (upstream: .refs/ens_v1/deployments/mainnet/NameWrapper.json:L1498 @ ens_v1@91c966f)
[^v1-publicresolver-deploy]: (upstream: .refs/ens_v1/deployments/mainnet/PublicResolver.json:L1104 @ ens_v1@91c966f)
[^v1-revreg-deploy]: (upstream: .refs/ens_v1/deployments/mainnet/ReverseRegistrar.json:L2 @ ens_v1@91c966f)
[^v1-revreg-deploy-l379]: (upstream: .refs/ens_v1/deployments/mainnet/ReverseRegistrar.json:L379 @ ens_v1@91c966f)
[^v1-l2rev-base-deploy]: (upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L2 @ ens_v1@91c966f)
[^v1-l2rev-base-args]: (upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L391 @ ens_v1@91c966f)
[^v1-l2rev-event]: (upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L98 @ ens_v1@91c966f)
[^v1-l2rev-nameforaddr]: (upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L154 @ ens_v1@91c966f)
[^v1-ur-deploy]: (upstream: .refs/ens_v1/deployments/mainnet/UniversalResolver.json:L2 @ ens_v1@91c966f)
[^v1-ursol-l8]: (upstream: .refs/ens_v1/contracts/universalResolver/UniversalResolver.sol:L8 @ ens_v1@91c966f)

[^v1-wrapethrc-l640]: (upstream: .refs/ens_v1/deployments/mainnet/WrappedETHRegistrarController.json:L640 @ ens_v1@91c966f)
[^v1-ethrc-l706]: (upstream: .refs/ens_v1/deployments/mainnet/ETHRegistrarController.json:L706 @ ens_v1@91c966f)

[^v1-revreg-l15]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L15 @ ens_v1@91c966f)
[^v1-revreg-l19]: (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L19 @ ens_v1@91c966f)

[^subgraph-l15]: (upstream: .refs/ens_subgraph/subgraph.yaml:L15 @ ens_subgraph@723f1b6)
[^subgraph-l39]: (upstream: .refs/ens_subgraph/subgraph.yaml:L39 @ ens_subgraph@723f1b6)
[^subgraph-l44]: (upstream: .refs/ens_subgraph/subgraph.yaml:L44 @ ens_subgraph@723f1b6)
[^subgraph-l122]: (upstream: .refs/ens_subgraph/subgraph.yaml:L122 @ ens_subgraph@723f1b6)
[^subgraph-l145]: (upstream: .refs/ens_subgraph/subgraph.yaml:L145 @ ens_subgraph@723f1b6)
[^subgraph-ts-l134]: (upstream: .refs/ens_subgraph/src/ensRegistry.ts:L134 @ ens_subgraph@723f1b6)
[^subgraph-ts-l230]: (upstream: .refs/ens_subgraph/src/ensRegistry.ts:L230 @ ens_subgraph@723f1b6)
[^subgraph-ts-l238]: (upstream: .refs/ens_subgraph/src/ensRegistry.ts:L238 @ ens_subgraph@723f1b6)
[^subgraph-ts-l246]: (upstream: .refs/ens_subgraph/src/ensRegistry.ts:L246 @ ens_subgraph@723f1b6)

[^v2-deploy-root]: (upstream: .refs/ens_v2/contracts/deployments/sepolia/RootRegistry.json:L2 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/deployments/sepolia/RootRegistry.json:L2792 @ ens_v2@ccaeb58)
[^v2-deploy-ethreg]: (upstream: .refs/ens_v2/contracts/deployments/sepolia/ETHRegistry.json:L2 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/deployments/sepolia/ETHRegistry.json:L2792 @ ens_v2@ccaeb58)
[^v2-deploy-ethrc]: (upstream: .refs/ens_v2/contracts/deployments/sepolia/ETHRegistrar.json:L2 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/deployments/sepolia/ETHRegistrar.json:L1372 @ ens_v2@ccaeb58)
[^v2-deploy-pres]: (upstream: .refs/ens_v2/contracts/deployments/sepolia/PermissionedResolverImpl.json:L2 @ ens_v2@ccaeb58)
[^v2-pres-uups]: (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L22 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L89 @ ens_v2@ccaeb58)
[^v2-pres-upgraded]: (upstream: .refs/ens_v2/contracts/deployments/sepolia/PermissionedResolverImpl.json:L627 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/deployments/sepolia/PermissionedResolverImpl.json:L637 @ ens_v2@ccaeb58)
[^v2-deploy-renewer]: (upstream: .refs/ens_v2/contracts/deployments/sepolia/ETHRenewerV1.json:L2 @ ens_v2@ccaeb58)
[^v2-deploy-public-resolver]: (upstream: .refs/ens_v2/contracts/deployments/sepolia/PublicResolverV2.json:L2 @ ens_v2@ccaeb58)
[^v2-public-resolver-discovery]: `PublicResolverV2` composes the standard resolver profiles and authorizes writes through registry ownership or approvals; locked-name migration can replace a recognized ENSv1 resolver with that public resolver before a nonzero registered resolver emits `ResolverUpdated`: (upstream: .refs/ens_v2/contracts/src/resolver/PublicResolverV2.sol:L4 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/resolver/PublicResolverV2.sol:L23 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/resolver/PublicResolverV2.sol:L179 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L139 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L474 @ ens_v2@ccaeb58)
[^v2-public-resolver-version]: The deployed resolver ABI includes `VersionChanged` and `clearRecords`: (upstream: .refs/ens_v2/contracts/deployments/sepolia/PublicResolverV2.json:L429 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/deployments/sepolia/PublicResolverV2.json:L598 @ ens_v2@ccaeb58)

[^v2-userreg-l15]: (upstream: .refs/ens_v2/contracts/deployments/sepolia/UserRegistryImpl.json:L2 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/registry/UserRegistry.sol:L15 @ ens_v2@ccaeb58)
[^v2-ethrc-l49]: (upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRegistrar.sol:L32 @ ens_v2@ccaeb58)
[^v2-ethrc-l173]: (upstream: .refs/ens_v2/contracts/src/registrar/ETHRegistrar.sol:L151 @ ens_v2@ccaeb58)

[^v2-pr-l22]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L23 @ ens_v2@ccaeb58)
[^v2-pr-l28]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L29 @ ens_v2@ccaeb58)

[^v2-pres-l38]: (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L33 @ ens_v2@ccaeb58)
[^v2-pres-l70]: (upstream: .refs/ens_v2/contracts/src/resolver/interfaces/IPermissionedResolver.sol:L19 @ ens_v2@ccaeb58)
[^v2-pres-data]: (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L46 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L161 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L437 @ ens_v2@ccaeb58)

[^v2-iperm-l22]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L23 @ ens_v2@ccaeb58)
[^v2-iperm-l34]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L38 @ ens_v2@ccaeb58)
[^v2-iperm-resolver-l14]: (upstream: .refs/ens_v2/contracts/src/resolver/interfaces/IPermissionedResolver.sol:L19 @ ens_v2@ccaeb58)
[^v2-iethreg-l32]: (upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRegistrar.sol:L32 @ ens_v2@ccaeb58)
[^v2-iethrenewer-l21]: (upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRenewer.sol:L21 @ ens_v2@ccaeb58)

[^v2-events-created]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L9 @ ens_v2@ccaeb58)
[^v2-events-l15]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L18 @ ens_v2@ccaeb58)
[^v2-events-l49]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L56 @ ens_v2@ccaeb58)
[^v2-events-l69]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L82 @ ens_v2@ccaeb58)
[^v2-events-l75]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L88 @ ens_v2@ccaeb58)
[^v2-events-uri]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L76 @ ens_v2@ccaeb58)

[^v2-eac-l19]: (upstream: .refs/ens_v2/contracts/src/access-control/interfaces/IEnhancedAccessControl.sol:L22 @ ens_v2@ccaeb58)

[^bn-readme-l22]: (upstream: .refs/basenames/README.md:L22 @ basenames@1809bbc)
[^bn-readme-l28]: (upstream: .refs/basenames/README.md:L28 @ basenames@1809bbc)
[^bn-readme-l29]: (upstream: .refs/basenames/README.md:L29 @ basenames@1809bbc)
[^bn-readme-l30]: (upstream: .refs/basenames/README.md:L30 @ basenames@1809bbc)
[^bn-readme-l33]: (upstream: .refs/basenames/README.md:L33 @ basenames@1809bbc)
[^bn-readme-l34]: (upstream: .refs/basenames/README.md:L34 @ basenames@1809bbc)
[^bn-readme-l36]: (upstream: .refs/basenames/README.md:L36 @ basenames@1809bbc)
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
[^bn-registry-l100]: (upstream: .refs/basenames/src/L2/Registry.sol:L100 @ basenames@1809bbc)
[^bn-registry-l113]: (upstream: .refs/basenames/src/L2/Registry.sol:L113 @ basenames@1809bbc)
[^bn-registry-l132]: (upstream: .refs/basenames/src/L2/Registry.sol:L132 @ basenames@1809bbc)
[^bn-registry-l223]: (upstream: .refs/basenames/src/L2/Registry.sol:L223 @ basenames@1809bbc)

[^bn-baseregistrar-l15]: (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L15 @ basenames@1809bbc)
[^bn-baseregistrar-l17]: (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L17 @ basenames@1809bbc)
[^bn-baseregistrar-l237]: (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L237 @ basenames@1809bbc)
[^bn-baseregistrar-l327]: (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L327 @ basenames@1809bbc)
[^bn-registrar-controller-l180]: (upstream: .refs/basenames/src/L2/RegistrarController.sol:L180 @ basenames@1809bbc)
[^bn-registrar-controller-l187]: (upstream: .refs/basenames/src/L2/RegistrarController.sol:L187 @ basenames@1809bbc)
[^bn-upgradeable-registrar-controller-l191]: (upstream: .refs/basenames/src/L2/UpgradeableRegistrarController.sol:L191 @ basenames@1809bbc)
[^bn-upgradeable-registrar-controller-l198]: (upstream: .refs/basenames/src/L2/UpgradeableRegistrarController.sol:L198 @ basenames@1809bbc)

[^bn-l2resolver-l4]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L4 @ basenames@1809bbc)
[^bn-l2resolver-l16]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L16 @ basenames@1809bbc)
[^bn-l2resolver-l22]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L22 @ basenames@1809bbc)
[^bn-l2resolver-l29]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L29 @ basenames@1809bbc)
[^bn-l2resolver-l49]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L49 @ basenames@1809bbc)
[^bn-l2resolver-l52]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L52 @ basenames@1809bbc)
[^bn-l2resolver-l182]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L182 @ basenames@1809bbc)
[^bn-l2resolver-l193]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L193 @ basenames@1809bbc)
[^bn-l2resolver-l209]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L209 @ basenames@1809bbc)
[^bn-l2resolver-l225]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L225 @ basenames@1809bbc)

[^bn-revreg-l12]: (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L12 @ basenames@1809bbc)
[^bn-revreg-l58]: (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L58 @ basenames@1809bbc)
[^bn-revreg-l150]: (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L150 @ basenames@1809bbc)
[^bn-revreg-l155]: (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L155 @ basenames@1809bbc)
[^bn-revreg-l156]: (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L156 @ basenames@1809bbc)
[^bn-revreg-l157]: (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L157 @ basenames@1809bbc)
[^bn-revreg-l193]: (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L193 @ basenames@1809bbc)
[^bn-revreg-l209]: (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L209 @ basenames@1809bbc)
[^bn-constants-l12]: (upstream: .refs/basenames/src/util/Constants.sol:L12 @ basenames@1809bbc)
[^bn-constants-l13]: (upstream: .refs/basenames/src/util/Constants.sol:L13 @ basenames@1809bbc)
[^bn-sha3-l15]: (upstream: .refs/basenames/src/lib/Sha3.sol:L15 @ basenames@1809bbc)
[^bn-sha3-l20]: (upstream: .refs/basenames/src/lib/Sha3.sol:L20 @ basenames@1809bbc)
[^bn-sha3-l31]: (upstream: .refs/basenames/src/lib/Sha3.sol:L31 @ basenames@1809bbc)
