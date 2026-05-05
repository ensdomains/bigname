# Source manifests

Manifests pin watched contracts, capability ownership, and rollout state per source family. They are part of the truth model, not deploy-time configuration. Architecture in [`architecture.md`](architecture.md), persistence in [`storage.md`](storage.md), intake in [`chain-intake.md`](chain-intake.md), execution in [`execution.md`](execution.md).

## File layout

```
manifests/<namespace>/<source_family>/<version>.toml
manifests-sepolia-dev/<namespace>/<source_family>/v1.toml
```

One runtime selects exactly one manifest root at startup. A runtime never loads two roots into the same canonical corpus, watch plan, discovery graph, or projection set.

TOML is chosen for deterministic diffs, hand editing, and straightforward Rust parsing.

## Required fields

| Field | Notes |
| --- | --- |
| `manifest_version` | integer; bump on schema-breaking changes |
| `namespace` | `ens` or `basenames` |
| `source_family` | from the source-family list |
| `chain` | e.g. `ethereum-mainnet`, `base-mainnet`, `ethereum-sepolia` |
| `deployment_epoch` | e.g. `ens_v1`, `ens_v2` |
| `rollout_status` | `draft` \| `shadow` \| `active` \| `deprecated` |
| `normalizer_version` | e.g. `uts46-v1` |
| `capability_flags` | per-capability `unsupported \| shadow \| supported` |
| `roots` | seed contracts for discovery |
| `contracts` | watched contracts |
| `discovery_rules` | edge admission rules |

`[[roots]]` and `[[contracts]]` may declare an optional `start_block`. Omitted means **unknown** — adapters preserve that state. Bootstrap doesn't infer zero, the current job range, the manifest activation height, or any other fallback.

`[[contracts]]` requires `proxy_kind`. `proxy_kind = "none"` omits `implementation`. Anything else requires `implementation`.

`[[discovery_rules]]` only authors `admission = "reachable_from_root"`. The discovered edge is authoritative while its `from_role` endpoint stays reachable from an active root under an allowed rule. `manifest_declared` and `manifest_successor` are storage tags, not authored values.

## Example shape

```toml
manifest_version = 1
namespace = "ens"
source_family = "ens_v2_registry_l1"
chain = "ethereum-mainnet"
deployment_epoch = "ens_v2"
rollout_status = "active"
normalizer_version = "uts46-v1"

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

[capability_flags]
declared_children = "supported"
```

## Capability ownership

Capability ownership attaches to the declaring `source_family`. It's never implied by another family's presence.

### ENS mainnet

| Family | Address | `start_block` | Notes |
| --- | --- | --- | --- |
| `ens_v1_registry_l1` (current) | `0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E` | `9380380` | Current ENSRegistry.[^subgraph-current] |
| `ens_v1_registry_l1` (old) | `0x314159265dd8dbb310642f98f50c066173c1259b` | `3327417` | Migration-aware input. Old- and current-registry logs aren't unioned by latest block: a current `NewOwner` marks the node migrated; later old-registry updates for that node are suppressed except for the root resolver.[^subgraph-old][^subgraph-handler] |
| `ens_v1_registrar_l1` | BaseRegistrar at `9380410` plus controllers | per controller | Legacy `9380471`,[^subgraph-legacy] Wrapped `16925618`,[^v1-wrapethrc] current `22764821`.[^v1-ethrc-deploy] Controllers don't split into a separate family. |
| `ens_v1_wrapper_l1` | `0xD4416b13d2b3a9aBae7AcD5D6C2BbDBE25686401` | `16925608` | NameWrapper authority, fuses, expiry.[^v1-namewrapper-deploy] |
| `ens_v1_resolver_l1` | seed `0xF29100983E058B709F3D539b0c765937B804AC15` | `22764828` | Latest PublicResolver. Plus admitted ENS Labs generations (table below).[^v1-publicresolver-deploy] |
| `ens_v1_reverse_l1` | `0xa58E81fe9b61B5c3fE2AFD33CF304c454AbFc7Cb` | `16925606` | Declared reverse-claim intake.[^v1-revreg-deploy] |
| `ens_execution` | Universal Resolver proxy `0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe` | — | Verified resolution. The pinned `.refs/` artifact is the implementation/ABI anchor; the route-facing entry is the proxy.[^ens-univ] `verified_resolution = "shadow"` records ownership; route-level support is gated separately. |

`ens_execution` also owns exact-tuple persisted `verified_primary_name` readback for ENS — no separate primary-name capability flag.

### Admitted ENS Labs PublicResolver generations

First-party app-known data:[^v1-app-resolvers]

| Address | Profile | Limitations |
| --- | --- | --- |
| `0xF291…AC15` | latest: address, multicoin, default coin-type fallback, name, ABI, text, contenthash, DNS, interface, name-wrapper-aware, VersionableResolver | no pubkey, no `DataResolver` |
| `0x231b…8E63` | as latest minus default coin-type fallback | as above |
| `0x4976…BA41` | address, multicoin, name, ABI, text, contenthash, DNS, interface | no name-wrapper, no fallback, no Versionable |
| `0xDaaF…dfBA` | same as `0x4976…` | same |
| `0x2261…68b8` | legacy: address, multicoin, name, ABI, text, contenthash, interface | no DNS, no name-wrapper, no Versionable |
| `0x5FfC…4cc4` | older legacy: ETH-address, name, ABI, text, interface | no multicoin, contenthash, DNS, name-wrapper |
| `0x1da0…6aB5` | oldest legacy: ETH-address, name, ABI, interface | no text, contenthash, multicoin, DNS |

Older rows don't inherit latest-only behavior. Unsupported interfaces and pending profiles surface explicitly through `coverage`, `UnsupportedSummary`, `resolver_family_pending`, or `resolver_family_unsupported`. They're never reported as absent records.

Address-specific resolver `start_block`s come from ENSNode datasource pins where available: `0x1da0…` `3648359`, `0x5FfC…` `3733668`, `0x2261…` `8659893`, `0x4976…` `9412610`, `0x231b…` `16925619`.[^ensnode-mainnet] `0xDaaF…` has no pinned datasource — uses the current ENSRegistry epoch `9380380` as a conservative basis.

`PubkeyChanged` is ignored by the current admission model. `DataResolver`-shaped events are unsupported on admitted generations and `pending` on unknown profiles. Generic `resolver_record` is an observation bucket — not a catch-all for unknown families.

### ENSv2 (`sepolia-dev` profile)

Admits four families under `manifests-sepolia-dev/ens/`:[^v2-deploy]

| Family | Address | `start_block` | Role |
| --- | --- | --- | --- |
| `ens_v2_root_l1` | `0x3a3e…3ed3` | `10462881` | RootRegistry seed. |
| `ens_v2_registry_l1` | `0x796f…d080` | `10462895` | ETHRegistry plus discovered UserRegistry instances. `UserRegistryImpl` at `0xea93…f702` is implementation metadata. |
| `ens_v2_registrar_l1` | `0x6858…c532` | `10462909` | ETHRegistrar. |
| `ens_v2_resolver_l1` | discovered via manifest/edges | — | PermissionedResolver. `PermissionedResolverImpl` at `0xe566…cb9c` is the initial implementation artifact. |

Exact-name profile promotion is profile-scoped: only `exact_name_profile = "supported"` on `ens_v2_registrar_l1` in the `sepolia-dev` root graduates `.eth` exact-name declared reads. The promotion doesn't apply to mainnet, other Sepolia profiles, or any runtime that hasn't selected `sepolia-dev`.

Upstream → adapter event mapping:

| Upstream | Adapter |
| --- | --- |
| `TokenResource` | `TokenResourceLinked` |
| `TokenRegenerated` | `TokenRegenerated` |
| `SubregistryUpdated` | `SubregistryChanged` |
| `ParentUpdated` | `ParentChanged` |
| `AliasChanged` | `AliasChanged` |
| `EACRolesChanged` | resource-, root-, or resolver-scoped permission events |

These are adapter semantics, not manifest schema fields.

Other `sepolia-dev` artifacts (`UniversalResolverV2`, `ReverseRegistry`, `DNSAliasResolver`, `WrapperRegistryImpl`, `LockedMigrationController`, `HCAFactory`, `StandardRentPriceOracle`, `BatchRegistrar`, `MockUSDC`, `MockDAI`) stay outside admission until a doc-first update.

### Basenames mainnet

Six families:[^bn-readme]

| Family | Address | Chain | Role |
| --- | --- | --- | --- |
| `basenames_base_registry` | `0xb947…5a95` | Base | Per-node owner/resolver/ttl state. |
| `basenames_base_registrar` | `0x03c4…dd9a` | Base | Tokenized authority owning `base.eth`, minting `*.base.eth` subdomains. |
| `basenames_base_resolver` | `0xC6d5…3BCD` | Base | Default `L2Resolver` profile seed. |
| `basenames_base_primary` | `0x79ea…e282` | Base | Reverse Registrar — declared primary-claim intake only. |
| `basenames_l1_compat` | `0xde90…F31` | Ethereum | L1 compatibility transport for `base.eth`. |
| `basenames_execution` | `0xde90…F31` | Ethereum | Verified resolution — exact-surface transport-assisted direct path only. |

The L1 Resolver address appears in both `basenames_l1_compat` and `basenames_execution`. Transport ownership stays with `basenames_l1_compat`; verified-resolution routing stays with `basenames_execution`.

`basenames_execution` v2 promotes only the exact-surface transport-assisted direct path: `resolver_path[0].logical_name_id` equals the route surface, `wildcard.source = null`, `alias.final_target = null`, `subregistry_path = []`, transport from `base-mainnet` to `ethereum-mainnet` through that L1 Resolver. Alias-participating, wildcard-derived, linked-subregistry, transport-free, and offchain-gateway classes return selector-local `unsupported`.

`verified_primary_name` for Basenames runs through `basenames_execution` under the same flag. The matching `primary_names_current(address, coin_type, namespace)` row is the only claim-side anchor; `verified_primary_name.provenance` carries `{execution_trace_id, manifest_versions}` matching the top-level `execution_trace_id`.

Basenames Base-side resolver discovery from registry `NewResolver` admits resolver instances and updates bindings. Resolver-local fact consumption requires `L2Resolver`-compatible profile admission. The discovery rule doesn't discover the L1 Resolver and doesn't admit offchain gateways.[^bn-l2resolver]

`basenames_offchain` is reserved for later gateway admission. Not part of the current split.

## Contract instance admission and continuity

Manifest loading admits source-graph nodes as `contract_instance_id`s, not raw addresses. Each active `[[roots]]` and `[[contracts]]` entry resolves to one admitted instance.

- `[[roots]]` seed canonical graph and watch-plan expansion; otherwise they follow the same identity rules as `[[contracts]]`.
- Reusing the same address on the same chain across manifest versions, even across an inactive gap, carries the existing `contract_instance_id` forward and appends a non-overlapping active range.
- Changing a declared address closes the prior active range and admits a new instance. Continuity uses a `migration` edge, not ID reuse.
- `proxy_kind = "none"` resolves the declared address directly.
- `proxy_kind != "none"` requires `implementation`. Proxy and implementation are separate instances linked by a time-ranged proxy/implementation edge.
- Changing only `implementation` keeps the proxy's identity. The implementation instance is reused if its address reappears, otherwise minted.

Contract addresses persist as time-ranged attributes for raw-fact matching and watch-plan expansion.

## Discovery admission

A discovered contract is authoritative when one of these holds:

- declared directly in an active manifest
- reachable from an active manifest root through an allowed `discovery_rules` edge
- explicitly allow-listed for a migration epoch

Each admitted edge stores `from_contract_instance_id`, `to_contract_instance_id`, source manifest version, edge kind, discovery source, active range, provenance.

Discovery resolves `(chain, address, point in time)` to endpoint `contract_instance_id`s before storing the edge. Re-admitting an address previously admitted on the same chain reuses the prior `contract_instance_id` and appends a new range; a new id is minted only for addresses never admitted on that chain. Manifest-declared and discovered proxy/implementation links share the same edge and active-range rules.

## Manifest change propagation

Manifest changes produce normalized events: `SourceManifestUpdated`, `ProxyImplementationChanged`, `CapabilityChanged`. They update discovery admission, invalidate execution cache entries, and trigger projection recomputation where capability boundaries change.

Live manifest drift and proxy-upgrade alerting is a worker-owned operational loop. The worker computes drift candidates from admitted manifests, code-hash facts, proxy/implementation edges, and watch-plan state, and persists them to the worker-owned alert observation family. The worker doesn't write `normalized_events`, mutate manifests, mutate discovery admission, change capability flags, write projections, or expose a public route. Remediation is an explicit manifest or discovery change that produces the normal events above.

| Command | Behavior |
| --- | --- |
| `bigname-worker manifest-drift audit --json` | computes candidates, persists alert observations, renders the persisted view alongside live counts |
| `bigname-worker manifest-drift audit --fail-on-alert --json` | nonzero exit when actionable persisted alerts remain |
| `bigname-worker inspect manifest-drift --json` | read-only over already persisted observations |

## Watch-plan expansion

Watch-plan expansion starts from active manifest roots by `contract_instance_id` and traverses active discovery edges by id.

- The chain-intake watch target is the address range attached to each active contract instance at the requested time.
- If a manifest target carries `start_block`, the materialized watch range starts there unless a later active-range boundary narrows it.
- If `start_block` is omitted, the historical start is unknown. Live watch may still produce a target; automatic historical bootstrap treats it as unbootstrapable until a finite start is declared.
- Watch rows may denormalize address and code-hash state, but their durable explanation path is `manifest root → discovery edge(s) → contract_instance_id`.
- Address-only watch state is rebuildable from manifests, instance attributes, and active discovery edges.

`bigname-worker inspect watch-plan --json` exposes active watched contracts with source kind (`manifest_root`, `manifest_contract`, `discovery_edge`), source families, contract instance IDs, chain addresses, source manifest IDs, and active block ranges. Read-only over existing state.

## Capability policy

Capabilities gate behavior, not public-contract existence. An unsupported capability surfaces as `coverage.unsupported_reason` or a typed error. Shadow capabilities write facts and traces without enabling general reads. Adding a new capability is additive only when it doesn't change prior semantics.

## Bootstrap `start_block` provenance

Known historical starts cite a pinned upstream source. Targets without a pinned source omit `start_block`; automatic bootstrap skips them rather than inventing values. Basenames mainnet families and the ENS Universal Resolver remain unknown.

| Target | `start_block` | Source |
| --- | --- | --- |
| ENSv1 ENSRegistry | `9380380` | [^subgraph-current] |
| ENSv1 ENSRegistryOld | `3327417` | [^subgraph-old] |
| ENSv1 BaseRegistrar | `9380410` | [^subgraph-baseregistrar] |
| LegacyEthRegistrarController | `9380471` | [^subgraph-legacy] |
| WrappedETHRegistrarController | `16925618` | [^v1-wrapethrc] |
| ETHRegistrarController | `22764821` | [^v1-ethrc-deploy] |
| ENSv1 NameWrapper | `16925608` | [^v1-namewrapper-deploy] |
| ENSv1 PublicResolver (latest) | `22764828` | [^v1-publicresolver-deploy] |
| ENSv1 ReverseRegistrar | `16925606` | [^v1-revreg-deploy] |
| ENSv2 RootRegistry (`sepolia-dev`) | `10462881` | [^v2-deploy] |
| ENSv2 ETHRegistry (`sepolia-dev`) | `10462895` | [^v2-deploy] |
| ENSv2 ETHRegistrar (`sepolia-dev`) | `10462909` | [^v2-deploy] |

## Ownership

- Manifest/discovery owners maintain the TOML files.
- Adapter owners consume manifest versions as inputs.
- Execution owners depend on manifest versions for cache keys and invalidation.
- Schema changes require a doc-first update to this file.

---

[^subgraph-current]: (upstream: .refs/ens_subgraph/subgraph.yaml:L15 @ ens_subgraph@723f1b6)
[^subgraph-old]: (upstream: .refs/ens_subgraph/subgraph.yaml:L39 @ ens_subgraph@723f1b6)
[^subgraph-handler]: (upstream: .refs/ens_subgraph/src/ensRegistry.ts:L238 @ ens_subgraph@723f1b6)
[^subgraph-baseregistrar]: (upstream: .refs/ens_subgraph/subgraph.yaml:L122 @ ens_subgraph@723f1b6)
[^subgraph-legacy]: (upstream: .refs/ens_subgraph/subgraph.yaml:L145 @ ens_subgraph@723f1b6)
[^v1-wrapethrc]: (upstream: .refs/ens_v1/deployments/mainnet/WrappedETHRegistrarController.json:L640 @ ens_v1@91c966f)
[^v1-ethrc-deploy]: (upstream: .refs/ens_v1/deployments/mainnet/ETHRegistrarController.json:L706 @ ens_v1@91c966f)
[^v1-namewrapper-deploy]: (upstream: .refs/ens_v1/deployments/mainnet/NameWrapper.json:L1498 @ ens_v1@91c966f)
[^v1-publicresolver-deploy]: (upstream: .refs/ens_v1/deployments/mainnet/PublicResolver.json:L1104 @ ens_v1@91c966f)
[^v1-revreg-deploy]: (upstream: .refs/ens_v1/deployments/mainnet/ReverseRegistrar.json:L379 @ ens_v1@91c966f)
[^v1-app-resolvers]: (upstream: .refs/ens_app_v3/src/constants/resolverAddressData.ts:L32 @ ens_app_v3@7175858)
[^ensnode-mainnet]: (upstream: .refs/ensnode/packages/datasources/src/mainnet.ts:L343 @ ensnode@9b8f590)
[^ens-univ]: <https://docs.ens.domains/resolvers/universal/>
[^v2-deploy]: (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/RootRegistry.json:L2 @ ens_v2@554c309)
[^bn-readme]: (upstream: .refs/basenames/README.md:L28 @ basenames@1809bbc)
[^bn-l2resolver]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L22 @ basenames@1809bbc)
