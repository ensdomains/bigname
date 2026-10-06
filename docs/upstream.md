# Upstream references

bigname anchors every ENSv1, ENSv2, Basenames, admitted upstream app-metadata, reference-indexer comparison, and reference execution-client comparison claim to a specific upstream commit pinned under `.refs/`. This doc is the human-readable companion to `.refs/MANIFEST.toml` — the pin table, rotation policy, and the intentional-divergence list.

## Pinned refs

| Key | Repo | Commit | Purpose |
|-----|------|--------|---------|
| `ens_v1` | `ensdomains/ens-contracts` | `91c966fe` | Canonical ENSv1 Solidity |
| `ens_v1_mainnet_1a2ac5c` | `ensdomains/ens-contracts` | `1a2ac5cb` | Historical deployment ABI for the admitted `0x231b0Ee…` Mainnet PublicResolver only |
| `ens_v1_sepolia_8209157` | `ensdomains/ens-contracts` | `82091575` | Historical deployment ABI for the admitted `0x8948458…` Sepolia PublicResolver only |
| `ens_v1_sepolia_ac32490` | `ensdomains/ens-contracts` | `ac324904` | Historical deployment ABI for the admitted `0x8FADE66…` Sepolia PublicResolver only |
| `ens_v1_lll` | `ensdomains/ens` | `7e377df8` | Historical evidence for the 2017 LLL registry only |
| `ens_v2` | `ensdomains/contracts-v2` | `a971bd64` | Post-audit ENSv2 contracts and pinned Sepolia deployment evidence |
| `ens_v2_sepolia_20261001` | `ensdomains/contracts-v2` | `07e55a05` | Official 2026-10-01 Sepolia redeploy artifacts, receipts, compiler inputs and matching source; current Sepolia manifest authority |
| `ens_v2_sepolia_20260916` | `ensdomains/contracts-v2` | `366de741` | Superseded 2026-09-15 Sepolia deployment; ENSv2 Solidity source evidence only where the cited lines are unchanged in the 2026-10-01 redeploy, and evidence of the dropped deployment's own history |
| `ens_v2_sepolia_20260903` | `ensdomains/contracts-v2` | `5da83f6a` | Record-ID PermissionedResolver and direct PublicResolverV2 source evidence; not deployment-address authority |
| `ens_v1_publicresolver_5141a2a` | `ensdomains/ens-contracts` | `5141a2ac` | Inherited PublicResolverV2 node-record source evidence only |
| `ens_v2_sepolia_20260629` | `ensdomains/contracts-v2` | `ccaeb58b` | Historical implementation evidence for the admitted 2026-06-29 old-model Sepolia deployment only |
| `ens_v2_sepolia_dev` | `ensdomains/contracts-v2` | `554c309b` | Historical evidence cited by deprecated pre-audit `sepolia-dev` manifests only |
| `basenames` | `base-org/basenames` | `1809bbc9` | Canonical Basenames Solidity |
| `zigens` | `ensdomains/zigens` | `77d106e9` | Reference indexer registry assignment counts and label holder counts only |
| `ens_subgraph` | `ensdomains/ens-subgraph` | `723f1b6a` | Reference ENSv1 indexer |
| `ens_rainbow` | `graphprotocol/ens-rainbow` | `bc44492` | Graph Protocol ENS rainbow-table tooling |
| `ensnode` | `namehash/ensnode` | `2017ae62` | Alternative ENS indexer |
| `ens_app_v3` | `ensdomains/ens-app-v3` | `71758582` | ENS app known resolver metadata |
| `ponder` | `ponder-sh/ponder` | `c8f6935f` | Reference EVM indexer framework |
| `graph_node` | `graphprotocol/graph-node` | `aefe1737` | Reference Graph Node indexer |
| `reth` | `paradigmxyz/reth` | `189c0df3` | Reference Ethereum execution client |

The `zigens` checkout is optional for builds and tests. Fetch or verify it with
`scripts/sync-refs --include-optional` (add `--check` to verify), using credentials
that can access its repository. Its pin and citations remain required when
changing the associated comparisons.

The `zigens` pin is reference-indexer evidence only, not protocol or deployment
address authority. Its registry count counts assignment rows across resources,
while its label count counts distinct accounts on the latest observed resource
version and excludes empty bitmaps and root roles.
(upstream: .refs/zigens/src/api/resolvers/admin.zig:L1150 @ zigens@77d106e9)
(upstream: .refs/zigens/src/storage/roles.zig:L505 @ zigens@77d106e9)
(upstream: .refs/zigens/src/storage/roles.zig:L567 @ zigens@77d106e9)

Full pin records (including per-ref `authoritative_for` lists) live in `.refs/MANIFEST.toml`. Sync with `scripts/sync-refs`.

`ens_v2` remains the general ENSv2 semantic and historical deployment authority.
`ens_v2_sepolia_20261001` supplies the current official Sepolia deployment
authority; see [the complete inventory](sepolia-deployment.md). It pins the
contracts-v2 commit that wrote the 2026-10-01 redeploy's `deployments/sepolia/`
and archived the previous set as `deployments/sepolia-20260915-r1/`. The
`ens_v2_sepolia_20260916` checkout is no longer deployment authority: the
manifests drop its deployment, and docs and code comments keep citing it only
for ENSv2 Solidity whose cited lines are unchanged at the new pin and for the
dropped deployment's own history. Deployment facts about the admitted set
(addresses, receipts, constructor arguments, deploy scripts) cite the new pin. The
`ens_v2_sepolia_20260903` pin supplies the record-ID resolver's matching
`PermissionedResolver`, `AbstractRecordResolver`, resolver interfaces and
`PermissionedResolverLib` sources. It does not establish correspondence of the
entire checkout to a deployed build; in particular its EnhancedAccessControl
dependency must not substitute for exact deployment compiler input. Its record
selection is defined by the resolver's `_record` implementation
(upstream: .refs/ens_v2_sepolia_20260903/contracts/src/resolver/PermissionedResolver.sol:L381 @ ens_v2_sepolia_20260903@5da83f6a).
The same pin also supplies direct PublicResolverV2 composition and authorization
source evidence. `ens_v1_publicresolver_5141a2a` supplies its inherited node-record
behavior from the pinned ENS submodule. These sources do not establish a
public-chain deployment or rotate either canonical authority.
The `ens_v2_sepolia_20260629` checkout retains implementation evidence for the
admitted 2026-06-29 old-model Sepolia families only where the archived ABI does
not prove the claim; it is not authority for a future deployment or current
ENSv2 source semantics. That generation's resolver `AliasChanged` event
(upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/interfaces/IPermissionedResolver.sol:L19-L24 @ ens_v2_sepolia_20260629@ccaeb58)
is no longer interpreted: no manifest declares it, and the official Sepolia
`PermissionedResolver` ABI has no such event, sharing records between names
through `Linked` record IDs instead
(upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/PermissionedResolverImpl.json:L3-L1313 @ ens_v2_sepolia_20261001@07e55a05).
The `ens_v2_sepolia_dev` checkout is retained only so deprecated manifest versions
keep their embedded ABI, behavior, address, and range evidence verifiable after
upstream removed the `sepolia-dev` artifact directory; it must not support new
behavior claims or active admission.

`ens_v1` is the sole current ENSv1 semantic authority. The
`ens_v1_mainnet_1a2ac5c`, `ens_v1_sepolia_8209157`, and
`ens_v1_sepolia_ac32490` checkouts retain only the deployment ABI evidence for
their named admitted PublicResolver generations; they are not authority for
current ENSv1 semantics or any other deployment. The `ens_v1_lll` checkout is
retained only so the 2017 LLL registry's unmasked-word rules in
`docs/architecture.md` and the divergence entry below keep a pinned source
citation; it must not support any other behavior claim. The pin is upstream's
`mainnet` tag (2017-04-30): its `contracts/ENS.lll` is identical to the file's
last pre-deployment edit, and the tag's committed `contracts/ENS.lll.bin`
runtime is byte-identical to the code deployed at
`0x314159265dd8dbb310642f98f50c066173c1259b` (the address its `ensutils.js`
binds `(upstream: .refs/ens_v1_lll/ensutils.js:L213 @ ens_v1_lll@7e377df)`).

The Reth reference now matches the direct reader dependency at v2.5.0. This
rotation preserves the existing receipt-retention and completeness rules; the
only observed change at their cited anchors is the RPC pruned-history error
carrying range details. Sepolia intake selects the built-in Sepolia chainspec
instead of the Mainnet spec. The monitored read-only factory continues to
refresh all storage providers with committed MDBX state
(upstream: .refs/reth/crates/chainspec/src/spec.rs:L146 @ reth@189c0df3)
(upstream: .refs/reth/crates/rpc/rpc/src/eth/filter.rs:L599 @ reth@189c0df3)
(upstream: .refs/reth/crates/storage/provider/src/providers/database/mod.rs:L292 @ reth@189c0df3).
See [Direct Reth reader](reth-db-reader.md) for the mount contract and bounded
read-only validation command. The rotation also rotates the
[interpreter content hash](glossary.md#interpreter-content-hash): Reth v2.5.0
requires the seven fingerprinted Alloy crates at 1.7.3 instead of 1.5.7, and the
accompanying Rust 1.98 update edits the hashed
`crates/interpret/src/recompute.rs`. An existing deployment needs the
full-history Interpret and Project redo described under
[interpretation replay](storage.md#interpretation-replay) before it serves with
this build.

## Citation format

```
(upstream: .refs/<key>/<path>:L<line> @ <key>@<short-commit>)
```

Use this exact shape everywhere — docs, ADRs, manifests, code comments, task writeups, agent output. Consistent format lets `$upstream-evidence`, `evidence_reader`, and `verification_reviewer` verify citations mechanically.

## Rotation policy

- **Bump when**: a cited upstream file changes materially, or we need to adopt a new upstream behavior (new contract, new event, new invariant). Staying behind upstream is fine — being silently wrong is not.
- **Do not bump for**: drive-by upstream refactors, test-only changes, comment edits, rename-only commits.
- **How to bump**:
  1. Update the `commit` field in `.refs/MANIFEST.toml`.
  2. Update the row in the table above.
  3. Run `scripts/sync-refs`.
  4. Re-grep the repo for `@ <key>@<old-short-commit>` citations; update any that point at content that changed across the bump.
  5. Add or edit entries in § Known divergences if the bump surfaced or resolved an intentional deviation.
  6. Commit with a message naming what upstream change motivated the bump, e.g. `chore(refs): bump ens_v1 to <new-sha> — adopt new reverseClaimer event`.
- **Who decides**: whoever owns the surface affected. Ambiguous cases route through `$contract-impact`; cross-surface bumps route through `verification_reviewer` after the sync.

### Rotation records

| Date | Ref | Pin change | Baseline census | Dispositions | Verification | Follow-up |
|---|---|---|---|---|---|---|
| 2026-08-27 | `ens_v2` | `ccaeb58` → `a971bd64` | 829 old-tag occurrences; 286 same-line stable; 543 changed | 286 tag refresh; 251 line re-anchor; 91 archive re-point; 201 deprecated/historical; 0 stale | `scripts/check-upstream-rotation verify-pr …`; baseline `e1833238e0c502ed744e24d5feaaffde7b858c74`; dispositions SHA-256 `b7ab2dc7d8f8f047615c62f390277cafc5fd3d977820335e6d678947f04be14f` | #566; #565 owns re-initialization admission and actual old-family deprecation |

This citation-only rotation changes the [interpreter content
hash](glossary.md#interpreter-content-hash) from
`keccak256:b65b9b0b6ffdfb7ce57a4c5c2c0c8cbe4cbe9d781d429cc038a949d720dcb27d`
to `keccak256:1216e2caa1e43be1453b0636e8087ac3bd93f8bb3b687c442acc6373d7ceeb40`
and the Sepolia manifest-profile hash from
`keccak256:dd573c2fce171a664a4bd3b6b271e32d401f3ec0d2b42659c0b5d696331d13b9`
to `keccak256:62100f3bc842bd35d49cb77a7e1c55e5ad176b16580868863b0ff03764de2047`;
the Mainnet manifest-profile hash is unchanged. The rotation does not widen a
[watch plan](glossary.md#watch-plan--watched-tuple) and requires no historical
ingest fetch. It does require the
[full-history Interpret→Project walk](storage.md#interpretation-replay):
after merge, record the new interpreter content hash as the walk's starting
hash, deploy the matching phase runner, complete and publish that walk, and
only then deploy the matching API as required by the
[deployment order](deployment.md#replacing-an-initialized-phase-schema).

On 2026-09-24 the
[ENSv1 mirror resolver](glossary.md#ensv1-mirror-resolver-ensv1_mirror_resolver)
citations in `projections.md`, `glossary.md`, `manifests.md`, and the Project
and history mirror code moved from `ens_v2@a971bd64` to
`ens_v2_sepolia_20260916@366de741`, the source of the deployed Sepolia mirror.
No pin changed. The cited content did change: at `a971bd64` `_findResolver`
returns the registry walk's result as it is, while at `366de741` it keeps a
resolver found above the queried node only when that resolver supports
`IExtendedResolver`
(upstream: .refs/ens_v2/contracts/src/resolver/ENSV1Resolver.sol:L39-L41 @ ens_v2@a971bd64)
(upstream: .refs/ens_v2_sepolia_20260916/contracts/src/resolver/ENSV1Resolver.sol:L40-L43 @ ens_v2_sepolia_20260916@366de741).
Project now follows the newer behaviour and never derives a mirrored name
through an ancestor's resolver
([`projections.md`](projections.md#resolver-and-records)). That change to
`crates/project/src` rotates the
[interpreter content hash](glossary.md#interpreter-content-hash); the rollout is
under [ENSv1 mirror ancestor gate](deployment.md#ensv1-mirror-ancestor-gate).

## Known divergences

### Product history actions and legacy ETH-address pairs

History is a read interpretation of retained events, with a closed `data.action` and the
scope rules in [history payloads](api-v1-routes.md#history-event-payloads-includedata-includeraw).
The public vocabulary is bigname's contract; it is not an upstream event catalogue.
Wrapping and unwrapping have explicit events, including a zero unwrap destination during
replacement (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L883-L902 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1022-L1031 @ ens_v1@91c966f).
We preserve the wrap action and select its derived transfer only when two different nonzero
owners are proved. Owner-wide approvals remain one account event, without per-name fanout
(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L108-L117 @ ens_v1@91c966f).
Reservation, record linking and token regeneration remain distinct from registration, pointer
writes and transfer (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/interfaces/IRegistryEvents.sol:L11-L38 @ ens_v2_sepolia_20261001@07e55a05)
(upstream: .refs/ens_v2_sepolia_20261001/contracts/src/resolver/interfaces/IRecordResolver.sol:L33-L38 @ ens_v2_sepolia_20261001@07e55a05)
(upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/interfaces/IRegistryEvents.sol:L78-L82 @ ens_v2_sepolia_20261001@07e55a05).
ReverseClaimed alone does not write a name; even bounded companion-name evidence retains
the `reverse_claimed` action (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L74-L85 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L123-L131 @ ens_v1@91c966f).

For proven deployed resolver generations only, one ETH setter emits adjacent AddressChanged
then AddrChanged. Public history keeps AddressChanged as the representative before paging
and counting; raw/normalized records stay unchanged. Both overloads delegate to that setter
(upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L26-L31 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L47-L65 @ ens_v1@91c966f).
The rule requires matching physical transaction/log/fork positions, emitter, node, resource
and values, plus exact declaration/source proof. Distinct repeated writes and unproved
custom emitters stay separate.

| Proven deployment | Pair value rule and source binding |
| --- | --- |
| Mainnet current PublicResolver `0xf29100983e058b709f3d539b0c765937b804ac15`; Sepolia current PublicResolver `0xe99638b40e4fff0129d56f03b55b6bbc4bbe49b5` | 20 bytes or modern empty clear. Deployment artifacts bind the current setter: (upstream: .refs/ens_v1/deployments/mainnet/PublicResolver.json:L2 @ ens_v1@91c966f) (upstream: .refs/ens_v1/deployments/sepolia/PublicResolver.json:L2 @ ens_v1@91c966f). |
| Mainnet `0x231b0ee14048e9dccd1d247744d114a4eb5e8e63` | Exactly 20 bytes. The historical reference also supports deployment-linked setter evidence, in addition to its approval ABI purpose: (upstream: .refs/ens_v1_mainnet_1a2ac5c/deployments/mainnet/PublicResolver.json:L2 @ ens_v1_mainnet_1a2ac5c@1a2ac5c) (upstream: .refs/ens_v1_mainnet_1a2ac5c/contracts/resolvers/profiles/AddrResolver.sol:L45-L54 @ ens_v1_mainnet_1a2ac5c@1a2ac5c). |
| Sepolia `0x8948458626811dd0c23eb25cc74291247077cc51` and `0x8fade66b79cc9f707ab26799354482eb93a5b7dd` | Exactly 20 bytes. These historical references likewise provide deployment-linked setter proof: (upstream: .refs/ens_v1_sepolia_8209157/deployments/sepolia/PublicResolver.json:L2 @ ens_v1_sepolia_8209157@8209157) (upstream: .refs/ens_v1_sepolia_8209157/contracts/resolvers/profiles/AddrResolver.sol:L45-L54 @ ens_v1_sepolia_8209157@8209157) (upstream: .refs/ens_v1_sepolia_ac32490/deployments/sepolia/PublicResolver.json:L2 @ ens_v1_sepolia_ac32490@ac32490) (upstream: .refs/ens_v1_sepolia_ac32490/contracts/resolvers/profiles/AddrResolver.sol:L45-L54 @ ens_v1_sepolia_ac32490@ac32490). |
| Basenames legacy L2Resolver `0xc6d566a56a1aff6508b41f6c90ff131615583bcd` | Exactly 20 bytes; this admitted legacy resolver inherits the vendored ENS profile, not the separate upgradeable empty-capable profile: (upstream: .refs/basenames/test/Fork/BaseMainnetConstants.sol:L9-L14 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L4-L5 @ basenames@1809bbc) (upstream: .refs/basenames/lib/ens-contracts/contracts/resolvers/profiles/AddrResolver.sol:L45-L54 @ basenames@1809bbc) (upstream: .refs/basenames/lib/ens-contracts/contracts/resolvers/profiles/AddrResolver.sol:L73-L76 @ basenames@1809bbc). |
| Official Sepolia PublicResolverV2 `0xdc4a563d00f5c3012b699794eb9e13a561be386f` | 20 bytes or modern empty clear. Exact deployed build inputs bind the inherited setter: (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/PublicResolverV2.json:L2 @ ens_v2_sepolia_20261001@07e55a05) (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/build-info/solc-0_8_25-b30e6dc9a03b37f6a0b89af5d02a73d3993944f7.json:L852-L854 @ ens_v2_sepolia_20261001@07e55a05). |

Other historical Mainnet declarations (`public_resolver_4976fb03`, `public_resolver_daaf96c3`,
`public_resolver_226159d5`, `public_resolver_5ffc0143`, `public_resolver_1da02271`) and Sepolia
`public_resolver_0ceec52` lack the required exact deployment-to-setter proof in this slice.
They remain uncollapsed. Source-family or ABI matching alone never widens this coverage.

The old-to-current registry handoff subtype is also bigname's interpretation: fallback reads
use the old registry until the current registry has a record
(upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L29-L34 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L48-L54 @ ens_v1@91c966f).
The API enriches only the first original readable direct current ownership row with an earlier
readable old-registry witness and no earlier ownership-triggered fallback-clear witness. A row
whose original authority fields were removed by registration or migration reconciliation keeps
its ordinary action, even if it might have been the first physical write. Later owner rows do not
inherit that marker. This conservative omission preserves the remaining rows, IDs, ordering,
membership and counts; it does not promise reconstruction of an erased ownership representative.
No event is created from a resolver clear or diagnostic block number, and no ENSv1→ENSv2
migration correlation or path changes.

> **Labels whose subregistry is their own registry: alias subtree not modelled** —
> a registry can set one of its labels' subregistry to the registry itself, for
> example when a registrant passes the `ETHRegistry` as the subregistry to
> `ETHRegistrar.register`. Resolution then walks a name below that label back
> into the same registry, reading the parent's own entries, so `x.label.eth`
> reads `x`'s entry and the subtree aliases the parent's children.
> **Upstream**: (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registrar/ETHRegistrar.sol:L151-L158 @ ens_v2_sepolia_20261001@07e55a05)
> (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/universalResolver/libraries/LibResolution.sol:L63-L84 @ ens_v2_sepolia_20261001@07e55a05)
> (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/universalResolver/libraries/LibResolution.sol:L127-L160 @ ens_v2_sepolia_20261001@07e55a05)
> **Our rule / why**: the self-pointer gives the label no canonical registry and
> no new canonical suffix. Interpret closes the label's previous `subregistry`
> edge, opens none and models no alias subtree through it; the
> `SubregistryChanged` normalized event keeps the pointer. See
> [discovery admission](manifests.md#discovery-admission).
> **Since**: `2026-10-04`

> **Registry count comparison after full revocation of a newer resource version** —
> the reference indexer deletes fully revoked assignment rows and determines the
> newest observed version from remaining rows.
> **Reference**: (upstream: .refs/zigens/src/indexer/v2/handlers_registry.zig:L1142 @ zigens@77d106e9)
> (upstream: .refs/zigens/src/storage/roles.zig:L171 @ zigens@77d106e9)
> **Our rule / why**: registry counts retain zero transitions while finding the
> newest observed resource version, then exclude zero assignments. Revocation
> cannot restore counts from an older version. Label counts additionally select
> the current registration resource. This preserves assignment-versus-holder
> meaning without using a deleted row as evidence that an old version is current.
> **Since**: `2026-09-14`

> **Numeric ENSv1 expiry representation** — admitted BaseRegistrar numeric registration and renewal, and the admitted ENSv1 .eth controller events that repeat the same expiry, retain expiry above the signed timestamp range as `i64::MAX`. Grace overflow does not release that retained lease; raw logs keep the original word. This does not narrow the on-chain event or change the exact Graveyard cleanup predicate.
> **Upstream**: BaseRegistrar emits and stores uint256 registration and renewal expiry. (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L130-L168 @ ens_v1@91c966f) The .eth controller's `NameRegistered` and `NameRenewed` carry the same value as `uint256 expires`. (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L116-L124 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L133-L139 @ ens_v1@91c966f)
> **Our rule / why**: See [storage semantics](storage.md). The adapter keeps its signed timestamp representation without failing an otherwise valid numeric lifecycle observation. The ENSv1 .eth controller-event decoder saturates the same `uint256` expiry word the same way, so a controller event that repeats an out-of-range expiry no longer fails interpretation of its log. Basenames controller events are unchanged: an out-of-range expiry there still fails.
> **Since**: `2026-09-10`

> **Registry records the admitted Graveyard holds are served with no owner** — the ENSv2
> Graveyard claims a lapsed `.eth` name and clears a subname of a name it holds by making
> itself the node's registry owner, so `owner(node)` returns the Graveyard.
> **Upstream**: (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/migration/Graveyard.sol:L20-L25 @ ens_v2_sepolia_20260916@366de741)
> (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/migration/Graveyard.sol:L142-L172 @ ens_v2_sepolia_20260916@366de741)
> **Our rule / why**: maintainer decision (2026-09-29): the Graveyard is the burn address, so a
> record it holds is burned. A current-registry owner write naming the Graveyard of the admitted
> migration manifest, at or after its declared start, is served as no owner (null, not the zero
> address) on the name, on its parent's subnames page and in address lists, where the Graveyard
> is not its controller; raw facts and history keep the Graveyard. The record wins over later
> NameWrapper token transfers of a subname wrapped without `PARENT_CANNOT_CONTROL`, whose token
> outlives the clear. Superseded Sepolia Graveyards are not declared and are served as the chain
> holds them. A live token sent to the Graveyard keeps its lease and is served as the chain holds
> it. See [projections](projections.md).
> **Since**: `2026-09-29`

> **NameWrapper `safeTransferFrom` self-transfer clears the token approval without a log** —
> `ERC1155Fuse._transfer` runs `_beforeTransfer`, which deletes the per-token
> approval unless `CANNOT_APPROVE` is burnt, and then returns before emitting
> `TransferSingle` when the token's owner is also the recipient. bigname derives
> the delegate's `PermissionChanged` revocation from observed transfer logs, so
> after such a self-transfer it keeps the approved delegate's row until the next
> observed `Approval`, transfer, burn, or unwrap of that name. The batch path
> has no early return: `safeBatchTransferFrom` still emits `TransferBatch` for a
> self-transfer, which the interpreter observes and handles (no holder change,
> approval revoked unless `CANNOT_APPROVE` is burnt).
> **Upstream**: (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L281-L306 @ ens_v1@91c966f)
> (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L155-L197 @ ens_v1@91c966f)
> (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L815-L840 @ ens_v1@91c966f)
> **Our rule / why**: there is no raw fact to attribute the clear to; inventing
> a state-derived revocation would require calling `getApproved`, which the
> interpreter does not do. The delegate row is served as still granted.
> **Since**: `2026-09-13`

> **A registry child the NameWrapper holds without a current wrap, or under a label failing normalization, is not served for its token holder** —
> once a registry write moves a wrapped name's record away from NameWrapper, bigname ends the
> NameWrapper authority. If the record is later written back to the NameWrapper address with
> `ENSRegistry.setOwner` or `setSubnodeOwner` and no wrap follows, the old token is live again on
> chain, because NameWrapper counts a name as wrapped whenever it holds the registry record and
> the token has an owner. A name with a [name row](glossary.md#composed-name-row) then serves the
> NameWrapper contract as `owner` and `manager` until the next `NameWrapped`. A child with no name
> row, which is what NameWrapper's `setSubnodeOwner` and `setSubnodeRecord` leave when they take
> the registry record and mint the token for a label that fails ENSIP-15 normalization, omits
> `owner` and `manager` on its parent's subnames page and is not listed for the NameWrapper
> contract under any relation while the NameWrapper that named it holds its registry record,
> including after such a write-back, whether or not a NameWrapper token survives for it. Neither
> case lists the token holder.
> **Upstream**: (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1076-L1079 @ ens_v1@91c966f)
> (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L579-L581 @ ens_v1@91c966f)
> (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L612-L619 @ ens_v1@91c966f)
> (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L63-L84 @ ens_v1@91c966f)
> **Our rule**: `docs/api-v1.md` § Manager; [projections](projections.md);
> `docs/api-v1-routes.md` (subnames and address names).
> **Why**: the parent's `setSubnodeOwner`, or `setOwner` by the record's owner, can move the
> record back, but reopening the old token's authority would need retained NameWrapper authority
> state that the interpreter does not keep. For a child with no name row, the address index
> records the registry owner, the NameWrapper contract, not the token holder, so the read side
> can only stop serving the contract; it keys on a NameWrapper having observed the child and
> holding its registry record now, not on a current token. Listing the holder needs a projection
> change and is deferred (TYR-148).
> **Since**: `2026-10-03`




Intentional differences between our docs/manifests and upstream. Every divergence lives here so that citations reading "differently than upstream" are legible instead of looking like bugs. If a divergence is not in this list, it should be treated as drift and closed — either by updating our doc or by adding the entry.

The API contract's [public record-field completeness
table](api-v1-routes.md#public-record-field-completeness) gives the
consumer-facing status of standard registry and resolver fields and links back
to the applicable entries below.

> **Stored zero address is served as absence**: bigname serves an exactly stored
> 20-byte zero `addr:60` through an ENSv1 authority pointer or Basenames registry
> pointer as `not_found`, including when a successful ENSIP-19 default exists.
> Empty or missing eligible exact data retains permitted fallback.
> **Upstream**: ENSv1 returns the selected coin-type bytes and consults the
> default only when that payload is empty
> `(upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L73-L85 @ ens_v1@91c966f)`;
> its direct `addr(bytes32)` getter converts the selected coin-60 bytes to an
> address
> `(upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L36-L40 @ ens_v1@91c966f)`.
> The admitted Basenames resolver reads exact storage without default fallback
> `(upstream: .refs/basenames/lib/ens-contracts/contracts/resolvers/profiles/AddrResolver.sol:L57-L62 @ basenames@1809bbc)`.
> Its legacy getter returns the zero address for empty bytes and otherwise
> converts the returned payload as an exact 20-byte address
> `(upstream: .refs/basenames/src/L2/resolver/AddrResolver.sol:L76-L82 @ basenames@1809bbc)`
> `(upstream: .refs/basenames/src/L2/resolver/AddrResolver.sol:L108-L110 @ basenames@1809bbc)`.
> **Our rule**: `docs/api-v1.md` § Resolver record answers and values,
> `docs/api-v1-routes.md` resolver-record route, and `docs/projections.md` §
> Resolver and records.
> **Why**: classifying `address(0)` as absence keeps indexed and verified
> answers aligned without changing retained normalized bytes.
> **Since**: `2026-09-05`

> **Indexed `default.reverse` fallback reads projected names only**: for an ENS
> `coin_type=60` primary name, the indexed answer serves the address's
> `default.reverse` name whenever the projected `addr.reverse` name has no
> bytes or its reverse node has no nonzero resolver. A reverse node that points
> at a resolver bigname does not admit, or at an event-silent resolver with no
> hydrated name, has no projected name, so the indexed answer falls back to
> `default.reverse` there too.
> **Upstream**: ENS's ETH reverse resolver first reads a standalone
> `addr.reverse` registrar, then calls `name()` with a 100,000 gas stipend on
> any nonzero registry resolver of the reverse node, and falls back to the
> `default.reverse` registrar only when that call returns an empty name; a
> revert, out-of-gas, or undecodable result ends the lookup with an empty name
> `(upstream: .refs/ens_v1/contracts/reverseResolver/ETHReverseResolver.sol:L42-L70 @ ens_v1@91c966f)`.
> **Our rule**: `docs/projections.md` § Primary names and `docs/api-v1-routes.md`
> § `GET /v1/addresses/{address}/primary-name`. The verified source follows the
> upstream order with live calls and gives `name()` the same gas. It skips the
> standalone registrar, which the Mainnet and Sepolia profiles do not declare,
> and a failed `name()` call ends it with `failed` instead of an
> empty name; neither reads the default.
> **Why**: the projection holds names only from admitted resolver events and
> hydration, and unlisted emitters are unsupported; the verified source reads
> the resolver itself.
> **Since**: `2026-10-01`

<a id="verified-reverse-name-call-context"></a>
> **Verified reverse `name()` call is not a static call from the reverse
> resolver**: the verified primary-name leg reads the reverse node's resolver
> `name(node)` with a top-level `eth_call` whose gas is set so the resolver
> frame gets 100,000 gas. That call is not a static call, and its caller is the
> zero address. The gas bound is the only part of the upstream call context it
> reproduces. Two differences follow, both reachable only through a resolver an
> address installs on its own reverse node through the admitted
> `ReverseRegistrar`, and both affecting only that address's own primary name:
> (1) a `name` that writes storage and then returns an empty string fails
> upstream's static call, which ends the lookup with no name, while bigname's
> call succeeds, sees the empty string and falls back to the `default.reverse`
> name; (2) a `name` whose answer depends on its caller can return one name to
> ETHReverseResolver and another to bigname. ETHReverseResolver has no
> deployment in either profile today: neither profile declares it and the
> pinned Sepolia and Mainnet deployment artifacts include none, so this
> describes the upstream source, not a live contract.
> **Upstream**: ETHReverseResolver reads the name with
> `resolver.staticcall{gas: 100_000}` from its own address
> `(upstream: .refs/ens_v1/contracts/reverseResolver/ETHReverseResolver.sol:L54-L69 @ ens_v1@91c966f)`;
> an address can claim its reverse node with any resolver
> `(upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L74-L98 @ ens_v1@91c966f)`.
> **Our rule**: `docs/api-v1-routes.md` § `GET /v1/addresses/{address}/primary-name`
> and `docs/execution.md`; implemented in `crates/lookup/src/primary_name.rs`.
> **Why**: with no ETHReverseResolver deployed there is no contract whose
> caller identity to reproduce, and a direct call keeps the reverse leg to plain
> hash-pinned `eth_call`s. Revisit if ETHReverseResolver ships.
> **Since**: `2026-10-01`

<a id="default-reverse-fallback-past-a-reverse-node-resolver"></a>
> **`default.reverse` fallback past an empty name on the reverse node's own
> resolver**: when `<address>.addr.reverse` has its own nonzero registry
> resolver whose `name` returns an empty string, both bigname sources follow
> ETHReverseResolver's order and take the address's `default.reverse` name as
> its reverse claim. The verified answer for that claim still depends on
> normalization, authority admission and the forward check.
> The Universal Resolver's ENSIP-19 `reverse` answers no primary name there: it
> uses the reverse node's own resolver, sees the empty name and stops. On
> Sepolia on 2026-10-02, near block `11826770`, an `eth_call` to
> `reverse(0x69420f05a11f617b4b74ffe2e04b2d300dfa556f, 60)` returned an empty
> name, while bigname's reverse leg read `hcathgq2e.eth` from
> `default.reverse`. That call's resolver lookup returned
> `0x322b…9cb0`, the 2026-10-01 redeploy's `ENSV1Resolver`
> `(upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/ENSV1Resolver.json:L2 @ ens_v2_sepolia_20261001@07e55a05)`,
> behind the redeploy's Universal Resolver, which the managed proxy has served
> since block `11821680`. Which order is canonical on Sepolia after the ENSv2
> cutover is an open question, and this entry records it without changing either
> source.
> **Upstream**: ETHReverseResolver falls back to `default.reverse` when the
> reverse node's resolver returns an empty name
> `(upstream: .refs/ens_v1/contracts/reverseResolver/ETHReverseResolver.sol:L42-L70 @ ens_v1@91c966f)`,
> but the Universal Resolver reaches it only as an ancestor resolver, because a
> reverse node's own nonzero resolver wins the resolver lookup
> `(upstream: .refs/ens_v1/contracts/universalResolver/RegistryUtils.sol:L33-L37 @ ens_v1@91c966f)`.
> The ENSv1 Universal Resolver's `reverse` calls `name` on that resolver and
> returns an empty name as no primary name
> `(upstream: .refs/ens_v1/contracts/universalResolver/AbstractUniversalResolver.sol:L200-L225 @ ens_v1@91c966f)`.
> The ENSv2 Universal Resolver does the same
> `(upstream: .refs/ens_v2_sepolia_20260916/contracts/src/universalResolver/AbstractNormalizedUniversalResolver.sol:L377-L399 @ ens_v2_sepolia_20260916@366de741)`
> `(upstream: .refs/ens_v2_sepolia_20260916/contracts/src/universalResolver/AbstractNormalizedUniversalResolver.sol:L240-L249 @ ens_v2_sepolia_20260916@366de741)`;
> its deployment gives `reverse` the `ENSV1Resolver`
> `(upstream: .refs/ens_v2_sepolia_20261001/contracts/deploy/01_ReverseMirror.ts:L30-L42 @ ens_v2_sepolia_20261001@07e55a05)`,
> which finds the reverse node's ENSv1 resolver and passes `name` to it with no
> `default.reverse` read
> `(upstream: .refs/ens_v2_sepolia_20260916/contracts/src/resolver/ENSV1Resolver.sol:L40-L43 @ ens_v2_sepolia_20260916@366de741)`
> `(upstream: .refs/ens_v2_sepolia_20260916/contracts/src/resolver/AbstractMirrorResolver.sol:L67-L69 @ ens_v2_sepolia_20260916@366de741)`.
> **Our rule**: `docs/api-v1-routes.md` § `GET /v1/addresses/{address}/primary-name`
> (ENSIP-19 default name).
> **Why**: bigname adopted ETHReverseResolver's order for `default.reverse`
> names; the live Universal Resolver does not run that order for a reverse node
> with its own resolver. Revisit once the canonical Sepolia order is settled.
> **Since**: `2026-10-02`

<a id="ensv1-authority-without-an-ensv2-entry"></a>
> **ENSv1 authority for a `.eth` name without an ENSv2 entry** — bigname follows the chain for ENSv1 and ENSv2 name authority: a current ENSv2 registration decides, a premigration reservation defers to ENSv1, a released or expired ENSv2 registration stays with ENSv2 as released, and a name that never had an ENSv2 registration is decided by a live ENSv1 registration, or by its history when neither arm holds it. For a name ENSv1 decides without a live ENSv2 entry the ENSv2 Universal Resolver answers nothing: it reads only ENSv2 registries, keeps the nearest ancestor's resolver when a label has no live entry, and the deployment registers `eth` without a resolver, so the lookup fails with `ResolverNotFound`. bigname still serves the name's ownership and registration from its live ENSv1 registration, or as the released ENSv1 registration when that has ended. What it resolves to depends on the [Universal Resolver cutover](glossary.md#universal-resolver-cutover): before it, clients resolve through ENSv1 and bigname serves the ENSv1 resolver and records; from it, bigname follows the Universal Resolver and serves no resolver or records for the name or any name below it, with `unresolvable_reason: "no_live_ens_v2_entry"` (`docs/api-v1.md` § Expiry and grace). On Sepolia at block `11807425`, under the dropped 2026-09-15 deployment, that covered 172 `.eth` second-level names held on ENSv1 (13 never reserved, 159 whose reservation passed its expiry unclaimed) and 149 live names below them.
> **Upstream**: the Universal Resolver walks only the ENSv2 root registry and its subregistries, taking an entry's resolver only when it is nonzero `(upstream: .refs/ens_v2_sepolia_20260916/contracts/src/universalResolver/UniversalResolverV2.sol:L56-L63 @ ens_v2_sepolia_20260916@366de741)` `(upstream: .refs/ens_v2_sepolia_20260916/contracts/src/universalResolver/libraries/LibResolution.sol:L58-L85 @ ens_v2_sepolia_20260916@366de741)`; the registry returns no resolver for an expired entry `(upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registry/PermissionedRegistry.sol:L283-L286 @ ens_v2_sepolia_20260916@366de741)`; the deployment registers `eth` with a zero resolver `(upstream: .refs/ens_v2_sepolia_20261001/contracts/deploy/01_ETHRegistry.ts:L39-L51 @ ens_v2_sepolia_20261001@07e55a05)`; a missing resolver fails the lookup `(upstream: .refs/ens_v2_sepolia_20260916/contracts/src/universalResolver/AbstractNormalizedUniversalResolver.sol:L406-L408 @ ens_v2_sepolia_20260916@366de741)`. Premigration normally reserves every live ENSv1 `.eth` name with `ENSV1Resolver` as its resolver, which closes this gap for those names `(upstream: .refs/ens_v2_sepolia_20261001/contracts/docs/premigration.md:L3-L8 @ ens_v2_sepolia_20261001@07e55a05)`.
> **Our rule**: [ADR 0007](adrs/0007-follow-the-chain-ens-authority.md) and `docs/architecture.md` § “ENSv1→ENSv2 current authority”; implemented in `crates/storage/src/families/name/selection.rs`, with resolution withheld past the cutover in `crates/storage/src/families/name/resolvability.rs`.
> **Why**: the ENSv1 registry still records the name, and a live ENSv1 name that never received a reservation (it was registered on ENSv1 after the premigration snapshot) or whose reservation lapsed unused is still held by its ENSv1 owner. A name whose reservation was used for a registration that was then released is not in this divergence: it stays released under ENSv2, as the Universal Resolver reads it. Serving nothing would hide a name that the ENSv1 read path answers.
> **Since**: `2026-09-24`

> **Retired: ENS no-proof overlap refusal versus chain-side era precedence** — From `2026-08-26` until [ADR 0007](adrs/0007-follow-the-chain-ens-authority.md), bigname refused an ordinary logical name with ENSv1 and ENSv2 candidates and no activated ENSv1→ENSv2 migration, release, or other admitted [authority proof](glossary.md#authority-proof), with `independent_ens_deployments_overlap` on Sepolia and `conflicting_current_ens_authority` on Mainnet, while a chain-facing resolution path answered from one era. bigname now follows the chain instead, so this is no longer a divergence: a current ENSv2 registration decides without a proof, as the ENSv2 registrar and Universal Resolver do `(upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registrar/ETHRegistrar.sol:L245-L257 @ ens_v2_sepolia_20260916@366de741)` `(upstream: .refs/ens_v2_sepolia_20260916/contracts/src/universalResolver/UniversalResolverV2.sol:L56-L63 @ ens_v2_sepolia_20260916@366de741)`. The root, `eth`, `reverse`, and `addr.reverse` no longer carry a separate classification and follow the same rule as every other name; the deployment registers `eth` and `reverse` in the root registry `(upstream: .refs/ens_v2_sepolia_20261001/contracts/deploy/01_ETHRegistry.ts:L39-L51 @ ens_v2_sepolia_20261001@07e55a05)` `(upstream: .refs/ens_v2_sepolia_20261001/contracts/deploy/01_ReverseMirror.ts:L30-L42 @ ens_v2_sepolia_20261001@07e55a05)`.
> **Our rule**: [ADR 0007](adrs/0007-follow-the-chain-ens-authority.md).
> **Why retired**: the proof the refusal waited for is a migration-script artefact that the chain does not require, and the ENSv2 contracts decide who holds a name from ENSv2 state alone.
> **Since**: `2026-08-26`; retired `2026-09-24`

> **Ownerless ENSv2 reservation resolver serving narrowing** — bigname retains
> reservation resolver facts for diagnostics, but product name, record, batch
> lookup, and resolver-listing routes classify an ownerless reservation as no
> current registration and do not serve that resolver or its record inventory.
> An unbound TLD with an observed ENSv2 root-registry resolver pointer is the
> exception: these routes serve its pointer and eligible records through the
> [serving resource](glossary.md#serving-resource), while its current authority
> remains null. A reservation keeps that pointer; a later release, expiry, or
> zero/null resolver update withdraws it.
> **Upstream**: `PermissionedRegistry` stores the supplied resolver before its
> owner-zero reservation branch and emits `ResolverUpdated` for a nonzero value
> `(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L461-L478 @ ens_v2@a971bd64)`;
> `getResolver` returns the stored value until the entry expires
> `(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L255-L258 @ ens_v2@a971bd64)`.
> **Our rule**: `docs/api-v1.md` § Field Budgets,
> `docs/api-v1-routes.md` name and resolver routes, and `docs/storage.md` §
> Projection storage rules.
> **Why**: ordinary reservation records remain outside the serving boundary.
> The root-registry TLD exception exposes observed resolution without inventing
> registration ownership. Diagnostics retain the other reservation facts for
> comparison without presenting them as current name data.
> **Since**: `2026-09-02`

> **ENSv1 and Basenames ownerless registry reads use event-linked reachability** — registry owner events retain their literal owner word as history, while control uses the [getter-visible owner](glossary.md#getter-visible-owner). In the current ENS Solidity registry and Basenames, a literal zero word and the emitting registry's own address are control-equivalent, but neither clears an independently selected resolver. ENSv1 emits the literal owner argument, maps current-registry self storage to getter zero, stores the resolver separately, and its fallback writes current-registry self when asked to store zero `(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L67-L68 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L81-L82 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L123-L141 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L170-L172 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L48-L55 @ ens_v1@91c966f)`. The admitted 2017 mainnet LLL registry instead returns its stored owner word unchanged, and the fallback delegates to that getter when the current registry has no record, so an owner equal to that emitter remains authentic `(upstream: .refs/ens_v1_lll/contracts/ENS.lll:L65-L66 @ ens_v1_lll@7e377df)` `(upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L29-L34 @ ens_v1@91c966f)`. The similarly named Sepolia legacy deployment uses the Solidity registry artifact and therefore retains the Solidity self-to-zero getter rule `(upstream: .refs/ens_v1/deploy/registry/00_deploy_registry.ts:L14-L19 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L123-L131 @ ens_v1@91c966f)`. Basenames emits the literal owner arguments, maps registry-self to getter zero, and stores owner and resolver independently `(upstream: .refs/basenames/src/L2/Registry.sol:L100-L134 @ basenames@1809bbc)` `(upstream: .refs/basenames/src/L2/Registry.sol:L165-L180 @ basenames@1809bbc)` `(upstream: .refs/basenames/src/L2/Registry.sol:L214-L216 @ basenames@1809bbc)`.
> **Our rule**: Getter-visible zero ends only registry-direct control. An event-linked nonzero resolver remains readable after its surface is known. For ENSv1, the first active [name surface](glossary.md#surface-name-surface) links retained [pre-surface](glossary.md#pre-surface) state to the registry [serving resource](glossary.md#serving-resource) without opening a control binding; a latest zero-address selection suppresses that [state-derived normalized event](glossary.md#state-derived-normalized-event). When no registrar state was previously known, a label-bearing registrar renewal establishes no registrar control while the getter-visible registry owner is zero. Registrar control already current before the registry owner becomes zero remains current. Direct-child enumeration requires either live control or a nonzero event-linked resolver; fallback `recordExists` alone is not enough.
> A registrar token transfer does not re-establish registry control: the registrar exposes the live token holder through its ERC-721 `ownerOf`, while changing the ENS registry owner requires the separate `reclaim` call `(upstream: .refs/ens_v1/contracts/ethregistrar/IBaseRegistrar.sol:L4-L7 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L67-L75 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L171-L175 @ ens_v1@91c966f)`.
> **Narrowing**: Linking the first active name surface is additive: it preserves the original pre-surface event with null `logical_name_id` and its original `resource_id`, whether null or already linked to a known authority or registry read resource, and uses the retained registry source for the new pointer. ENSv1 stores and emits resolver selection by node and returns that stored node resolver `(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L86-L95 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L134-L140 @ ens_v1@91c966f)`. The admitted 2017 LLL registry likewise keys its resolver slot by node `(upstream: .refs/ens_v1_lll/contracts/ENS.lll:L86-L98 @ ens_v1_lll@7e377df)`.
> **Since**: `2026-08-27`

> **ENSv2 max-expiry projection narrowing** — bigname current projections expose expiry timestamps as finite Unix-second values or `null`; they do not fabricate far-future dates for `type(uint64).max`.
> **Upstream**: ENSv2 reverse registration explicitly uses `type(uint64).max` for names that never expire `(upstream: .refs/ens_v2/contracts/src/reverse-registrar/StandaloneReverseRegistrar.sol:L175 @ ens_v2@a971bd64)` `(upstream: .refs/ens_v2/contracts/src/reverse-registrar/StandaloneReverseRegistrar.sol:L176 @ ens_v2@a971bd64)`. ENSv2 registry renewal accepts a non-decreasing `uint64 newExpiry` and stores it as the registry expiry `(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L212 @ ens_v2@a971bd64)` `(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L223-L224 @ ens_v2@a971bd64)` `(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L226 @ ens_v2@a971bd64)` `(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L227 @ ens_v2@a971bd64)`.
> **Our rule**: `docs/storage.md` § Projection storage rules.
> **Why**: composed name timestamps and the REST list surfaces use representable timestamp semantics. Mapping max or otherwise unrepresentable numeric expiry values to `null` preserves the "no public finite expiry" meaning without inventing a date that route types and ordering helpers cannot represent.
> **Since**: `2026-06-30`

> **ENSv2 expired-role projection narrowing** — after a state-derived ENSv2 path-expiry release, bigname removes that resource's effective current permission rows until a same-resource renewal or later grant or reservation readmits retained grants. Whether the expiry release named a surface does not affect the resource revival. The partial-coverage resource summary remains available during the expired interval.
> **Upstream**: `PermissionedRegistry.getResource` resolves an identifier through the resource constructor `(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L304-L305 @ ens_v2@a971bd64)`, whose expiry branch bumps the EAC version for an expired entry, so live role reads during the expired interval land on a fresh empty scope while pre-expiry grants stay stored under the prior version `(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L642-L644 @ ens_v2@a971bd64)`; `roles` reads EnhancedAccessControl roles through that resource `(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L356-L364 @ ens_v2@a971bd64)`. A renewal — including the post-expiry revive path — extends expiry without touching either version counter, making the stored grants live again `(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L212-L228 @ ens_v2@a971bd64)`, while unregistering a registered entry burns its token and increments both version counters, so a later registration uses the already-fresh permission scope `(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L195-L206 @ ens_v2@a971bd64)` `(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L29-L34 @ ens_v2@a971bd64)`.
> **Our rule**: `docs/storage.md` § Projection storage rules.
> **Why**: bigname models the expired interval by removing the effective permission rows from current serving rather than by serving upstream's version-bumped empty scope; the partial-coverage summary and event history retain audit context. Readmission mirrors upstream's version semantics: a same-versioned continuation (revival) makes retained grants current again, and a new versioned resource receives grants only from its own permission events.
> **Since**: `2026-08-31`

> **ENSv1 `.eth` registration and expiry derived from an allow-listed controller set** — upstream treats the BaseRegistrar's own `NameRegistered` / `NameRenewed` as the authority for `.eth` expiry; bigname derives ordinary registration and expiry facts only from an explicit list of `ETHRegistrarController` addresses declared in `manifests/mainnet/ethereum/ens/ens_v1_registrar_l1/v1.toml`. The registrar's own numeric `NameRegistered` / `NameRenewed` are admitted, but route only into ENSv1→ENSv2 migration correlation and are gated on `migration_enabled`; they never widen the watched controller set and never produce registration or expiry facts for this source family.
> **Upstream**: the registrar declares its own `NameRegistered` `(upstream: .refs/ens_v1/contracts/ethregistrar/IBaseRegistrar.sol:L15 @ ens_v1@91c966f)`, and it is the registrar — not a controller — that writes `expiries[]`, on registration `(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L142 @ ens_v1@91c966f)` and on renewal, emitting `NameRenewed` from the same call `(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L166 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L167 @ ens_v1@91c966f)`. The controller set is mutable governance state: `addController` is `onlyOwner` and emits `ControllerAdded` `(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L79 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L81 @ ens_v1@91c966f)`, with a matching `removeController` `(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L85 @ ens_v1@91c966f)`. The reference subgraph binds the registrar's own events directly `(upstream: .refs/ens_subgraph/subgraph.yaml:L137 @ ens_subgraph@723f1b6)` `(upstream: .refs/ens_subgraph/subgraph.yaml:L138 @ ens_subgraph@723f1b6)` `(upstream: .refs/ens_subgraph/subgraph.yaml:L139 @ ens_subgraph@723f1b6)` `(upstream: .refs/ens_subgraph/subgraph.yaml:L140 @ ens_subgraph@723f1b6)` and takes expiry from that event's payload `(upstream: .refs/ens_subgraph/src/ethRegistrar.ts:L53 @ ens_subgraph@723f1b6)`, using its separate controller handlers for label and cost metadata rather than for expiry.
> **Our rule**: `docs/manifests.md` § ENS mainnet (`ens_v1_registrar_l1`); mirrored in `docs/architecture.md` § Source families and `docs/chain-intake.md`.
> **Why**: controller-sourced events carry the human-readable label, which registrar-only events do not, and the admitted controller list is what binds a registration fact to a name rather than a token id. The cost is that a controller rotation is silent: a registration through an unadmitted controller yields no label, registration, or expiry fact for this source family, and a renewal through one leaves a stale expiry that can later settle a spurious `RegistrationReleased`. Ownership degrades differently: the separately declared `ens_v1_registry_l1` family still observes that registration's registry write, so a name whose surface is already materialized keeps a current registry-only owner while its registration and expiry stay missing, and a name whose label is first seen in the rotated controller's event materializes no public surface at all. There is no live manifest-drift or proxy-upgrade alert loop to catch that rotation, so this narrowing is a standing risk rather than a closed decision — tracked as finding 05 in the readiness review. Widening intake to corroborate expiry from the registrar's own events, or treating an unknown controller as an explicit unsupported signal, would close it.
> **Since**: `2026-09-01`

> **ENS Universal Resolver proxy entrypoint vs pinned implementation artifact** — bigname uses the official ENS Universal Resolver proxy address as the route-facing `ens_execution` entrypoint, even though the pinned ENSv1 deployment artifact under `.refs/ens_v1` records the implementation / ABI anchor.
> **Upstream**: official ENS docs list `0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe` as the Universal Resolver proxy on Mainnet and testnets (https://docs.ens.domains/resolvers/universal/; https://docs.ens.domains/learn/deployments/). The pinned ENSv1 deployment artifact records `0xED73a03F19e8D849E44a39252d222c6ad5217E1e` as the implementation artifact `(upstream: .refs/ens_v1/deployments/mainnet/UniversalResolver.json:L2 @ ens_v1@91c966f)` and the ABI/behavior anchor remains the pinned Universal Resolver Solidity `(upstream: .refs/ens_v1/contracts/universalResolver/UniversalResolver.sol:L8 @ ens_v1@91c966f)`.
> **Our rule**: `docs/manifests.md` § Required Fields and Capability Policy, `docs/execution.md` § Resolution flow and § Retained legacy support boundary, and `docs/architecture.md` § Source Families / Coverage And Exhaustiveness Rules / Deterministic Execution And Verification Plane.
> **Why**: callers and manifests should target the official proxy entrypoint, while `.refs/ens_v1` remains the pinned implementation/ABI source for behavior citations.
> **Since**: `2026-04-22`

> **ENS and Basenames reverse-claim normalization narrowing** — bigname reports verified primary-name success for admitted ENS and Basenames tuples only when the untrimmed declared reverse claim already byte-equals its ENSIP-15 normalized form. The ENS Universal Resolver instead forward-resolves the literal claim without normalizing it, and the ENSv1 Base reverse registrar used for Basenames claim intake stores the supplied string unchanged.
> **Upstream**: The ENS reverse callback decodes the reverse resolver's string result `(upstream: .refs/ens_v1/contracts/universalResolver/AbstractUniversalResolver.sol:L222 @ ens_v1@91c966f)` and passes it directly to `NameCoder.encode` for the forward lookup `(upstream: .refs/ens_v1/contracts/universalResolver/AbstractUniversalResolver.sol:L226 @ ens_v1@91c966f)`. `NameCoder.encode` takes the string's literal byte length `(upstream: .refs/ens_v1/contracts/utils/NameCoder.sol:L258 @ ens_v1@91c966f)` and copies those bytes directly into the DNS buffer `(upstream: .refs/ens_v1/contracts/utils/NameCoder.sol:L265 @ ens_v1@91c966f)` before the reverse callback rejects only a resolved-address mismatch `(upstream: .refs/ens_v1/contracts/universalResolver/AbstractUniversalResolver.sol:L270 @ ens_v1@91c966f)`. On Base, `L2ReverseRegistrar.setName` accepts the supplied string `(upstream: .refs/ens_v1/contracts/reverseRegistrar/L2ReverseRegistrar.sol:L61 @ ens_v1@91c966f)`, and `StandaloneReverseRegistrar._setName` stores and emits that string unchanged `(upstream: .refs/ens_v1/contracts/reverseRegistrar/StandaloneReverseRegistrar.sol:L28 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/reverseRegistrar/StandaloneReverseRegistrar.sol:L30 @ ens_v1@91c966f)`.
> **Our rule**: `docs/architecture.md` § Primary and reverse names, `docs/api-v1.md` § Result status vocabulary, `docs/api-v1-routes.md` § `GET /v1/addresses/{address}/primary-name`, `docs/projections.md` § Primary names, `docs/execution.md` § Primary-name verification, and `docs/storage.md` § Projection storage rules.
> **Why**: make ENS verified successes a strict subset of Universal Resolver successes and apply the same public invariant to Basenames claims even though Base claim intake preserves arbitrary string spelling. The Universal Resolver can verify a non-normalized ENS claim when records exist at its literal node, while bigname returns `claim_not_normalized` without attempting that lookup.
> **Since**: `2026-07-21`

> **ENSv2 primary-name-only authority verification narrowing** — bigname refuses live primary-name forward verification when a readable exact-name projection selects the `ens_v2` [authority arm](glossary.md#authority-epoch), even though the Sepolia ENSv1 Universal Resolver can reach the ENSv2-backed wildcard resolver and serve live ENSv2 state. A name with no readable exact-name projection row remains admitted because the projection has no authority statement for it. The records route is not part of this narrowing: on Ethereum Mainnet, a readable ENS name with a null exact resolver and no other projected resolution shape executes the manifest-admitted Universal Resolver, which performs the ancestor walk itself.
> **Upstream**: The ENSv2 deployment script installs `ENSV2Resolver` at the ENSv1 `eth` node, and that resolver is backed by ENSv2 `(upstream: .refs/ens_v2/contracts/deploy/00_ENSV2Resolver.ts:L60-L81 @ ens_v2@a971bd64)` `(upstream: .refs/ens_v2/contracts/src/resolver/ENSV2Resolver.sol:L13-L14 @ ens_v2@a971bd64)`. The ENSv1 Universal Resolver walks up to an ancestor resolver and accepts an ENSIP-10 extended resolver `(upstream: .refs/ens_v1/contracts/universalResolver/RegistryUtils.sol:L25-L38 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/universalResolver/AbstractUniversalResolver.sol:L63-L87 @ ens_v1@91c966f)`. Locked ENSv1→ENSv2 migration with `CANNOT_SET_RESOLVER` burned retains the ENSv1 resolver entry and passes the replacement PublicResolver into ENSv2 registration when the retained resolver is listed `(upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L137-L175 @ ens_v2@a971bd64)`.
> **Our rule**: `docs/api-v1-routes.md` § `GET /v1/addresses/{address}/primary-name` and § `GET /v1/names/{name}/records`.
> **Why**: current ENS/60 primary-name verification has only a Mainnet `ens_execution` entrypoint. Exact-name authority therefore fails closed after a readable projection selects ENSv2 instead of silently treating the ENSv1 entrypoint as authority for that arm. The records route separately admits [Universal Resolver ancestor discovery](glossary.md#universal-resolver-ancestor-discovery) and lets the Universal Resolver enforce the ancestor's ENSIP-10 support. The locked-name `CANNOT_SET_RESOLVER` case is not reachable through the primary-name path; if a deployment with the Sepolia redirect gains a verified route entrypoint, the admitted live path outside indexed coverage could expose it until the exact-name projection publishes the ENSv2 selection. The route contracts state those conditional limitations explicitly.
> **Since**: `2026-08-20`

<a id="verified-resolution-text-selector-key-narrowing"></a>
> **Verified-resolution text selector-key narrowing** — bigname's public `text:<key>` record selectors accept only non-empty keys containing no ASCII whitespace and no commas. Request parsing first strips boundary whitespace from each comma-separated request item, so a request like `text:display ` selects the `display` key; after that trim, a key that still falls outside the grammar is rejected as `400 invalid_input` rather than resolved. The upstream setter permits keys containing whitespace or commas, including boundary whitespace, but those keys are not requestable through the product record routes.
> **Upstream**: ENSv1 defines text records by node and unconstrained string key `(upstream: .refs/ens_v1/contracts/resolvers/profiles/ITextResolver.sol:L4-L19 @ ens_v1@91c966f)`. Its `TextResolver` setter stores and emits the supplied key without a whitespace or comma check `(upstream: .refs/ens_v1/contracts/resolvers/profiles/TextResolver.sol:L15-L21 @ ens_v1@91c966f)`.
> **Our rule**: `docs/api-v1-routes.md` § Public record-field completeness.
> **Why**: route parsing separates multiple requested record keys with commas and rejects duplicate canonical record keys as `400 invalid_input`; whitespace- and comma-free keys keep selector identity unambiguous in request parameters and stored diagnostics.
> **Since**: `2026-04-18`

<a id="verified-resolution-addr-coin-type-selector-narrowing"></a>
> **Verified-resolution addr coin-type selector narrowing** — bigname's public `addr:<coin_type>` record selectors accept only digit text that fits unsigned 64-bit decimal form and canonicalize it before selector dedupe and request-scoped lookup. Declared projection storage may still retain upstream-width resolver facts; selectors outside the public grammar are not requestable through verified lookup until a wider representation is designed. Upstream resolver contracts use `uint256 coinType`.
> **Upstream**: ENSv1's multicoin address resolver emits and reads `uint256 coinType` `(upstream: .refs/ens_v1/contracts/resolvers/profiles/IAddressResolver.sol:L8 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/resolvers/profiles/IAddressResolver.sol:L14 @ ens_v1@91c966f)`. Basenames' Base resolver also reads `uint256 coinType` and stores by `uint256 coinType` `(upstream: .refs/basenames/src/L2/resolver/AddrResolver.sol:L93 @ basenames@1809bbc)` `(upstream: .refs/basenames/src/L2/resolver/AddrResolver.sol:L94 @ basenames@1809bbc)`.
> **Our rule**: `docs/api-v1.md` § Record-key grammar, `docs/api-v1-routes.md` § `GET /v1/names/{name}/records`, and `docs/execution.md` § Resolver-record lookup.
> **Why**: bigname's API and lookup path use textual selector keys across route parsing and sort/dedupe. The `u64` boundary keeps that selector identity canonical and fail-closed until a wider coin-type representation is deliberately designed.
> **Since**: `2026-06-12`

<a id="ens-verified-resolution-ccip-read-non-following"></a>
> **ENS verified-resolution CCIP-Read non-following** — for ENS record resolution, bigname does not follow EIP-3668 `OffchainLookup` continuations through the records route / resolver-record lookup. The affected selector is returned as explicit `unsupported` with reason `offchain_lookup_required`; no gateway request is made. The forward check in ENS/60 primary-name verification is outside this divergence because it follows the resolver-supplied gateway URLs from the EIP-3668 `OffchainLookup` `urls` field; see `docs/api-v1.md` § Error Model.
> **Upstream**: ENSv1's Universal Resolver passes configured batch gateways into a forward-resolution path whose caller is expected to enable EIP-3668 `(upstream: .refs/ens_v1/contracts/universalResolver/AbstractUniversalResolver.sol:L81-L110 @ ens_v1@91c966f)`. Its CCIP reader propagates resolver `OffchainLookup` data, including the supplied URLs and callback data `(upstream: .refs/ens_v1/contracts/ccipRead/CCIPReader.sol:L44-L88 @ ens_v1@91c966f)`, and the batch path routes outstanding offchain requests through its gateway set `(upstream: .refs/ens_v1/contracts/ccipRead/CCIPBatcher.sol:L70-L125 @ ens_v1@91c966f)`.
> **Our rule**: `docs/execution.md` § Resolver-record lookup and `docs/api-v1-routes.md` § `GET /v1/names/{name}/records`.
> **Why**: the admitted ENS verified record-resolution path is fail-closed over direct/on-chain execution. Gateway continuation is outside that record support slice, so bigname reports the explicit unsupported reason instead of following transport or treating the outcome as a missing record.
> **Since**: `2026-04-18`

> **Basenames verified/explain public support narrowing** — bigname narrows the upstream Basenames L1Resolver and CCIP entrypoint into one first public support class instead of publishing every upstream-reachable non-`base.eth` path immediately.
> **Upstream**: `(upstream: .refs/basenames/README.md:L69 @ basenames@1809bbc)` `(upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc)` `(upstream: .refs/basenames/README.md:L71 @ basenames@1809bbc)` `(upstream: .refs/basenames/src/L1/L1Resolver.sol:L154 @ basenames@1809bbc)` `(upstream: .refs/basenames/src/L1/L1Resolver.sol:L173 @ basenames@1809bbc)`
> **Our rule**: `docs/api-v1-routes.md` § `GET /v1/names/{name}/records`; mirrored in `docs/execution.md` § Resolver-record lookup and `docs/manifests.md` § Basenames source-family ownership.
> **Why**: freeze the first Basenames consumer-replacement slice on the declared Base-authority plus L1-transport boundary before widening alias-participating, wildcard-derived, linked-subregistry, transport-free, or offchain-gateway path classes.
> **Since**: `2026-04-19`

> **Basenames declared primary-name value authority narrowing** — bigname treats ENSv1's Base `L2ReverseRegistrar` as the declared Basenames primary-name value authority for Base coin type `2147492101`, even though pinned upstream Basenames also ships a `ReverseRegistrar` that writes network-specific primary records.
> **Upstream**: Basenames describes its `ReverseRegistrar` as allowing registrants to establish a primary record and implements `claimForBaseAddr` / `setNameForAddr` on that contract `(upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L12 @ basenames@1809bbc)` `(upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L150 @ basenames@1809bbc)` `(upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L193 @ basenames@1809bbc)`. ENSv1's Base deployment records `L2ReverseRegistrar` at `0x0000000000D8e504002cC26E3Ec46D81971C1664`, emits `NameForAddrChanged(address,string)`, exposes `nameForAddr(address)`, and carries constructor coin type `2147492101` `(upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L2 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L98 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L154 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L391 @ ens_v1@91c966f)`.
> **Our rule**: `docs/manifests.md` § Basenames mainnet, `docs/api-v1-routes.md` § `GET /v1/addresses/{address}/primary-name`, `docs/projections.md` § Primary names, and `docs/storage.md` § Replay / rebuild safety classification.
> **Why**: keep declared Basenames primary-name values aligned with the ENSv1 Base L2 primary-name path while preserving the Basenames Base registry/registrar/resolver families as the declared exact-name, address-name, children, and record authority.
> **Since**: `2026-06-04`

> **Permission enumeration with partially indexed approval paths** — the manifest-scoped ENSv1 and Basenames registry `ApprovalForAll` logs from admitted Solidity registries are normalized into [account permission state](glossary.md#account-permission-state), and Project records their applicability through current registry ownership. The API synthesizes effective registry-operator rows from that account state and current binding, with request-relative partial reasons. NameWrapper `ApprovalForAll` logs are normalized into account permission state and fanned out per wrapped name, and NameWrapper `Approval` logs become per-token delegate rows; registrar token/operator and resolver operator/delegate approval logs remain retained [raw facts](glossary.md#raw-fact) without permission output, and the uncaptured Mainnet LLL registry approvals also remain outside this slice. ENSv2 registry `ApprovalForAll` logs are normalized into account permission state with no stored power, and Project keeps each registry entry's current token owner beside them; the API joins the two and serves each operator with the token owner's own token roles while the entry has not expired. `ens_v2_registry_operators` stays an unlisted surface for a registration or root of a registry no active manifest declares (one discovery admitted, or one only an inactive or retired declaration covers), whose code bigname does not read, and on every address-only read, which may reach such a registry. Permission summaries retain `operator_approval_surfaces_not_ingested` and remain request-relative partial rather than full or authoritative. NameWrapper resources are partial under `wrapper_parent_and_resolver_delegation_not_projected` because parent control of a wrapped subname is not enumerated as rows.
> **Upstream**: ENSv1 registry ownership checks include approved operators and `setApprovalForAll` persists that authority `(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L17-L20 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L108-L118 @ ens_v1@91c966f)`. BaseRegistrar accepts both per-token approvees and owner-wide operators for `reclaim` `(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L42-L50 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L171-L174 @ ens_v1@91c966f)`. PublicResolver accepts owner-wide operators and node delegates `(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L78-L103 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L114-L129 @ ens_v1@91c966f)`. Basenames has equivalent registry operator and resolver operator/delegate paths, while its registrar delegates authorization to ERC-721 approval checks `(upstream: .refs/basenames/src/L2/Registry.sol:L46-L52 @ basenames@1809bbc)` `(upstream: .refs/basenames/src/L2/Registry.sol:L148-L158 @ basenames@1809bbc)` `(upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L319-L329 @ basenames@1809bbc)` `(upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L448-L465 @ basenames@1809bbc)` `(upstream: .refs/basenames/src/L2/L2Resolver.sol:L141-L166 @ basenames@1809bbc)` `(upstream: .refs/basenames/src/L2/L2Resolver.sol:L180-L198 @ basenames@1809bbc)`. ENSv2's ERC-1155 base exposes owner-wide approval and `PermissionedRegistry` inherits approved-owner roles for non-root resources `(upstream: .refs/ens_v2/contracts/src/erc1155/ERC1155Singleton.sol:L70-L84 @ ens_v2@a971bd64)` `(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L575-L592 @ ens_v2@a971bd64)`. NameWrapper separately exposes token approval and owner/operator mutation paths `(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L124-L135 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L210-L221 @ ens_v1@91c966f)`.
> **Our rule**: `docs/projections.md` § Permissions and `docs/storage.md` § Projection publication.
> **Why**: account state represents future and current registry-owned names without per-name fan-out. Remaining approval paths keep the served permission contract partial.
> **Since**: `2026-08-27`

> **ENSv1 wrapper/resolver admission narrowing** — bigname admits the NameWrapper and declared PublicResolver generations on both the mainnet and Sepolia deployment profiles as source-family inputs for current declared-state normalization without claiming every upstream wrapper or resolver capability as supported public coverage. The narrowing is a property of the source families, not of one deployment profile: it applies wherever `ens_v1_wrapper_l1` or `ens_v1_resolver_l1` is admitted.
> **Upstream**: `(upstream: .refs/ens_v1/deployments/mainnet/NameWrapper.json:L2 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/deployments/sepolia/NameWrapper.json:L2 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/deployments/mainnet/PublicResolver.json:L2 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/deployments/sepolia/PublicResolver.json:L2 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/deployments/sepolia/LegacyPublicResolver.json:L2 @ ens_v1@91c966f)` `(upstream: .refs/ens_app_v3/src/constants/resolverAddressData.ts:L149-L221 @ ens_app_v3@7175858)` `(upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L27 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L35 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L37 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L38 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L479 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L500 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L5 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L13 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L114 @ ens_v1@91c966f)`
> **Our rule**: `docs/manifests.md` § capability ownership, § capability policy, and § ENSv1 (`sepolia` deployment profile); mirrored in `docs/architecture.md` § source families and § permissions, `docs/projections.md`, `docs/api-v1-routes.md`, `docs/consumer-capabilities.md`, and `docs/storage.md` § table families. Current wrapper fuse intake retains `PermissionScopeChanged` history and derives the wrapped/emancipated/locked lifecycle state. It does not synthesize wrapper-holder grants; when a compatible holder grant exists, projections apply wrapper expiry, `.eth` grace, and fuse masks to it. (upstream: .refs/ens_v1/contracts/wrapper/README.md:L32 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/README.md:L34 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L48 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L218 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L221 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L820 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L825 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L843 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L852 @ ens_v1@91c966f) A wrap of an existing registrar name can also retain stale pre-wrap control inputs internally, which public exact-name reads suppress behind the documented unsupported control summary.
> **Why**: bind the wrapper/resolver adapter boundary to source-family ownership, identity continuity, and declared resolver record state without overstating current permission publication. Wrapper-holder power materialization, wrapper-upgrade history, and migration history require their own evidence before those surfaces graduate.
> **Since**: `2026-04-21`

> **ENSIP-19 resolver-generation narrowing** — bigname derives an eligible requested EVM coin-type address from the projected default coin-type entry only when the selected direct resolver or current proxy implementation has the manifest-declared [`ensip19_default_address` resolver read feature](glossary.md#resolver-read-feature). The current Mainnet ENS PublicResolver, the Sepolia PublicResolver at `0xE99638b40E4Fff0129D56f03b55b6bbC4BBE49b5`, and the admitted archived-Sepolia ENSv2 `PermissionedResolver` implementation are flagged. Admitted legacy ENS resolver generations and the admitted legacy Basenames resolver remain unflagged. The fallback-bearing Basenames upgradeable resolver proxy is not admitted and is deferred to a follow-up admission decision. This is an authority narrowing, not a claim that every unflagged bytecode generation lacks fallback behavior. Coin type `2147483648` is the source key and is never a target.
> **Upstream**: `(upstream: .refs/ens_v1/contracts/utils/ENSIP19.sol:L9-L38 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L47-L85 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L20-L31 @ ens_v1@91c966f)` `(upstream: .refs/ens_app_v3/src/constants/resolverAddressData.ts:L32-L40 @ ens_app_v3@7175858)` `(upstream: .refs/ens_app_v3/src/constants/resolverAddressData.ts:L151-L166 @ ens_app_v3@7175858)` `(upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/PermissionedResolverImpl.json:L2 @ ens_v2@a971bd64)` `(upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/PermissionedResolverImpl.json:L2398 @ ens_v2@a971bd64)` `(upstream: .refs/basenames/test/Fork/BaseMainnetConstants.sol:L9-L14 @ basenames@1809bbc)` `(upstream: .refs/basenames/src/L2/L2Resolver.sol:L4-L32 @ basenames@1809bbc)` `(upstream: .refs/basenames/lib/ens-contracts/contracts/resolvers/profiles/AddrResolver.sol:L35-L61 @ basenames@1809bbc)` `(upstream: .refs/basenames/src/L2/UpgradeableL2Resolver.sol:L11-L40 @ basenames@1809bbc)` `(upstream: .refs/basenames/src/L2/resolver/AddrResolver.sol:L84-L99 @ basenames@1809bbc)`.
> **Our rule**: `docs/manifests.md` § Required fields, `docs/projections.md` § Resolver and records, and `docs/api-v1-routes.md` § `GET /v1/names/{name}/records`.
> **Why**: read derivation is admitted per resolver generation or active implementation. Event retention, source-family membership, and runtime code hashes do not authorize the getter behavior.
> **Since**: `2026-08-27`

> **ENSv1 registry-owner divergence as active authority** — bigname treats a live registrar lease whose ENS registry owner diverges from the registrar token holder as registry-only authority while the two differ.
> **Upstream**: ENS declares registry `NewOwner(bytes32,bytes32,address)` for subnode-owner assignments and `Transfer(bytes32,address)` for node-owner transfers `(upstream: .refs/ens_v1/contracts/registry/ENS.sol:L6 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/registry/ENS.sol:L9 @ ens_v1@91c966f)`. `ENSRegistry.setOwner` emits `Transfer`, and `ENSRegistry.setSubnodeOwner` writes the subnode owner and emits `NewOwner` `(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L63 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L68 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L75 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L80 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L81 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L82 @ ens_v1@91c966f)`. ENS registry resolver/TTL writes are authorized by the registry owner `(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L92 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L103 @ ens_v1@91c966f)`. BaseRegistrar registration with `updateRegistry` calls `ens.setSubnodeOwner`, and `reclaim` requires registrar ownership or approval before calling `ens.setSubnodeOwner` `(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L148 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L149 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L172 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L173 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L174 @ ens_v1@91c966f)`. NameWrapper wrap/unwrap moves both tokenized and registry authority, including `unwrapETH2LD` returning the registrar token and setting the registry owner `(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L264 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L268 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L390 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L391 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1029 @ ens_v1@91c966f)`.
> **Our rule**: `docs/architecture.md` § Identity model and `docs/adrs/0002-surface-resource-identity.md` § ENSv1 authority-anchor rules / worked examples.
> **Why**: registry owner divergence creates a registry-only authority interval while the registry owner differs from the registrar holder. Keeping that interval on the registrar `resource_id` would merge distinct control and permission histories. If registry-side `Transfer(bytes32,address)` / `setOwner` or `NewOwner(bytes32,bytes32,address)` / `reclaim` returns ownership to the same live unreleased registrar holder, bigname closes that interval and restores the prior registrar `resource_id` and `token_lineage_id`. After release, or when ownership returns to a different holder or controller, the interval remains distinct registry-only authority.
> **Since**: `2026-06-10`

> **ENSv1 generic resolver-event intake and declared-address classification narrowing** — bigname may retain generic ENSv1 resolver-local record events from match-all signature intake, but the schema-v2 project phase classifies only exact resolver addresses declared by the active manifest. The checked-in Mainnet and Sepolia manifests directly admit their first-party app known PublicResolver generations; one runtime still selects exactly one deployment profile. Classification permits projection of retained canonical observations; it does not claim complete family coverage, authorization semantics, or event-to-call parity. Runtime code hashes are deliberately not classification evidence.
> **Upstream**: `(upstream: .refs/ens_v1/contracts/registry/ENS.sol:L12 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L89 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L174 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/resolvers/profiles/IAddrResolver.sol:L6 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/resolvers/profiles/IAddressResolver.sol:L6 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/resolvers/profiles/ITextResolver.sol:L5 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/resolvers/profiles/IVersionableResolver.sol:L5 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L59 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L63 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L73 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L84 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/resolvers/profiles/TextResolver.sol:L20 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/resolvers/profiles/TextResolver.sol:L21 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/resolvers/profiles/TextResolver.sol:L28 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/resolvers/profiles/TextResolver.sol:L32 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L20 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L31 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L131 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L150 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/resolvers/ResolverBase.sol:L17 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/resolvers/ResolverBase.sol:L21 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/resolvers/ResolverBase.sol:L22 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/resolvers/ResolverBase.sol:L23 @ ens_v1@91c966f)`
> **ENS app known-resolver metadata**: `(upstream: .refs/ens_app_v3/src/constants/resolverAddressData.ts:L32-L147 @ ens_app_v3@7175858)` `(upstream: .refs/ens_app_v3/src/constants/resolverAddressData.ts:L149-L221 @ ens_app_v3@7175858)`
> **Our rule**: `docs/manifests.md` § ENSv1 NameWrapper and PublicResolver admission and § capability policy; mirrored in `docs/storage.md`, `docs/projections.md`, `docs/api-v1-routes.md` (name and resolver routes), and `docs/consumer-capabilities.md`.
> **Why**: a registry-observed resolver is not the same as a manifest-declared resolver. Unknown dynamic resolvers and unsupported legacy interfaces remain explicitly unsupported even when generic resolver events were retained.
> **Since**: `2026-04-21`

> **Basenames match-all Base resolver intake** — bigname selects the resolver manifest's ENS-specific event signatures across every Base emitter. Registry `NewResolver` observations update only the name's resolver pointer; they do not admit a watched contract instance or create a discovery edge. Current record visibility still requires that pointer to match the record emitter. The widened live scope does not retroactively supply Base resolver history, so the mandatory one-time historical fetch remains a separate ingest operation.
> **Upstream**: `(upstream: .refs/basenames/src/L2/Registry.sol:L19 @ basenames@1809bbc)` `(upstream: .refs/basenames/src/L2/Registry.sol:L132 @ basenames@1809bbc)` `(upstream: .refs/basenames/src/L2/Registry.sol:L223 @ basenames@1809bbc)`
> **Our rule**: `docs/manifests.md` § Basenames source-family ownership and § Discovery; mirrored in `docs/chain-intake.md` § ENSv1 and Basenames resolver intake.
> **Why**: signature selection retains resolver-local history without turning the emitter into a watched instance. A Base-side resolver lacking supported `L2Resolver`-compatible profile admission does not satisfy declared record reads. The L1 resolver and offchain gateways are separate surfaces.
> **Since**: `2026-04-21`

> **Basenames declared-address resolver narrowing** — a current resolver pointer or match-all-selected resolver event may identify an emitter, but the schema-v2 project phase classifies it as supported only when its exact address is declared by the active `basenames_base_resolver` manifest. Runtime code hashes are deliberately not classification evidence.
> **Upstream**: `(upstream: .refs/basenames/src/L2/Registry.sol:L132 @ basenames@1809bbc)` `(upstream: .refs/basenames/src/L2/L2Resolver.sol:L4 @ basenames@1809bbc)` `(upstream: .refs/basenames/src/L2/L2Resolver.sol:L16 @ basenames@1809bbc)` `(upstream: .refs/basenames/src/L2/L2Resolver.sol:L22 @ basenames@1809bbc)` `(upstream: .refs/basenames/src/L2/L2Resolver.sol:L29 @ basenames@1809bbc)` `(upstream: .refs/basenames/src/L2/L2Resolver.sol:L182 @ basenames@1809bbc)` `(upstream: .refs/basenames/src/L2/L2Resolver.sol:L193 @ basenames@1809bbc)` `(upstream: .refs/basenames/src/L2/L2Resolver.sol:L209 @ basenames@1809bbc)` `(upstream: .refs/basenames/src/L2/L2Resolver.sol:L225 @ basenames@1809bbc)`
> **Our rule**: `docs/manifests.md` § Basenames source-family ownership and § capability policy; mirrored in `docs/architecture.md`, `docs/storage.md`, `docs/projections.md`, `docs/api-v1-routes.md` (name and resolver routes), and `docs/consumer-capabilities.md`.
> **Why**: match-all selection and resolver-pointer state are separate from exact manifest admission. The declared-address gate is also separate from Basenames L1 transport and execution.
> **Since**: `2026-04-22`

<a id="basenames-contenthash-admission-narrowing"></a>
> **Basenames Base resolver record-family admission narrowing** — bigname derives the seven Base resolver events declared by `basenames_base_resolver`: `ABIChanged`, `AddrChanged`, `AddressChanged`, `ContenthashChanged`, `NameChanged`, `TextChanged`, and `VersionChanged`. The `L2Resolver`'s DNS, interface, and public-key events are not declared, so those record families are not normalized even though the contract emits them.
> **Upstream**: Basenames' `L2Resolver` composes the ABI, address, contenthash, DNS, interface, name, public-key, and text profiles `(upstream: .refs/basenames/src/L2/L2Resolver.sol:L29-L38 @ basenames@1809bbc)`; its contenthash profile is ENS's, whose `setContenthash` writes the versioned value and emits `ContenthashChanged` `(upstream: .refs/basenames/src/L2/L2Resolver.sol:L6 @ basenames@1809bbc)` `(upstream: .refs/basenames/lib/ens-contracts/contracts/resolvers/profiles/ContentHashResolver.sol:L16-L22 @ basenames@1809bbc)`.
> **Our rule**: `manifests/mainnet/base/basenames/basenames_base_resolver/v1.toml` is the event-family admission boundary; `docs/api-v1-routes.md` keeps the record-key grammar closed to the undeclared families.
> **Why**: keep a narrow, replayable Base record-family contract. Adding an omitted normalized resolver family requires deliberate manifest/adapter admission rather than being inferred from compatible bytecode. `ContenthashChanged` was admitted on 2026-09-29 (TYR-89 follow-up); before that the contenthash family was also excluded.
> **Since**: `2026-07-10`

> **ENSv1 old-registry admission narrowing** — bigname may admit `ENSRegistryOld` as old-registry [fallback-handoff](glossary.md#registry-fallback-handoff) input under `ens_v1_registry_l1`, but it does not treat the current registry `startBlock: 9380380` as original ENS history and does not union old and current registry logs by latest block.
> **Upstream**: `(upstream: .refs/ens_subgraph/subgraph.yaml:L10 @ ens_subgraph@723f1b6)` `(upstream: .refs/ens_subgraph/subgraph.yaml:L15 @ ens_subgraph@723f1b6)` `(upstream: .refs/ens_subgraph/subgraph.yaml:L39 @ ens_subgraph@723f1b6)` `(upstream: .refs/ens_subgraph/subgraph.yaml:L42 @ ens_subgraph@723f1b6)` `(upstream: .refs/ens_subgraph/subgraph.yaml:L44 @ ens_subgraph@723f1b6)` `(upstream: .refs/ens_subgraph/src/ensRegistry.ts:L134 @ ens_subgraph@723f1b6)` `(upstream: .refs/ens_subgraph/src/ensRegistry.ts:L230 @ ens_subgraph@723f1b6)` `(upstream: .refs/ens_subgraph/src/ensRegistry.ts:L238 @ ens_subgraph@723f1b6)` `(upstream: .refs/ens_subgraph/src/ensRegistry.ts:L246 @ ens_subgraph@723f1b6)` `(upstream: .refs/ens_subgraph/src/ensRegistry.ts:L252 @ ens_subgraph@723f1b6)` `(upstream: .refs/ens_subgraph/src/ensRegistry.ts:L259 @ ens_subgraph@723f1b6)` `(upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L18 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L29 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L40 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L153 @ ens_v1@91c966f)`
> **Our rule**: `docs/manifests.md` § Required Fields and § Capability Policy; mirrored in `docs/architecture.md` § Source Families / Source Manifests And Capability Registry, `docs/chain-intake.md` § ENSv1 and Basenames resolver intake, `docs/storage.md` § ID Strategy and § Table Families And Write Ownership, and `docs/consumer-capabilities.md` § Current Status. Unlike the ENS subgraph, which sets `isMigrated` only in its current-registry `NewOwner` handler and leaves `Transfer` to update the owner, bigname treats both current-registry ownership events as the fallback handoff `(upstream: .refs/ens_subgraph/src/ensRegistry.ts:L131-L135 @ ens_subgraph@723f1b6)` `(upstream: .refs/ens_subgraph/src/ensRegistry.ts:L146-L165 @ ens_subgraph@723f1b6)`. Solidity `setOwner` writes the current record before emitting `Transfer`, and fallback resolver reads delegate only while that record does not exist `(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L60-L68 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L75-L82 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L48-L54 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L150-L172 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L18-L24 @ ens_v1@91c966f)`.
> **Why**: preserve current-registry topology and resolver truth after the fallback handoff, keep the root resolver as the explicit old-registry exception, and prevent historical backfill or old-registry admission from graduating coverage or consumer replacement without route-level evidence.
> **Since**: `2026-04-24`

> **ENSv2 ETHRegistry cutover suffix anchor** — upstream canonical-name reconstruction terminates only when its registry walk reaches the supplied `RootRegistry`; during the ENSv2 cutover window, bigname additionally treats the manifest-declared `ETHRegistry` as a suffix anchor.
> **Upstream**: `findCanonicalName` walks until the current registry equals the supplied root registry. (upstream: .refs/ens_v2/contracts/src/universalResolver/libraries/LibRegistry.sol:L79 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/universalResolver/libraries/LibRegistry.sol:L88 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/universalResolver/libraries/LibRegistry.sol:L89 @ ens_v2@a971bd64)
> **Our rule**: `docs/manifests.md` § Discovery admission; mirrored in `docs/architecture.md` § Discovery graph.
> **Why**: preserve stable `.eth`-rooted names across the staged ENSv2 cutover while requiring the bidirectional parent claim and subregistry pointer below that anchor.
> **Since**: `2026-08-01`

> **ENSv2 renewal payload compatibility alias** — post-audit `NameRenewed` calls its terminal payment value `amount`. For post-audit logs, bigname publishes that canonical `amount` field and also retains `base` with the same value in normalized `RegistrationRenewed.after_state`; explicitly decoding a two-topic log admitted by the deprecated pre-audit manifest preserves its historical `base`-only shape. Deprecated emitter addresses are not part of the active post-audit replay plan.
> **Upstream**: The deprecated pre-audit `IETHRegistrar.NameRenewed` declaration indexes only `tokenId` and ends with `uint256 base` `(upstream: .refs/ens_v2_sepolia_dev/contracts/src/registrar/interfaces/IETHRegistrar.sol:L53 @ ens_v2_sepolia_dev@554c309)` `(upstream: .refs/ens_v2_sepolia_dev/contracts/src/registrar/interfaces/IETHRegistrar.sol:L54 @ ens_v2_sepolia_dev@554c309)` `(upstream: .refs/ens_v2_sepolia_dev/contracts/src/registrar/interfaces/IETHRegistrar.sol:L59 @ ens_v2_sepolia_dev@554c309)` `(upstream: .refs/ens_v2_sepolia_dev/contracts/src/registrar/interfaces/IETHRegistrar.sol:L60 @ ens_v2_sepolia_dev@554c309)`. Post-audit `IETHRenewer.NameRenewed` also indexes `referrer` and ends with `uint256 amount` `(upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRenewer.sol:L30 @ ens_v2@a971bd64)` `(upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRenewer.sol:L31 @ ens_v2@a971bd64)` `(upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRenewer.sol:L36 @ ens_v2@a971bd64)` `(upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRenewer.sol:L37 @ ens_v2@a971bd64)`.
> **Our rule**: `docs/architecture.md` § Normalized Event Taxonomy.
> **Why**: expose the audited upstream vocabulary without changing the shape of already-persisted historical renewal events or dropping their published `base` key.
> **Since**: `2026-07-10`

> **Direct PublicResolverV2 record admission narrowing** — an owned-chain manifest or the official Sepolia deployment can declare an exact `public_resolver_v2` address with `proxy_kind = "none"` in `ens_v2_resolver_l1`. Its admitted node-event set is limited to `ABIChanged`, `AddrChanged`, `AddressChanged`, value-bearing `TextChanged`, `ContenthashChanged`, `NameChanged`, and `VersionChanged`. Reusing the node decoder does not classify it as ENSv1 or attach a stale ENSv1 resource. Current ENSv2 pointer, namespace, and emitter matching govern Project inventory attribution.
> **Upstream**: PublicResolverV2 composes ABI, address, contenthash, name, and text profiles, among others (upstream: .refs/ens_v2_sepolia_20260903/contracts/src/resolver/PublicResolverV2.sol:L23-L35 @ ens_v2_sepolia_20260903@5da83f6). Authorization resolves a NameWrapper-known node through current exact ENSv2 ownership or owner approvals (upstream: .refs/ens_v2_sepolia_20260903/contracts/src/resolver/PublicResolverV2.sol:L174-L184 @ ens_v2_sepolia_20260903@5da83f6) (upstream: .refs/ens_v2_sepolia_20260903/contracts/src/resolver/PublicResolverV2.sol:L192-L194 @ ens_v2_sepolia_20260903@5da83f6). The inherited address setter emits `AddressChanged` and additionally `AddrChanged` for coin type 60, preserving bytes in versioned storage (upstream: .refs/ens_v1_publicresolver_5141a2a/contracts/resolvers/profiles/AddrResolver.sol:L47-L65 @ ens_v1_publicresolver_5141a2a@5141a2a). Text and contenthash setters preserve their values, including empty values (upstream: .refs/ens_v1_publicresolver_5141a2a/contracts/resolvers/profiles/TextResolver.sol:L15-L21 @ ens_v1_publicresolver_5141a2a@5141a2a) (upstream: .refs/ens_v1_publicresolver_5141a2a/contracts/resolvers/profiles/ContentHashResolver.sol:L14-L19 @ ens_v1_publicresolver_5141a2a@5141a2a). `clearRecords` increments the node version and emits `VersionChanged` (upstream: .refs/ens_v1_publicresolver_5141a2a/contracts/resolvers/ResolverBase.sol:L20-L22 @ ens_v1_publicresolver_5141a2a@5141a2a).
> **Our rule**: `docs/manifests.md` § Direct PublicResolverV2 declarations on an owned local chain; `docs/consumer-capabilities.md` § Direct PublicResolverV2 record support; `docs/storage.md` § Interpret process memory. These supplementary pins prove source semantics only. Exact local deployment address/start/provenance and runtime acceptance require separate evidence. PermissionedResolver proxies retain canonical upgrade-history checks, and the official Sepolia profile uses the pinned deployment artifacts recorded in [deployment coverage](sepolia-deployment.md). This direct node-record path establishes no exhaustive binding, alias, permission, or selector enumeration and adds no schema, REST, or record-ID vocabulary.
> **Why**: admit only the demonstrated node-record family under exact manifest authority without treating compatible events or source availability as deployment admission or runtime success.
> **Since**: `2026-09-09`

<a id="ensv2-data-event-admission-narrowing"></a>
> **Official Sepolia deployment admission** — `manifests/sepolia` now selects the 2026-10-01 redeploy, which replaced the 2026-09-15 deployment in place; the 2026-09-15 contracts are dropped, not kept as retired history. The [deployment inventory](sepolia-deployment.md) lists every upstream address, its intake or execution role, and exclusions. The previous June and hackathon profiles are removed. (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/.deployment.json:L4 @ ens_v2_sepolia_20261001@07e55a05)
> **Our rule**: exact receipt-based starts, canonical ENSv1 dependencies, both declared verified authority arms, implementation-based resolver discovery, and the existing direct PublicResolverV2 subset. Helpers do not grant additional indexed capability. DNS, smart-account sessions, pricing, and exhaustive selector inventories remain outside this deployment change.
> **Since**: `2026-09-16`; the 2026-10-01 redeploy since `2026-10-02`

> **Retired automatic-bootstrap start-block narrowing** — the deleted old indexer treated manifest `start_block` as optional inclusive bootstrap metadata for `[[roots]]` and `[[contracts]]`, not as inferred deployment truth. Among the targets that bootstrap covered, the mainnet ENSv1 registry and `.eth` registrar values were reference candidates from `ens_subgraph` only, while ENSv1 NameWrapper, PublicResolver, ReverseRegistrar, and post-audit ENSv2 Sepolia RootRegistry / ETHRegistry / ETHRegistrar values came from pinned deployment receipt metadata — for the ENSv2 rows, the receipts of the admitted 2026-06-29 Sepolia deployment, which upstream now keeps under `contracts/deployments/sepolia-20260629-r1/`. That split records how each of those specific targets was sourced; it is not a provenance rule for the families they belong to. The Sepolia ENSv1 registry, superseded registry, and NameWrapper rows admitted later all source their start blocks from pinned ENSv1 deployment receipts, as the provenance table in `docs/manifests.md` records. Basenames mainnet source families and ENS UniversalResolver remain unknown, so that bootstrap skipped those targets instead of defaulting to block zero or job-range start. Manifest storage still preserves an omitted value as null. Runtime watch and Interpret selection now use block zero as its conservative lower bound within an admitted phase range; this authorizes intake without treating zero as deployment provenance.
> **Upstream**: `(upstream: .refs/ens_subgraph/subgraph.yaml:L15 @ ens_subgraph@723f1b6)` `(upstream: .refs/ens_subgraph/subgraph.yaml:L122 @ ens_subgraph@723f1b6)` `(upstream: .refs/ens_v1/deployments/mainnet/NameWrapper.json:L1498 @ ens_v1@91c966f)` `(upstream: .refs/ens_subgraph/subgraph.yaml:L200 @ ens_subgraph@723f1b6)` `(upstream: .refs/ens_v1/deployments/mainnet/PublicResolver.json:L1104 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/deployments/mainnet/ReverseRegistrar.json:L379 @ ens_v1@91c966f)` `(upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/RootRegistry.json:L2792 @ ens_v2@a971bd64)` `(upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/ETHRegistry.json:L2792 @ ens_v2@a971bd64)` `(upstream: .refs/ens_v2/contracts/deployments/sepolia-20260629-r1/ETHRegistrar.json:L1372 @ ens_v2@a971bd64)`
> **Our rule**: `docs/manifests.md` § required fields, § watch-plan expansion,
> and § Bootstrap `start_block` provenance; mirrored in `docs/architecture.md`
> § Source manifests and `docs/storage.md`.
> **Why**: keep automatic historical bootstrap from silently widening unknown source history, address-only target identity, or chain checkpoint state.
> **Since**: `2026-04-22`

> **Sepolia ENSv1 BaseRegistrar start block from reference-indexer metadata** — bigname directly admits Sepolia BaseRegistrar `0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85` from block `3702731`. The pinned ENSv1 deployment artifact supports the address but not that historical floor: it records `numDeployments = 2`, and its retained receipt belongs to `0xa2c122be93b0074270ebee7f6b7292c7deb45047`. The floor therefore follows the pinned ENS subgraph network metadata even though `ens_subgraph` is normally a cross-check-only reference.
> **Upstream**: The ENSv1 artifact names the admitted address `(upstream: .refs/ens_v1/deployments/sepolia/BaseRegistrarImplementation.json:L2 @ ens_v1@91c966f)`, while its receipt and deployment count show why the receipt block cannot prove that address's deployment `(upstream: .refs/ens_v1/deployments/sepolia/BaseRegistrarImplementation.json:L733-L768 @ ens_v1@91c966f)`. The reference indexer pairs the admitted address with block `3702731` `(upstream: .refs/ens_subgraph/networks.json:L47-L49 @ ens_subgraph@723f1b6)`.
> **Our rule / why**: `docs/manifests.md` § ENSv1 (`sepolia` deployment profile) and § Bootstrap `start_block` provenance admit the registrar from the selected reference-indexer ingest floor for ordinary ENSv1 predecessor and fallback state. Citing the stale receipt would attribute a superseded deployment's block to the active address; this is an explicit exception to deployment-receipt authority, not a general widening of reference-indexer authority.
> **Since**: `2026-08-20`

> **Sepolia receipt-backed registrar controllers remain unadmitted under the numeric-gap disposition** — bigname does not admit `LegacyETHRegistrarController` at `0x7e02892cfc2Bfd53a75275451d73cF620e793fc0` from block `3790197` or `ETHRegistrarController` at `0xfb3cE5D01e0f33f41DbB39035dB9745962F1f968` from block `8579988`.
> **Upstream**: The Legacy controller artifact records the first address and its successful deployment receipt `(upstream: .refs/ens_v1/deployments/sepolia/LegacyETHRegistrarController.json:L2 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/deployments/sepolia/LegacyETHRegistrarController.json:L562-L599 @ ens_v1@91c966f)`. The later controller artifact records the second address and retained receipt `(upstream: .refs/ens_v1/deployments/sepolia/ETHRegistrarController.json:L2 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/deployments/sepolia/ETHRegistrarController.json:L680-L720 @ ens_v1@91c966f)`.
> **Our rule / why**: `docs/manifests.md` § ENSv1 (`sepolia` deployment profile) adopts #515 option (b). Admitting one or both receipt-backed controllers would only partially widen label-bearing registration intake. The two separately deployed wrapped registrar controllers are admitted for their renewals only (see the note below). A later controller capability slice owns any such admission.
> **Since**: `2026-08-20`

> **Second Sepolia wrapped registrar controller admitted from chain evidence** — bigname admits the Sepolia `WrappedETHRegistrarController` at `0xFED6a969AaA60E4961FCD3EBF1A2e8913ac65B72` from its receipt-backed deployment block `3790244`, and a second NameWrapper-enabled wrapped controller at `0x4477cAc137F3353Ca35060E01E5aEb777a1Ca01B` from block `7035078` under the role `wrapped_registrar_controller_4477cac`, both for renewals only. No pinned reference names the second address, so its address and floor rest on chain receipts rather than a deployment artifact. No pinned reference records either controller's NameWrapper activation, so both activations rest on chain logs.
> **Upstream**: The first controller's deployment artifact, pinned with the Basenames `ens-contracts` checkout, records the address, a successful creation receipt at block `3790244` and the Sepolia NameWrapper as a constructor argument `(upstream: .refs/basenames/lib/ens-contracts/deployments/sepolia/ETHRegistrarController.json:L2 @ basenames@1809bbc)` `(upstream: .refs/basenames/lib/ens-contracts/deployments/sepolia/ETHRegistrarController.json:L604 @ basenames@1809bbc)` `(upstream: .refs/basenames/lib/ens-contracts/deployments/sepolia/ETHRegistrarController.json:L654-L656 @ basenames@1809bbc)` `(upstream: .refs/basenames/lib/ens-contracts/deployments/sepolia/ETHRegistrarController.json:L665-L668 @ basenames@1809bbc)`. No pinned reference records its activation, which rests on chain evidence: the Sepolia NameWrapper's `ControllerChanged(0xFED6a969…, true)` at block `3790246` (transaction `0x3d990937aa69cf6202d71914d48648c1f0ea48cc9f744f20aaecc0ae6f29c041`, log 335) enabled it, and that is the only `ControllerChanged` for the address through block `11807218`, so it was never disabled. For the second: on chain it was created at block `7035077` (transaction `0x42f5bd5cd19b7bb4775a05dd1517deab393abc2f53b4c132f5538c15ed639f18`, receipt status 1, contract address `0x4477cAc137F3353Ca35060E01E5aEb777a1Ca01B`) and enabled by the Sepolia NameWrapper's `ControllerChanged(0x4477cAc1…, true)` at block `7035078` (transaction `0xcc030c7df411235634713641bc9685c7f0fc6582436adc991eaeac6aff67ef89`, log 19), an event and only-owner setter the pinned NameWrapper declares `(upstream: .refs/ens_v1/contracts/wrapper/Controllable.sol:L9-L19 @ ens_v1@91c966f)`. That is the only `ControllerChanged` for the address through block `11804114`, so it was never disabled. Its label-bearing `NameRenewed` has the same topic as the first controller's (`0x3da24c02…`), and through block `11800628` it emitted 167 of them, the first at block `7077533`. `NameWrapper.renew` is `onlyController` and writes the wrapper expiry with no event `(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L312-L337 @ ens_v1@91c966f)`.
> **Our rule / why**: `docs/manifests.md` § ENSv1 (`sepolia` deployment profile). A wrapped controller's renewal is the only fact that carries the wrapper expiry, so leaving the second controller out kept a stale wrapper expiry on every name it renewed. The chain receipts pin the address and the block from which the NameWrapper accepts its renewals, which is what the floor needs; this is an explicit exception to deployment-artifact authority for this one address, not a general widening. Contract roles are singleton declaration names within a manifest version, so the second controller has its own role, which the registrar adapter treats exactly as `wrapped_registrar_controller`.
> **Since**: `2026-09-29`

> **Sepolia Universal Resolver proxies admitted from chain evidence** — bigname admits `Upgraded(address)` from the Sepolia client-facing Universal Resolver proxy `0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe` from block `8928790` and from the managed proxy `0x6d80F2172CFdEc5730fE683860C33d26fC42e6F1` it points at from block `10922008`, and lists the 2026-10-01 redeploy's UniversalResolverV2 `0x24e1d8e068620b647ca097f961a61055f4f42d72` as the implementation that marks the [Universal Resolver cutover](glossary.md#universal-resolver-cutover). The pinned artifacts name all three addresses but carry no receipts, so both start blocks and the upgrade history rest on chain logs.
> **Upstream**: the artifacts record the addresses and ABIs `(upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/UpgradableUniversalResolverProxy.json:L2 @ ens_v2_sepolia_20261001@07e55a05)` `(upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/ManagedUniversalResolverProxy.json:L2 @ ens_v2_sepolia_20261001@07e55a05)` `(upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/UniversalResolverV2.json:L2 @ ens_v2_sepolia_20261001@07e55a05)`; the proxy forwards every call to its implementation, its constructor sets the first implementation without an event, and `upgradeTo` emits `Upgraded` `(upstream: .refs/ens_v2_sepolia_20260916/contracts/src/universalResolver/UpgradableUniversalResolverProxy.sol:L71-L75 @ ens_v2_sepolia_20260916@366de741)` `(upstream: .refs/ens_v2_sepolia_20260916/contracts/src/universalResolver/UpgradableUniversalResolverProxy.sol:L83-L84 @ ens_v2_sepolia_20260916@366de741)` `(upstream: .refs/ens_v2_sepolia_20260916/contracts/src/universalResolver/UpgradableUniversalResolverProxy.sol:L111-L115 @ ens_v2_sepolia_20260916@366de741)`. On chain the client-facing proxy was created at block `8928790` (constructor `AdminChanged`, transaction `0x174522103c23879297dbfc7b089897bc6f0d00ba26fee5f5041fd16e6563efb7`, log 108) and the managed proxy at block `10922008` (transaction `0xaa61fc98451c1fcc271e0184a3c016fb940e90402a0c812d917591916199ab37`, log 215). The client-facing proxy has one `Upgraded`, to the managed proxy, at block `10928435` (transaction `0xa4e0ad04ea2f3a0043df1345936340feabd46d72c64492c9f0a24b420ef58e30`, log 511). The managed proxy's are: `0x2f8a1806…` at `10928444` (`0x7dc02b35bf4684b250dab2f4d12fa0ef4ba1f8c5fbd6d9a935df91c6ef93f485`, log 606); `0x85edf8b6…` at `11163906` (`0x8119e98c207cfa8096a04cb3bc7890a829004576488168b9e978cc001ead5aae`, log 924); back to `0x2f8a1806…` at `11171315` (`0xdb49c85e266e85d4e0050c85ccbd3fab377a71eb59543e5b19ccb2f02b2e7311`, log 1201); `0x4a1817d1…` at `11390403` (`0x15276405247e511b2956bb1e4ef02506b6fb62659cfe48da84b093a5f66ad0fa`, log 364); `0xc1059765…` at `11708897` (`0xbe11f6ed1973b02570f4d0ad8a84560c5a849cacf59ba20126d9fa3bc1903dd4`, log 207); `0x5d25c1d6…`, the dropped 2026-09-15 deployment's implementation, at `11710193` (`0x86274e8c365fd11e8adac214c87e0dbe4b628fe5472bb4792ff24e2af7d830bf`, log 324); and `0x24e1d8e0…` at `11821680` (`0x6e1bf0d7822142b8aae5b34ddf2545eb313a61b00c06730cbb839ce024db15d8`, log 248). The proxies' implementation slots at the head (block `11827257`, 2026-10-02) match: the client-facing proxy holds the managed proxy, and the managed proxy holds `0x24e1d8e0…`.
> **Our rule / why**: `docs/manifests.md` § `universal_resolver_implementations` and § ENS execution (`sepolia` deployment profile). Which implementation clients resolve through decides whether a reserved name's ENSv2 expiry applies and whether a `.eth` name without a live ENSv2 entry resolves, and only the proxies' own events record it per block. Starting both proxies at their creation blocks reads every upgrade; the earlier implementations, including the dropped 2026-09-15 deployment's `0x5d25c1d6…`, are left unlisted, so the Sepolia cutover is block `11821680` and blocks from `11710193` to `11821679`, when the dropped deployment answered resolution, read as not cut over.
> **Since**: `2026-09-29`; cutover moved to `11821680` on `2026-10-02`

> **Intake retention floor widening over the reference client's own guard** — bigname refuses an ingest range whose start falls below the lowest block a local reth datadir can serve, using reth's expired-history floor raised to the lowest block still covered by its receipt static files. reth's own RPC refuses only below the expired-history floor, so a node whose receipts were pruned while its transactions were kept answers such a range with empty logs upstream while bigname refuses the plan. bigname also refuses at the read itself: a fetched block whose receipt count contradicts the transaction count in its retained body indices fails the log read, where reth serves the same block as having no logs. That refusal reaches one upstream-healthy configuration — a node pruning receipts by log filter keeps only the receipts its filter matched, so every other bloom-positive block fails our read and such a node cannot serve historical intake.
> **Upstream**: reth's `eth_getLogs` reads `earliest_block_number()` and returns `PrunedHistoryUnavailable` when the requested range starts lower `(upstream: .refs/reth/crates/rpc/rpc/src/eth/filter.rs:L597 @ reth@189c0df3)` `(upstream: .refs/reth/crates/rpc/rpc/src/eth/filter.rs:L599 @ reth@189c0df3)`; that floor is the expired-history height `(upstream: .refs/reth/crates/storage/provider/src/providers/database/mod.rs:L723 @ reth@189c0df3)`, which tracks the lowest transaction static file `(upstream: .refs/reth/crates/storage/provider/src/providers/static_file/manager.rs:L1226 @ reth@189c0df3)` `(upstream: .refs/reth/crates/storage/provider/src/providers/static_file/manager.rs:L1229 @ reth@189c0df3)`. Pruning receipts separately deletes whole receipt static-file ranges `(upstream: .refs/reth/crates/prune/prune/src/segments/receipts.rs:L34 @ reth@189c0df3)` `(upstream: .refs/reth/crates/prune/prune/src/segments/mod.rs:L41 @ reth@189c0df3)`, a deleted range reads back as no rows and no error `(upstream: .refs/reth/crates/storage/provider/src/providers/static_file/manager.rs:L2047 @ reth@189c0df3)` `(upstream: .refs/reth/crates/storage/provider/src/providers/static_file/manager.rs:L2049 @ reth@189c0df3)`, and reth's own log filter contributes no logs for a block whose receipts are missing or empty instead of failing `(upstream: .refs/reth/crates/rpc/rpc/src/eth/filter.rs:L1282 @ reth@189c0df3)` `(upstream: .refs/reth/crates/rpc/rpc/src/eth/filter.rs:L1289 @ reth@189c0df3)`. A node that keeps receipts in database tables rather than static files reports no receipt floor to us and is bounded only by the expired-history floor `(upstream: .refs/reth/crates/storage/provider/src/either_writer.rs:L193 @ reth@189c0df3)` `(upstream: .refs/reth/crates/storage/provider/src/either_writer.rs:L195 @ reth@189c0df3)`.
> **Our rule**: `docs/chain-intake.md` § Download range planning.
> **Why**: intake reads the datadir directly for logs rather than going through the RPC layer, so the receipt segment — not the transaction segment — bounds what it can actually serve. That segment start is an optimistic bound rather than a completeness guarantee, because reth advances a receipt static file's block position before deciding whether to write that block's receipts `(upstream: .refs/reth/crates/storage/provider/src/providers/database/provider.rs:L2635 @ reth@189c0df3)` `(upstream: .refs/reth/crates/storage/provider/src/providers/database/provider.rs:L2643 @ reth@189c0df3)`, which is why the read-level refusal carries the guarantee. Coverage must be explicit, and a silently empty pruned window is indistinguishable from real absence downstream. The read-level refusal covers what no floor can express — receipts pruned out of database tables, and a partial receipt list, which would otherwise attribute a log to the wrong transaction — and errs toward refusing a node we could partly read rather than recording a window we cannot stand behind.
> **Since**: `2026-08-06`

> **ENSv1→ENSv2 migration is single-chain; upstream wording still says "namechain"** — bigname documents the ENSv1→ENSv2 migration as happening entirely on one chain, and admits no ENSv2 source family on any chain other than the one the registries are deployed to. Pinned upstream source still carries a doc-comment describing migration as moving wrapped names into a second, separate registry system, left over from the cancelled ENSv2 L2 (Namechain) design.
> **Upstream**: `WrapperRegistry` documents itself as "supporting migration of wrapped names into the namechain registry system" `(upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L26-L27 @ ens_v2@a971bd64)`. The implemented behavior is same-chain throughout: each migrated locked name gets its own `WrapperRegistry` proxy deployed by `VerifiableFactory.deployProxy` with the name's namehash as the CREATE2 salt `(upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L151 @ ens_v2@a971bd64)` `(upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L153 @ ens_v2@a971bd64)`, bound to its parent entry by a `SubregistryUpdated` emit on the same registry `(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L475 @ ens_v2@a971bd64)`, and a not-yet-migrated name resolves through a fallback resolver that reads the ENSv1 registry directly rather than crossing a chain boundary `(upstream: .refs/ens_v2/contracts/src/resolver/ENSV1Resolver.sol:L40 @ ens_v2@a971bd64)`. No bridge, message passing, or second chain appears anywhere in the mechanism.
> **Our rule**: `docs/glossary.md` § ENSv1→ENSv2 migration and the migration-mechanism entries that follow it.
> **Why**: the comment is vestigial wording from a design that was cancelled, and the file it sits in is one an indexer author would read first. Recording the divergence keeps a future reader from citing that comment as evidence that bigname must index an ENSv2 deployment on a second chain.
> **Since**: `2026-08-06`

<a id="ensv1-lll-era-registry-word-decoding"></a>
> **ENSv1 LLL-era registry logs with unmasked argument words decode as the word's low bytes** — for the `ens_v1_registry_l1` family, bigname decodes the address slot of `NewOwner`/`NewResolver`/`Transfer` data as the low 20 bytes of an exactly-32-byte word, and validates the uint64 slot of `NewTTL` data as the word's low 8 bytes, rather than rejecting a word whose bytes above the declared slot width are nonzero as a strict ABI type check does. A masked `NewOwner`/`Transfer` owner word is recorded with explicit markers but is never granted authority. The masked tail never appears in interpreter state, permission grants, effective-controller relations, or composed name control. It remains visible in the child row's owner display field and in resolver addresses, exactly as the fallback registry's delegated reads return it, and the source normalized event always carries the corresponding marker fields. A prior registry-direct authority closes.
> **Upstream**: the pinned ENSv1 registry stores and emits resolver updates from a typed Solidity `address`, so its logged words are always zero-padded `(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L93 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L94 @ ens_v1@91c966f)`, and likewise stores and emits TTL updates from a typed `uint64` `(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L104 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L105 @ ens_v1@91c966f)`. The 2017 deployment those fallback reads delegate to is the LLL registry pinned as `ens_v1_lll`, whose source shows the dirty-word mechanism directly: each setter stores the raw 32-byte calldata word without masking it to the declared slot width — the owner setter loads `new-owner` from calldata `(upstream: .refs/ens_v1_lll/contracts/ENS.lll:L194 @ ens_v1_lll@7e377df)` and stores it whole `(upstream: .refs/ens_v1_lll/contracts/ENS.lll:L200 @ ens_v1_lll@7e377df)` through an unmasked `sstore` `(upstream: .refs/ens_v1_lll/contracts/ENS.lll:L73 @ ens_v1_lll@7e377df)` `(upstream: .refs/ens_v1_lll/contracts/ENS.lll:L74 @ ens_v1_lll@7e377df)`, and the subnode-owner, resolver, and TTL setters share that load-then-`sstore` shape `(upstream: .refs/ens_v1_lll/contracts/ENS.lll:L82 @ ens_v1_lll@7e377df)` `(upstream: .refs/ens_v1_lll/contracts/ENS.lll:L97 @ ens_v1_lll@7e377df)` `(upstream: .refs/ens_v1_lll/contracts/ENS.lll:L112 @ ens_v1_lll@7e377df)`. The fallback registry serves the 2017 deployment's stored records through typed reads `(upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L20 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L31 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L42 @ ens_v1@91c966f)`, and executing the deployed fallback bytecode over an archive node confirms the caller-visible value of an unmasked stored word is the word truncated to the declared slot width: with the delegated old-registry read returning a full 32-byte word, the fallback's typed `resolver`/`owner` answer masks it to its low 20 bytes and its `ttl` answer to its low 8 bytes, rather than reverting. The registry authorizes a caller against its own stored owner record `(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L17 @ ens_v1@91c966f)`; the 2017 LLL source makes the comparison exact — the owner gate loads the stored word whole `(upstream: .refs/ens_v1_lll/contracts/ENS.lll:L65 @ ens_v1_lll@7e377df)` `(upstream: .refs/ens_v1_lll/contracts/ENS.lll:L66 @ ens_v1_lll@7e377df)` and jumps to an invalid location `(upstream: .refs/ens_v1_lll/contracts/ENS.lll:L30 @ ens_v1_lll@7e377df)` whenever the 20-byte caller differs from that full word `(upstream: .refs/ens_v1_lll/contracts/ENS.lll:L119 @ ens_v1_lll@7e377df)` `(upstream: .refs/ens_v1_lll/contracts/ENS.lll:L120 @ ens_v1_lll@7e377df)` `(upstream: .refs/ens_v1_lll/contracts/ENS.lll:L121 @ ens_v1_lll@7e377df)`, so an unmasked stored word equals no 20-byte caller. Corroborating archive-node execution of the deployed bytecode matches the source reading: owner-gated calls from the low-20 value revert on the 2017 deployment. Reference indexers decode such a log tolerantly: graph-node decodes event bodies with alloy-dyn-abi's non-validating decoder, which reads an address as the word's low 20 bytes `(upstream: .refs/graph_node/graph/src/abi/event_ext.rs:L17 @ graph_node@aefe173)`, and the canonical ENS subgraph's `NewResolver` handler consumes that decoded value directly `(upstream: .refs/ens_subgraph/src/ensRegistry.ts:L168 @ ens_subgraph@723f1b6)`. The `ens_v1_lll` pin carries the 2017 registry's original LLL source at upstream's `mainnet` tag; the tag's committed `contracts/ENS.lll.bin` runtime is byte-for-byte identical to the code deployed at `0x314159265dd8dbb310642f98f50c066173c1259b` (verified against archive-node `eth_getCode`), so the cited source is the deployed contract. The 33 affected mainnet logs (30 `NewResolver`, 2 `NewOwner`, 1 `NewTTL`) are the census recorded in issue #361, extended by a chain-wide archive-node log sweep.
> **Our rule**: `docs/architecture.md` § Source families (`ens_v1_registry_l1` / `ENSRegistryOld`).
> **Why**: the 33 logs are genuine chain history, and a strict typed decode halts the full re-walk on the first of them (block 3,800,374). Low-byte decode matches what on-chain readers of the old registry receive through the fallback registry and what reference indexers decode, and is a pure function of the logged word. Recording the decoded owner without authority matches the on-chain effect of the two dirty owner writes: the previous owner was locked out and the stored word empowered no one. The rule is confined to `ens_v1_registry_l1` and to exactly-32-byte data on its four single-word events; `basenames_base_registry` shares the adapter source but keeps the strict decode, and the tolerant retry never accepts a log the strict decoder would not have accepted with a masked word.
> **Since**: `2026-08-08`

> **ENSv1 unindexed-key `TextChanged` widening** — bigname decodes two-topic no-value `TextChanged(bytes32,string,string)` logs — the key unindexed, both string arguments in event data — wherever the `ens_v1_resolver_l1` signature-set selection applies: every emitting address, the same all-emitter scope the indexed layout already has. The 2019 PublicResolver at `0x226159d592e2b063810a10ebf6dcbada94ed68b8` is the only admitted instance that emits that shape on mainnet. The mainnet emission shape and counts in this entry come from a census of bigname's stored raw mainnet logs (blocks 0–25,678,800), recorded in #382; the pinned upstream artifacts independently corroborate the legacy ABI layout and duplicate-key emit behavior but do not identify the unpinned mainnet deployment source. Every pinned reference ABI used for mainnet indexing instead declares the key indexed.
> **Upstream**: The pinned goerli `LegacyPublicResolver` deployment ABI directly records the legacy unindexed-key layout `(upstream: .refs/ens_v1/deployments/goerli/LegacyPublicResolver.json:L508-L532 @ ens_v1@91c966f)`. For contrast, the ENSv1 archived mainnet artifact declares the key indexed `(upstream: .refs/ens_v1/deployments/archive/PublicResolver_mainnet_9412610.sol/PublicResolver_mainnet_9412610.json:L269-L293 @ ens_v1@91c966f)`, as do the ENS subgraph ABI `(upstream: .refs/ens_subgraph/abis/PublicResolver.json:L505-L529 @ ens_subgraph@723f1b6)` and ENSNode ABI `(upstream: .refs/ensnode/packages/datasources/src/abis/shared/LegacyPublicResolver.ts:L267-L291 @ ensnode@2017ae6)`. The vendored legacy source emits the same key in both string positions `(upstream: .refs/ens_v1/deployments/mainnet/solcInputs/08371ea78d6ca0259dbc9b2f768cf73e.json:L71 @ ens_v1@91c966f)`. The Basenames resolver likewise stores and emits the supplied key without a blank-key check `(upstream: .refs/basenames/src/L2/resolver/TextResolver.sol:L31-L33 @ basenames@1809bbc)`. The admitted ENSv2 resolver also stores and emits the supplied text key `(upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L475-L488 @ ens_v2_sepolia_20260629@ccaeb58)`, and its permission-part helper hashes every supplied string without a blank-key check `(upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/libraries/PermissionedResolverLib.sol:L92-L97 @ ens_v2_sepolia_20260629@ccaeb58)`. Given their indexed-key ABIs, graph-node discards the log after no candidate event ABI decodes `(upstream: .refs/graph_node/chain/ethereum/src/data_source.rs:L745-L774 @ graph_node@aefe173)`, while Ponder catches the missing-topic decode error and continues without the event `(upstream: .refs/ponder/packages/core/src/utils/decodeEventLog.ts:L34-L47 @ ponder@c8f6935)` `(upstream: .refs/ponder/packages/core/src/runtime/events.ts:L556-L581 @ ponder@c8f6935)`.
> **Our rule**: `docs/manifests.md` § ENS mainnet (`ens_v1_resolver_l1`) carries the family acceptance rule. `crates/adapters/src/schema_v2/protocol/v1/resolver.rs` dispatches the no-value event by topic count, without changing the manifest fragment. Three-topic logs retain the indexed-key decoder. Two-topic logs decode both strings from event data and are accepted only when the strings are byte-equal and the key is nonempty. Non-whitespace UTF-8 without NUL bytes uses the plain selector; every other nonempty key, including whitespace-only bytes, uses the opaque selector. The same selector rule applies to value-bearing ENSv1, ENSv2, and Basenames text events. Accepted logs produce the same `RecordChanged` state as the indexed-key layout, with no retained value.
> **Why**: all 1,450 mainnet two-topic logs from the 2019 PublicResolver contain byte-equal strings, matching the vendored `emit TextChanged(node, key, key)` site. Decoding that emitted shape indexes 1,450 legacy text-record changes that both pinned reference-indexer paths drop. The rule is deliberately scoped to the event shape across the family’s all-emitter selection rather than to the single 2019 address: the indexed three-topic layout already produces record-history observations from any emitting address, the two-topic layout now receives the same treatment, and the byte-equality guard keeps acceptance safe for any emitter — a `(key, key)` emitter decodes correctly, and a hypothetical `(key, value)` emitter whose strings happen to be equal still yields the correct record key with no value claim. Unequal-string logs using the same topic0 have different semantics and remain unsupported, as do all other unmatched event shapes tracked in #382.

> **ENSv2 reservation-history marker appears in the permission vocabulary** — bigname exposes registry role bit 32 as `was_reserved` in `effective_powers` so a marker-only `EACRolesChanged` transition remains observable, while documenting that it grants no authorization.
> **Upstream**: `ROLE_WAS_RESERVED` is a token-only, non-revokable tag `(upstream: .refs/ens_v2/contracts/src/registry/libraries/RegistryRolesLib.sol:L47-L48 @ ens_v2@a971bd64)`.
> **Our rule**: `docs/architecture.md` § Source families and `docs/api-v1.md` § Naming Dictionary.
> **Why**: omitting the marker makes an otherwise-empty ENSv2 reserved-to-registered transition disappear from the permissions projection; retaining a named marker preserves history without claiming an executable permission.
> **Since**: `2026-08-31`

> **`.eth` grace-period wrapper powers keep an approve-only exception** — during `.eth` registrar grace, bigname's projected wrapper-holder effective powers remove owner modification and transfer powers but retain `approve` and `approve_wrapper` (still subject to `CANNOT_APPROVE`). Upstream has no such composite lifecycle state: `canModifyName` rejects owner/operator modification during grace `(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L214 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L222 @ ens_v1@91c966f)`, while per-token `approve` routes through the ERC-1155-fuse owner/operator authorization path rather than that helper `(upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L37 @ ens_v1@91c966f)` `(upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L47 @ ens_v1@91c966f)`, so the approve exception is bigname's policy interpretation of which calls remain executable, not an upstream-declared state.
> **Upstream**: citations above; the grace window itself is `_isETH2LDInGracePeriod` `(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1082 @ ens_v1@91c966f)`.
> **Our rule**: `docs/api-v1.md` § Naming Dictionary (wrapper fuse fields, grace-period paragraph).
> **Why**: consumer capability accuracy — during grace the approve call remains executable on-chain through ERC-1155 authorization while `canModifyName`-gated calls do not; projecting either zero powers or full powers would misstate what the holder can actually do.
> **Since**: `2026-08-10`

> **ENSv2 creation announcements establish capture, not support or name binding** — bigname reads `ResolverCreated()` from every emitter and admits the emitting address as an `ens_v2_resolver_l1` instance from the creation block onward, then fetches that address's record events from creation onward. The announcement proves neither a deployed implementation nor a name's resolver binding: the resolver stays `unsupported` with `resolver_implementation_unknown` until a canonical `Upgraded` observation names a declared implementation, and a name is bound to it only by a registry `ResolverUpdated`. Registry `RegistryCreated()` is read the same way: it establishes indexability, not membership in the currently declared root's name tree, so older announcements are not rejected solely because they predate a declared root.
> **Upstream**: the resolver documents the event as marking a created or initialized resolver and emits it from both its constructor and its proxy initializer `(upstream: .refs/ens_v2_sepolia_20260916/contracts/src/resolver/interfaces/IRecordResolver.sol:L30-L31 @ ens_v2_sepolia_20260916@366de741)` `(upstream: .refs/ens_v2_sepolia_20260916/contracts/src/resolver/PermissionedResolver.sol:L108 @ ens_v2_sepolia_20260916@366de741)` `(upstream: .refs/ens_v2_sepolia_20260916/contracts/src/resolver/PermissionedResolver.sol:L121 @ ens_v2_sepolia_20260916@366de741)`; the registry announces itself the same way `(upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L9 @ ens_v2@a971bd64)` `(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L113 @ ens_v2@a971bd64)`. Neither event carries an implementation address or a name.
> **Our rule**: `docs/manifests.md` § Resolver creation capture and § Resolver admission by implementation announcement; `docs/glossary.md` § Resolver creation capture.
> **Why**: coverage must be explicit. Reading a resolver's record events from its first block keeps its history complete, but serving reads from an address whose implementation is unobserved would present unverified code as a supported resolver, and treating an announcement as a name binding would invent topology that only `ResolverUpdated` and `SubregistryUpdated` carry.
> **Since**: `2026-09-17` (#905)

<a id="resolves-to-matched-coin-types"></a>
> **Names resolving to an address: EVM-wide discovery with matched coin types.** `GET /v1/addresses/{address}/names?relation=resolves_to&coin_type=evm` returns the names whose stored `addr:<coin_type>` record for any EVM coin type (`60`, or `2147483648` through `4294967295`) holds exactly the path address, and each row's `resolutions` lists only the EVM coin types whose record matched. The pinned subgraph differs in two ways. Its per-domain `resolvedAddress`, the subgraph's field for the address a name resolves to, is only ever populated from the coin-60 `addr` value: the `AddrChanged` handler writes it, a resolver change copies the new resolver's stored coin-60 `addr` or clears it, and a record-version change clears it. A name set only for another chain therefore never appears. Its multicoin handler adds each observed coin type to `Resolver.coinTypes` without comparing the new value with any address, so that list is every coin type the resolver has records for, not the coin types that point at the queried address.
> **Upstream**: `(upstream: .refs/ens_subgraph/src/resolver.ts:L45-L48 @ ens_subgraph@723f1b6)` `(upstream: .refs/ens_subgraph/src/ensRegistry.ts:L194-L199 @ ens_subgraph@723f1b6)` `(upstream: .refs/ens_subgraph/src/resolver.ts:L219-L222 @ ens_subgraph@723f1b6)` `(upstream: .refs/ens_subgraph/src/resolver.ts:L59-L79 @ ens_subgraph@723f1b6)` `(upstream: .refs/ens_subgraph/schema.graphql:L294-L295 @ ens_subgraph@723f1b6)`. The EVM coin-type set is ENSIP-19's `(upstream: .refs/ens_v1/contracts/utils/ENSIP19.sol:L9-L38 @ ens_v1@91c966f)`.
> **Our rule**: `docs/api-v1-routes.md` § `GET /v1/addresses/{address}/names` and `docs/api-v1.md` § Naming Dictionary (`resolutions`).
> **Why**: an address page must find names set for chains the client does not know in advance, and every coin type shown for a name must be one whose record holds the address. `evm` is also narrower than every stored coin type: legacy SLIP-44 coin types of EVM-compatible chains and non-EVM coin types stay reachable only through a decimal `coin_type`. This is not complete parity with the subgraph's `coinTypes`, and the numeric lookup input of `POST /v1/lookup` keeps its single-coin meaning.
> **Since**: `2026-09-23`

Per-entry format:

> **Surface** — one-line description of what differs.
> **Upstream**: `(upstream: .refs/<key>/<path>:L<line> @ <key>@<short-commit>)`
> **Our rule**: `docs/<file>.md` § section.
> **Why**: the constraint that drove the divergence (consumer capability, storage invariant, coverage narrowing, etc.).
> **Since**: commit or date the divergence was introduced.

## Evidence checks

Use `$upstream-evidence` or `evidence_reader` when a change adds or relies on ENSv1, ENSv2, Basenames, admitted app-metadata, reference-indexer, or execution-client behavior claims. The check produces a claim-to-citation ledger and flags any divergence that belongs in this file.

Pin drift checks are deliberate, not scheduled automation. Run them when manifests, ADRs, or load-bearing citations change. Stale pins are not urgent by default — material upstream behavior change is the trigger, not calendar time.

### WrapperRegistry permission history

The permission reader supports the exact 2026-10-01 Sepolia WrapperRegistry
implementation `0xbe768b63e5fbbfbb0ae97e9064e0002df8001880`, and the exact
UserRegistry implementation `0x9bd8a88719068d09ecee662f36c0e3856708366a`
as a parent. Recognition requires an authenticated declared-factory origin,
ordinary canonical `RegistryCreated` admission linked to the current address
instance, and no canonical departure from that implementation through the
served Project publication. Namespace, chain, source manifest, factory and
implementation declaration intervals must agree. Duplicate/conflicting
origins establish no support. The creation block is included because the
initial `Upgraded` precedes initialization and the factory's final log.
Delayed initialization can follow the factory origin. Canonical adverse
upgrades count regardless of consumer visibility; orphaned evidence does not.
Same-code upgrades preserve support; a later return after a different-code
upgrade does not establish compatible storage history. An undo removing the
departure restores the previous proof.
(upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/build-info/solc-0_8_25-b30e6dc9a03b37f6a0b89af5d02a73d3993944f7.json:L1561 @ ens_v2_sepolia_20261001@07e55a05)
(upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/build-info/solc-0_8_25-b30e6dc9a03b37f6a0b89af5d02a73d3993944f7.json:L1564 @ ens_v2_sepolia_20261001@07e55a05)
(upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/build-info/solc-0_8_25-b30e6dc9a03b37f6a0b89af5d02a73d3993944f7.json:L1171 @ ens_v2_sepolia_20261001@07e55a05)

This intentionally narrows upstream's mutable upgrade allowlist: admission
of a target by that allowlist does not prove compatible permission behavior.
The parent may instead be one of the pinned nonproxy root/ETH declarations,
within its actual admitted declaration interval. A proven Wrapper parent
needs no recursive ancestor classification to read its entries and approvals.
The reader uses the latest parent and raw label, never the initializer's stale
parent. Unknown histories keep ordinary direct rows and conservative coverage.
(upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/WrapperRegistry.sol:L297-L305 @ ens_v2_sepolia_20261001@07e55a05)
(upstream: .refs/ens_v2_sepolia_20261001/contracts/src/utils/PermissionedAddressSet.sol:L51-L61 @ ens_v2_sepolia_20261001@07e55a05)
(upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L311-L314 @ ens_v2_sepolia_20261001@07e55a05)
(upstream: .refs/ens_v2_sepolia_20261001/contracts/src/erc1155/ERC1155Singleton.sol:L70-L75 @ ens_v2_sepolia_20261001@07e55a05)
