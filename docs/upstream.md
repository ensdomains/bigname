# Upstream references

bigname anchors every claim about ENSv1, ENSv2, Basenames, and admitted upstream app metadata to a specific upstream commit pinned under `.refs/`. This doc is the human-readable companion to `.refs/MANIFEST.toml` — the pin table, rotation policy, and the intentional-divergence list.

## Pinned refs

| Key | Repo | Commit | Purpose |
| --- | --- | --- | --- |
| `ens_v1` | `ensdomains/ens-contracts` | `91c966fe` | Canonical ENSv1 Solidity |
| `ens_v2` | `ensdomains/contracts-v2` | `554c309b` | ENSv2 contracts |
| `basenames` | `base-org/basenames` | `1809bbc9` | Canonical Basenames Solidity |
| `ens_subgraph` | `ensdomains/ens-subgraph` | `723f1b6a` | Reference ENSv1 indexer |
| `ensnode` | `namehash/ensnode` | `2017ae62` | Alternative ENS indexer |
| `ens_app_v3` | `ensdomains/ens-app-v3` | `71758582` | ENS app known-resolver metadata |

Full pin records (including per-ref `authoritative_for` lists) live in `.refs/MANIFEST.toml`. Sync with `scripts/sync-refs`; verify with `scripts/sync-refs --check`.

## Citation format

```
(upstream: .refs/<key>/<path>:L<line> @ <key>@<short-commit>)
```

Use this exact shape everywhere — docs, ADRs, manifests, code comments, task writeups, agent output. Consistent format lets `verification_reviewer` and `upstream_auditor` verify citations mechanically.

In docs, citations live in footnotes so they don't break inline reading flow. The first instance:

```md
The wrapper masks effective powers via fuses.[^v1-nw-fuses]

[^v1-nw-fuses]: (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L31 @ ens_v1@91c966f)
```

## Rotation policy

**Bump when** a cited upstream file changes materially, or you need to adopt new upstream behavior (new contract, new event, new invariant). Staying behind upstream is fine. Being silently wrong is not.

**Don't bump for** drive-by upstream refactors, test-only changes, comment edits, rename-only commits.

**How to bump:**

1. Update the `commit` field in `.refs/MANIFEST.toml`.
2. Update the row in the table above.
3. Run `scripts/sync-refs`.
4. Re-grep the repo for `@ <key>@<old-short-commit>` citations; update any that point at content that changed across the bump.
5. Add or edit entries in § Known divergences if the bump surfaced or resolved an intentional deviation.
6. Commit with a message naming what upstream change motivated the bump: e.g. `chore(refs): bump ens_v1 to <new-sha> — adopt new reverseClaimer event`.

**Who decides:** whoever owns the surface affected. Ambiguous cases route through `$change-gate`; cross-surface bumps route through `verification_reviewer` after the sync.

## Audit loop

`upstream_auditor` (read-only codex agent, `.codex/agents/upstream-auditor.toml`) diffs each pinned commit against its upstream `main`, identifies cited files that changed, and reports pins plausibly worth bumping. It doesn't bump — bumping is manual per the policy above. Run opportunistically when manifests/ADRs change, or on a weekly `$schedule`. Stale pins aren't urgent by default — material upstream behavior change is the trigger, not calendar time.

## Known divergences

Intentional differences between bigname's docs/manifests and upstream. Every divergence lives here so citations that read "differently than upstream" are legible instead of looking like bugs. Not in this list → drift, not divergence: close it by updating the doc or by adding an entry.

Per-entry format:

```md
> **Surface** — one-line description of what differs.
> **Upstream**: `(upstream: .refs/<key>/<path>:L<line> @ <key>@<short-commit>)`
> **Our rule**: `docs/<file>.md` § section.
> **Why**: the constraint that drove the divergence.
> **Since**: commit or date the divergence was introduced.
```

---

> **ENS Universal Resolver: proxy entrypoint vs pinned implementation**
> bigname uses the official ENS Universal Resolver proxy as the route-facing `ens_execution` entrypoint, even though the pinned `.refs/ens_v1` artifact records the implementation address.
>
> **Upstream**: ENS docs list `0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe` as the Mainnet/testnet proxy (<https://docs.ens.domains/resolvers/universal/>). The pinned artifact records `0xED73a03F19e8D849E44a39252d222c6ad5217E1e` as the implementation `(upstream: .refs/ens_v1/deployments/mainnet/UniversalResolver.json:L2 @ ens_v1@91c966f)`. ABI/behavior anchor stays with `(upstream: .refs/ens_v1/contracts/universalResolver/UniversalResolver.sol:L8 @ ens_v1@91c966f)`.
>
> **Our rule**: `docs/manifests.md` § Required fields and § Capability ownership; `docs/execution.md` § Entrypoints and § Support boundary; `docs/architecture.md` § Source families.
>
> **Why**: callers and manifests should target the official proxy entrypoint while `.refs/ens_v1` remains the pinned ABI/behavior source.
>
> **Since**: 2026-04-22

---

> **Basenames verified/explain support narrowing**
> bigname narrows the upstream Basenames L1Resolver and CCIP entrypoint into one first public support class instead of publishing every upstream-reachable non-`base.eth` path immediately.
>
> **Upstream**: `(upstream: .refs/basenames/README.md:L69 @ basenames@1809bbc)`, `(upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc)`, `(upstream: .refs/basenames/README.md:L71 @ basenames@1809bbc)`, `(upstream: .refs/basenames/src/L1/L1Resolver.sol:L154 @ basenames@1809bbc)`, `(upstream: .refs/basenames/src/L1/L1Resolver.sol:L173 @ basenames@1809bbc)`.
>
> **Our rule**: `docs/api-v1-routes.md` § `GET /v1/resolutions/{namespace}/{name}` and § `GET /v1/explain/resolutions/{namespace}/{name}/execution`; `docs/execution.md` § Support boundary; `docs/manifests.md` § Basenames mainnet.
>
> **Why**: freeze the first Basenames consumer-replacement slice on the declared Base-authority + L1-transport boundary before widening alias-participating, wildcard-derived, linked-subregistry, transport-free, or offchain-gateway path classes.
>
> **Since**: 2026-04-19

---

> **ENSv1 wrapper/resolver admission narrowing**
> bigname admits the mainnet NameWrapper and PublicResolver as source-family inputs for current declared-state normalization without claiming every upstream wrapper or resolver capability as supported public coverage.
>
> **Upstream**: `(upstream: .refs/ens_v1/deployments/mainnet/NameWrapper.json:L2 @ ens_v1@91c966f)`, `(upstream: .refs/ens_v1/deployments/mainnet/PublicResolver.json:L2 @ ens_v1@91c966f)`, `(upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L27 @ ens_v1@91c966f)`, `(upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L35 @ ens_v1@91c966f)`, `(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L114 @ ens_v1@91c966f)`.
>
> **Our rule**: `docs/manifests.md` § Capability ownership and § Capability policy; `docs/architecture.md` § Source families; `docs/storage.md`; `docs/projections.md`.
>
> **Why**: bind the wrapper/resolver adapter boundary to source-family ownership, identity continuity, and declared resolver record state. Wrapper-upgrade and migration history are admitted separately when those surfaces ship.
>
> **Since**: 2026-04-21

---

> **ENSv1 generic resolver-event intake and known PublicResolver-generation profile narrowing**
> bigname may retain generic ENSv1 resolver-local record events as observed selector/cache or version-boundary facts even when the emitter's resolver profile is still pending. ENS Labs PublicResolver-generation profiles are semantic admissions for complete family coverage, resolver-overview support, latest-only behavior, and event-to-call parity claims — not the default address set for generic resolver-event intake. The mainnet manifest directly admits the latest PublicResolver plus first-party app-known mainnet generations.
>
> **Upstream**: `(upstream: .refs/ens_v1/contracts/registry/ENS.sol:L12 @ ens_v1@91c966f)`, `(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L89 @ ens_v1@91c966f)`, `(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L20 @ ens_v1@91c966f)`, `(upstream: .refs/ens_v1/contracts/resolvers/ResolverBase.sol:L17 @ ens_v1@91c966f)`. ENS app-known data: `(upstream: .refs/ens_app_v3/src/constants/resolverAddressData.ts:L32 @ ens_app_v3@7175858)`.
>
> **Our rule**: `docs/manifests.md` § Capability ownership and § Capability policy; mirrored in `docs/storage.md`, `docs/projections.md`, `docs/api-v1-routes.md`, `docs/consumer-capabilities.md`.
>
> **Why**: a registry-observed resolver isn't the same as an admitted resolver-profile. Unknown dynamic resolvers and unsupported legacy interfaces stay `pending` or `unsupported` for profile-gated behavior; observed selector/cache facts still flow from generic resolver events.
>
> **Since**: 2026-04-21

---

> **Basenames dynamic Base resolver discovery deferred**
> bigname currently has the static Base `L2Resolver` seed, but the frozen consumer-replacement contract requires resolver discovery from Basenames registry `NewResolver` observations before declared record reads can claim complete Base-side resolver coverage.
>
> **Upstream**: `(upstream: .refs/basenames/src/L2/Registry.sol:L19 @ basenames@1809bbc)`, `(upstream: .refs/basenames/src/L2/Registry.sol:L132 @ basenames@1809bbc)`.
>
> **Our rule**: `docs/manifests.md` § Basenames mainnet; `docs/chain-intake.md` § Resolver discovery.
>
> **Why**: a Base-side resolver outside the statically admitted `L2Resolver` (or lacking supported profile admission) doesn't satisfy declared record reads. The L1 Resolver and offchain gateways are separate surfaces.
>
> **Since**: 2026-04-21

---

> **Basenames `L2Resolver`-compatible profile narrowing**
> bigname admits Basenames registry-observed Base resolver addresses as watched contract instances, but supports resolver-local fact consumption only for instances explicitly admitted as `L2Resolver`-compatible.
>
> **Upstream**: `(upstream: .refs/basenames/src/L2/L2Resolver.sol:L22 @ basenames@1809bbc)`, `(upstream: .refs/basenames/src/L2/L2Resolver.sol:L182 @ basenames@1809bbc)`.
>
> **Our rule**: `docs/manifests.md` § Basenames mainnet; mirrored in `docs/architecture.md`, `docs/projections.md`, `docs/api-v1-routes.md`, `docs/consumer-capabilities.md`.
>
> **Why**: registry-observed Base resolvers are watched contract instances, but resolver-local fact consumption requires `L2Resolver`-compatible profile admission. The gate is separate from ENSv1 PublicResolver-generation admission and from Basenames L1 transport / execution.
>
> **Since**: 2026-04-22

---

> **ENSv1 old-registry admission narrowing**
> bigname admits `ENSRegistryOld` as migration-aware `ens_v1_registry_l1` input, but doesn't treat the current registry `startBlock: 9380380` as original ENS history and doesn't union old and current registry logs by latest block.
>
> **Upstream**: `(upstream: .refs/ens_subgraph/subgraph.yaml:L15 @ ens_subgraph@723f1b6)`, `(upstream: .refs/ens_subgraph/subgraph.yaml:L39 @ ens_subgraph@723f1b6)`, `(upstream: .refs/ens_subgraph/src/ensRegistry.ts:L238 @ ens_subgraph@723f1b6)`, `(upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L40 @ ens_v1@91c966f)`.
>
> **Our rule**: `docs/manifests.md` § Required fields and § Capability ownership; mirrored in `docs/architecture.md`, `docs/chain-intake.md`, `docs/storage.md`, `docs/consumer-capabilities.md`.
>
> **Why**: preserve current-registry topology after a node migrates, keep the root resolver as the explicit old-registry exception, and prevent historical backfill from graduating coverage without route-level evidence.
>
> **Since**: 2026-04-24

---

> **ENSv2 sepolia-dev source-family narrowing**
> bigname admits only `ens_v2_root_l1`, `ens_v2_registry_l1`, `ens_v2_registrar_l1`, `ens_v2_resolver_l1` for `sepolia-dev`, not every upstream `sepolia-dev` deployment artifact.
>
> **Upstream**: `(upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/RootRegistry.json:L2 @ ens_v2@554c309)`, `(upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistry.json:L2 @ ens_v2@554c309)`, `(upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ETHRegistrar.json:L2 @ ens_v2@554c309)`, `(upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/PermissionedResolverImpl.json:L2 @ ens_v2@554c309)`. Other artifacts (`UniversalResolverV2`, `ReverseRegistry`, `DNSAliasResolver`, `WrapperRegistryImpl`, `LockedMigrationController`, `HCAFactory`, `StandardRentPriceOracle`, `BatchRegistrar`, `MockUSDC`, `MockDAI`) stay outside admission.
>
> **Our rule**: `docs/manifests.md` § ENSv2 (`sepolia-dev` profile); `docs/architecture.md` § Source families; `docs/chain-intake.md` § ENSv2 sepolia-dev intake.
>
> **Why**: scope the dev profile to root, registry, registrar, and resolver resource/event semantics. Reverse, DNS, wrapper, migration, universal-resolver/execution, factory, oracle, batch, and mock-payment surfaces are admitted separately when needed.
>
> **Since**: 2026-04-20

---

> **Automatic bootstrap `start_block` narrowing**
> bigname treats manifest `start_block` as optional inclusive bootstrap metadata for `[[roots]]` and `[[contracts]]`, not as inferred deployment truth. ENSv1 registry and `.eth` registrar values are reference candidates from `ens_subgraph` only; ENSv1 NameWrapper, PublicResolver, ReverseRegistrar, and ENSv2 `sepolia-dev` RootRegistry / ETHRegistry / ETHRegistrar values come from pinned deployment receipt metadata. Basenames mainnet families and ENS UniversalResolver remain unknown — bootstrap skips those targets.
>
> **Upstream**: `(upstream: .refs/ens_subgraph/subgraph.yaml:L15 @ ens_subgraph@723f1b6)`, `(upstream: .refs/ens_v1/deployments/mainnet/NameWrapper.json:L1498 @ ens_v1@91c966f)`, `(upstream: .refs/ens_v1/deployments/mainnet/PublicResolver.json:L1104 @ ens_v1@91c966f)`, `(upstream: .refs/ens_v1/deployments/mainnet/ReverseRegistrar.json:L379 @ ens_v1@91c966f)`, `(upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/RootRegistry.json:L2617 @ ens_v2@554c309)`.
>
> **Our rule**: `docs/manifests.md` § Required fields and § Watch-plan expansion; `docs/chain-intake.md` § Automatic bootstrap; `docs/storage.md`.
>
> **Why**: keep automatic historical bootstrap from silently widening unknown source history, address-only target identity, or chain checkpoint state.
>
> **Since**: 2026-04-22
