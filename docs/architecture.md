# Architecture

bigname indexes ENS (v1 and v2) and Basenames into a versioned `v1` REST API. Every answer it returns is point-in-time, has provenance attached, declares its coverage, and can be replayed from raw facts.

This doc covers the model. The wire format lives in [`api-v1.md`](api-v1.md), persistence in [`storage.md`](storage.md), and the rest of the pipeline in their own files.

## The pipeline at a glance

```
                       ┌────────────────────────────────────────────────┐
                       │                                                │
   Ethereum  ──┐       │   manifests/discovery ──► adapters             │
               ├──► raw facts ──► normalized events ──► projections ──► API
   Base      ──┘                                          ▲              ▲
                                                          │              │
                              execution (Universal/L1 Resolver, CCIP) ───┘
```

- **Indexer** (`apps/indexer`) does chain intake: blocks, logs, lineage, reorg reconciliation.
- **Adapters** (`crates/adapters`) turn raw facts into normalized events keyed by stable identity.
- **Worker** (`apps/worker`) builds projections, runs replay, drives execution.
- **API** (`apps/api`) reads projections and persisted execution output. It never reads raw facts directly.

Raw facts are immutable. Projections are disposable — they rebuild from canonical events.

## Namespaces

Two public namespaces:

| Namespace | Covers | Notes |
| --- | --- | --- |
| `ens` | `*.eth`, DNS-imported names, `base.eth` itself | Spans ENSv1 and ENSv2 as internal authority epochs. |
| `basenames` | `*.base.eth` issued on Base | `base.eth` itself stays under `ens` because upstream treats it as the L1 root domain.[^bn-l1resolver] |

A runtime serves one **deployment profile** at a time:

- `manifests/` — shipped mainnet (Ethereum + Base).
- `manifests-sepolia-dev/` — ENSv2 Sepolia dev profile, narrower in scope.

Profiles never mix. Mainnet and Sepolia don't share a canonical corpus, watch plan, or projection set.

## Identity

Four identity layers, each with its own continuity rules.

| Layer | Form | Purpose |
| --- | --- | --- |
| `logical_name_id` | `<namespace>:<normalized_name>` (text) | The public surface. Survives backing-resource rotation. |
| `resource_id` | UUID | The backing authority object — anchor for permissions, control, token lineage. |
| `token_lineage_id` | UUID | Tokenized ownership history. Survives token-id changes. |
| `contract_instance_id` | UUID | A registry, registrar, resolver, wrapper, or transport contract. |

```
   logical_name_id ───► SurfaceBinding ───► resource_id
                                                 │
                                                 ├─► token_lineage_id  (when tokenized)
                                                 │
                                                 └─► permissions, control
```

### Why four

A name's public surface, its current authority, the token that represents it, and the contract that records it are **four different things** that change at different rates. Wrapping doesn't change a surface but rotates the authority. Token regeneration in ENSv2 changes a token id but not the resource. Implementation upgrades change a contract's bytecode but not its identity. Modeling these separately keeps wrapping, migration, regeneration, and aliasing representable without distortion.

### Continuity rules

`SurfaceBinding` records how a public surface binds to a resource through time:

| Binding kind | Meaning |
| --- | --- |
| `declared_registry_path` | Direct registry/registrar/wrapper-backed authority. |
| `linked_subregistry_path` | ENSv2 subregistry link. |
| `resolver_alias_path` | ENSv2 alias mapping. |
| `observed_wildcard_path` | Wildcard-derived surface. |
| `migration_rebind` | Authority moved to a different anchor. |
| `observed_only` | Surface seen but not yet authoritatively bound. |

For ENSv1, ordinary lifecycle (transfer, expiry, grace, fuse changes) keeps the same `resource_id` and `binding_kind=declared_registry_path`. Anchor changes rotate it:

| Case | `resource_id` | `token_lineage_id` |
| --- | --- | --- |
| Registry-only sub.alice.eth | one registry-anchored | none |
| Register alice.eth | mint registrar-anchored | mint registrar lineage |
| Wrap alice.eth | close registrar binding, mint wrapper-anchored | mint wrapper lineage |
| Unwrap before lease ends | reactivate prior registrar | reactivate prior registrar lineage |
| Expiry / grace | unchanged | unchanged |
| Re-registration after lapse | mint new | mint new |

For ENSv2, `resource_id` keys to the upstream EAC resource via `getResource(anyId)`, not to the current ERC-1155 token id. `TokenRegenerated(oldTokenId, newTokenId)` updates the token attribute and preserves identity; unregister/re-register increments both `eacVersionId` and `tokenVersionId` and mints fresh ids.[^v2-pr-token]

For Basenames, `resource_id` anchors the Base-side authority object even when L1 transport is involved.[^bn-readme-base]

`contract_instance_id` is one stable id per admitted contract address per chain across all admission epochs. Re-admission after an inactive gap reuses the prior id with a new active range. A proxy keeps its id when its implementation changes; the proxy/implementation edge updates.

## Source families

Source families are the atomic units of capability ownership. Each one owns specific events from specific contracts.

**ENS:**

| Family | Covers |
| --- | --- |
| `ens_v1_registry_l1` | Current `ENSRegistry` plus migration-aware `ENSRegistryOld`. |
| `ens_v1_registrar_l1` | `BaseRegistrar` plus legacy/wrapped/current `ETHRegistrarController`s. |
| `ens_v1_wrapper_l1` | NameWrapper authority, fuses, expiry. |
| `ens_v1_resolver_l1` | PublicResolver plus admitted ENS Labs generations. |
| `ens_v1_reverse_l1` | Reverse Registrar (claim intake only). |
| `ens_dns_l1` | DNS-imported names. |
| `ens_v2_root_l1` | ENSv2 RootRegistry (sepolia-dev). |
| `ens_v2_registry_l1` | ETHRegistry plus discovered UserRegistry instances. |
| `ens_v2_registrar_l1` | ETHRegistrar. |
| `ens_v2_resolver_l1` | PermissionedResolver. |
| `ens_execution` | Verified resolution at the Universal Resolver proxy `0xeEeE…EeEe`.[^ens-univ] |

**Basenames:**

| Family | Covers |
| --- | --- |
| `basenames_base_registry` | Base `Registry`. |
| `basenames_base_registrar` | Base `BaseRegistrar`. |
| `basenames_base_resolver` | Base `L2Resolver`-compatible resolvers. |
| `basenames_base_primary` | Base `ReverseRegistrar` (claim intake). |
| `basenames_l1_compat` | L1 transport for `base.eth`. |
| `basenames_execution` | Verified resolution via the L1 Resolver. |

Capability ownership attaches to one declaring family. It is never implied by another family's presence — for example, `ens_v1_resolver_l1` owning the resolver doesn't widen `ens_v1_wrapper_l1`'s coverage.

## Manifests and discovery

Each source family is pinned by a TOML manifest at `manifests/<namespace>/<source_family>/<version>.toml`. Manifests declare watched contracts, capability flags (`unsupported` | `shadow` | `supported`), and discovery rules.

A discovered contract is authoritative when it's reachable from a manifest root through an admitted edge, or directly declared. Discovery edges include resolver, subregistry, parent, alias, proxy/implementation, migration, and transport. See [`manifests.md`](manifests.md) for the schema and capability rules.

Manifest changes are themselves normalized events: `SourceManifestUpdated`, `ProxyImplementationChanged`, `CapabilityChanged`.

## Normalization and preimage observation

Names are normalized via UTS-46 (version-pinned through `normalizer_version`). Each `NameSurface` carries one canonical normalization result; alternate spellings persist as immutable preimage observations.

Preimages can come from registrar/registry events with explicit labels, wrapper events, reverse/primary flows, and metadata where the manifest allows. They never synthesize ownership, resolver, or record facts on their own.

ENSv1 resolver `NameChanged` text observed via reverse/primary flows is preimage-only.[^v1-namechanged] It can attach already-observed forward-node facts to a human-readable name, but doesn't make `GET /v1/resolve/{name}` return success and doesn't prove primary truth.

## Canonicality

Block hash is identity. Block number is position.

Per-chain checkpoints:

| Checkpoint | Meaning |
| --- | --- |
| `canonical_head` | latest reconciled canonical block |
| `safe_head` | safe per consensus |
| `finalized_head` | finalized per consensus |

`API` consistency maps directly: `consistency=head|safe|finalized`.

Every fact carries `canonicality_state ∈ {observed, canonical, safe, finalized, orphaned}`. On reorg, affected rows are marked `orphaned` rather than deleted — the audit trail survives. Repair emits deterministic invalidation for normalized events and execution-cache outcomes derived from orphaned blocks. Detail in [`chain-intake.md`](chain-intake.md).

## What the API answers

`v1` resource families: `Namespace`, `Name`, `Address`, `Resolver`, `Resolution`, `PrimaryName`, `Permissions`, `History`, `Explain`, `SourceManifest`, `Coverage`. `Registration` is a sub-document of `Name`.

Every response carries:

```json
{
  "data": {…},
  "declared_state": {…},
  "verified_state": null,
  "provenance": {…},
  "coverage": {…},
  "chain_positions": {…},
  "consistency": "head",
  "last_updated": "2026-04-16T00:00:00Z"
}
```

Compact app-facing routes use a slimmer envelope. See [`api-v1.md`](api-v1.md).

### Snapshot selection

Before any read, the API resolves caller input to one `ChainPositions`:

| Inputs | Result |
| --- | --- |
| `chain_positions` only | use them exactly |
| `at` only | resolve per-chain positions at `consistency` |
| neither | latest available at `consistency` |
| both | reject `invalid_input` |

That one snapshot keys every join in the response: `data`, declared sections, coverage, verified output. Mixing rows from different snapshots is a bug.

### Coverage, not silence

If a section can't be answered, the response says so. Coverage is contractual:

| `status` | Meaning |
| --- | --- |
| `full` | authoritative for the supported source classes |
| `partial` | some classes covered, others not |
| `observed_only` | observed facts only; no exhaustive scan |
| `unsupported` | route can't produce this answer |
| `stale` | selector is valid but projection isn't built yet |

| `exhaustiveness` | Meaning |
| --- | --- |
| `authoritative` | exhaustive over the source classes considered |
| `best_effort` | best-effort enumeration |
| `observed_only` | only what was observed |
| `non_enumerable` | by nature not enumerable (wildcards, primary names) |
| `not_applicable` | the route doesn't have an exhaustiveness concept |

Per-section, mixed routes use `ResultStatus`: `success`, `not_found`, `mismatch`, `unsupported`, `invalid_name`, `execution_failed`. Only `success` guarantees a concrete value.

## Verified execution

`Resolution` and `PrimaryName` are mixed routes — they carry both declared state and verified output.

```
                                                  Universal Resolver
                                                  (0xeEeE…EeEe, ENS)
   topology + selector + ChainPositions  ─►  ────────────────────────►
                                                  L1 Resolver
                                                  (0xde90…F31, Basenames)
   ◄─ trace, decoded value, status  ───────────────────────
```

Every verified answer persists an `ExecutionTrace`: entrypoint, resolver discovery path, contracts called, gateway digests, proof checks, final value, errors, chain positions. Cache outcomes key by request, chain positions, manifest versions, and topology/record version boundaries. Reorg invalidates cache entries whose dependencies hit an orphaned block. Traces stay durable.

Public verified support is narrower than the topology model — see [`execution.md`](execution.md). The short version:

- ENS supports three exact-surface classes: direct, alias-only non-direct, wildcard-derived.
- Basenames supports one: exact-surface transport-assisted direct path through the L1 Resolver (CCIP-Read participating).

Anything else returns selector-local `unsupported`.

## Permissions

Permissions are projections, not raw event reads. Every grant records source, revocation source, inheritance path, transfer behavior, scope, and effective powers. Public reads expose `effective_powers` directly so callers don't reconstruct authority from raw role bitmaps.

The first-class anchor is `resource_id`. Name-, address-, and resolver-centric views are summaries over the same resource-anchored truth.

For ENSv1 wrapper-backed resources, the active fuse state masks `effective_powers` before publication. A fuse that prohibits an operation removes any power that depends on that operation.[^v1-nw-fuses]

For ENSv2, permissions consume `EACRolesChanged` events and key to the bigname `resource_id` linked to the upstream EAC resource. Resolver-scoped permissions live in the same model with resolver-scope metadata.[^v2-eac]

## Primary names

`PrimaryName` is `(address, namespace, coin_type)`-keyed. It carries:

- `claimed_primary_name` — the candidate from the reverse registrar, declared-only.
- `verified_primary_name` — the result of resolving that claim back to the requested address through verified execution.

A claim alone never verifies. `verified_primary_name.status=success` means the claim normalized, resolved for the requested coin type, and matched the requested address. `mismatch` means it resolved but to a different address.[^v1-aur-verify]

The route is currently scoped to exact-tuple persisted readback for ENS and Basenames. Out-of-class tuples return `unsupported`.

## Execution boundaries

A few rules that the rest of the docs depend on:

- Adapters write identity rows and normalized events. They never write projections.
- Projection workers write projections. They never write raw facts or normalized events.
- API code reads projections and execution output. It never reads raw facts directly except on documented audit endpoints.
- Execution workers write traces, steps, and cache outcomes. Reorg repair is the only other path that can invalidate cache outcomes.

These boundaries are enforced by file ownership in `crates/storage` — see [`storage.md`](storage.md).

## Test layers

Tests run at four layers: raw facts, normalized events, projections / API, and verified execution traces. The TypeScript conformance harness under `tests/conformance/` checks protocol and consumer-capability behavior end-to-end.

---

[^bn-l1resolver]: (upstream: .refs/basenames/src/L1/L1Resolver.sol:L13 @ basenames@1809bbc)
[^bn-readme-base]: (upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc)
[^v2-pr-token]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L451 @ ens_v2@554c309)
[^ens-univ]: <https://docs.ens.domains/resolvers/universal/>
[^v1-namechanged]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/NameResolver.sol:L10 @ ens_v1@91c966f)
[^v1-nw-fuses]: (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L10 @ ens_v1@91c966f)
[^v2-eac]: (upstream: .refs/ens_v2/contracts/src/access-control/interfaces/IEnhancedAccessControl.sol:L19 @ ens_v2@554c309)
[^v1-aur-verify]: (upstream: .refs/ens_v1/contracts/universalResolver/AbstractUniversalResolver.sol:L217 @ ens_v1@91c966f)
