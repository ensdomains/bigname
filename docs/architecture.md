# Architecture

bigname is a versioned read API for ENS (v1 and v2) and Basenames, served from projections built off chain events. This doc describes the model: namespaces, identity, intake, projections, execution, and how they fit together. Wire format is in [`api-v1.md`](api-v1.md); persistence in [`storage.md`](storage.md); intake in [`chain-intake.md`](chain-intake.md); manifests in [`manifests.md`](manifests.md); projections and execution have their own docs.

## What we promise

For any supported name or address, an answer is:

- **Point-in-time.** A response pins the `ChainPositions` it was derived from. The same input + positions replays to the same output.
- **Provenance-tagged.** Every projected row knows which normalized events produced it, which manifest version was active, and where on the chain it sits.
- **Coverage-explicit.** Routes report what they cover (`full`, `partial`, `observed_only`, `unsupported`, `stale`) and why.
- **Reorg-safe.** Block hash is identity. Forks unwind by walking parent hashes. Losing branches stay around as audit, marked `orphaned`.

## Pipeline at a glance

```
                       ┌──────────────────────────────────────────┐
                       │          manifests + discovery           │
                       │  (which contracts to watch, capabilities)│
                       └────────────────────┬─────────────────────┘
                                            ▼
   chain head ──► intake ──► raw facts ──► adapters ──► normalized events
                  (lineage)  (logs, calls)            (per-source-family)
                                                              │
                                                              ▼
                                                   ┌──────────────────┐
                       API reads ◄─────────────────┤   projections    │  ◄─ replayable
                       (declared)                  │   (current_*)    │
                                                   └──────────────────┘
                                                              │
                                                              ▼
                       API reads ◄─────────────  execution traces + cache  ──► verified answers
                       (verified)               (Universal Resolver, CCIP)
```

Live ingestion and backfill share the same downstream stages. Backfill is just a bounded job that drives the same pipeline against historical ranges.

## Namespaces

Public namespaces are exactly two:

| Namespace | Covers | Authority chain |
| --- | --- | --- |
| `ens` | ENSv1 + ENSv2 names, including `base.eth` itself | Ethereum L1 |
| `basenames` | Basenames-issued `*.base.eth` | Base |

`ens` absorbs ENSv1 and ENSv2 as internal authority epochs. `base.eth` stays under `ens` because upstream treats it as the L1 root domain handled by the Mainnet `L1Resolver` [<sup>1</sup>](#refs).

A `NamespaceRegistry` decides assignment with versioned rules — `exact_name` first, then `suffix`, then `authority_root`. The current policy is: `base.eth → ens`; `*.base.eth → basenames`; everything else ENS-supported → `ens`. Conflicts reject canonical admission *before* a `logical_name_id` is minted.

Deployment **profile** is separate from namespace. A profile (`mainnet`, `sepolia-dev`) selects which chains and manifest root are admitted. One runtime answers under one profile at a time.

## Identity

Four identity layers, each with its own continuity rules. Mixing them is the most common modelling mistake; they are deliberately separate.

```
NameSurface (logical_name_id)        public surface, e.g. "ens:alice.eth"
       │
       │ SurfaceBinding             how a surface binds to a backing object
       ▼   (active_from..active_to)
BackingResource (resource_id)        the authority anchor
       │
       │ token-lineage rotation
       ▼
TokenLineage (token_lineage_id)      tokenized ownership history
       │
       ▼
ContractInstance (contract_instance_id)
                                     a registry/registrar/resolver/wrapper instance
```

| Layer | What it survives | What rotates it |
| --- | --- | --- |
| `logical_name_id` | backing-resource swaps, token regeneration, lapses, re-registration | nothing — it's the public surface |
| `resource_id` | holder/controller/resolver/expiry/grace/fuse changes; ENSv2 token regeneration | authority moves to a new anchor (registry-only ↔ registrar ↔ wrapper, full lapse + re-registration) |
| `token_lineage_id` | renewal, transfer, expiry, grace within the same anchor; `TokenRegenerated` | authority moves to a different tokenized anchor; new lease after lapse |
| `contract_instance_id` | proxy implementation churn; manifest re-admission | a different watched contract address |

Format:

- `logical_name_id = "<namespace>:<normalized_name>"` — e.g. `ens:wallet.linked.parent.eth`. Derivable, human-readable, no DB lookup needed.
- `resource_id`, `token_lineage_id`, `contract_instance_id`, `surface_binding_id`, `execution_trace_id` are opaque UUIDs.

### ENSv2 specifics

`resource_id` keys to the upstream EAC resource via `getResource(anyId)`, not the current ERC-1155 token id. `TokenResource(tokenId, resource)` links a fresh token to the resource; `TokenRegenerated(oldTokenId, newTokenId)` swaps tokens while leaving the resource alone — `eacVersionId` stays put while `tokenVersionId` increments [<sup>2</sup>](#refs). Unregister + re-register increments both and mints fresh `resource_id` and `token_lineage_id`.

### ENSv1 anchor moves

The same `.eth` name moves through three possible anchors over its life:

| Anchor change | `resource_id` | `token_lineage_id` |
| --- | --- | --- |
| Register `alice.eth` (registrar lease) | new registrar-anchored | new registrar lineage |
| Wrap `alice.eth` | close registrar binding, open wrapper-anchored | new wrapper lineage |
| Unwrap before lease ends | reactivate prior registrar | reactivate prior registrar lineage |
| Expiry / grace, no anchor change | unchanged | unchanged |
| Re-register after full lapse | new | new |
| Registry-only `sub.alice.eth` | one registry-anchored | none (untokenized) |

A new `SurfaceBinding` row appears only when the bound `resource_id` changes — transfer and expiry within the same anchor don't produce one.

## Name surface and bindings

Two layers separate the public name from its backing object:

- **`NameSurface`** — one canonical row per `logical_name_id`. Stores `input_name`, `canonical_display_name`, `normalized_name`, `dns_encoded_name`, `namehash`, `labelhashes`, `normalizer_version`, plus normalization warnings. Per-observation spellings live in immutable preimage facts and normalized events, not in extra surface rows.
- **`SurfaceBinding`** — `(surface_binding_id, logical_name_id, resource_id, binding_kind, active_from, active_to, provenance, canonicality)`. Records how a surface binds to a backing resource through time.

Binding kinds: `declared_registry_path`, `linked_subregistry_path`, `resolver_alias_path`, `observed_wildcard_path`, `migration_rebind`, `observed_only`.

This split captures one resource under multiple public names, alias-resolved names without direct registry entries, observed wildcard names, and surfaces that rebind across time.

## Normalization and preimages

Normalization is version-pinned via `normalizer_version`. The canonical surface carries one representative result; alternate spellings persist as immutable `PreimageObserved` facts.

Preimages come from registrar/registry events with explicit labels, wrapper events with human-readable names, reverse/primary flows that reveal names, and metadata when a manifest allows. Invalid input doesn't get silently coerced into a valid identity.

For ENSv1, resolver `NameChanged(node, name)` strings observed via reverse/primary flows are preimage-only [<sup>3</sup>](#refs) — they can attach an existing forward-node fact to a human-readable name, but they can't synthesize ownership, resolver, or record facts on their own.

For ENSv2, admitted registry, registrar, and resolver name-bearing events (`LabelRegistered`, `LabelReserved`, `ParentUpdated`, `NameRegistered`, `NameRenewed`, `AliasChanged`, `NamedResource`, `NamedTextResource`, `NamedAddrResource`) all produce preimage observations [<sup>4</sup>](#refs). Preimages don't write projections or change manifest capability state.

## Source families

Each event source belongs to a `source_family`. Family ownership is fixed and exclusive — capability ownership attaches to the declaring family only.

**ENS**

| Family | Owns |
| --- | --- |
| `ens_v1_registry_l1` | current ENS registry + migration-aware `ENSRegistryOld` intake |
| `ens_v1_registrar_l1` | `.eth` BaseRegistrar + label-bearing controller intake |
| `ens_v1_wrapper_l1` | NameWrapper authority, fuses, wrapper-revealed names |
| `ens_v1_resolver_l1` | PublicResolver and admitted ENS Labs PublicResolver-generation profiles |
| `ens_v1_reverse_l1` | mainnet ReverseRegistrar claim intake |
| `ens_dns_l1` | DNS-imported names |
| `ens_offchain_metadata` | offchain metadata where a manifest allows |
| `ens_v2_root_l1` / `ens_v2_registry_l1` / `ens_v2_registrar_l1` / `ens_v2_resolver_l1` | the ENSv2 stack on `sepolia-dev` |
| `ens_execution` | verified resolution at the official Universal Resolver proxy `0xeEeE…EeEe` [<sup>5</sup>](#refs) |

**Basenames**

| Family | Owns |
| --- | --- |
| `basenames_base_registry` / `_base_registrar` / `_base_resolver` | Base-side authority [<sup>6</sup>](#refs) |
| `basenames_base_primary` | Base-side reverse claim intake (claim only) |
| `basenames_l1_compat` | L1 Resolver as compatibility transport |
| `basenames_execution` | verified resolution at the same L1 Resolver address |

**Shared:** `shared_manifests`, `shared_normalization_rules`, `shared_capability_registry`.

### A few non-obvious rules

- ENS verified resolution lives in `ens_execution` at the Universal Resolver **proxy**, not in `ens_v1_registry_l1`. The pinned `.refs/` artifact is the implementation; the route-facing entry is the proxy.
- `basenames_l1_compat` and `basenames_execution` reference the same L1 Resolver address but stay distinct families — one is transport, one is execution.
- ENSv1 dynamic resolver admission gates *complete* family coverage. Unadmitted resolvers stay `pending` or `unsupported`; `NewResolver` observation alone isn't enough.
- `ENSRegistryOld` is migration-aware: a current-registry `NewOwner` marks a node migrated, after which old-registry updates for that node are suppressed (root resolver excepted) [<sup>7</sup>](#refs).

## Manifests and discovery

Manifests pin watched contracts per source family at `manifests/<namespace>/<source_family>/<version>.toml`. They carry `manifest_version`, `namespace`, `source_family`, `chain`, `deployment_epoch`, `rollout_status` (`draft|shadow|active|deprecated`), `normalizer_version`, `capability_flags` (`unsupported|shadow|supported`), `roots`, `contracts`, `discovery_rules`. `start_block` is optional — if omitted, adapters preserve "unknown" rather than guessing zero.

Manifest changes are themselves normalized events: `SourceManifestUpdated`, `ProxyImplementationChanged`, `CapabilityChanged`.

**Discovery** expands the canonical graph through time-versioned edges: resolver, subregistry, parent, alias, metadata, proxy/implementation, migration, transport. Watch-plan expansion starts from active root `contract_instance_id`s and traverses active edges by id. Address-only watch rows derive from instance + active range and stay explainable through the graph.

A discovered contract is authoritative only if reachable from a canonical root or admitted by a manifest. Re-declaring the same address mints no new instance — it appends an active range. Proxy and implementation are separate `contract_instance_id`s; implementation churn updates the edge, not the proxy id.

Schema and discovery edge model live in [`manifests.md`](manifests.md).

## Intake

Three intake planes for the selected profile:

1. Ethereum L1 chain intake
2. Base chain intake
3. Execution intake (verified reads, CCIP)

A profile may not need every plane — an Ethereum-only run boots without a Base RPC, and Base intake reports idle/unavailable rather than failing startup.

Per-block stages:

```
1. block lineage           5. adapter routing
2. txs/receipts/logs       6. normalized event persistence
3. raw fact persistence    7. projection updates
   + payload-cache meta    8. execution-cache invalidation
4. manifest/discovery
```

Postgres is the hot indexed store. Lineage anchors, selected target logs, replay-required call snapshots, code-hash observations, and compact payload-cache metadata are durable. Large block payloads, non-indexed transaction/receipt bodies, and non-audit raw-log staging rows are evictable cache once their replay contract is satisfied. Empty historical blocks retain only lineage anchors and audit metadata.

Backfill is bounded persisted jobs with resumable range checkpoints. It uses the same downstream stages as live intake. Backfill range checkpoints are operational state — they don't promote `canonical_head`, `safe_head`, or `finalized_head`. Reconciliation, fetch, notification, and historical-backfill detail are in [`chain-intake.md`](chain-intake.md).

## What's immutable, what rebuilds

| Immutable (durable facts) | Rebuildable (projections) |
| --- | --- |
| blocks, transactions, receipts, logs | `name_current`, `address_names_current`, `children_current` |
| contract code hashes | `permissions_current`, `resolver_current` |
| manifests, discovery edges | `record_inventory_current`, `primary_names_current` |
| normalized events, normalization results | history materializations, coverage snapshots |
| preimage observations | execution cache (outcomes, not traces) |
| selected `eth_call` snapshots | subscriptions, feeds |
| CCIP request/response digests, verification outcomes | resource-role indexes, resolver indexes |
| metadata responses, sync cursors | reverse / address indexes |

For large payloads, the durable fact may be selected replay fields plus optional cache metadata or a digest, not the full body. Compaction can evict non-critical bytes after replay facts are extracted.

Every projected row carries provenance pointers, manifest version, canonicality state, and chain-position context.

## Domain objects

| Object | Purpose |
| --- | --- |
| `NameSurface`, `SurfaceBinding`, `BackingResource` | identity layers |
| `NameClass`, `RegistrationSnapshot`, `AuthoritySnapshot` | what kind of name and how it was registered |
| `ControlVector` | the full control picture — `token_holder`, `registrant`, `effective_controller`, `record_manager`, `delegates`, `reverse_manager`, `resolved_address_target`, `status`, `expiry`, `authority_epoch`, `resolution_epoch`. **Never a single-owner field.** |
| `PermissionSnapshot`, `TokenLineage` | who can do what to a resource; tokenized ownership |
| `ResolutionTopology`, `RecordInventory`, `RecordCache`, `PrimaryNameSnapshot` | resolution state |
| `SourceProvenance`, `CoverageSnapshot` | where this came from and how complete it is |
| `ExecutionResult` | a verified answer + trace |

`Registration.kind` ∈ `{lease, subname_assignment, reservation, dns_control, offchain_policy, observed_only}`.

Permissions and control are anchored to `resource_id`, never to surface text. The chain `logical_name_id → SurfaceBinding → resource_id → token_lineage` must remain reconstructible across time.

## Normalized event taxonomy

Every normalized event carries: namespace, `logical_name_id` (when applicable), `resource_id` (when applicable), source family, manifest version, chain position, raw fact reference, derivation kind, canonicality flag, and before/after state where possible.

Grouped by purpose:

- **Identity / preimage / discovery:** `PreimageObserved`, `NameClassified`, `SurfaceBound`, `SurfaceUnbound`, `ContractDiscovered`, `MetadataChanged`, `SourceManifestUpdated`.
- **Registration / authority:** `RegistrationReserved`, `RegistrationGranted`, `RegistrationRenewed`, `RegistrationReleased`, `ExpiryChanged`, `AuthorityTransferred`, `AuthorityEpochChanged`, `MigrationApplied`, `CommitmentMade`, `PricingPolicyChanged`.
- **Lineage / control:** `TokenResourceLinked`, `TokenRegenerated`, `TokenControlTransferred`, `ResolutionEpochChanged`.
- **Topology / resolution:** `ResolverChanged`, `SubregistryChanged`, `ParentChanged`, `AliasChanged`, `WildcardCoverageChanged`, `RecordChanged`, `RecordDeleted`, `RecordVersionChanged`, `RecordInventoryObserved`.
- **Permissions:** `PermissionChanged`, `RootPermissionChanged`, `DelegateRetainedAfterTransfer`, `PermissionScopeChanged`.
- **Reverse / primary:** `ReverseChanged`, `PrimaryNameClaimed`, `PrimaryNameVerified`, `PrimaryNameInvalidated`.
- **Execution / coverage:** `VerifiedResolutionObserved`, `VerifiedResolutionInvalidated`, `CoverageChanged`.

Selected upstream mappings:

| Upstream | Normalized | Notes |
| --- | --- | --- |
| ENSv2 `TokenResource(tokenId, resource)` | `TokenResourceLinked` | the only event linking current token id to upstream EAC resource |
| ENSv2 `TokenRegenerated(old, new)` | `TokenRegenerated` | preserves `resource_id`, `token_lineage_id`, active binding |
| ENSv2 `SubregistryUpdated` / `ParentUpdated` | `SubregistryChanged` / `ParentChanged` | endpoints resolve to `contract_instance_id` first |
| ENSv2 `AliasChanged` (resolver) | `AliasChanged` | source + destination DNS-encoded names |
| ENSv2 `EACRolesChanged` | `PermissionChanged` / `RootPermissionChanged` | root-vs-resource distinction preserved (root fallback) |
| ENSv1 NameWrapper events | `SurfaceBound/Unbound`, `AuthorityTransferred`, `ExpiryChanged`, `TokenControlTransferred`, `PermissionScopeChanged`, etc. | `PermissionScopeChanged` carries fuse changes; never mints subject grants |
| ENSv1 PublicResolver events | `ResolverChanged`, `RecordChanged`, `PermissionChanged`, … | gated on profile admission for full coverage |

## Resolution

`Resolution` is one mixed envelope with three declared sections plus one verified section: `topology`, `record_inventory`, `record_cache`, `verified_queries`.

### Declared topology

```
topology
├─ registry_path        ordered NameRef[]: surface → registry authority (never empty when supported)
├─ subregistry_path     ordered NameRef[]: surface → nearest declared subregistry (empty if none)
├─ resolver_path        ordered hops with chain_id, address, latest_event_kind, …
├─ wildcard             { source: NameRef|null, matched_labels: string[] }
├─ alias                { final_target: NameRef|null, hops: NameRef[] }
├─ version_boundaries   { topology_version_boundary, record_version_boundary }
└─ transport            { source_chain_id, target_chain_id, contract_address, latest_event_kind }
                        all-null = no transport. Basenames promotion = base→ethereum via L1 Resolver.
```

For ENSv2, `alias` is declared topology only when `PermissionedResolver` provides an `AliasChanged` mapping (resolved by longest suffix). `wildcard` is observed topology — populated only when execution input identifies an ancestor/source resolver and matched labels.

### Record inventory and cache

`record_inventory` defines the stable selector space admitted by the route (`record_version_boundary`, `enumeration_basis`, `selectors`, `explicit_gaps`, `unsupported_families`, `last_change`). It's not a global enumeration. Selectors carry `record_key`, `record_family`, `selector_key`, `cacheable`. `record_key` is always text — `record_family + ":" + selector_key` for parameterized families, or just `record_family` for scalars. Coin types are textual on the wire so `record_key` round-trips.

`record_cache` carries last-known declared values for supported records. Status ∈ `{success, not_found, unsupported}`. `value` appears only on `success` and uses the family-native JSON shape.

A version change invalidates inventory and cache for the prior boundary.

### Verified queries

Execution-derived answers per requested record selector, reusing `ResultStatus`. Verified queries don't backfill `record_inventory` or `record_cache` in the same response.

Public verified support is narrower than the topology model. ENS supports three exact-surface classes:

| Class | When it fires |
| --- | --- |
| direct path | `resolver_path[0].logical_name_id == route surface`, no wildcard, no alias, no transport |
| alias-only non-direct | same, but `alias.final_target` non-null with non-empty `hops` |
| wildcard-derived | `wildcard.source` non-null with matched labels; resolver lives at the source surface; `subregistry_path=[]`, `transport=null` |

Other ENS classes (non-alias ancestor-selected, linked-subregistry ancestor-selected, transport-assisted, CCIP-participating) return per-selector `unsupported`.

Basenames currently supports only **exact-surface transport-assisted direct path** through active `basenames_execution` v2 at the L1 Resolver. Other Basenames classes are `unsupported`.

Verified answers persist an `ExecutionTrace`. `ExplainResolution` shows resolver selection, wildcard traversal, alias rewriting, record version boundary, CCIP steps, and the source event or execution result that last changed the answer.

## Permissions

Permissions are first-class projections and explain views. Grants are tracked by scope: `root`, `registry`, `resource`, `resolver`, `record_manager` / `operator`, `migration_derived`, `transport_derived`. Each grant records source, revocation source, inheritance path, transfer behaviour, scope, and effective powers.

Reads expose `effective_powers` directly so callers don't reconstruct authority from raw role bitmaps. The first declared route is resource-centric: `GET /v1/resources/{resource_id}/permissions`. Name-, address-, and resolver-centric views summarize or filter the same resource-anchored truth.

For ENSv1 wrapper-backed resources, `effective_powers` is masked by the active NameWrapper fuse state before publication. `PermissionScopeChanged` carries fuse changes that *remove* powers — it never invents subject grants.

For ENSv2, `getResource(anyId)` keys permissions by upstream resource, so public permissions key by the bigname `resource_id` linked to that resource — not by token id. Resolver-scoped permissions live in the same resource-anchored model with resolver scope metadata; `PermissionedResolver` uses name-, text-key-, and coin-type-specific EAC resources for setters.

Indexes: by resource, by account, by resolver; permission history by resource and by account.

## Primary and reverse names

`PrimaryName` is address- and `coin_type`-centric, not just a reverse projection. It persists `claimed_primary_name`, `verified_primary_name`, `reverse_namespace`, `coin_type`, `resolver`, provenance, coverage.

| Rule | Where it bites |
| --- | --- |
| `claimed_primary_name` is candidate-only | `mismatch` and `execution_failed` apply only to verified |
| `verified_primary_name` is authoritative only on `success` | reverse claims alone don't verify; verification must resolve back to the requested address [<sup>8</sup>](#refs) |
| Unnormalizable raw claims surface `invalid_name` | not silently dropped |

For ENS, declared claim precedence is reverse-only through `ens_v1_reverse_l1`. `claimed_primary_name.name` comes only from the matching `primary_names_current(address, coin_type, namespace)` row — never synthesized from manifest presence, resolver identity, or verified execution.

For Basenames, declared claim intake is `basenames_base_primary`. Verified primary names come from `basenames_execution` against the L1 Resolver.

Verified-primary cache identity is `request_type=verified_primary_name` with key `{namespace}:{normalized_address}:{coin_type}`. The matching `primary_names_current` row is the only claim-side anchor.

Provenance:

- `claimed_primary_name.provenance` — exact-tuple declared-only. **No `execution_trace_id`.**
- `verified_primary_name.provenance` — `{execution_trace_id, manifest_versions}`, with the trace id matching top-level `provenance.execution_trace_id`.

## Coverage and exhaustiveness

Coverage is contractual. Every response carries `coverage.status`, `coverage.exhaustiveness`, `coverage.source_classes_considered`, `coverage.unsupported_reason`, `coverage.enumeration_basis`.

| Read | Coverage shape |
| --- | --- |
| Exact-name lookup | authoritative for supported source classes; route-level coverage may stay authoritative even when individual subdocuments are unsupported |
| Address → names | exhaustive only for enumerable source classes |
| Wildcard / offchain names | not globally enumerable |
| Record inventory | `best_effort` unless a resolver family enumerates explicitly |
| Children | authoritative for declared direct children only |
| Primary name | `partial`, `non_enumerable`, `enumeration_basis=primary_name_lookup`, namespace-local `source_classes_considered` |

## Verified execution

Default entrypoints:

| Namespace | Entrypoint | Address |
| --- | --- | --- |
| `ens` | Universal Resolver proxy [<sup>5</sup>](#refs) | `0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe` |
| `basenames` | L1 Resolver (active v2) | `0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31` |

The execution engine handles onchain calls, wildcard resolution, alias-aware execution, nested CCIP-Read, batch/multicall, and proof/verification persistence.

Each verified answer persists an `ExecutionTrace`: entrypoint, resolver discovery path, contracts called, gateway URLs or digests, proof and callback checks, final result, errors, chain positions.

Cache identity = `(request, chain_positions, manifest_versions, relevant topology/version boundaries)`. Invalidate on reorg, manifest change, relevant topology change, relevant record change, relevant alias/wildcard change.

## Reorg, replay, backfill

Reorgs are explicit unwinds, not "latest row wins." The recipe:

1. detect fork point via parent hashes
2. mark affected facts `orphaned` (kept, not deleted)
3. invalidate dependent normalized events and execution cache
4. rebuild projections deterministically

Orphaned rows persist for explanation and rebuild. Detail is in [`chain-intake.md`](chain-intake.md).

Backfills use the same path as live ingestion (raw → manifest/discovery → normalized → projection). Source-scoped backfill is selected-target-only — it doesn't turn unselected block-wide bodies into hot rows just because they were fetched. Operational catch-up to the finalized head runs as bounded idempotent chunks; capacity failures pause the chunk explicitly.

Wildcard and offchain names can't be assumed enumerable; backfill for those classes is discovery- and observed-answer-based.

## Operations

**Metrics:** chain lag, safe/finalized lag, reorg depth, adapter failure rate, manifest drift, proxy upgrade detection, execution latency, CCIP error rate, verification failure rate, coverage partial rate, replay duration, backfill capacity checks (Postgres size, free disk).

**Worker tools.** None expose public `v1` routes; none mutate truth.

| Command | Purpose |
| --- | --- |
| `bigname-worker inspect canonicality --chain-id <id> --block-hash <hash>` | one-block lineage, canonicality, fact counts |
| `bigname-worker inspect ...` | execution traces, manifest drift / proxy alerts, surface bindings, resolver topology, raw facts, manifest versions |
| replay from checkpoint, backfill source range, rerun projections, invalidate execution cache, diff declared vs verified | repair |
| finalized-head catch-up | bounded chunks with capacity preflight (DB size, free disk, object-cache budget) |

Live manifest drift / proxy upgrade alerting is a worker loop. It writes nothing into `normalized_events`, doesn't mutate manifests or discovery, and exposes no public route.

## Constraints (held line)

- Versioned native public contract from day one.
- Namespace is explicit; never inferred silently.
- Public surface identity is distinct from backing resource, token, resolver instance, and reverse namespace identity.
- Provenance, coverage, and finality are part of the response, not a sidecar.
- Resolution isn't event-only; verified execution is a required subsystem.
- Permissions and source manifests are in the model, not bolted on.
- Projections are disposable. Protocol-specific logic lives in adapters and execution drivers, not in the public contract.
- No silent cross-source fallback. Every fallback shows up in provenance/explain.
- No requirement to preserve the ENSv1 indexer API surface.

## Implementation shape

Rust modular monolith. PostgreSQL is the hot indexed/replay store. Hash-addressed object storage for execution artifacts and durable raw payloads. Workers handle ingestion, projection, replay, execution. The public `v1` API is read-only over projections and execution output. A small TypeScript conformance harness checks protocol and consumer-capability behaviour.

```
apps/{api,indexer,worker}
crates/{domain,storage,manifests,adapters,execution,test-support}
tests/conformance
```

## Test matrix

| Class | Cases |
| --- | --- |
| ENSv1 + wrapper | ENSv1-only name, wrapped name, wrapped expiry/grace edge, fuse changes that alter control, wrapped owner ≠ registrant, reverse claim vs verified primary mismatch |
| ENSv2 | root-scope role grant, delegate retained after transfer, token regeneration without ownership change, shared subregistry creating multiple surfaces for one resource, alias-derived surface with no direct registry entry, subregistry swap replacing a subtree, re-registration with same resource and new token id |
| DNS / wildcard / offchain | imported DNS name, gasless DNS or metadata-discovered name, wildcard-derived subname, CCIP success, CCIP failure, offchain gateway mismatch |
| Basenames | NFT-only transfer, management-only transfer, address-resolution change, full transfer, primary-name set/unset, L1 compatibility resolution, current single-address capability |
| Operational | reorg across authority events, reorg across verified execution cache, replay determinism from raw facts, replay determinism from normalized events, proxy implementation change, manifest version change |

Validation runs at four layers: raw facts, normalized events, execution traces, public API output.

## Open questions

- exact Postgres partitioning strategy
- exact cache invalidation granularity for verified queries
- which execution artifacts stay inline in Postgres vs object storage
- exact raw-payload cache retention windows and which payload classes are durable
- whether subscriptions ship in the first stable read milestone or later

---

## References <a id="refs"></a>

Each upstream claim cites a pinned `.refs/` source. Format: `(.refs/<key>/<path>:L<line> @ <key>@<short-commit>)`.

1. `.refs/basenames/src/L1/L1Resolver.sol:L13`, `:L154` @ `basenames@1809bbc`; `.refs/basenames/README.md:L70` @ `basenames@1809bbc`.
2. ENSv2 EAC resource model: `.refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L34`, `:L67`, `:L72`; `.refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L69`; `.refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L28`, `:L203`, `:L237`, `:L451`, `:L461`, `:L536`, `:L545` @ `ens_v2@554c309`.
3. ENSv1 reverse `NameChanged` preimage rules: `.refs/ens_v1/contracts/resolvers/profiles/NameResolver.sol:L10`, `:L18`; `.refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L129`, `:L130` @ `ens_v1@91c966f`.
4. ENSv2 name-bearing events: `.refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L15`, `:L30`, `:L75`; `.refs/ens_v2/contracts/src/registrar/interfaces/IETHRegistrar.sol:L32`, `:L53`; `.refs/ens_v2/contracts/src/resolver/interfaces/IPermissionedResolver.sol:L14`; `.refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L132`, `:L142`, `:L153` @ `ens_v2@554c309`.
5. ENS Universal Resolver proxy: <https://docs.ens.domains/resolvers/universal/>; `.refs/ens_v1/deployments/mainnet/UniversalResolver.json:L2`; `.refs/ens_v1/contracts/universalResolver/UniversalResolver.sol:L8` @ `ens_v1@91c966f`.
6. Basenames Base-side authority: `.refs/basenames/README.md:L28-L34`, `:L69-L70`; `.refs/basenames/src/L2/Registry.sol:L10`, `:L19`, `:L132`, `:L223`; `.refs/basenames/src/L2/BaseRegistrar.sol:L15`; `.refs/basenames/src/L2/L2Resolver.sol:L22`, `:L182`, `:L193`, `:L209`, `:L225` @ `basenames@1809bbc`.
7. ENS migration-aware old-registry rules: `.refs/ens_subgraph/subgraph.yaml:L15`, `:L39`, `:L44`; `.refs/ens_subgraph/src/ensRegistry.ts:L134`, `:L230`, `:L238`, `:L246` @ `ens_subgraph@723f1b6`.
8. ENS verification-back-resolves rule: `.refs/ens_v1/contracts/universalResolver/AbstractUniversalResolver.sol:L217`, `:L226`, `:L263`, `:L269` @ `ens_v1@91c966f`.
