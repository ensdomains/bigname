# Development plan

Internal reference. Implementation sequencing and the phased plan that drove bootstrap and the first vertical slices. This isn't the source of truth for what works today — that's the top-level `docs/` files. This file describes ordering and what's still ahead.

Parallel ownership boundaries live in [`workstreams.md`](./workstreams.md).

## Principles

- Freeze public semantics before optimizing internals.
- Freeze surface / resource identity before building APIs.
- Live ingestion and backfill share one downstream pipeline.
- Raw facts immutable, projections disposable.
- Verified execution is a subsystem from the start, not bolted on.
- Measure replacement by consumer-capability coverage, not legacy API parity.
- Cohesive end-to-end slices over broad scaffolding with undefined semantics.
- Deployment profiles are explicit and mutually exclusive within one canonical corpus.

## Milestones

| Milestone | Means |
| --- | --- |
| **A. Truth core** | The system can ingest, normalize, bind public surfaces to backing resources, and replay deterministically. |
| **B. Declared-state slice** | Native `v1` reads for exact names, address collections, child collections, roles, and history from declared state. |
| **C. Verified slice** | Verified resolution and primary-name answers with full provenance. |
| **D. Consumer replacement** | First-party apps can switch to native `v1` without relying on the existing ENSv1/v2 indexer API shape. |

## Phases

### Phase 0 — Scope freeze

Turn the architecture into implementation-ready semantics: versioned `v1` resources, query semantics for `namespace`/`at`/`chain_positions`/`consistency`/`mode`, frozen coverage and exhaustiveness, frozen canonicality, namespace assignment, `NameSurface`/`SurfaceBinding` taxonomy, collection semantics for the read families, `RecordInventory` semantics, preimage observation, chain-intake contract, the consumer-capability matrix, workstream boundaries.

Exit: no unresolved ambiguity in the first `Name`, `Address`, `Resolution`, `PrimaryName`, `Resolver` reads, in surface-vs-resource behavior, in `ens` vs `basenames` ownership, or in what "replacement" means for first-party consumers.

### Phase 1 — Repo skeleton

Rust workspace; initial API/indexer/worker binaries; shared crates (domain, storage, manifests, adapters, execution); local dev env; migration framework; baseline observability and config; CI.

### Phase 2 — Storage foundation

Chain-lineage schema; raw transaction/receipt/log tables; manifest tables; discovery tables; `NameSurface`/`SurfaceBinding`/`resource_id` schema; normalized-event tables; preimage observation tables; canonicality model; provenance refs; execution-trace schema; replay checkpoints.

Exit: one chain segment can be ingested into raw facts and replayed into normalized events; a public name surface can be bound to a backing resource; orphaned facts can be marked and excluded; execution traces can persist before full verified execution exists.

### Phase 3 — Intake, manifests, discovery

Ethereum + Base intake; code-hash capture; manifest bootstrapping; contract discovery graph; proxy/implementation tracking; discovery-edge provenance; watch-list expansion.

Exit: both chains follow forward; every watched contract is explainable through a manifest or discovery edge; manifest changes propagate; later additive Sepolia support remains a separate profile choice.

### Phase 4 — ENSv1 adapter slice

ENSv1 registry, `.eth` registrar, NameWrapper, resolver adapters; dynamic resolver discovery from registry `NewResolver`; reverse/primary adapter; preimage observation from name-bearing events; first normalized-event rules for registration/renewal/expiry, wrapper changes, resolver changes, record changes, reverse changes; first declared projections (name snapshot, address names, record inventory, history, primary claim).

Exit: minimal ENSv1 `.eth` lookup works; wrapped/unwrapped names without identity confusion; declared direct child enumeration; history reconstructs from normalized events; declared record/resolver-overview support requires admitted resolver-profile state.

### Phase 5 — ENSv2 adapter slice

`sepolia-dev` profile under `manifests-sepolia-dev/`. Admits `ens_v2_root_l1`, `ens_v2_registry_l1`, `ens_v2_registrar_l1`, `ens_v2_resolver_l1`. RootRegistry/ETHRegistry/UserRegistry/ETHRegistrar/PermissionedResolver adapters; `TokenResource`/`TokenRegenerated`/`SubregistryUpdated`/`ParentUpdated`/`AliasChanged`/`EACRolesChanged` mappings; resource/token-lineage handling; alias and wildcard topology; role and resolver projections; surface-binding projection across all binding kinds.

Exit-name profile promotes only on `sepolia-dev` when `ens_v2_registrar_l1` declares `exact_name_profile = "supported"`. Doesn't widen mainnet, reverse/primary, wrapper, migration, universal-resolver, verified resolution, or execution-explain.

### Phase 6 — Declared-state API slice

`Name`, `Address`, `Resolver`, `History`, `Permissions` reads; declared `Resolution` with topology + record inventory + record cache; explain views (`surface-binding`, `authority-control`, `coverage`); the OpenAPI artifact frozen at `docs/api-v1.openapi.json`; replay-stable `cursor`/`page_size` plus default sorts.

Minimum reads: exact name lookup, `Address.names` (with `include=role_summary`), name → children, name/resource → role holders, name/resource/address history, resolver overview.

Exit: first-party data needs that rely only on declared state can be served from native `v1`; every response carries provenance/coverage/chain-position context; collection semantics match Phase 0.

### Phase 7 — Verified execution slice

ENS Universal Resolver execution (direct, alias-only, wildcard-derived classes); CCIP-Read; primary-name verification engine; execution trace persistence; cache and invalidation; `Resolution` and `PrimaryName` in `verified` and `both` modes; `GET /v1/resolve/{name}`; `GET /v1/explain/resolutions/{namespace}/{name}/execution`.

Exit: explicit record reads work in `verified` mode for the supported slice; primary-name reads distinguish claim from verified; every verified answer produces a traceable trace; relevant changes invalidate cached answers.

### Phase 8 — Basenames slice

Base-side registry/registrar/resolver adapters; dynamic Base resolver discovery from registry `NewResolver`; Basenames primary-name support; Base authority projections; L1 compatibility transport context in resolution; Basenames-specific control facets (token ownership, management, address-resolution); Basenames coverage and explain rules; Basenames history.

`basenames_execution` v2 carries `verified_resolution=supported` for one class only: exact-surface transport-assisted direct path through the L1 Resolver, including CCIP-Read participation. Everything else stays `unsupported`.

Exit: `basenames` reads served through the same native `v1`; Base authority and L1 transport clearly separated in provenance; Basenames transfer scenarios map onto `ControlVector`; declared record/resolver-overview support requires Base-side `L2Resolver`-compatible profile admission.

### Phase 9 — Reorg, replay, backfill

Fork detection; canonical invalidation; deterministic replay tooling; historical backfill; persisted backfill jobs (bounded selector + finite range, idempotent helpers); resumable backfill runner; source-scoped selector modes; automatic bootstrap from manifest `start_block`; reorg-driven `execution_cache_outcomes` invalidation; canonicality, lineage-range, backfill-job, execution-trace, and replay inspection tooling; raw-fact normalized-event replay runner; conformance jobs against backfilled data.

Exit: simulated reorg rebuilds correct answers; backfill reuses the same adapter and projection path as live ingestion; replay determinism from raw facts and from normalized events; orphan-block-dependent cache outcomes invalidated without deleting traces or steps; backfill helpers resumable, idempotent, bounded; backfill never promotes chain checkpoints.

### Phase 10 — Consumer replacement and hardening

Consumer adapter / SDK layer for the apps monorepo; app-by-app migration plan; capability-by-capability cutover checklist; capability-level parity tests; dashboards and SLOs; live audit jobs; chaos / reorg drills; manifest drift and proxy-upgrade alerting; release and rollback runbooks.

Exit: first-party apps no longer require the existing ENSv1/v2 indexer API surface; remaining gaps tracked as unsupported native `v1` capabilities; operations can detect drift before it becomes user-visible.

## First vertical slice

To validate the architecture before broader expansion:

1. exact ENS name lookup for a small canonical subset
2. surface binding + backing resource summary
3. declared authority and control
4. declared resolution topology
5. declared record inventory
6. verified address resolution
7. claimed vs verified primary-name answer
8. provenance and coverage in every response

## Second vertical slice

To prove the architecture handles parts the legacy model can't express cleanly:

1. ENSv2 dynamic subregistries
2. linked-subregistry surfaces
3. token regeneration with stable resource identity
4. resource-centric permissions
5. resolver overview
6. role history
7. alias-aware verified resolution

## Consumer migration

The apps monorepo is migrated by capability group, not by endpoint mirroring.

| Group | Capabilities |
| --- | --- |
| 1. Profile and records | exact name profile, registration/expiry, record inventory, verified record reads |
| 2. Collections | address → names, with role summary, name → children, child counts |
| 3. History and permissions | name history, address history, role holders, role history, resolver overview |
| 4. Primary names | claimed primary, verified primary, failure states |

Each group needs contract tests, app integration tests, explain/provenance checks, rollout/rollback criteria.

## Test layers

| Layer | Where |
| --- | --- |
| raw facts | crate tests, fixtures |
| normalized events | adapter tests |
| projections / API | conformance harness, integration tests |
| verified execution traces | execution tests, conformance harness |

Required suites: fixture-based protocol tests, forked-chain integration, replay determinism, reorg, manifest drift, consumer-capability conformance, performance for collection reads, execution cache invalidation.

## Supersession

bigname supersedes the existing indexers when:

- first-party apps use native `v1` for all required naming capabilities,
- exact lookup, collections, permissions, history, resolution, and primary-name answers are served with explicit coverage / provenance,
- ENSv2-native features are represented without legacy identity distortions,
- Basenames is served as a first-class public namespace,
- operational replay, reorg handling, and drift detection are production-hardened.

This is capability replacement, not schema impersonation.
