# Chain intake

Chain intake is canonical-chain reconciliation with a fact log attached. Subscriptions, filters, and provider notifications are latency hints; raw facts are append-only; canonicality and head promotion are explicit state. **Block hash is identity, block number is position.** Live ingestion and backfill share one downstream pipeline.

A deployment selects one chain profile at a time. Mainnet and Sepolia facts don't share a canonical corpus, checkpoints, or projection state. The ENSv2 `sepolia-dev` profile selects `manifests-sepolia-dev/` as a whole alternate profile and never loads beside `manifests/` in the same intake runtime.

Companion docs: [`architecture.md`](architecture.md), [`storage.md`](storage.md), [`manifests.md`](manifests.md).

## In scope

Truth-core intake covers durable replay facts and cache metadata for:

- blocks and lineage metadata
- selected and admitted target logs
- transaction, receipt, and block fields needed to decode selected logs or rebuild retained normalized events and execution outputs
- code-hash observations
- block-anchored call snapshots used by verified execution or enrichment
- optional cache metadata or digests for large payloads fetched outside the hot replay set

Block-anchored `raw_call_snapshots` remain intake-owned even when verified execution supplied the candidate request/response pair. Execution may hand off only snapshots anchored to the resolved chain position and only for paths that admit them. That handoff doesn't create a general execution-owned raw-fact write surface.

## Out of scope

- mempool / pending tx indexing
- node-local txpool APIs
- client-specific trace or state-diff indexing as a correctness dependency
- historical state reconstruction from non-archive upstreams

These may exist later as separate capabilities; they don't enter the core correctness model.

## Live and backfill share one path

```
   provider notification          backfill range
          │                              │
          ▼                              ▼
   ┌──────────────────────────────────────────┐
   │  fetch by hash → reconcile parent_hash  │
   └──────────────────────────────────────────┘
                       │
                       ▼
   ┌──────────────────────────────────────────┐
   │  one block admission unit (atomic):      │
   │   • chain_lineage row                    │
   │   • selected raw_logs/tx/receipts        │
   │   • normalized_events                    │
   │   • invalidations downstream             │
   └──────────────────────────────────────────┘
                       │
                       ▼
                advance heads
                (canonical, safe, finalized)
```

## Heads and the recent window

Per chain, intake tracks three persisted checkpoints that map directly to API consistency:

| Checkpoint | API `consistency` |
| --- | --- |
| `canonical_head` | `head` |
| `safe_head` | `safe` |
| `finalized_head` | `finalized` |

The recent reconciled window keys by `(chain_id, block_hash)` with at least `parent_hash`, `block_number`, `timestamp`. With auditable retention it also stores `logs_bloom`, `transactions_root`, `receipts_root`, `state_root` when the upstream exposes them. The window detects parent mismatch immediately, walks back to a common ancestor on reorg, backfills short parent gaps, and answers recent canonicality disputes. Number-to-hash mappings inside this window are derived views — block hash is the primary key.

## Block identity rules

- Block hash is the identity anchor for every block-scoped object.
- `parent_hash` is required in lineage storage.
- Lineage ancestry repair needs only block hash, parent hash, block number, timestamp, and canonicality state. Header audit fields are nullable in minimal mode.
- Every chain-derived raw fact carries `chain_id`, `block_number`, `block_hash`.
- Live indexing may fetch full block, tx, and receipt payloads, but Postgres retains only replay-critical hot facts and optional cache metadata for non-critical full bodies.
- Caches key by block hash first; block number is a secondary lookup.
- A downstream key that needs "current block number" resolves it to a block hash before reading block-scoped data.

## Notification and fetch contract

Subscriptions, filters, and polling are low-latency triggers, not durable truth. Connection loss does not imply data loss or canonical confirmation. Live ingestion never relies on subscription payloads alone as the persisted source of truth.

Live path:

1. Receive a head notification (poll or subscription).
2. Fetch the referenced block or header by hash where possible.
3. Reconcile `parent_hash` against the recent window.
4. Fetch exact block-scoped data.
5. Persist one block admission unit atomically.
6. Advance canonical, safe, and finalized checkpoints only after reconciliation.

For exact block-scoped data: logs are fetched by `blockHash`, not just block number. Providers that can't support that contract aren't acceptable for the live path. Receipts are fetched block-scoped first; transaction-by-transaction receipt fan-out is a fallback.

## Reconciliation

Reorg handling is an explicit unwind and replay. For each candidate canonical block:

1. If already known, update checkpoint promotion only.
2. If `parent_hash` matches the current canonical head, append.
3. If the parent is missing, backfill parents until continuity or an existing checkpoint.
4. If the parent conflicts, walk back through the recent window to a common ancestor.
5. Mark the losing branch `orphaned`.
6. Emit deterministic invalidation for normalized events and `execution_cache_outcomes` derived from orphaned blocks.
7. Admit the winning branch in canonical order.
8. Move the canonical head pointer last.
9. Promote blocks under safe and finalized checkpoints asynchronously and monotonically.

Reconciliation never depends on ad hoc deletes or "latest row wins" semantics.

Execution-cache invalidation from reorg is block-hash-scoped. It invalidates `execution_cache_outcomes` whose dependency set contains an orphaned `(chain_id, block_hash)` or a boundary resolved through one. It doesn't delete execution traces, steps, raw facts, or normalized events — those stay durable for replay and audit. Cache outcomes without explicit block-hash-bearing dependencies fail closed.

## Upstream requirements

For each chain in the selected profile, intake needs:

- block fetch by hash
- block fetch by number or canonical tag
- log fetch by exact block identity
- receipt fetch (block-scoped where supported, with a bounded fallback)
- code and call reads at pinned chain positions
- safe and finalized head visibility

A self-hosted post-Merge Ethereum upstream means an execution client and a consensus client together.

Production correctness depends on `safe` and `finalized` support. Sources that can't surface those checkpoints are bootstrap or shadow only.

Historical state-heavy enrichment requires archive-capable upstreams, a separately retained durable replay corpus, or fail-closed behavior when the cache-fill path can't satisfy its block-hash-scoped fetch and digest checks. Provider history is bounded; intake retains its own durable hot replay facts.

### Provider configuration

The indexer reads provider sources from environment:

| Variable | Format |
| --- | --- |
| `BIGNAME_INDEXER_MANIFESTS_ROOT` | path to the chosen manifest root |
| `BIGNAME_INDEXER_CHAIN_RPC_URLS` | `<chain>=<url>,…` (HTTP only) |
| `BIGNAME_INDEXER_CHAIN_RETH_DB_SOURCES` | `<chain>=<datadir>,…` for same-host Reth |
| `BIGNAME_INDEXER_RETAIN_HEADER_AUDIT_FIELDS` | `true` to retain logs_bloom et al. |

At most one provider per chain. JSON-RPC is the portable source. A Reth DB source is an optional intake source, not a protocol adapter — adapters still consume bigname raw facts. The Reth-backed reader satisfies the same block-hash-first contract as JSON-RPC; it fails closed when the local store is unavailable, pruned, inconsistent, or can't surface the requested checkpoint or historical payload.

Provider availability is per profile and per active watched chain. A Base provider isn't a global startup prerequisite. An Ethereum-only profile starts without a Base provider; a profile whose Base chain has no provider leaves Base intake idle with `no_provider` rather than failing startup. A provider for a chain outside the selected profile is invalid.

## Backfill

Backfill runs as bounded persisted jobs scoped to one selected profile, chain, source selector, scan mode, and explicit block range. The job range is finite at creation time; open-ended tail following is live intake.

Selector modes:

| Mode | Targets |
| --- | --- |
| `whole_active_watched_chain` (default) | every active watched target whose range intersects the job range |
| `source_family` (`--source-family <family>`) | active targets in that family with intersecting range |
| `watched_target_set` | explicit set, identified by `contract_instance_id` |

The persisted source identity is the resolved sorted target tuple — not the CLI spelling. If the active watch plan has shifted such that the same selector now resolves differently, idempotency-key reuse conflicts instead of mutating the existing job.

For very large source-family target sets, the persisted identity uses `source_identity_payload_format=selected_targets_digest_v1` with the digest of the sorted canonical target tuple plus a first/last sample.

### Automatic bootstrap

`bigname-indexer run` creates historical backfill work from the selected manifest root and materialized watch plan as finite persisted jobs. It doesn't run an implicit unbounded scanner.

Rules:

- Runs after manifest sync, discovery admission, watch-plan materialization, and per-chain checkpoint setup.
- Active watched chains without configured providers stay idle.
- Bootstrap covers each eligible target from its manifest/discovery admitted start through the finite provider head observed at job creation.
- Each candidate target keys by `contract_instance_id`, source family, chain, normalized address, effective range — not raw address.
- Eligible targets whose finite ranges overlap share raw-fact job segments by default. Source-scoped jobs remain an explicit operational mode.
- A finite segment may partition into contiguous child `backfill_ranges` for internal worker leases. Child ranges preserve the same job source identity, declared bounds, and ownership.
- A target with declared `start_block` is eligible from that block, narrowed by its active watch range and the bootstrap end.
- A target with omitted `start_block` is skipped explicitly. Bootstrap doesn't infer the start from block zero, the current job range, manifest activation, provider history, or any default.
- Every created job has finite declared start and end before insertion.
- Bootstrap lifecycle never advances `canonical_head`, `safe_head`, or `finalized_head`.

Automatic bootstrap is operational readiness only. It doesn't widen public routes, route-level coverage, manifest capability flags, or consumer-replacement meaning.

### Job lifecycle

```
   pending ──► reserved ──► running ──► completed
                  │             │
                  └─► failed    └─► failed
```

| State | Meaning |
| --- | --- |
| `pending` | job/range exists, no worker owns it |
| `reserved` | a worker has a lease |
| `running` | the reserved worker is advancing |
| `completed` | every range checkpoint reached its declared end |
| `failed` | stopped with recorded failure metadata; retries create or reserve explicit remaining work |

`bigname-indexer backfill` supplies or reuses an idempotency key for one immutable shape: profile, chain, source selector, scan mode, finite start, finite end. Same key + same shape → reuse. Same key + different shape → fail with explicit conflict (no widening, no source-identity change, no checkpoint reset).

### Selected-target intake

Source-scoped jobs are selected-target-only and block-hash-scoped. The runner may use block-number ranges to enumerate candidate blocks, but every persisted block-scoped fact is anchored to the resolved block hash before admission. A source-scoped job doesn't opportunistically admit unselected watched targets merely because they appear in the same block.

For ENSv1 generic resolver events, source-scoped or per-target backfill is an operational repair mode. It is not the default semantic model for generic resolver-local event intake. PublicResolver-generation profile admission isn't the address set for baseline `AddrChanged`, `AddressChanged`, `TextChanged`, or `VersionChanged` observations. Whole-active-watched-chain backfill may combine the generic resolver topic scan with address-scoped families in one raw-fact range: resolver events are topic-scanned across all emitters, while non-resolver families keep their address-scoped filters. Topic matches whose ABI payload doesn't decode as the upstream resolver event are retained raw facts but don't become selector/cache evidence.

### Canonicality at admission

When historical backfill admits finalized or safe ranges, persisted lineage, raw facts, and normalized events carry the best canonicality state supported by checkpoint evidence: `finalized` for ranges proven below the finalized checkpoint, `safe` for ranges proven below the safe checkpoint, `canonical` otherwise. They don't stay `observed` merely because they entered through backfill. If the provider or retained lineage can't prove the relationship, the runner fails closed or persists the weaker explicit state and reports the gap. Backfill lifecycle still doesn't promote `canonical_head`, `safe_head`, or `finalized_head`.

Source-scoped backfill avoids retaining unselected block-wide tx, receipt, or full block bodies. If the runner fetches broader payloads to locate or verify selected target facts, Postgres keeps selected-target logs, minimal lineage and header anchors, replay-required enrichments, and any cache metadata needed for block-hash-scoped admission or audit. Historical blocks with no selected target facts retain only one `chain_lineage` header anchor.

### Operational finalized catch-up

Catch-up to finalized head is a sequence of bounded backfill jobs, not a hidden scanner. Each chunk has an immutable shape, an idempotency key, a finite start, and a finite end no greater than the finalized head observed when the chunk is created. Following finalized means repeatedly creating the next chunk after the prior one completes.

Before reserving or running a chunk, the worker checks current Postgres size, writable free disk, and any configured object-cache budget against the chunk's estimated write amplification. If capacity is below the configured minimum or the estimate would exceed the budget, the chunk pauses or fails with explicit capacity metadata. Capacity failure doesn't widen the job, drop retained replay facts, downgrade canonicality, or silently switch to retaining fewer facts.

Catch-up uses the same selected-target retention contract as other backfill: durable selected facts, lineage and header anchors, selected target logs, and replay-required enrichments are retained; empty historical blocks and unselected full payloads stay cache or absent.

### Storage helpers

| Helper | Behavior |
| --- | --- |
| `create_backfill_job` | inserts a new bounded job or returns the existing job for the same key + shape; never widens range or replaces child bounds |
| `reserve_backfill_range` | atomically claims pending or reclaimable work; expired leases reclaim without duplicating |
| `advance_backfill_range` | requires the current lease; moves checkpoint forward monotonically, never below the prior or beyond the declared end |
| `complete_backfill_range`, `complete_backfill_job` | no-op when complete; require all child checkpoints to reach declared ends |
| `fail_backfill_range`, `fail_backfill_job` | record bounded failure state and metadata without rewinding or mutating raw facts |

Range checkpoints belong to the backfill substrate. They record operational fetch/resume progress only, never reused as chain or projection checkpoints.

## Adapter intake specifics

### ENSv1 old-registry migration

`ENSRegistryOld` stays under `ens_v1_registry_l1` as an allow-listed migration-epoch input at `0x314159265dd8dbb310642f98f50c066173c1259b` with `start_block = 3327417`. The current registry `start_block: 9380380` is the current registry's pinned start, not original ENS history.[^subgraph-migration]

A current-registry `NewOwner` marks the affected subnode migrated. Later old-registry `NewOwner`, `Transfer`, `NewTTL`, and non-root `NewResolver` observations for that node are retained as facts but don't overwrite the current owner, resolver, TTL, child edge, resolver-discovery edge, or projection input. The root resolver is the single exception: old-registry `NewResolver(ROOT_NODE, resolver)` may still update the root resolver binding and feed `ens_v1_resolver_l1` discovery.[^v1-fallback]

### Resolver discovery

Resolver discovery feeds declared record indexing. Static manifest admission isn't enough.

- ENSv1 `NewResolver(node, resolver)` from admitted `ens_v1_registry_l1` emitters → resolver discovery observation for `ens_v1_resolver_l1`. Nonzero addresses create or refresh the node-to-resolver binding and the resolver contract instance; zero closes only the affected binding.[^v1-ensreg]
- Basenames `NewResolver(node, resolver)` from admitted `basenames_base_registry` emitters → resolver discovery observation for `basenames_base_resolver`. Same rules.[^bn-registry]

The observed resolver address alone isn't enough. Contract-instance admission, node-to-resolver binding state, generic event intake, and supported resolver-profile admission are separate gates.

For ENSv1, retained generic resolver-local record/version events (`AddrChanged`, `AddressChanged`, `TextChanged`, `VersionChanged`) feed observed selector/cache and version-boundary facts when the emitter and node match the selected resolver binding. Unobserved selectors stay explicit gaps or `resolver_family_pending` rather than silently going absent. Generic resolver-topic intake is topic-first: a raw log whose payload doesn't ABI-decode to the upstream resolver event shape is retained but doesn't emit observed selector/cache or version-boundary facts.

Resolver-profile admission gates complete record-family coverage, resolver-overview completeness, resolver-local authorization, latest-only behavior, and event-to-call parity. The first dynamic ENSv1 admission is limited to ENS Labs PublicResolver-generation profiles. The gate may use direct manifest admission, first-party known-resolver admission, code-hash observations, proxy/implementation edges, or another explicit non-schema rule. Registry `NewResolver` observation alone isn't enough.

### ENSv2 sepolia-dev intake

The ENSv2 `sepolia-dev` intake starts from four admitted source families: `ens_v2_root_l1`, `ens_v2_registry_l1`, `ens_v2_registrar_l1`, `ens_v2_resolver_l1`. Direct watched roots come from the pinned upstream `sepolia-dev` deployment metadata for `RootRegistry`, `ETHRegistry`, `ETHRegistrar`. `PermissionedResolverImpl` is implementation metadata; resolver instances enter the watch plan only through manifest admission or discovery edges.[^v2-deploy]

Adapters normalize log-derived facts after raw block admission:

| Upstream | Normalized | Notes |
| --- | --- | --- |
| `TokenResource(tokenId, resource)` | `TokenResourceLinked` | Only adapter event linking current token id to upstream EAC resource. |
| `TokenRegenerated(oldTokenId, newTokenId)` | `TokenRegenerated` | Preserves `resource_id` and `token_lineage_id`. |
| `SubregistryUpdated`, `ResolverUpdated`, `ParentUpdated` | graph and topology events | After endpoint addresses resolve to current `contract_instance_id`. |
| `AliasChanged` | `AliasChanged` | On admitted resolver instances. |
| `EACRolesChanged` | resource-, root-, or resolver-scoped permissions | After resolving the upstream EAC resource to bigname identity. |

Any ENSv2 enrichment call used to repair or disambiguate a log-derived fact (`getResource(anyId)`, `getTokenId(anyId)`, `getState(anyId)`, `getAlias(fromName)`, EAC role reads) anchors to the same block identity as the raw log. Log-derived state is never rewritten through ambiguous number-only calls.

## Batching and retries

Batching applies only to independent work: many block fetches for historical backfill, exact block-scoped log fetches, receipt lookups inside a bounded fallback, code-hash or ABI lookups.

- Later pipeline stages don't assume earlier batched results are canonical until reconciliation finishes.
- Every batch item is retryable independently.
- Partial batch failure doesn't corrupt intake ordering.
- Batch size stays bounded and measurable.

## State enrichment

When intake or execution enriches facts with state reads (calls, storage, balances):

- Anchor the read to the exact block hash whenever the RPC surface supports it.
- Otherwise treat the result as provisional until the source block is at least `safe`.
- Never attach number-based enrichment to a block-scoped fact as if it were reorg-proof.

Historical state-heavy enrichment is an archive requirement, not a best-effort full-node feature.

## Atomicity boundary

The raw admission transaction boundary is one block. That transaction writes:

- one `chain_lineage` header-anchor row
- optional `chain_header_audit` fields when auditable retention is enabled
- hot raw tx, receipt, and log facts needed for selected replay contracts
- optional cache metadata or digests for non-critical full block-scoped payloads
- any block-scoped call snapshots captured through the intake-owned raw-fact handoff
- normalized events emitted from those facts
- invalidation signals for downstream workers

The canonical head pointer writes last inside that admission unit. Projection workers stay downstream and asynchronous, but they consume deterministic block-scoped invalidation and replay inputs.

## Replay (re-running adapters)

Replay is bounded operational tooling over already persisted canonical raw facts. A replay request selects a finite profile, chain, and block range or explicit block-hash set. Canonical raw facts are rows whose block identity is `canonical`, `safe`, or `finalized`; `observed` and `orphaned` are excluded unless an audit-only contract admits them.

The runner performs an upsert-only adapter resync by invoking the same `normalized_events` boundary used after live or backfill raw admission. It reads persisted raw facts, lineage state, optional header-audit state, and the persisted manifest/source identity needed to route those facts. It advances its own indexer-owned `normalized_replay_*` cursor.

Automatic catch-up uses one all-source chain cursor over canonical raw facts. Cross-family adapters need registry, registrar, wrapper, resolver, and reverse-claim facts in the same chronological stream; per-family cursors would tear identity histories. Source-scoped replay is an explicit repair selector, not the automatic catch-up default.

Replay does not delete stale `normalized_events`, purge derived rows, or replace existing payloads for an already persisted normalized-event identity. Conflicting payloads stay a hard storage mismatch. Raw facts and lineage stay immutable; projection rebuild is downstream.

## Observability

Minimum metrics: lag to canonical/safe/finalized; reorg depth histogram; orphaned block rate; RPC latency and error rate by method; partial batch failure rate; recent-window cache hit/miss; backlog depth; replay duration; backfill chunk capacity-pause rate.

Required failure drills:

- dropped subscription connection during a reorg
- duplicate headers at the same height
- missing parent gap requiring parent backfill
- partial batch failures
- crash and resume from a persisted checkpoint
- crash and resume from a backfill range checkpoint
- raw-fact normalized-event replay restart over the same bounded canonical selection
- safe / finalized promotion lagging canonical intake

## Acceptance

The intake contract is acceptable when:

- Live notifications can be lost without losing correctness.
- Short forks reconcile by hash and parent hash alone.
- Block-scoped data ingestion never depends on ambiguous number-only reads when a block-hash-scoped primitive exists.
- Raw facts are sufficient to rebuild canonical declared state after a reorg or decoder rewrite.
- Backfill reuses the same downstream semantics as live ingestion.
- Replay upserts normalized events only from persisted canonical selected replay facts.
- Any cache refill uses provider re-fetch only as a block-hash-scoped, retained-digest-checked, fail-closed cache-fill path.

---

[^subgraph-migration]: (upstream: .refs/ens_subgraph/subgraph.yaml:L39 @ ens_subgraph@723f1b6)
[^v1-fallback]: (upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L40 @ ens_v1@91c966f)
[^v1-ensreg]: (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L89 @ ens_v1@91c966f)
[^bn-registry]: (upstream: .refs/basenames/src/L2/Registry.sol:L132 @ basenames@1809bbc)
[^v2-deploy]: (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/RootRegistry.json:L2 @ ens_v2@554c309)
