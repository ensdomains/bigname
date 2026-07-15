# Data Completeness Runbook

`bigname-worker inspect data-completeness` answers one question: **is this database
complete enough to serve, or to cut traffic over to?**

It is read-only. It opens the inspect connection with
`default_transaction_read_only = on`, reads raw facts, cursors, projections, and the
runtime watch plan, and writes nothing.

## Command

```sh
bigname-worker inspect data-completeness --json
```

Gate a promotion or cutover on it:

```sh
bigname-worker inspect data-completeness \
  --manifests-root manifests/mainnet \
  --retention-mode minimal \
  --json --fail-on-incomplete
```

Exit `0` means every check passed and `data_complete` is `true`. With
`--fail-on-incomplete`, exit `1` means at least one check failed. Without the flag the
command always exits `0` and is a report.

Point it at any database with `--database-url`, or `BIGNAME_DATABASE_URL` /
`DATABASE_URL`. Comparing a warmed candidate database against the serving one is the
intended use.

`--manifests-root` is optional so the command remains usable against a database without a
local checkout, but promotion automation should always supply the same profile root as the
indexer. With a root, the on-disk active manifests are an external expectation and must match
the database bidirectionally, including version and complete serialized payload. Without one,
the report keeps database-derived behavior and sets `advisories.manifest_corpus_unverified` to
`true`.

`--retention-mode` selects the storage contract to verify. It defaults to `minimal`, where
compacted raw-log staging is valid. `log-audit` additionally requires the exact retained
serving-canonical raw log and lineage anchor for every active normalized event whose
`raw_fact_ref.kind` is `raw_log`.

## Why `/v1/status` cannot do this

`/v1/status` reports `projection_lag_blocks` from the projection queue. When the queue is
empty it reports `latest_projected_block` as the stored canonical checkpoint, so an
*empty work queue* reads as *caught up to head*. Each cursor in the pipeline measures
itself against the previous stage's frontier, so if an upstream stage stalls, or the watch
set silently excludes targets, every cursor still reports "done".

An empty database and a complete database are indistinguishable to that endpoint. Do not
gate a cutover on it.

## Checks

Order is dependency order. Later checks are meaningless if earlier ones fail.

The active chain set is the authority for every per-chain check: active manifest-payload roots,
contracts, and proxy implementations; the materialized watch view (manifest-declared contracts
unioned with active discovery edges); and every chain an active `manifest_versions` row declares
directly. A partial restore that lost `manifest_contract_instances` or
`contract_instance_addresses` rows therefore cannot delete a chain or directly declared target
from its own expectations. A chain present in storage but not in that active set is a foreign or
retired chain and is reported as an advisory, not gated for the per-chain checks.

| Check | Passes when |
| --- | --- |
| `manifest_corpus_matches_repository` | when `--manifests-root` is supplied, every active on-disk manifest has an active database row with the same identity, version, and payload, and every active database row has an on-disk counterpart; without the argument the check is non-gating and explicitly unverified |
| `reconciliation_frontier_at_head` | every active chain has a stored frontier, the checkpoint's exact hash and number resolve to a canonical/safe/finalized lineage row, checkpoint-ahead lag is at most `--max-head-lag-blocks` (default 8), and lineage-ahead lag is at most the live reconciler's shared contiguous gap-fill limit (currently 1,024 blocks) |
| `reconciliation_lineage_contiguous` | distinct canonical/safe/finalized block numbers equal `head - floor + 1`, no retained canonical height has an additional non-orphaned hash, and every row above the retained floor points by `parent_hash` to the canonical/safe/finalized row at the preceding height |
| `reconciliation_history_from_declared_start` | for every active watched chain, the retained lineage floor is at or below the earliest finite start block its active targets declare; a chain whose targets are all open-ended fails, since no floor can be established |
| `stored_lineage_backfill_coverage` | every active log-producing watched tuple intersecting a persisted bounded-backfill range inside retained lineage is contained in a durable address- or family-scoped `backfill_coverage_facts` row over each shared promotion-verification chunk, and each topic-filtered completed job in that range recorded the exact topic set the active manifest ABI still declares |
| `watch_set_code_observation_coverage` | there is at least one active target, and every target whose inclusive start is at or below its chain's stored canonical head — direct active-manifest declarations unioned with the materialized watch view and active discovery edges — has a non-orphaned `raw_code_hashes` observation with an exact retained non-orphaned lineage anchor at or after that start; future-start targets are reported as `pending_activation` |
| `manifest_declared_targets_present` | every active manifest-payload root, contract, and proxy implementation has its matching materialized instance and open address whose start does not narrow the payload start; every proxy implementation also has the exact open managed `proxy_implementation` edge, with a range-preserving start, consumed by the watch view |
| `discovery_targets_present` | every target endpoint of an open non-migration discovery edge has an open `contract_instance_addresses` row on the edge's chain whose start does not narrow the edge's admitted start, and every open resolver edge from an active registry source retains its matching active resolver target manifest; bounded edges are historical and impose no current target requirement |
| `active_event_lineage_retained` | every matching canonical/safe/finalized normalized event for an active event-producing source still resolves by exact chain, block hash, and block number to a canonical/safe/finalized `chain_lineage` anchor; event kinds are the union of manifest ABI and adapter-owned declarations |
| `active_raw_logs_retained` | in `minimal` mode, always; in `log-audit` mode, every active serving-canonical normalized event sourced from a raw log retains the exact canonical/safe/finalized `raw_logs` row and exact serving-canonical lineage anchor |
| `normalization_no_failure` | no replay cursor for an active chain under the deployment profile inferred from the active manifest corpus carries a `last_failure_reason` |
| `normalization_caught_up_to_raw_head` | the active manifest corpus resolves to one supported deployment profile; every active chain with retained canonical raw logs has that profile's `raw_fact_normalized_events` cursor, each applicable cursor has reached its target, and canonical tail logs above a closure-replay latch require a complete post-replay backlog that starts no later than the raw-fact target plus one (see below) |
| `projection_apply_drained` | the `normalized_events_to_projection_invalidations` cursor, when present, exactly equals the retained change-log high-water mark (`max(change_id)`, or zero for an empty log); a non-empty log also requires the cursor, and unrelated cursor rows do not count |
| `projection_invalidations_drained` | `projection_invalidations` is empty — every enqueued invalidation has been applied and deleted |
| `projection_no_dead_letters` | `projection_invalidation_dead_letters` is empty — no invalidation exhausted its retries |
| `projection_replay_complete` | every current projection has a `current_projection_replay_status` marker at this worker's `CURRENT_PROJECTION_REPLAY_VERSION`; until a non-empty retained projection change log exactly corroborates the durable apply cursor, each marker must also cover the target bootstrap would request now |
| `current_projection_content_present` | all seven projections in the worker's `ALL_CURRENT_PROJECTION_ORDER` were counted through their serving validity rules, and every namespace or chain whose active manifest-plus-adapter event declarations can feed that projection has at least one servable row; raw counts remain diagnostic |
| `active_dataset_non_empty` | every active manifest source with non-empty manifest- or adapter-declared normalized output has a canonical/safe/finalized matching event under that exact `source_manifest_id`, manifest version, chain, namespace, source family, and declared event kind; every such namespace also has `name_current` rows |
| `normalized_events_chain_id_present` | no non-orphaned `normalized_events` row has a NULL `chain_id` |
| `deferred_projection_indexes_present` | the eight deferred `normalized_events` projection indexes all exist on `public.normalized_events` and have `pg_index.indisvalid = true` and `indisready = true` (a fresh replay drops them and rebuilds them after catch-up) |

`watch_set_code_observation_coverage` is the check with teeth, and the reason the command
exists. Most others are *relative* invariants: each compares a stage to the stage before
it, so they stay green while the pipeline faithfully processes an incomplete input.
`reconciliation_history_from_declared_start`, `stored_lineage_backfill_coverage`,
`manifest_corpus_matches_repository`, `active_event_lineage_retained`,
`active_dataset_non_empty`, `current_projection_content_present`, and
`projection_replay_complete` also compare against external or retained truth, the declared
world, or the pipeline's own handoff authority rather than the previous stage.

`projection_apply_drained` only proves the derive scan finished — that the apply cursor
exactly matches the change-log frontier. A cursor behind the frontier has not derived all
changes; a cursor ahead of it proves retained change-log history was lost, because derive only
scans IDs greater than the cursor. It does not prove the resulting invalidations were
applied, because those move through a separate claim/apply queue. `projection_invalidations_drained`
and `projection_no_dead_letters` close that gap: the queue must be empty (a successful
apply deletes the row) and nothing may have dead-lettered. All three must pass for
projections to be fully applied.

### Replay targets: raw-log head vs latched target

`normalization_caught_up_to_raw_head` compares each chain's `raw_fact_normalized_events`
cursor against the **canonical** raw-log head — the newest raw log whose lineage block is
canonical, safe, or finalized, mirroring the bounds replay actually consumes. It does not
use the non-orphaned head, which includes `observed` logs replay cannot yet touch; that
head is reported only, so trailing unpromoted logs do not read as permanent lag.

Cursor selection is keyed by both chain and the deployment profile inferred from the active
manifest corpus, using the same `mainnet` / `sepolia` classification as replay admission.
A cursor left by another profile cannot satisfy an active chain, and failures or lag on an
inactive chain or a non-active profile are reported in `advisories.ignored_replay_cursors`
instead of gating the serving corpus.

A chain with an active closure- or dependency-replay source family is an exception. The writer
uses the shared source-family policy to preserve that chain's first raw-fact cursor target below
the live head; newer logs are carried by a one-shot `post_replay_live_adapter_backlog` sweep and
then live adapter sync. The gate uses the same policy. A quiet chain whose canonical raw-log head
is at or below the latched target may complete without a backlog row, matching a sweep that found
no tail logs and therefore wrote no cursor. If the canonical raw-log head is above the latch, a
backlog cursor must exist, both cursors must reach their own targets, and the backlog's inclusive
range start must be no later than the raw-fact target plus one. An earlier start is a safe overlap;
a later one leaves an unnormalized gap that completed cursors cannot reveal. The backlog row is
therefore optional only when retained canonical logs independently corroborate the quiet latch.
**Limitation:** the live tail beyond the backlog target has no cursor, so the gate cannot verify
it. This is one reason a passing gate is necessary but not sufficient (see Scope).

The catch-up writer clears `last_failure_reason` and `last_failure_at` after either a successful
advance or a successful idle iteration. A transient failure therefore remains gating until the
same chain completes a healthy poll, but it cannot remain stuck forever on an already-complete
latched cursor.

Completion is read from the cursor's `next_block_number > target_block_number` pair, the
same authority the catch-up loop uses, not from `last_completed_block_number`. A reorg
rewind lowers `next_block_number` back below the target while leaving
`last_completed_block_number` at its high-water mark, so a gate that trusted
`last_completed_block_number` would read a rewound cursor as still caught up.
`last_completed_block_number` is reported only.

The check also requires the cursor to exist: a chain with retained canonical raw logs but
no `raw_fact_normalized_events` cursor row — a truncated restore, or a chain absent from
catch-up configuration — fails with a `chains_missing_raw_fact_cursor` entry rather than
passing because there was no cursor to measure.

It works because code observations are keyed on the watch set rather than on activity: a
watched target acquires a code observation from the live tailer's baseline pass even if it
never emits a log. Coverage preserves the latest non-orphaned observation block with a matching
non-orphaned `chain_lineage` row per `(chain, address)` and compares it with the target's
inclusive active start; an unanchored observation or one retained from a pre-admission range
cannot satisfy a newly active target. Direct active
manifest payload targets, including proxy implementation addresses, are read independently of
`manifest_contract_instances` and `contract_instance_addresses`, so losing a materialized
declaration or address row makes the target unobserved instead of removing it from the expected
set. When several active entries share an address, coverage uses the latest finite start, the
strictest lower bound across those entries.

A finite start above the chain checkpoint's canonical block is a pre-activation declaration.
It remains part of the active target and history authority, but coverage cannot require an
observation from a block the chain has not reached. The entry is excluded from only the code
observation comparison and appears in `advisories.pending_activation` with its declared start
and current canonical head. Once the head reaches the start, the same target becomes gating.
If the chain has no stored canonical checkpoint, the gate cannot prove pre-activation and does
not skip the target.

`manifest_declared_targets_present` diagnoses the materialization edge separately. Every payload
root or contract must have the matching `manifest_contract_instances` declaration; a proxy
implementation must also have the matching implementation instance. An open address row only
satisfies a target when its `chain_id` and lowercased address match the payload; a row for the
same instance on another chain or at another address does not count. An address with a finite
`active_to_block_number` is closed even when `deactivated_at` is null. Proxy implementations
must additionally retain the exact open managed discovery edge that the source-graph writer
creates; the direct payload fallback cannot substitute for the runtime watch-plan edge. Both the
address row and managed edge must start no later than the payload target (or remain open-ended
when the payload start is unknown). Otherwise the runtime watch view's `GREATEST` start silently
narrows the manifest-declared interval.

`discovery_targets_present` closes the corresponding gap for dynamic discovery. The active edge
and its target `contract_instance_id` remain authority if a restore loses the target's open
address row or the active resolver manifest that assigns the target's source family, even though
`load_watched_contracts` can no longer render that target. Resolver target manifests are matched
to the active registry source by namespace, chain, deployment epoch, and the registry-to-resolver
family mapping. Only open
edges impose this current-authority requirement: a finite `active_to_block_number` is legitimate
retained discovery history. The named check fails on a missing or closed current materialization
rather than allowing coverage and history to shrink. When the edge has a finite admitted start,
the open address row must have no start or an equal/earlier start. When the edge start is unknown,
the address start must also remain unknown; inventing a finite materialization start would silently
discard an unbounded earlier interval. A later address start would otherwise make the runtime watch
view take `GREATEST(edge_start, address_start)` and discard the edge's earlier required interval, so
it is reported as a materialization gap.
See [`chain-intake.md`](../chain-intake.md).

### Frontier, history, and content against the declared world

Three checks measure the chain and its content against what the watch set declares, not
only against the previous pipeline stage:

- **Frontier.** `reconciliation_frontier_at_head` applies asymmetric writer bounds. A positive
  lag, where the checkpoint is ahead of retained lineage, may not exceed
  `--max-head-lag-blocks` (default 8). Reconcile commits an entire canonical lineage path before
  advancing the checkpoint, so a negative lag may reach the live reconciler's shared contiguous
  gap-fill limit (currently 1,024 blocks). Beyond that limit the writer itself refuses live gap
  fill and requires bounded catch-up or hash-pinned backfill. A zero numeric lag still
  fails when the checkpoint's `canonical_block_hash` does not join the canonical/safe/finalized
  lineage row at that exact height; later reconciliation uses that hash as its branch anchor. The
  check also
  synthesizes a failing frontier row for any active chain that has no checkpoint or lineage
  row at all, so a declared chain missing from storage (reported with
  `missing_from_storage: true`) cannot pass by absence.
- **History.** `reconciliation_history_from_declared_start` compares each active watched
  chain's retained lineage floor against the earliest finite start block its active targets
  declare. A floor above that start means history was truncated — the shape of a
  live-tail-only restore, where the truncated span is itself contiguous, cursors are caught
  up over the short raw set, and projections are non-empty, so every other check passes. A
  target whose start block is open-ended (`active_from_block_number` is null) imposes no
  floor and is skipped, matching bootstrap's own authority; but a chain whose targets are
  *all* open-ended has no floor to establish and fails closed. Direct manifest-payload starts
  remain part of this union even when a materialized address row narrows its runtime
  `active_from_block_number`; the address row cannot raise the manifest's historical floor.
- **Stored-path fetch coverage.** `stored_lineage_backfill_coverage` runs the same indexed
  watched-tuple anti-join as stored-lineage checkpoint promotion over merged persisted backfill-job
  ranges intersected with retained lineage. The job is the durable signal that a span came from
  bounded backfill rather than ordinary provider-fetched live reconciliation. Incomplete and failed
  jobs remain in scope because retained lineage can be crash residue that the writer refuses;
  durable facts from a later successful retry can satisfy the interval. Evidence remains required
  after the checkpoint consumes the span: a checkpoint regression or database restore can make it
  promotion input again, and deleting facts must not preserve a completeness pass. The check uses
  the writer's shared `131072`-block verification chunks and active
  log-producing source-family authority. Each required tuple interval within a chunk must fit
  inside one durable address-scoped fact for that exact family/address or one family-scoped fact.
  Before those facts are trusted, the inspector invokes the same topic-set drift guard as
  stored-lineage promotion. For every intersecting completed topic-filtered job, its persisted
  per-family topic set must exactly equal the active manifest ABI's current set; a missing
  persisted set also fails closed. Address-enumerated, topic-unfiltered jobs are unaffected.
  Facts from a later successful retry can satisfy the interval, so failed job/range lifecycle rows
  remain advisory. A chain with no bounded-backfill range inside retained lineage has no bounded
  fetch span requiring facts and passes this check vacuously.
- **Content.** `active_dataset_non_empty` derives expected event-producing sources from the union
  of active manifest ABI `normalized_events` entries and adapter-owned emitted-kind declarations
  for those admitted source families. The adapter declaration covers normalized output synthesized
  outside a one-to-one manifest ABI event, including reverse-claim and block-derived events; it
  does not admit a source or widen its watch scope. Execution- or transport-only manifests with no
  declaration from either authority do not create event-content expectations. Each expected source
  must have a canonical, safe, or finalized event matching its exact
  `source_manifest_id`, manifest version, chain, namespace, source family, and one of its
  declared event kinds. Rows from a deprecated manifest version therefore cannot make a newly
  active source pass. Each expected namespace must also have `name_current` rows.
  `name_current` carries no chain column, so names are scoped to the namespace — the finest
  dimension a name projection has; a name in a namespace shared across chains is not
  attributed to a specific one. Failed sources are reported in
  `manifest_sources_without_events` with their manifest IDs and source families. This exact
  identity rule intentionally makes a manifest-version rollout fail until normalized events
  have been re-derived under the newly active `source_manifest_id`; residual rows from the
  previous version are not proof that the new declaration was indexed.
- **Retained event lineage.** `active_event_lineage_retained` joins every matching active
  canonical/safe/finalized normalized event, including synthetic block-boundary events, back to
  `chain_lineage` by exact chain, block hash, and block number and requires that anchor to remain
  canonical, safe, or finalized. An `observed` event does not count as active serving content,
  because projection readers consume only serving-canonical states. This is the durable
  reorg-repair authority. In the documented minimal retention
  mode, `raw_logs` are replay staging and may be compacted after normalized replay and downstream
  durability; that valid compaction does not fail this check. With `--retention-mode log-audit`,
  `active_raw_logs_retained` additionally joins every active normalized raw-log event to its
  exact `(chain, block hash, block number, transaction hash, log index)` raw-log row and requires
  both that row and its exact lineage anchor to be canonical, safe, or finalized.

### Projections rebuilt, and structural integrity

`current_projection_content_present` enumerates the same seven-family
`ALL_CURRENT_PROJECTION_ORDER` that replay publishes. It reports raw and servable total/scoped
row counts for every family and evaluates missing scopes only from servable counts. Expectations
come from the union of active manifest and adapter-owned normalized event-kind declarations, never
from the projection rows being checked:

| Projection | Required scope | Serving validity mirrored by the count | A scope is expected when an active source declares |
| --- | --- | --- | --- |
| `name_current` | namespace | canonical/safe/finalized surface; when bound, canonical/safe/finalized resource and binding, open binding, and canonical/safe/finalized token lineage when present (`DEFAULT_NAME_CURRENT_READ_FILTER`) | any normalized event output |
| `children_current` | namespace | declared rows with a canonical/safe/finalized parent and a missing, label-preimage-backed, or canonical/safe/finalized child surface (`DEFAULT_CHILDREN_CURRENT_READ_FILTER`) | `SubregistryChanged`, `ParentChanged`, `RegistrationGranted`, `RegistrationRenewed`, or `RegistrationReleased` |
| `permissions_current` | resource chain | joined resource is canonical/safe/finalized (`DEFAULT_PERMISSIONS_CURRENT_READ_FILTER`) | `PermissionChanged`, `RootPermissionChanged`, or `PermissionScopeChanged` |
| `record_inventory_current` | resource chain | joined resource is canonical/safe/finalized (`DEFAULT_RECORD_INVENTORY_CURRENT_READ_FILTER`) | `RecordChanged`, `RecordVersionChanged`, or `ResolverChanged` |
| `resolver_current` | chain | no additional consumer-side identity/canonicality filter | `ResolverChanged`, `AliasChanged`, `PermissionChanged`, or `PermissionScopeChanged` (resolver-scoped permission rows are resolver rebuild targets) |
| `address_names_current` | namespace | canonical/safe/finalized surface, resource, and binding; open binding; canonical/safe/finalized token lineage when present (`DEFAULT_ADDRESS_NAMES_CURRENT_READ_FILTER`) | `RegistrationGranted`, `TokenControlTransferred`, `AuthorityTransferred`, `AuthorityEpochChanged`, `PermissionChanged`, `PermissionScopeChanged`, or `TokenRegenerated` |
| `primary_names_current` | namespace | no additional consumer-side identity/canonicality filter | `ReverseChanged` |

An unlisted input does not create an expectation for that projection. This lets a small healthy
corpus with no declared permission events keep an empty `permissions_current`, while truncating
`resolver_current` for a chain whose active sources declare `ResolverChanged` fails by chain.
Permissions and record inventory are resource-keyed, so residue attached only to a foreign chain's
resources cannot satisfy an active chain. Likewise, a raw projection row whose supporting identity
row is orphaned or whose binding is closed remains visible in diagnostics but cannot satisfy a
servable scope.

`projection_replay_complete` requires every current projection to have a marker in
`current_projection_replay_status` at the inspecting worker's
`CURRENT_PROJECTION_REPLAY_VERSION`. The newest stored version is still reported as
`replay_version` for diagnosis, while `required_replay_version` reports the version the gate
requires. Each current-version marker's `completed_normalized_target_block` must also reach the
target automatic bootstrap would use now —
`max(global raw_fact_normalized_events target, furthest persisted chain checkpoint)` — until a
durable `normalized_events_to_projection_invalidations` apply cursor is exactly corroborated by a
non-empty retained `projection_normalized_event_changes` high-water mark. Bootstrap
publishes projections in order and `name_current` is first, so a candidate mid-bootstrap has
`name_current` but not the rest. After handoff, the worker deliberately stops advancing replay
markers: the apply cursor/change log and invalidation queue own later normalized blocks. At that
point current-version marker presence plus the separately drained apply/change/invalidation
checks is the writer's authority; comparing the old bootstrap marker target with a newer chain
frontier would false-fail a healthy live database. Complete markers from an older worker image
are never accepted. A cursor over an empty change log is treated as restore residue and does not
waive marker target coverage, even when its stored position is zero.

When target coverage is required before apply handoff, the replay target is intentionally global
even though foreign or retired chains are advisory for the per-chain frontier, history,
normalization, and coverage checks. Current projections and their replay marker are global, and
the automatic replay writer computes the same unscoped maximum across persisted raw-fact cursors
and chain checkpoints. Scoping only the inspector to active chains could approve a marker below
the target that the writer would request.

`deferred_projection_indexes_present` checks that the eight deferred `normalized_events`
projection indexes exist on the expected table and are valid and ready in `pg_index`. A failed
`CREATE INDEX CONCURRENTLY` can leave a named but invalid catalog entry that PostgreSQL cannot
use; that entry fails the check just like an absent index. A fresh replay drops the indexes for
speed and a later pass rebuilds them after catch-up, so a missing or invalid index marks a
candidate whose replay has not finished rebuilding them — complete data, but not yet ready to
serve efficiently.

`normalized_events_chain_id_present` fails on any non-orphaned `normalized_events` row with a
NULL `chain_id`. Those rows are excluded from the per-chain content counts (and would
otherwise abort the read), so they are surfaced here as a data-integrity fault.

## Interpreting failures

- **`manifest_corpus_matches_repository` fails.** The supplied profile root is missing or
  invalid, an active on-disk manifest is absent or payload-mismatched in the database, or the
  database retains an active manifest the root does not contain. Run manifest sync from the
  intended root and investigate unexpected active rows; do not accept a smaller surviving
  database corpus as its own expectation.
- **`watch_set_code_observation_coverage` fails, everything else passes.** The runtime
  watch scope is narrower than the manifests and discovery edges declare. The reported
  `unobserved_targets` were never watched, so their logs were never indexed and their
  derived state is missing, silently. Check the indexer's
  `BIGNAME_INDEXER_HASH_PINNED_BACKFILL_ADAPTER_SYNC` and the resulting
  `RuntimeWatchScope`; `manifest_declared_only` excludes discovery edges. Verify with
  `inspect watch-plan --json`.
- **`manifest_declared_targets_present` fails.** An active payload target has no matching
  materialized declaration/implementation instance or no open address row matching both its
  declared chain and address, or a materialized proxy/implementation pair lacks its open managed
  edge. A missing declaration, missing proxy implementation, missing or bounded proxy edge,
  a later-than-declared address or edge start, deactivated, bounded, wrong-chain, or wrong-address
  row breaks the watch-view authority even if
  retained code observations let the separate coverage check pass. Reseed manifests or repair
  the materialized graph before promoting.
- **`discovery_targets_present` fails.** An open discovery edge points to a contract instance
  with no open address on that chain, or its address start is later than the edge's admitted
  start, or an open resolver edge from an active registry has lost its matching active resolver
  target manifest. Restore or rederive the range-preserving target address or manifest before promoting; closing or
  deleting a current edge merely to silence the check changes admission authority.
- **`stored_lineage_backfill_coverage` fails.** A bounded-backfill range inside retained lineage
  contains an active log-producing watched tuple whose required interval is not contained in one
  durable coverage fact, or a completed topic-filtered job's persisted topic set differs from the
  current manifest ABI (or was not persisted). Run hash-pinned or Coinbase SQL backfill for the reported tuple/range on the current manifest, or
  derive facts for a compatible completed legacy job, before retrying promotion.
- **`active_event_lineage_retained` fails.** Active normalized content has lost one or more exact
  canonical lineage anchors. Restore `chain_lineage` and run reorg/projection repair as needed;
  raw-log compaction by itself is not this failure.
- **`active_raw_logs_retained` fails.** The operator selected `log-audit`, but an active
  normalized raw-log event has lost its exact serving-canonical raw-log row or exact canonical
  lineage anchor. Restore or refetch and verify the retained audit fact. Select `minimal` only
  when compaction is the deployment's actual retention policy, not merely to silence the check.
- **Coverage fails on a database warmed by backfill alone.** Backfill only observes a
  block's selected log emitters. A watched target that never emits acquires its single
  baseline observation from the live tailer, on canonical head reconciliation. Let the
  tailer reconcile a head before treating coverage as authoritative on a freshly
  backfilled database.
- **`normalization_no_failure` fails.** The adapter pass is crash-looping; the cursor
  records why. Normalization has stopped at the first affected block, so every downstream
  count is stale even though the cursors look drained.
- **`normalization_caught_up_to_raw_head` reports no active deployment profile.** The active
  manifest corpus is empty or mixes chains/epochs that do not classify as the writer's
  `mainnet` or `sepolia` profile. Correct manifest rollout state before interpreting cursor
  progress.
- **`reconciliation_frontier_at_head` fails.** Head reconciliation has stalled, or the
  checkpoint and lineage exceed the applicable asymmetric bound. A positive
  `head_lag_blocks` beyond `--max-head-lag-blocks` means lineage trails the checkpoint. A
  negative value is allowed through the reconciler's live gap-fill limit because lineage is
  committed before checkpoint advancement; beyond that limit the checkpoint writer is stale
  or the restore is mixed. A `missing_from_storage: true` chain is declared by the watch set but has no
  frontier row. `checkpoint_canonical_lineage_match: false` with zero numeric lag means the
  checkpoint hash is missing, orphaned, or points at a different branch at that height.
- **`reconciliation_lineage_contiguous` fails with a non-zero `duplicate_canonical_height_count`.**
  A retained canonical/safe/finalized height also has another non-orphaned lineage hash. That is
  a canonicality violation — every competing hash at a retained canonical height must be
  orphaned — not a gap, and it points at a
  reorg-repair or canonicality-assignment bug rather than missing intake.
- **`reconciliation_lineage_contiguous` fails with a non-zero
  `disconnected_canonical_parent_count`.** The retained heights are present, but one or more
  child rows do not point to the canonical/safe/finalized hash at the preceding height. Treat
  this as a broken restored or repaired branch, even if `missing_block_count` is zero.
- **`reconciliation_history_from_declared_start` fails.** The chain's lineage floor is above
  the earliest block its watched targets declare, so history is truncated. Common on a
  restore that kept only a recent window; the reported `declared_start_block` and
  `lineage_floor_block` bound the missing span.
- **`active_dataset_non_empty` fails with an entry in
  `manifest_sources_without_events`.** The exact active event-producing manifest source has no
  matching canonical/safe/finalized normalized event. Observed rows, rows from another chain or
  source family, and deprecated manifest identities do not satisfy it, even if the global event
  table is non-empty.
- **`projection_replay_complete` fails with entries in `missing_projections`.** The candidate
  is mid projection-bootstrap, its marker belongs to a version other than
  `required_replay_version`, or (before apply handoff) it completed below
  `required_target_block`. `target_coverage_required` states whether the target comparison is
  active. `name_current` is published first, so a candidate with only it is early in the rebuild;
  old-version markers always require replay, while stale-target markers require replay only
  before the durable apply cursor takes over.
- **`current_projection_content_present` fails.** At least one expected namespace, chain, or
  global current-projection table is empty even though active manifest event declarations can
  feed it. Use the reported table and `missing_scopes`; replay markers and drained cursors are
  not substitutes for the missing serving rows. Rebuild the named projection from canonical
  inputs before promotion.
- **`deferred_projection_indexes_present` fails.** A fresh replay dropped the listed indexes,
  the rebuild pass has not run, or a concurrent build left a named but invalid catalog entry.
  Data may be complete, but the database is not serve-ready; rebuild valid indexes before
  promoting.
- **`normalized_events_chain_id_present` fails.** Non-orphaned normalized events have a NULL
  `chain_id` — a decode or write fault. Those rows are not attributable to a chain.
- **`projection_invalidations_drained` or `projection_no_dead_letters` fails while
  `projection_apply_drained` passes.** The derive scan finished but the resulting projection
  writes have not landed: invalidations are still queued, or some exhausted their retries and
  dead-lettered. Projections are stale or partial even though the apply cursor looks caught
  up.
- **`projection_apply_drained` fails because the apply cursor is ahead.** Retained rows were
  removed from `projection_normalized_event_changes` without rewinding or rebuilding the cursor.
  Restore the missing change log or rebuild projections and reseed the handoff; do not advance or
  delete rows merely to make the cursor and log agree.
- **`normalization_caught_up_to_raw_head` fails on a `:range_start` cursor label.** The
  post-replay backlog starts after `raw_fact_normalized_events.target_block_number + 1`, or the
  raw-fact target needed to establish continuity is absent. Recreate the backlog from the replay
  target so the normalized ranges overlap or meet before promotion.
- **`watch_set_code_observation_coverage` fails with zero active watched targets.** The
  database has no manifests loaded — a restore that dropped or never applied them. The check
  fails rather than passing vacuously, since an ENS deployment always watches at least the
  registry.
- **`active_dataset_non_empty` fails while every cursor is drained.** The pipeline is healthy
  and there is nothing in the active dataset. This is the empty-database case, and it is the
  shape prod presented on 2026-07-06.

## Advisories

The `advisories` block reports state that is worth an operator's attention but does not gate:

- **`manifest_corpus_unverified`** — no `--manifests-root` was supplied, so all manifest-derived
  expectations still come from surviving database rows. This is acceptable for diagnosis, not
  for a promotion gate that must detect a partially restored manifest corpus.
- **`pending_activation`** — active targets whose finite start is above their chain's stored
  canonical head. They do not yet require code-observation coverage; they become gating when
  the checkpoint reaches the declared start.
- **`foreign_chains`** — chains with residual checkpoint or lineage rows that the active watch
  set does not cover (a retired chain, or one whose manifest was removed while its chain state
  remained). Their per-chain checks are not gated; the listing is for cleanup.
- **`ignored_replay_cursors`** — cursor labels outside the inferred active deployment profile or
  active chain set. Their failures and lag do not gate serving, but the rows remain visible for
  cleanup and profile-rollout diagnosis.
- **`backfill_lifecycle`** — per-profile counts of `failed`, incomplete, and expired-lease
  backfill jobs and ranges. Backfill lifecycle failures are *not* gated because a later successful
  retry may supply the durable facts for the same required tuples; the separate
  `stored_lineage_backfill_coverage` check gates the resulting promotion evidence. Treat a
  non-zero `failed_range_count` or
  `expired_lease_range_count` as a prompt to investigate before promoting.

## Caveats

- **Coinbase SQL sample-mode candidates.** In `sample` validation mode a Coinbase SQL backfill
  persists lineage only for the blocks whose logs it returned, while completing the whole
  range. The reconciled span is then sparse, so `reconciliation_lineage_contiguous` fails on a
  candidate that is legitimately complete for its sampled coverage. A sample-backed candidate
  is not gate-promotable without a full-mode pass over the same range; coverage facts cannot
  substitute for the missing lineage rows.

## Scope

This gate is database-level. It does not exercise HTTP routes, compare name counts across
two databases, or spot-check GraphQL and REST answers. It also cannot verify the live tail
beyond a latched chain's backlog target, which has no cursor, and it does not reconcile
Coinbase SQL sample-mode coverage (see Caveats). Those remain a separate, explicitly deferred
layer on top of this command; a passing gate is a necessary condition for a cutover, not a
sufficient one.
