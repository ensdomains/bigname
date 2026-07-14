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
bigname-worker inspect data-completeness --json --fail-on-incomplete
```

Exit `0` means every check passed and `data_complete` is `true`. With
`--fail-on-incomplete`, exit `1` means at least one check failed. Without the flag the
command always exits `0` and is a report.

Point it at any database with `--database-url`, or `BIGNAME_DATABASE_URL` /
`DATABASE_URL`. Comparing a warmed candidate database against the serving one is the
intended use.

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

The active chain set is the authority for every per-chain check: active
`manifest_contract_instances` declarations, the materialized watch view (manifest-declared
contracts unioned with active discovery edges), and every chain an active `manifest_versions`
row declares directly. A partial restore that lost `contract_instance_addresses` rows therefore
cannot delete a chain or directly declared target from its own expectations. A chain present in
storage but not in that active set is a foreign or retired chain and is reported as an advisory,
not gated.

| Check | Passes when |
| --- | --- |
| `reconciliation_frontier_at_head` | every active chain has a head lag within `±--max-head-lag-blocks` (default 8) between the stored canonical checkpoint and the canonical lineage head, and every active chain has a frontier row at all |
| `reconciliation_lineage_contiguous` | distinct canonical/safe/finalized block numbers equal `head - floor + 1`, no height has multiple canonical hashes, and every row above the retained floor points by `parent_hash` to the canonical/safe/finalized row at the preceding height |
| `reconciliation_history_from_declared_start` | for every active watched chain, the retained lineage floor is at or below the earliest finite start block its active targets declare; a chain whose targets are all open-ended fails, since no floor can be established |
| `watch_set_code_observation_coverage` | there is at least one active target, and every **active** target — direct active-manifest declarations unioned with the materialized watch view and active discovery edges — has a non-orphaned `raw_code_hashes` observation at or after its inclusive active start (when finite) |
| `manifest_declared_targets_present` | every active manifest-declared contract instance has a live `contract_instance_addresses` row whose chain and address match the declaration |
| `normalization_no_failure` | no `normalized_replay_cursors` row carries a `last_failure_reason` |
| `normalization_caught_up_to_raw_head` | every active chain with retained canonical raw logs has a `raw_fact_normalized_events` cursor, and each replay cursor has reached its applicable target (see below) |
| `projection_apply_drained` | the change log is empty, or the `normalized_events_to_projection_invalidations` cursor exists and its `last_change_id` has reached `max(projection_normalized_event_changes.change_id)`; unrelated cursor rows do not count |
| `projection_invalidations_drained` | `projection_invalidations` is empty — every enqueued invalidation has been applied and deleted |
| `projection_no_dead_letters` | `projection_invalidation_dead_letters` is empty — no invalidation exhausted its retries |
| `projection_replay_complete` | every current projection has a `current_projection_replay_status` marker at the newest replay version present whose `completed_normalized_target_block` covers the target bootstrap would request now: the greater of the raw-fact replay target and chain-checkpoint frontier |
| `active_dataset_non_empty` | every active manifest source whose ABI declares non-empty `normalized_events` output has a non-orphaned matching event under that exact `source_manifest_id`, manifest version, chain, namespace, source family, and declared event kind; every such namespace also has `name_current` rows |
| `normalized_events_chain_id_present` | no non-orphaned `normalized_events` row has a NULL `chain_id` |
| `deferred_projection_indexes_present` | the eight deferred `normalized_events` projection indexes all exist on `public.normalized_events` and have `pg_index.indisvalid = true` and `indisready = true` (a fresh replay drops them and rebuilds them after catch-up) |

`watch_set_code_observation_coverage` is the check with teeth, and the reason the command
exists. Most others are *relative* invariants: each compares a stage to the stage before
it, so they stay green while the pipeline faithfully processes an incomplete input.
`reconciliation_history_from_declared_start`, `active_dataset_non_empty`, and
`projection_replay_complete` also compare against the declared world or the pipeline's own
handoff authority rather than the previous stage.

`projection_apply_drained` only proves the derive scan finished — that the apply cursor
reached the change-log frontier. It does not prove the resulting invalidations were
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

A chain that ran closure or dependency replay is an exception. Its raw-fact cursor target
is latched permanently below the live head; newer logs are carried by a one-shot
`post_replay_live_adapter_backlog` sweep and then live adapter sync. The gate detects the
latch by the presence of a `post_replay_live_adapter_backlog` cursor row and, for such a
chain, requires both the raw-fact cursor and the backlog cursor to have reached their own
targets rather than the raw-log head. **Limitation:** the live tail beyond the backlog
target has no cursor, so the gate cannot verify it. This is one of the reasons a passing
gate is necessary but not sufficient (see Scope).

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
never emits a log. Coverage preserves the latest non-orphaned observation block per
`(chain, address)` and compares it with the target's inclusive active start; a pre-admission
observation retained from an older range cannot satisfy a newly active target. Direct active
manifest declarations are read independently of `contract_instance_addresses`, so losing the
materialized address row makes the declaration unobserved instead of removing it from the
expected set. When several active entries share an address, coverage uses the latest finite
start, the strictest lower bound across those entries.

`manifest_declared_targets_present` diagnoses the materialization edge separately. A live
address row only satisfies a declaration when its `chain_id` and lowercased address match the
manifest's chain and declared address; a live row for the same instance on another chain or at
another address does not count. See [`chain-intake.md`](../chain-intake.md).

### Frontier, history, and content against the declared world

Three checks measure the chain and its content against what the watch set declares, not
only against the previous pipeline stage:

- **Frontier.** `reconciliation_frontier_at_head` tolerates a head lag within `±max` blocks.
  Reconcile commits canonical lineage and then advances the checkpoint, so on a live database
  the lineage head routinely leads the checkpoint by a small margin — a negative lag is a
  normal committed state, not a fault, and only a gap beyond `max` in either direction fails
  (a checkpoint far behind the lineage is a stale checkpoint writer). The check also
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
- **Content.** `active_dataset_non_empty` derives expected event-producing sources from active
  manifest ABI entries whose `normalized_events` list is non-empty. Execution- or
  transport-only manifests with no declared normalized output do not create event-content
  expectations. Each expected source must have a non-orphaned event matching its exact
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

### Projections rebuilt, and structural integrity

`projection_replay_complete` requires every current projection to have a marker in
`current_projection_replay_status` at the newest replay version present. Each marker's
`completed_normalized_target_block` must also reach the target automatic bootstrap would use
now: `max(raw_fact_normalized_events target, furthest chain checkpoint)`. Bootstrap publishes
projections in order and `name_current` is first, so a candidate mid-bootstrap has
`name_current` but not the rest; requiring all markers and their target coverage matches the
worker's own handoff authority. The version is read from the data rather than hardcoded, so a
database built by an older image is judged complete at its own version, but stale markers from
an earlier target are not accepted.

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

- **`watch_set_code_observation_coverage` fails, everything else passes.** The runtime
  watch scope is narrower than the manifests and discovery edges declare. The reported
  `unobserved_targets` were never watched, so their logs were never indexed and their
  derived state is missing, silently. Check the indexer's
  `BIGNAME_INDEXER_HASH_PINNED_BACKFILL_ADAPTER_SYNC` and the resulting
  `RuntimeWatchScope`; `manifest_declared_only` excludes discovery edges. Verify with
  `inspect watch-plan --json`.
- **`manifest_declared_targets_present` fails.** An active declaration has no live address row
  matching both its declared chain and address. A missing, deactivated, wrong-chain, or
  wrong-address row breaks the watch-view authority even if retained code observations let the
  separate coverage check pass. Reseed manifests or repair the materialized address row before
  promoting.
- **Coverage fails on a database warmed by backfill alone.** Backfill only observes a
  block's selected log emitters. A watched target that never emits acquires its single
  baseline observation from the live tailer, on canonical head reconciliation. Let the
  tailer reconcile a head before treating coverage as authoritative on a freshly
  backfilled database.
- **`normalization_no_failure` fails.** The adapter pass is crash-looping; the cursor
  records why. Normalization has stopped at the first affected block, so every downstream
  count is stale even though the cursors look drained.
- **`reconciliation_frontier_at_head` fails.** Head reconciliation has stalled, or the
  checkpoint and lineage have diverged by more than `±max` blocks. A large positive
  `head_lag_blocks` means the lineage trails the checkpoint; a large negative value means the
  checkpoint writer is stale and far behind the lineage (a small negative lag is normal — see
  above). A `missing_from_storage: true` chain is declared by the watch set but has no
  frontier row.
- **`reconciliation_lineage_contiguous` fails with a non-zero `duplicate_canonical_height_count`.**
  Two or more non-orphaned lineage rows share one block height. That is a canonicality
  violation — at most one hash per height may be canonical — not a gap, and it points at a
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
  matching non-orphaned normalized event. Rows from another chain, source family, or deprecated
  manifest identity do not satisfy it, even if the global event table is non-empty.
- **`projection_replay_complete` fails with entries in `missing_projections`.** The candidate
  is mid projection-bootstrap, or the listed projection's marker completed below the reported
  `required_target_block`. `name_current` is published first, so a candidate with only it is
  early in the rebuild; a complete marker set with stale targets requires replay through the
  current bootstrap target.
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
- **`watch_set_code_observation_coverage` fails with zero active watched targets.** The
  database has no manifests loaded — a restore that dropped or never applied them. The check
  fails rather than passing vacuously, since an ENS deployment always watches at least the
  registry.
- **`active_dataset_non_empty` fails while every cursor is drained.** The pipeline is healthy
  and there is nothing in the active dataset. This is the empty-database case, and it is the
  shape prod presented on 2026-07-06.

## Advisories

The `advisories` block reports state that is worth an operator's attention but does not gate:

- **`foreign_chains`** — chains with residual checkpoint or lineage rows that the active watch
  set does not cover (a retired chain, or one whose manifest was removed while its chain state
  remained). Their per-chain checks are not gated; the listing is for cleanup.
- **`backfill_lifecycle`** — per-profile counts of `failed`, incomplete, and expired-lease
  backfill jobs and ranges. Backfill failures are *not* gated, because without coverage-fact
  reconciliation a `failed` range cannot be distinguished from one superseded by a later
  successful retry, and the data-level checks (frontier, history, coverage, content) already
  gate on the resulting completeness. Treat a non-zero `failed_range_count` or
  `expired_lease_range_count` as a prompt to investigate before promoting.

## Caveats

- **Retired deployment profiles.** The gate reads replay cursors across every
  `deployment_profile`. A stale cursor left by a retired profile — one no longer being
  advanced — can fail `normalization_caught_up_to_raw_head` for a chain that the live profile
  serves correctly. Remove cursor rows for retired profiles, or read the reported cursor
  labels (`<profile>/<chain>/<kind>`) to confirm the lagging cursor belongs to the active
  profile before treating the failure as real.
- **Coinbase SQL sample-mode candidates.** In `sample` validation mode a Coinbase SQL backfill
  persists lineage only for the blocks whose logs it returned, while completing the whole
  range. The reconciled span is then sparse, so `reconciliation_lineage_contiguous` fails on a
  candidate that is legitimately complete for its sampled coverage. A sample-backed candidate
  is not gate-promotable without a full-mode pass over the same range; the gate does not
  reconcile sampled coverage facts.

## Scope

This gate is database-level. It does not exercise HTTP routes, compare name counts across
two databases, or spot-check GraphQL and REST answers. It also cannot verify the live tail
beyond a latched chain's backlog target, which has no cursor, and it does not reconcile
Coinbase SQL sample-mode coverage (see Caveats). Those remain a separate, explicitly deferred
layer on top of this command; a passing gate is a necessary condition for a cutover, not a
sufficient one.
