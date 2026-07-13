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

The active chain set is the authority for every per-chain check: the materialized watch view
(manifest-declared contracts unioned with active discovery edges) plus every chain an active
`manifest_versions` row declares directly, so a partial restore that lost
`contract_instance_addresses` rows cannot delete a chain from its own expectations. A chain
present in storage but not in that active set is a foreign or retired chain and is reported
as an advisory, not gated.

| Check | Passes when |
| --- | --- |
| `reconciliation_frontier_at_head` | every active chain has a head lag within `±--max-head-lag-blocks` (default 8) between the stored canonical checkpoint and the canonical lineage head, and every active chain has a frontier row at all |
| `reconciliation_lineage_contiguous` | distinct non-orphaned block numbers equal `head - floor + 1` (no gap in the reconciled span) and no block height carries more than one canonical/safe/finalized lineage row |
| `reconciliation_history_from_declared_start` | for every active watched chain, the retained lineage floor is at or below the earliest finite start block its active targets declare; a chain whose targets are all open-ended fails, since no floor can be established |
| `watch_set_code_observation_coverage` | there is at least one active watched target, and every **active** watched target — manifest-declared contracts unioned with active discovery edges — has at least one non-orphaned `raw_code_hashes` row |
| `normalization_no_failure` | no `normalized_replay_cursors` row carries a `last_failure_reason` |
| `normalization_caught_up_to_raw_head` | every active chain with retained canonical raw logs has a `raw_fact_normalized_events` cursor, and each replay cursor has reached its applicable target (see below) |
| `projection_apply_drained` | the change log is empty, or an apply cursor exists and each `projection_apply_cursors.last_change_id` has reached `max(projection_normalized_event_changes.change_id)` |
| `projection_invalidations_drained` | `projection_invalidations` is empty — every enqueued invalidation has been applied and deleted |
| `projection_no_dead_letters` | `projection_invalidation_dead_letters` is empty — no invalidation exhausted its retries |
| `projection_replay_complete` | every current projection has a `current_projection_replay_status` marker at the newest replay version present, matching the worker's bootstrap handoff |
| `active_dataset_non_empty` | every `(chain, namespace)` an active manifest declares has non-orphaned `normalized_events`, and every such namespace has `name_current` rows |
| `normalized_events_chain_id_present` | no non-orphaned `normalized_events` row has a NULL `chain_id` |
| `deferred_projection_indexes_present` | the eight deferred `normalized_events` projection indexes all exist (a fresh replay drops them and rebuilds them after catch-up) |

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
never emits a log. A target with zero non-orphaned observations was therefore never
watched. See [`chain-intake.md`](../chain-intake.md).

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
  *all* open-ended has no floor to establish and fails closed.
- **Content.** `active_dataset_non_empty` derives the expected `(chain, namespace)` set from
  active `manifest_versions` rows — the declared authority — not from observed events, so a
  chain declared to produce two namespaces fails if one has no rows even when the other does.
  Each expected pair must have non-orphaned `normalized_events` (by `chain_id`), and each
  expected namespace must have `name_current` rows. `name_current` carries no chain column,
  so names are scoped to the namespace — the finest dimension a name projection has; a name
  in a namespace shared across chains is not attributed to a specific one.

### Projections rebuilt, and structural integrity

`projection_replay_complete` requires every current projection to have a marker in
`current_projection_replay_status` at the newest replay version present. Bootstrap publishes
projections in order and `name_current` is first, so a candidate mid-bootstrap has
`name_current` but not the rest; requiring all markers matches the worker's own handoff
authority. The version is read from the data rather than hardcoded, so a database built by an
older image is judged complete at its own version.

`deferred_projection_indexes_present` checks that the eight deferred `normalized_events`
projection indexes exist. A fresh replay drops them for speed and a later pass rebuilds them
after catch-up, so an absent index marks a candidate whose replay has not finished
rebuilding them — complete data, but not yet ready to serve efficiently.

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
- **`reconciliation_history_from_declared_start` fails.** The chain's lineage floor is above
  the earliest block its watched targets declare, so history is truncated. Common on a
  restore that kept only a recent window; the reported `declared_start_block` and
  `lineage_floor_block` bound the missing span.
- **`active_dataset_non_empty` fails with a `(chain, namespace)` in
  `chain_namespaces_without_events`.** A declared chain/namespace has produced no non-orphaned
  normalized events, while another chain's rows keep the global table non-empty. It has not
  been indexed even though the pipeline looks drained.
- **`projection_replay_complete` fails with entries in `missing_projections`.** The candidate
  is mid projection-bootstrap: some projections have replay markers at the newest version but
  not all. `name_current` is published first, so a candidate with only it is early in the
  rebuild.
- **`deferred_projection_indexes_present` fails.** A fresh replay dropped the listed indexes
  and the rebuild pass has not run. Data is complete; let catch-up finish before promoting.
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
