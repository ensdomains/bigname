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

| Check | Passes when |
| --- | --- |
| `reconciliation_frontier_at_head` | every chain's canonical lineage head is within `--max-head-lag-blocks` (default 8) of the stored canonical checkpoint |
| `reconciliation_lineage_contiguous` | distinct non-orphaned block numbers equal `head - floor + 1`, so no block in the reconciled span is missing |
| `watch_set_code_observation_coverage` | every **active** watched target — manifest-declared contracts unioned with active discovery edges — has at least one non-orphaned `raw_code_hashes` row |
| `normalization_no_failure` | no `normalized_replay_cursors` row carries a `last_failure_reason` |
| `normalization_caught_up_to_raw_head` | each `raw_fact_normalized_events` cursor's `last_completed_block_number` has reached that chain's non-orphaned raw-log head |
| `projection_apply_drained` | each `projection_apply_cursors.last_change_id` has reached `max(projection_normalized_event_changes.change_id)` |
| `projections_non_empty` | `normalized_events` and `name_current` are both non-empty |

`watch_set_code_observation_coverage` is the check with teeth, and the reason the command
exists. The others are *relative* invariants: each compares a stage to the stage before
it, so all of them stay green while the pipeline faithfully processes an incomplete
input. Coverage is the only check that compares what was indexed against what the
manifests and discovery edges say *should* be indexed.

It works because code observations are keyed on the watch set rather than on activity: a
watched target acquires a code observation from the live tailer's baseline pass even if it
never emits a log. A target with zero non-orphaned observations was therefore never
watched. See [`chain-intake.md`](../chain-intake.md).

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
  checkpoint writer has advanced past the lineage the indexer actually reconciled.
- **`projections_non_empty` fails while every cursor is drained.** The pipeline is healthy
  and there is nothing in it. This is the empty-database case, and it is the shape prod
  presented on 2026-07-06.

## Scope

This gate is database-level. It does not exercise HTTP routes, compare name counts across
two databases, or spot-check GraphQL and REST answers. Those remain a separate,
explicitly deferred layer on top of this command; a passing gate is a necessary condition
for a cutover, not a sufficient one.
