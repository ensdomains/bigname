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
| `watch_set_code_observation_coverage` | there is at least one active watched target, and every **active** watched target — manifest-declared contracts unioned with active discovery edges — has at least one non-orphaned `raw_code_hashes` row |
| `normalization_no_failure` | no `normalized_replay_cursors` row carries a `last_failure_reason` |
| `normalization_caught_up_to_raw_head` | every chain with retained canonical raw logs has a `raw_fact_normalized_events` cursor, and each replay cursor has reached its applicable target (see below) |
| `projection_apply_drained` | the change log is empty, or an apply cursor exists and each `projection_apply_cursors.last_change_id` has reached `max(projection_normalized_event_changes.change_id)` |
| `projection_invalidations_drained` | `projection_invalidations` is empty — every enqueued invalidation has been applied and deleted |
| `projection_no_dead_letters` | `projection_invalidation_dead_letters` is empty — no invalidation exhausted its retries |
| `projections_non_empty` | `normalized_events` and `name_current` are both non-empty |

`watch_set_code_observation_coverage` is the check with teeth, and the reason the command
exists. The others are *relative* invariants: each compares a stage to the stage before
it, so all of them stay green while the pipeline faithfully processes an incomplete
input. Coverage is the only check that compares what was indexed against what the
manifests and discovery edges say *should* be indexed.

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

The check also requires the cursor to exist: a chain with retained canonical raw logs but
no `raw_fact_normalized_events` cursor row — a truncated restore, or a chain absent from
catch-up configuration — fails with a `missing_cursor` entry rather than passing because
there was no cursor to measure.

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
- **`projection_invalidations_drained` or `projection_no_dead_letters` fails while
  `projection_apply_drained` passes.** The derive scan finished but the resulting projection
  writes have not landed: invalidations are still queued, or some exhausted their retries and
  dead-lettered. Projections are stale or partial even though the apply cursor looks caught
  up.
- **`watch_set_code_observation_coverage` fails with zero active watched targets.** The
  database has no manifests loaded — a restore that dropped or never applied them. The check
  fails rather than passing vacuously, since an ENS deployment always watches at least the
  registry.
- **`projections_non_empty` fails while every cursor is drained.** The pipeline is healthy
  and there is nothing in it. This is the empty-database case, and it is the shape prod
  presented on 2026-07-06.

## Caveats

- **Retired deployment profiles.** The gate reads replay cursors across every
  `deployment_profile`. A stale cursor left by a retired profile — one no longer being
  advanced — can fail `normalization_caught_up_to_raw_head` for a chain that the live profile
  serves correctly. Remove cursor rows for retired profiles, or read the reported cursor
  labels (`<profile>/<chain>/<kind>`) to confirm the lagging cursor belongs to the active
  profile before treating the failure as real.

## Scope

This gate is database-level. It does not exercise HTTP routes, compare name counts across
two databases, or spot-check GraphQL and REST answers. It also cannot verify the live tail
beyond a latched chain's backlog target, which has no cursor. Those remain a separate,
explicitly deferred layer on top of this command; a passing gate is a necessary condition
for a cutover, not a sufficient one.
