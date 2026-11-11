# Phase state

The phase runner keeps five rows per chain in `chain_phase_state`, one for each phase: Ingest, Interpret, Project, Verify and Live (`crates/storage/schema/baseline/10_phase_state.sql:89-97`, `apps/phase-runner/src/phase.rs:33-39`). A row's state is not one column. It is `phase_status` together with a redo marker, the owner text in `last_error`, and a few flags.

This page names those states, lists every statement that moves a row, and marks where two statements do the same job by different rules. [`storage.md` § Table ownership](storage.md#table-ownership) describes the lifecycle in prose.

`apps/phase-runner/src/phase_state_table_tests.rs` holds this page against the code:

- Each named state is inserted as a row and must pass the CHECKs.
- Each combination this page calls unreachable is inserted. It must meet the CHECK named here, or the test records that the CHECKs accept it.
- Each transition runs its site's own statement on a row in each "from" state. The test checks the state the row ends in, and that no column outside the statement's write set moved.

Line numbers are those of the tree this page was added in. Paths under `apps/phase-runner/src/` are given by file name.

## Terms

- **Normal run**: a phase working forward from its cursor, `current_block_number`, toward its target.
- **Explicit redo**: a run over a fixed block range that re-derives what the phase wrote there. While one is recorded, `redo_in_progress` is true. The `redo_*` columns hold its range and progress, and the four `redo_previous_*` columns hold the status, error and times the redo restores when it finishes (`10_phase_state.sql:171-250`). This page calls these columns the redo marker.
- **Operator redo**: an explicit redo begun by `phase-runner redo`.
- **Required redo**: an explicit redo the system records ("stamps") because earlier work became invalid. A reorg that orphans readable blocks stamps one (`heads.rs:312-314`). So do an Ingest redo (`redo_stamp.rs:106-122`), a completed Interpret redo (`redo_hash_adoption.rs:108-171`), a flag recomputation that found visibility changes (`redo_recompute.rs:21-42`), and a manifest or discovery change that widens the watched set over ingested blocks (`crates/manifests/src/lib/required_ingest.rs:47-145`).
- **Owner text**: a required redo's `last_error` starts `required downstream redo: ` while no runner has begun it, and `required downstream redo active: ` once one has (`redo_stamp.rs:8-9`). The runner treats any `last_error` that starts `required downstream redo` as its own (`redo_stamp.rs:10, :51-53`).

## Columns

| Group | Columns | CHECKs |
| --- | --- | --- |
| Lifecycle | `phase_status`, `started_at`, `finished_at`, `last_error` | `10_phase_state.sql:98-100, :262-288` |
| Progress | `current_block_*`, `target_block_*`, `live_handoff_block_*` (Ingest only), `verification_level` (Verify only) | `:101-110, :115-124, :251-261` |
| Inputs | `input_content_hash` | `:125-128` |
| Redo marker | `redo_in_progress`, `redo_mode`, `redo_previous_*`, `redo_from_block_number`, `redo_to_block_number`, `redo_current_block_*`, `redo_target_block_*` | `:129-132, :146-153, :171-250` |
| Redo, one phase only | `redo_requested_*` (Project), `redo_source_boundary_markers` and `redo_manifest_authority_fingerprint` (Ingest) | `:133-143, :154-170` |
| Attempt | `redo_attempt_generation` | `:144-145` |
| Settlement | `settled_while_unconfigured` | `:111-114` |

PostgreSQL names the CHECKs that have no name in the order they appear: `chain_phase_state_check` (`:115-118`) through `chain_phase_state_check8` (`:276-287`). `chain_phase_state_check4` is the redo-marker CHECK at `:171-250`, `check7` the lifecycle times at `:262-275`, and `check8` the error rule at `:276-287`. A CHECK on one column is named after it, such as `chain_phase_state_phase_status_check`. PostgreSQL tests a table's CHECKs in name order, so a refused row names the first refusing CHECK by name.

## States

| State | Columns that define it | Phases | What it means for an operator | Written at |
| --- | --- | --- | --- | --- |
| Idle | `phase_status = 'idle'`. `started_at`, `finished_at` and `last_error` are NULL (`:262-263, :282-286`). No redo marker. | All | The phase has not started since its row was created. | `state.rs:101-104` |
| Running | `'running'`, `started_at` set, `finished_at` and `last_error` NULL, no redo marker. | All | A normal run. The row does not say whether a process still holds it. A stopped process leaves the same row, and the next start settles it while holding the phase's advisory lock (`runner_recovery.rs:54-78`). | `state.rs:186-213`, `completed_phase_recovery.rs:150-154` |
| Paused | As Running, with `'paused'`. | All | A normal run waiting for storage capacity. It resumes by itself (`runner_capacity.rs:146-156`). | `state.rs:300-307` |
| Completed | `'completed'`, both times set, `last_error` NULL, no redo marker, `settled_while_unconfigured` NULL. | All | Finished. Ingest also carries its live handoff and Verify its verification level, when the completion wrote them. | `state.rs:363-364` through `state_persistence.rs:95-109`, `state_settlement.rs:65-84`, `runner_recovery.rs:113-146`, `completed_phase_recovery.rs:166-170`, `redo_state.rs:492-530` |
| Completed, settled while unconfigured | As Completed, with `settled_while_unconfigured = true`. | All | Start-up completed this active row while its chain was not configured, whatever its cursor (`runner_chain.rs:65-110`). When the chain is configured again, the next start re-checks the row before it trusts it (`state.rs:144-153, :565-571`). | `state_settlement.rs:65-84` |
| Failed | `'failed'`, `last_error` set, both times set, no redo marker. | All | Stopped on an error. A normal start retries it. `last_error` tells which kind: the run's own error (`state.rs:390-399`), `phase stopped before completion; ...` from start-up recovery (`runner_recovery.rs:139-140`), `phase was interrupted before redo; ...` when a redo began over a normal run and finished (`redo_state.rs:521-524`, `redo_supersede.rs:99-102`), or Live's `live phase stopped because verify reported a verification mismatch: ...` (`state_persistence.rs:306-319`). | as listed |
| Retained completion | Failed, with the completed extent kept: `current_block_*` equal `target_block_*`. Ingest also needs `last_error` to start `completed phase validation failed: ` and its handoff at the target. Verify needs a verification level and no error prefix (`completed_phase_recovery.rs:38-58`). | Ingest, Verify | A finished phase whose completion a later check failed. The next start repeats the check and moves the row through running to completed without replaying it (`state.rs:139-140, :168-176`, `completed_phase_recovery.rs:124-192`). | `completed_phase_recovery.rs:92-96`, `runner_recovery.rs:131-138` |
| Operator redo | Redo marker set, `redo_mode = 'redo'`, `phase_status` running or paused. `last_error` is NULL, or the error of a failed attempt that does not start `required downstream redo`. | Ingest, Interpret, Project, Verify | An operator redo, under way or interrupted. A normal start refuses it and prints the command that resumes it (`transitions.rs:166-192`). | `redo_state.rs:209-257`, `redo_failure.rs:18-38` |
| Required redo pending | Redo marker set, `redo_mode = 'redo'`, `last_error` starts `required downstream redo: `. | Interpret, Project, Verify (`redo_stamp.rs:154-161`), and Ingest (`required_ingest.rs:117-132`) | Work the runner recorded and owns, not yet begun. The runner runs it ahead of the phase's normal run (`runner_required_redo.rs:230-260`). While a required Ingest redo is pending, an Interpret or Project redo is refused with the `phase-runner redo --phase ingest` command (`transitions.rs:194-229`). | `redo_stamp.rs:16-32`, `required_ingest.rs:94-103, :117-132`, `redo_failure.rs:22-24`, `redo_completion.rs:144-147`, `runner_recovery.rs:87-93` |
| Required redo active | As pending, with `last_error` starting `required downstream redo active: `. | Ingest, Interpret, Project, Verify | A runner has begun the required redo. A failed attempt (`redo_failure.rs:22-24`) or a widened range (`redo_completion.rs:144-147`) returns it to pending. Start-up recovery returns an Ingest one to pending (`runner_recovery.rs:86-112`). On the other phases it stays active after its process is gone. | `redo_state.rs:246-248` |
| Recompute-flags redo | Redo marker set, `redo_mode = 'recompute_flags'`. | Interpret only (`:245-248`) | An operator `phase-runner redo --phase recompute-flags`, which recomputes normalization flags over the range. | `redo_state.rs:209-257`, `redo_state.rs:327-335` |

### Flags that ride on a state

| Flag | Columns | On | Meaning |
| --- | --- | --- | --- |
| Redo progress | `redo_current_block_*`, `redo_target_block_*`, and on Ingest `redo_source_boundary_markers` | The four redo states | How far this attempt got. Every stamp clears it (`redo_stamp.rs:26-29, :44-47`, `required_ingest.rs:98-101, :126-129`), as do a widened completion (`redo_completion.rs:138-143`), a boundary divergence (`redo_failure.rs:29-35`) and a begin that does not resume (`redo_state.rs:235-241`). `redo_attempt_generation` counts attempts. A progress write from a stale attempt changes nothing (`state_persistence.rs:178-184, :223-229`). |
| Saved lifecycle | `redo_previous_*` | The four redo states | The status, error and times the finish restores. `running` or `paused` there means the redo began over a normal run, and the finish restores that run as failed (`redo_state.rs:305, :495, :521-524`). |
| Settled marker carried | `settled_while_unconfigured = true` on a row that is not completed | Running, Paused, Failed, the redo states | A settled row was restarted. Starting, pausing, failing and every redo statement leave the marker (`state.rs:186-213, :300-307, :390-399`). A completion clears it (`state.rs:363`, `completed_phase_recovery.rs:168`, and `redo_state.rs:497` when the restored extent is complete). |
| Stale content hash | `input_content_hash` is not the hash this binary expects of the phase, or is a [manifest-authority marker](glossary.md#manifest-authority-marker) | Any state. Manifest sync writes the marker on Interpret and Project only (`crates/manifests/src/schema_v2_sync_state.rs:239-255, :272-279`). | Derived work recorded under other inputs. The CHECKs cannot see it. A derived phase's start refuses it and names the redo (`transitions.rs:401-458`, `content_hash_refusal.rs:16`). |
| Ingest completion without handoff | Ingest completed with `live_handoff_block_*` NULL or not at the target | Completed (Ingest) | Start-up settled an unconfigured chain's Ingest row, which clears the handoff (`state_settlement.rs:72-79`). The next start restarts Ingest (`state.rs:143`, `transitions.rs:65-71`). |
| Completed without a cursor | Completed with `current_block_*` NULL | Completed | A row settled before its first progress write. An Interpret or Project start restarts it (`state.rs:573-575`). |

## Combinations no statement writes

The CHECKs refuse these. `phase_state_table_tests/state_tests.rs` inserts each and checks the constraint.

| Combination | Refused by |
| --- | --- |
| A redo marker on a completed, failed or idle row | `chain_phase_state_check4` |
| Redo columns, or a saved status, with no marker | `chain_phase_state_check4` |
| `recompute_flags` on Project or Ingest | `chain_phase_state_check4` |
| A saved `failed` status without its saved error | `chain_phase_state_check4` |
| A redo cursor above its redo target, or outside the range | `chain_phase_state_check4` |
| A requested range outside Project, outside the execution range, or without a marker | `chain_phase_state_project_redo_request_check` |
| Boundary markers outside an Ingest redo | `chain_phase_state_ingest_redo_source_boundaries_check` |
| A manifest fingerprint outside an Ingest redo | `chain_phase_state_ingest_redo_manifest_authority_check` |
| Failed with no error; running or completed with an error and no marker | `chain_phase_state_check8` |
| Completed with no finish time, a finish before the start, idle with a start time, a redo with a finish time | `chain_phase_state_check7` |
| A verification level outside Verify | `chain_phase_state_verification_phase_check` |
| A live handoff outside Ingest | `chain_phase_state_check6` |
| `settled_while_unconfigured = false` | `chain_phase_state_unconfigured_settlement_check` |
| A block number without its hash | `chain_phase_state_check` |
| An unknown status | `chain_phase_state_check7`, which sorts before `chain_phase_state_phase_status_check` |

The CHECKs accept these, and no statement writes them, by the trace given:

| Combination | Why no statement writes it |
| --- | --- |
| A redo whose saved status is `idle` | A redo begin needs a cursor (`redo_extent.rs:17-21`). `CREATE_STAMP` needs `current_block_number >= $3` (`redo_stamp.rs:32`) and the Ingest installer `current_block_number IS NOT NULL` (`required_ingest.rs:132`). Idle rows have no cursor: see the last row. |
| A Live redo marker | `phase-runner redo --phase live` is refused (`runner_operator_redo.rs:54-57`). The runner stamps only Interpret, Project and Verify (`redo_stamp.rs:154-161`), and the installer only Ingest. |
| Owner text on a row with no marker | Every statement that writes owner text also writes or requires the marker. A finish restores the error saved before the redo, not the owner text (`redo_state.rs:521-524`). |
| A recompute-flags redo with owner text | `CREATE_STAMP` writes `redo_mode = 'redo'`. `EXTEND_STAMP` keeps `last_error`. A begin cannot change the mode of a recorded redo (`redo_state.rs:353-355`). |
| A redo marker with `redo_attempt_generation = 0` | Every begin and stamp adds one (`redo_state.rs:213`, `redo_stamp.rs:17, :38`, `required_ingest.rs:96, :119`). Only a marker recorded before the column existed can hold 0 (`migrations/20260814124000_redo_attempt_generation.sql:9-10`). |
| An idle row with a cursor, a settled marker or a verification level | Idle is written only by the insert, which sets none of them, and by a finish that restores a saved `idle`, which the first row rules out. A normal progress write has no status guard (`state_persistence.rs:95-109`), but its callers write only after `start_phase` set the row running. |

## Transitions

One row per statement. "Guard" is what must hold for the statement to run or to change the row: the statement's own `WHERE`, and the Rust checks in front of it. `can_transition_to` (`state.rs:37-50`) is the Rust rule on `phase_status`. "Leaves" lists what a reader might expect it to touch and it does not.

| # | Site | From | To | Guard | Writes | Leaves |
| --- | --- | --- | --- | --- | --- | --- |
| T1 | `state.rs:101` (`initialize_chain`) | No row | Idle | `ON CONFLICT DO NOTHING` | Every column at its default | An existing row |
| T2 | `state.rs:188` (`start_phase`) | Idle, Running, Paused, Failed. Completed only when it restarts: Live always, an Ingest completion without its handoff, Interpret or Project behind the head, a settled Verify that is incomplete (`state.rs:141-155`). | Running | No redo marker (`transitions.rs:166-192`), no conflicting active phase (`:275-316`), prerequisite completed (`:357-383`), content hash (`:404-425`), `can_transition_to` (`state.rs:177`). A Completed row that does not restart, and a retained completion, skip the statement (`state.rs:158-176`). | `phase_status`, `input_content_hash`, `last_error = NULL`, `started_at = now()`, `finished_at = NULL`. From Idle also clears the cursor, the target, the Verify level and the Ingest handoff (`state.rs:190-206`). | `settled_while_unconfigured`, every redo column |
| T3 | `state.rs:302` (`pause_phase`, `resume_phase`) | Running, Paused, with or without a redo marker | Paused, Running | `can_transition_to` only | `phase_status` | Everything else |
| T4 | `state.rs:363` through `state_persistence.rs:97` (`complete_phase_on_connection`) | Running, Paused | Completed | `can_transition_to`, the progress check (`state_persistence.rs:255-271`) | Cursor, target, Ingest handoff, Verify level, `phase_status`, `settled_while_unconfigured = NULL`, `finished_at` | `started_at`, `last_error`, `input_content_hash`, every redo column |
| T5 | `state.rs:392` (`fail_phase`) | Running, Paused | Failed | `can_transition_to` | `phase_status`, `last_error`, `finished_at` | Cursor, `settled_while_unconfigured`, every redo column |
| T6 | `state_persistence.rs:97` (`record_progress`, normal mode) | Running, Paused, by its callers | The same state | None in SQL or Rust | Cursor, target, Ingest handoff, Verify level | Lifecycle, every redo column |
| T7 | `state_persistence.rs:168` (`record_progress`, redo mode) | The four redo states | The same state | The marker's generation, mode and range equal the attempt's. On Ingest, the fingerprint equals the active manifests' and the owner is not a pending required redo (`:178-194`). | Redo cursor and target. Ingest boundaries merge in (`:173-176`). | Everything else |
| T8 | `state_persistence.rs:308` (`record_live_verification_mismatch`) | Live in any status, no marker | Failed | `NOT redo_in_progress` only | `phase_status`, `last_error`, `started_at` when NULL, `finished_at` | Cursor, `input_content_hash` |
| T9 | `state_settlement.rs:66` (`complete_stopped_phase`, `complete_unconfigured_phase`) | Running, Paused, no marker. For an unconfigured chain, also `updated_at` as observed. | Completed, or Completed settled while unconfigured | The statement's `WHERE`. The callers hold the phase lock (`runner_recovery.rs:18-37`, `runner_chain.rs:76-86`). | `phase_status`, `last_error = NULL`, `settled_while_unconfigured = true` when unconfigured, Ingest handoff NULL, `finished_at` | Cursor, target, Verify level, `input_content_hash`, `started_at` |
| T10 | `state_settlement.rs:108` (`clear_unconfigured_settlement`) | Completed settled while unconfigured | Completed | `phase_status = 'completed'` | `settled_while_unconfigured` | Everything else. A settled row that is not completed keeps the marker. |
| T11 | `completed_phase_recovery.rs:93` (`fail_completed_validation`) | Completed, Ingest or Verify | Retained completion | Status completed, phase, message prefix (`:68-91`) | `phase_status`, `last_error`, `finished_at` | Extent, handoff, level |
| T12 | `completed_phase_recovery.rs:151` then `:167` (`complete_revalidated_phase`) | Retained completion | Running, then Completed, in one transaction | `is_retained_completion` (`:142-149`) | `phase_status`, `last_error = NULL`, `started_at`, `finished_at`, then `settled_while_unconfigured = NULL` | Extent, handoff, level |
| T13 | `runner_recovery.rs:88` (`resolve_stopped_phase`, Ingest) | Required redo active (Ingest) | Required redo pending | Owner text is active. The caller holds the Ingest lock. | `last_error` | Range, progress, boundaries, fingerprint |
| T14 | `runner_recovery.rs:114` (`resolve_stopped_phase`, other phases) | Running, Paused, no marker (Interpret, Project, Verify) | Completed when the cursor equals the target and the phase is not Verify. A retained completion for a Verify final checkpoint with a level. Otherwise Failed. | The statement's `WHERE`. The caller holds the phase lock. | `phase_status`, `last_error`, `finished_at` | `settled_while_unconfigured`, cursor |
| T15 | `redo_state.rs:211` (`begin`) | Running, Paused, Completed, Failed. A redo state when the new range covers the recorded one and the mode matches (`redo_state.rs:337-366`). Idle has no cursor and is refused (`redo_extent.rs:17-21`). | Operator redo or recompute-flags redo. Over a pending required redo: Required redo active. | `require_start` (`transitions.rs:140-164`), the recorded extent, the coverage check, `can_transition_to(.., true)` (`redo_state.rs:160`) | `phase_status = 'running'`, generation + 1, mode, range, Project's requested range, progress (kept only on a resume), Ingest fingerprint, `input_content_hash` for derived phases, `last_error`, `started_at`, `finished_at = NULL`. The saved lifecycle only when no marker was recorded. | Cursor, target, handoff, level, `settled_while_unconfigured` |
| T16 | `redo_state.rs:494` (`finish`) | A redo state whose attempt the completion covers exactly (`redo_completion.rs:101-127`) | The saved lifecycle: Idle, Completed or Failed. Failed with `phase was interrupted before redo; ...` when it was Running or Paused. | `lock_completion_coverage` (`redo_completion.rs:50-175`) | Every column but `redo_attempt_generation` | `redo_attempt_generation` |
| T17 | `redo_completion.rs:137` (widened completion) | A redo state whose recorded range is wider than the attempt's | The same state. Required redo active returns to pending with `; range widened; rerun the full persisted range`. | Mode equal, recorded range covers the attempt's (`:128-134`) | Progress, Ingest boundaries and fingerprint, `last_error` | Range, lifecycle |
| T18 | `redo_failure.rs:20` (`record`) | The four redo states | The same state. Required redo active returns to pending with `; last attempt failed: ...`. | `redo_in_progress` | `last_error`. Progress and boundaries only on a boundary divergence. | Range, lifecycle |
| T19 | `redo_supersede.rs:97` (`supersede`) | A Project operator redo under another projection content hash, met by an Interpret redo while Interpret records another interpreter content hash (`redo_supersede.rs:25-46`) | The saved lifecycle, as T16 | The Project lock is free (`:64-85`). Generation and hash equal the locked row's. | Lifecycle, the whole redo marker | Cursor, target, `input_content_hash`, generation, `settled_while_unconfigured` |
| T20 | `redo_stamp.rs:16` (`CREATE_STAMP`, through `stamp`) | A row with a cursor at or above the range start and no marker: Completed, Failed, and also Running or Paused | Required redo pending | `current_block_number >= $3 AND NOT redo_in_progress`. `stamp` allows Interpret, Project and Verify, and clips the range to the cursor (`redo_stamp.rs:146-212`). | The redo marker with the saved lifecycle, `phase_status = 'running'`, `last_error`, `started_at = now()`, `finished_at = NULL`, Project's requested range | Cursor, target, handoff, level, `input_content_hash`, `settled_while_unconfigured` |
| T21 | `redo_stamp.rs:37` (`EXTEND_STAMP`, through `stamp`) | The four redo states | The same state. The range becomes the union, clipped to the cursor. | `redo_in_progress` | Generation + 1, range, Project's requested range (`LEAST`, `GREATEST`), progress, Ingest boundaries and fingerprint | `phase_status`, mode, saved lifecycle, `last_error`, `started_at` |
| T22 | `crates/manifests/src/lib/required_ingest.rs:95` (widen) | An Ingest redo state | Required redo pending: the owner text is rewritten (`:147-156`) | `redo_in_progress`. In front: a cursor, an ingest source and a range that reaches the readable head (`:53-85`). | Generation + 1, range, progress, boundaries, fingerprint, `last_error` | `phase_status`, mode, saved lifecycle, `started_at` |
| T23 | `crates/manifests/src/lib/required_ingest.rs:118` (create) | An Ingest row with a cursor and no marker, in any status | Required redo pending | `current_block_number IS NOT NULL AND NOT redo_in_progress` | As T20, without the requested range | As T20 |
| T24 | `crates/manifests/src/schema_v2_sync_state.rs:241` | Interpret and Project rows with a content hash | The same state, with a stale content hash | `input_content_hash IS NOT NULL` | `input_content_hash` | Everything else |
| T25 | `crates/manifests/src/schema_v2_sync_state.rs:273` | The Base Project row with a content hash | The same state, with a stale content hash | `input_content_hash IS NOT NULL` | `input_content_hash` | Everything else |

The test drives every statement but T24 and T25. They are reached only through a full manifest sync (`crates/manifests/src/schema_v2.rs:55`), and the manifest-sync tests drive them: `checked_in_sepolia_resolver_admission_clamps_retained_redo_to_cursor_floor` and `basenames_execution_retirement_invalidates_the_base_project_epoch` in `apps/phase-runner/tests/production_manifest_sync.rs`. T15 and T16 are driven for Verify and Ingest redos. Their Interpret and Project branches, the requested range and the derived content hash, are not.

## Where two statements do one job

### Settling a stopped run

Four statements settle a run whose process stopped:

| | T14 start-up recovery | T9 settlement | T16 finish of a redo begun over a run | T19 supersede |
| --- | --- | --- | --- | --- |
| Finds | Running or paused, no marker | Running or paused, no marker | Saved status running or paused | Saved status running or paused |
| Ends | Completed if the cursor reached the target and the phase is not Verify. A retained completion for a Verify final checkpoint. Otherwise failed. | Completed, whatever the cursor | Failed | Failed |
| `last_error` | NULL, the validation prefix, or `phase stopped before completion; ...` | NULL | `phase was interrupted before redo; ...` | `phase was interrupted before redo; ...` |
| Ingest | Not settled. Only an active required redo returns to pending (T13). | Settled, and the handoff cleared | Settled | Project only |
| `settled_while_unconfigured` | Left | Set when the chain is unconfigured | Cleared when the restored extent is complete | Left |

### Restoring the saved lifecycle

T16 and T19 restore the same saved columns differently. For a saved `completed`, the finish writes `finished_at = now()` (`redo_state.rs:526`) and the supersede restores the saved time (`redo_supersede.rs:104`). The finish rewrites the cursor hash, the Verify level, the Ingest handoff and the content hash from the redo (`redo_state.rs:496-520`). The supersede keeps them, by design (`redo_supersede.rs:54-58`).

### Beginning a redo, and the required stamp

The operator begin (T15) and the required stamps (T20, T21, and the Ingest copy T22, T23) each write the redo marker, by different rules:

| Column | Begin (T15) | `CREATE_STAMP` (T20) | `EXTEND_STAMP` (T21) | Ingest create (T23) | Ingest widen (T22) |
| --- | --- | --- | --- | --- | --- |
| SQL guard | None | Cursor at or above the start, no marker | Marker | Cursor not NULL, no marker | Marker |
| `phase_status` | `running` | `running` | Kept | `running` | Kept |
| Saved lifecycle | Written only when no marker was recorded | Written | Kept | Written | Kept |
| Range | The request, or Project's execution range | The request, clipped to the cursor | Union, clipped to the cursor | From the request to the readable head or the cursor | Union |
| Project requested range | The request | The range | `LEAST`, `GREATEST` | Not written | Not written |
| Progress | Kept on a resume, else NULL | NULL | NULL | NULL | NULL |
| Ingest fingerprint | Bound to the active manifests | NULL | NULL | NULL | NULL |
| `input_content_hash` | This binary's, on derived phases | Kept | Kept | Kept | Kept |
| `last_error` | Pending owner text becomes active. Kept while a marker is recorded. Else NULL. | The stamp's owner text | Kept | The installer's owner text | The merged owner (`required_ingest.rs:147-156`) |
| `started_at` | Kept on a resume, else `now()` | `now()` | Kept | `now()` | Kept |
| Generation | + 1 | + 1 | + 1 | + 1 | + 1 |

The Ingest copy is a hand copy of the two runner statements in a file that feeds the interpreter content hash. It differs from them in three ways:

- Its cursor predicate is `current_block_number IS NOT NULL`, where `CREATE_STAMP` has `current_block_number >= $3`. The range it stamps reaches the published head, which can be above the cursor (`required_ingest.rs:80-85`).
- It does not write the requested range. The CHECK keeps that NULL on Ingest (`10_phase_state.sql:133-143`), so the rows it writes are the same.
- Its widen writes the owner text. `EXTEND_STAMP` keeps it.

## The Rust rule and the CHECKs

The CHECKs constrain states, not moves. Any statement whose result passes them is accepted. `can_transition_to` is consulted at T2, T3, T4, T5 and T15 only. The other statements write moves it has no arm for:

| Move | Written by |
| --- | --- |
| completed to failed | T8, T11 |
| idle to failed, failed to failed | T8 |
| running or paused to idle, completed or failed, as a redo ends | T16, T19 |
| completed to running | T20, T23, T12 |

And it allows moves the CHECKs refuse, because the statement writes too little:

| Move | At | Refused by |
| --- | --- | --- |
| failed to running | T3, which writes only `phase_status` | `chain_phase_state_check7` |
| running to completed or failed with a redo marker present | T4, T5 | `chain_phase_state_check4` |
