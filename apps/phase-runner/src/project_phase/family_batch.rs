//! The Project batch with the [publication switch](bigname_storage::publication_source) on: the
//! owned key family loop is the batch (TYR-36 step 7b). The served engine, the served hydrator and
//! the redo target they read from the Project row do not run, so the served tables stop changing;
//! step 7c deletes them. The batch's progress is the family marker it committed, so the Project
//! row acknowledges the publication the serving fences read.
//!
//! One call is one budgeted family run. A run that spent its budget and moved the families
//! answers `Continue` with the marker it reached, and the runner records it and calls again: the
//! phase heartbeat and a stop are observed between runs, as between served batches. A run that
//! ends short of its target without spending its budget, or fails, fails the batch the way a
//! family failure fails the Project run with the switch off (`ProjectPhase::family_error`).
use bigname_project::{
    Marker,
    families::{FamilyMode, FamilyOptions, FamilyOutcome},
};

use super::{ProjectPhase, project_marker};
use crate::{
    error::{ErrorKind, RunnerError, RunnerResult},
    heads::BlockMarker,
    phase::{PhaseBatchOutcome, PhaseContext, PhaseProgress, RedoAttemptFence, RunMode},
};

/// A family batch that answered `Continue`. The next batch of the same run and redo attempt
/// resumes it in normal mode, as the switch-off path continues a family run (`run_families_once`):
/// in its own mode an unfinished rebuild would start again and a redo would undo its replayed
/// prefix again. A redo keeps the target it started with, since its undo moves the marker the
/// target is read from.
#[derive(Clone, Debug)]
pub(super) struct Continuation {
    mode: RunMode,
    redo_attempt: Option<RedoAttemptFence>,
    target: Marker,
}

impl ProjectPhase {
    pub(super) async fn run_family_batch(
        &self,
        context: PhaseContext,
    ) -> RunnerResult<PhaseBatchOutcome> {
        let chain_id = context.chain_id.as_str();
        if !self.families.enabled {
            return Err(RunnerError::new(
                ErrorKind::Configuration,
                format!(
                    "chain {chain_id}: the publication switch serves the owned key families, so \
                     Project cannot run with them turned off"
                ),
            ));
        }
        // Before this batch can move the Project row past the served tables.
        self.record_served_stop(chain_id).await?;
        let Some(available) = context.available_heads.as_ref() else {
            if let Some(range) = context.mode.range() {
                return Err(RunnerError::new(
                    ErrorKind::DataIntegrity,
                    format!(
                        "project redo block {} for chain {chain_id} is not readable (canonical, \
                         safe, or finalized)",
                        range.to
                    ),
                ));
            }
            return Ok(PhaseBatchOutcome::Complete(PhaseProgress::default()));
        };
        let continued = self
            .family_continuations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(chain_id)
            .filter(|continuation| {
                continuation.mode == context.mode
                    && continuation.redo_attempt == context.redo_attempt
            });
        let (target, mode) = match (&continued, context.mode.clone()) {
            (Some(continuation), RunMode::Redo(_) | RunMode::RecomputeFlags(_)) => (
                BlockMarker::new(continuation.target.number, continuation.target.hash.clone())?,
                FamilyMode::Normal,
            ),
            (Some(_), RunMode::Normal) => (available.latest.clone(), FamilyMode::Normal),
            (None, mode) => {
                self.fresh_target(chain_id, mode, &context, &available.latest)
                    .await?
            }
        };
        let target = project_marker(&target);
        let token = match bigname_project::families::input_token(&self.pool, chain_id).await {
            Ok(token) => token,
            Err(error) => {
                let standing =
                    bigname_project::families::standing(&self.pool, chain_id, &target).await;
                self.report_families(chain_id, &standing);
                return Err(self.family_error(chain_id, &target, &standing, &error));
            }
        };
        let options = FamilyOptions::new(bigname_content_hash::INTERPRETER_CONTENT_HASH)
            .with_max_blocks_per_run(self.families.max_blocks_per_run)
            .with_rebuild_ranges(self.families.rebuild_ranges);
        let options = match &self.hydrator {
            Some(hydrator) => options.with_hydration(hydrator.rpc_urls().clone()),
            None => options,
        };
        let (outcome, error) =
            bigname_project::families::run(&self.pool, chain_id, &target, mode, &token, &options)
                .await;
        self.report_families(chain_id, &outcome);
        if let Some(error) = error {
            return Err(self.family_error(chain_id, &target, &outcome, &error));
        }
        let progress = family_progress(&context, &target, &outcome)?;
        if outcome.lag_blocks() == 0 && outcome.marker.as_ref() == Some(&target) {
            return Ok(PhaseBatchOutcome::Complete(progress));
        }
        if outcome.budget_exhausted && outcome.blocks + outcome.undone_blocks > 0 {
            self.family_continuations
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .insert(
                    chain_id.to_owned(),
                    Continuation {
                        mode: context.mode.clone(),
                        redo_attempt: context.redo_attempt,
                        target,
                    },
                );
            return Ok(PhaseBatchOutcome::Continue(progress));
        }
        let standing = outcome.marker.as_ref().map_or_else(
            || "no block".to_owned(),
            |marker| format!("block {} ({})", marker.number, marker.hash),
        );
        Err(RunnerError::new(
            ErrorKind::Transient,
            format!(
                "owned key families of chain {chain_id} stopped at {standing}, short of Project \
                 target block {} ({}), without spending their block budget",
                target.number, target.hash
            ),
        ))
    }

    /// The target and mode of a batch that continues nothing.
    async fn fresh_target(
        &self,
        chain_id: &str,
        mode: RunMode,
        context: &PhaseContext,
        latest: &BlockMarker,
    ) -> RunnerResult<(BlockMarker, FamilyMode)> {
        Ok(match mode {
            // A batch with no recorded progress starts the families from scratch, as the served
            // batch rebuilt the served tables.
            RunMode::Normal if context.resume.current.is_none() => {
                (latest.clone(), FamilyMode::Rebuild)
            }
            RunMode::Normal => (latest.clone(), FamilyMode::Normal),
            RunMode::Redo(range) | RunMode::RecomputeFlags(range) => (
                self.family_redo_target(chain_id, range.to, latest.number)
                    .await?,
                FamilyMode::Redo {
                    from: range.from,
                    to: range.to,
                },
            ),
        })
    }

    /// Where a redo under the switch replays the families to: the block they stood on before
    /// it, which the Project row acknowledges, or the redo range's end when that is higher, never
    /// above the readable head. The served path reads the same block from the Project row
    /// (`redo_target`); the families' own marker is the publication, so it is read here.
    async fn family_redo_target(
        &self,
        chain_id: &str,
        range_to: i64,
        latest: i64,
    ) -> RunnerResult<BlockMarker> {
        let standing: Option<i64> = sqlx::query_scalar(
            "SELECT current_block_number FROM project_family_marker WHERE chain_id = $1",
        )
        .bind(chain_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|error| {
            RunnerError::database(
                format!("failed to load the family marker for a redo on chain {chain_id}"),
                error,
            )
        })?
        .flatten();
        let number = standing
            .map_or(range_to, |standing| standing.max(range_to))
            .min(latest);
        crate::heads::load_marker(&self.pool, chain_id, number)
            .await?
            .ok_or_else(|| {
                RunnerError::new(
                    ErrorKind::DataIntegrity,
                    format!(
                        "cannot redo project on chain {chain_id}: family redo target {number} is \
                         not readable (canonical, safe, or finalized)"
                    ),
                )
            })
    }
}

/// The batch's progress: the family marker the run committed, the acknowledgment of the
/// publication, toward the batch's target. With no marker yet (a rebuild that has committed
/// nothing) the recorded position stays where it was.
fn family_progress(
    context: &PhaseContext,
    target: &Marker,
    outcome: &FamilyOutcome,
) -> RunnerResult<PhaseProgress> {
    let current = match &outcome.marker {
        Some(marker) => Some(BlockMarker::new(marker.number, marker.hash.clone())?),
        None => context.resume.current.clone(),
    };
    let rows = outcome
        .rows
        .values()
        .copied()
        .fold(0u64, u64::saturating_add);
    Ok(PhaseProgress {
        current,
        target: Some(BlockMarker::new(target.number, target.hash.clone())?),
        estimated_write_bytes: rows.saturating_mul(1_024),
        ..PhaseProgress::default()
    })
}
