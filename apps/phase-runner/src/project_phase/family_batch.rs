//! Project publishes owned key families in bounded runs. Each run reports its committed marker
//! before the runner continues, so heartbeats and cancellation remain observable during rebuild.
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

/// The chain whose follow blocks hydrate reverse names and text records from RPC.
const HYDRATED_CHAIN: &str = "ethereum-mainnet";

/// A family batch that answered `Continue`. The next batch of the same run and redo attempt
/// resumes it in normal mode:
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
        // Follow blocks on this chain hydrate from RPC. A configured runner without its URL is
        // stopped before any publication, so a rebuild cannot publish values that the first
        // follow block would then fail to refresh. The one-shot redo, which only undoes and
        // replays, runs without it (`FamilySettings::require_hydration_url`).
        if let Some(rpc_urls) = &self.hydration_rpc_urls
            && self.families.require_hydration_url
            && chain_id == HYDRATED_CHAIN
            && rpc_urls.url_for(chain_id).is_none()
        {
            return Err(RunnerError::new(
                ErrorKind::Configuration,
                format!("canonical-head hydration requires an RPC URL for {chain_id}"),
            ));
        }
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
            (Some(continuation), RunMode::Redo(_)) => (
                BlockMarker::new(continuation.target.number, continuation.target.hash.clone())?,
                FamilyMode::Normal,
            ),
            (Some(_), RunMode::Normal) => (available.latest.clone(), FamilyMode::Normal),
            (_, mode) => {
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
            .with_rebuild_ranges(self.families.rebuild_ranges)
            .with_resumed_redo(
                context
                    .redo_attempt
                    .is_some_and(|attempt| attempt.resumes_interrupted),
            );
        let options = match &self.hydration_rpc_urls {
            Some(rpc_urls) => options.with_hydration(rpc_urls.clone()),
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
            // A batch with no recorded progress rebuilds from retained events.
            RunMode::Normal if context.resume.current.is_none() => {
                (latest.clone(), FamilyMode::Rebuild)
            }
            RunMode::Normal => (latest.clone(), FamilyMode::Normal),
            RunMode::RecomputeFlags(_) => {
                return Err(RunnerError::new(
                    ErrorKind::InvalidTransition,
                    "normalization flag recomputation runs in Interpret",
                ));
            }
            RunMode::Redo(range) => (
                self.family_redo_target(chain_id, range.to, latest.number)
                    .await?,
                FamilyMode::Redo {
                    from: range.from,
                    to: range.to,
                },
            ),
        })
    }

    /// Replay to the prior publication or the redo range end, whichever is higher, bounded by
    /// the readable head. The family marker remains the publication authority. An unfinished
    /// repair has already moved the marker below the publication it started from, so its replay
    /// target counts as that publication: a redo rerun after a stop still reaches it.
    async fn family_redo_target(
        &self,
        chain_id: &str,
        range_to: i64,
        latest: i64,
    ) -> RunnerResult<BlockMarker> {
        let standing: Option<i64> = sqlx::query_scalar(
            "SELECT GREATEST(
                 (SELECT current_block_number FROM project_family_marker WHERE chain_id = $1),
                 (SELECT replay_target_number FROM project_repair_record
                  WHERE chain_id = $1 AND state <> 'complete'))",
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
