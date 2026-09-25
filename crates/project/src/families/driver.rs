//! One run of the family loop. It decides whether the families follow the served marker, resume
//! an interrupted repair, undo and replay a redo, or rebuild from scratch, then does it one block
//! per transaction, stopping when the run's block budget is spent. The repair record says which
//! of these is under way, so a run that stops between two blocks is resumed by the next; every
//! transaction fences on the marker generation and on the repair record it planned from.
mod transitions;

use sqlx::PgPool;

use self::transitions::Transition;

use super::{
    FamilyMode, FamilyOptions, FamilyOutcome, block,
    input::{self, InputToken, Revision},
    manifests,
    marker::{self, FamilyMarker},
    reduce,
    repair::{self, NewRepair, Reason, Record, State},
    undo,
};
use crate::{Marker, ProjectError, Result};

/// Blocks applied or undone in one run; the run stops cleanly when it is spent.
struct Budget {
    left: u64,
}

impl Budget {
    /// Spend one block, or say the run must stop.
    fn take(&mut self, outcome: &mut FamilyOutcome) -> bool {
        if self.left == 0 {
            outcome.budget_exhausted = true;
            return false;
        }
        self.left -= 1;
        true
    }
}

struct Run<'a> {
    pool: &'a PgPool,
    chain_id: &'a str,
    target: &'a Marker,
    options: &'a FamilyOptions,
    budget: Budget,
}

pub(super) async fn run(
    pool: &PgPool,
    chain_id: &str,
    target: &Marker,
    mode: &FamilyMode,
    session: &InputToken,
    options: &FamilyOptions,
    outcome: &mut FamilyOutcome,
) -> Result<()> {
    let mut run = Run {
        pool,
        chain_id,
        target,
        options,
        budget: Budget {
            left: options.max_blocks_per_run,
        },
    };
    let token = input::input_token(pool, chain_id).await?;
    let family = marker::read(pool, chain_id).await?;
    let record = repair::read(pool, chain_id).await?;
    let attempt = token.project_redo_attempt_generation;
    let recorded = record.as_ref().map_or(0, |record| record.attempt);
    let active = record.as_ref().filter(|record| record.active());

    // A retried redo whose repair completed at this target, with no block or undo since and
    // under the same binary, changes nothing.
    if matches!(mode, FamilyMode::Redo { .. })
        && record.as_ref().is_some_and(|record| {
            record.state == State::Complete
                && record.attempt == attempt
                && record.completed_marker.as_ref() == Some(target)
                && record.completed_sequence == Some(family.sequence)
                && record.completed_input_hash.as_deref()
                    == Some(options.input_content_hash.as_str())
        })
        && family.current.as_ref() == Some(target)
    {
        return Ok(());
    }

    let rebuilding = active.filter(|record| record.state == State::Rebuilding);
    let rebuild = match mode {
        FamilyMode::Rebuild => Some(Reason::ContentHashRebuild),
        // A redo attempt the families never saw (an earlier one was lost) cannot be undone from
        // the journal, and neither can a redo that lands on an unfinished rebuild.
        FamilyMode::Redo { .. } if attempt > recorded + 1 || rebuilding.is_some() => {
            Some(Reason::of_redo(session))
        }
        FamilyMode::Redo { from, .. } if *from < 1 => Some(Reason::of_redo(session)),
        FamilyMode::Normal if attempt > recorded => Some(Reason::OperatorRedo),
        // Families another binary wrote: a served rebuild whose family run was skipped leaves
        // them, and nothing else would rebuild them.
        _ if family.current.is_some()
            && family.input_content_hash.as_deref()
                != Some(options.input_content_hash.as_str()) =>
        {
            Some(Reason::ContentHashRebuild)
        }
        _ if family.current.is_none() && rebuilding.is_none() => Some(Reason::ContentHashRebuild),
        _ => None,
    };
    if let Some(reason) = rebuild {
        return run
            .rebuild(reason, attempt, &family, record.as_ref(), &token, outcome)
            .await;
    }
    if let Some(rebuilding) = rebuilding {
        return run
            .resume_rebuild(rebuilding, &family, &token, outcome)
            .await;
    }

    match (mode, active) {
        (FamilyMode::Redo { from, .. }, active)
            if active.is_none_or(|record| record.attempt != attempt) =>
        {
            let limit = (from - 1).min(target.number);
            let Some(base) = run.undo_limit(&family, limit).await? else {
                return run
                    .rebuild(
                        Reason::of_redo(session),
                        attempt,
                        &family,
                        record.as_ref(),
                        &token,
                        outcome,
                    )
                    .await;
            };
            // The trusted base is the redo range's predecessor, or the block the undo stops on
            // when the families must go below it.
            let trusted_base = if base.number < from - 1 {
                Some(base.clone())
            } else {
                input::readable_hash(pool, chain_id, from - 1)
                    .await?
                    .map(|hash| Marker {
                        number: from - 1,
                        hash,
                    })
            };
            let repair = NewRepair {
                attempt,
                reason: Reason::of_redo(session),
                trusted_base: trusted_base.as_ref(),
                replay_target: target,
                pending_undo_target: base.number,
            };
            run.begin_undo(&family, record.as_ref(), &repair).await?;
            run.undo_then_replay(attempt, base.number, &token, outcome)
                .await
        }
        (mode, Some(active)) => {
            let floor = match mode {
                FamilyMode::Redo { from, .. } => Some(from - 1),
                _ => None,
            };
            run.resume(active, &family, floor, &token, outcome).await
        }
        (_, None) => {
            if run.readable(family.current.as_ref()).await? {
                return run.follow(family, &token, outcome).await;
            }
            // The families stand on a block that left the readable lineage or above the served
            // target: undo to the highest readable block at or below it, then replay.
            let current = family.current.clone().expect("no rebuild, so a marker");
            let Some(base) = run
                .undo_limit(&family, target.number.min(current.number - 1))
                .await?
            else {
                return run
                    .rebuild(
                        Reason::OrphanedLineage,
                        attempt,
                        &family,
                        record.as_ref(),
                        &token,
                        outcome,
                    )
                    .await;
            };
            let repair = NewRepair {
                attempt,
                reason: Reason::OrphanedLineage,
                trusted_base: Some(&base),
                replay_target: target,
                pending_undo_target: base.number,
            };
            run.begin_undo(&family, record.as_ref(), &repair).await?;
            run.undo_then_replay(attempt, base.number, &token, outcome)
                .await
        }
    }
}

impl Run<'_> {
    /// Whether a marker stands on the readable lineage at or below the served target.
    async fn readable(&self, marker: Option<&Marker>) -> Result<bool> {
        let Some(marker) = marker else {
            return Ok(true);
        };
        Ok(marker.number <= self.target.number
            && input::readable_hash(self.pool, self.chain_id, marker.number)
                .await?
                .as_deref()
                == Some(marker.hash.as_str()))
    }

    /// The marker undo must reach so that the families stand at or below `limit` on the
    /// readable lineage; the current marker when it already does. `None` means a rebuild.
    async fn undo_limit(&self, family: &FamilyMarker, limit: i64) -> Result<Option<Marker>> {
        let Some(current) = family.current.as_ref() else {
            return Ok(None);
        };
        if current.number <= limit
            && input::readable_hash(self.pool, self.chain_id, current.number)
                .await?
                .as_deref()
                == Some(current.hash.as_str())
        {
            return Ok(Some(current.clone()));
        }
        undo::undo_target(self.pool, self.chain_id, current, limit).await
    }

    /// Follow the served marker block by block. Every block reads the input token inside its
    /// transaction and requires the revision this run started from. A run that starts under a
    /// revision other than the one the last block recorded, with no Project redo the families
    /// missed (that case rebuilt above), adopts it: in the serial runner an Interpret redo that
    /// reaches a block Project published stamps a Project redo in the transaction that completes
    /// it (redo_state.rs), so such a change covers only blocks Project had not published.
    async fn follow(
        &mut self,
        family: FamilyMarker,
        token: &InputToken,
        outcome: &mut FamilyOutcome,
    ) -> Result<()> {
        let next = family
            .current
            .as_ref()
            .map_or(0, |marker| marker.number + 1);
        let Some(revision) = token.revision() else {
            return Err(block::revision_error(
                self.chain_id,
                next,
                None,
                &(None, None),
            ));
        };
        if let Some(recorded) = family.token.revision()
            && recorded != revision
            && family.current.as_ref() != Some(self.target)
        {
            tracing::info!(
                target: "bigname_project::families",
                chain_id = self.chain_id,
                recorded = ?recorded,
                current = ?revision,
                "the family loop adopts a new input revision with no Project redo to follow"
            );
            outcome.revision_adopted = true;
        }
        self.apply_blocks(family, &revision, None, outcome).await
    }

    /// Apply every block above `family` to the target, contiguously, as followers or as the
    /// replay of repair `attempt`.
    async fn apply_blocks(
        &mut self,
        mut family: FamilyMarker,
        revision: &Revision,
        attempt: Option<i64>,
        outcome: &mut FamilyOutcome,
    ) -> Result<()> {
        let from = family
            .current
            .as_ref()
            .map_or(0, |marker| marker.number + 1);
        if from > self.target.number {
            return Ok(());
        }
        let manifests =
            manifests::History::read(self.pool, self.chain_id, self.target.number).await?;
        for number in from..=self.target.number {
            if !self.budget.take(outcome) {
                return Ok(());
            }
            let role = match attempt {
                None => block::Role::Follow,
                Some(attempt) => block::Role::Replay {
                    attempt,
                    completes: number == self.target.number,
                },
            };
            let plan = block::Plan {
                predecessor: family.current.as_ref(),
                sequence: family.sequence,
                contiguous: true,
                bootstrap: false,
                revision,
                role,
                manifests: &manifests,
            };
            let (next, stats) = block::apply(self.pool, self.chain_id, number, &plan, self.options)
                .await
                .map_err(|error| at_block(number, error))?;
            outcome.record(stats);
            family = next;
        }
        Ok(())
    }

    /// Resume an active undo-then-replay from where the record says it stands.
    async fn resume(
        &mut self,
        record: &Record,
        family: &FamilyMarker,
        floor: Option<i64>,
        token: &InputToken,
        outcome: &mut FamilyOutcome,
    ) -> Result<()> {
        let attempt = record.attempt;
        match record.state {
            State::Undoing => {
                let pending = [record.pending_undo_target, floor, Some(self.target.number)]
                    .into_iter()
                    .flatten()
                    .min()
                    .unwrap_or(self.target.number);
                // The target may have left the readable lineage since the undo began.
                let Some(base) = self.undo_limit(family, pending).await? else {
                    return self
                        .rebuild(
                            Reason::OrphanedLineage,
                            attempt,
                            family,
                            Some(record),
                            token,
                            outcome,
                        )
                        .await;
                };
                if Some(base.number) != record.pending_undo_target {
                    self.transition(family, record, Transition::LowerTarget(base.number))
                        .await?;
                }
                self.undo_then_replay(attempt, base.number, token, outcome)
                    .await
            }
            State::Replaying => {
                let next = family
                    .current
                    .as_ref()
                    .map_or(0, |marker| marker.number + 1);
                let Some(revision) = token.revision() else {
                    return Err(block::revision_error(
                        self.chain_id,
                        next,
                        None,
                        &(None, None),
                    ));
                };
                // A changed input revision invalidates the replayed prefix back to the trusted
                // base; a prefix off the readable lineage or above the target is undone to the
                // highest readable block below; a lower redo floor lowers it further.
                let mut limit = self.target.number;
                if record.prefix_revision.as_ref() != Some(&revision) {
                    limit = limit.min(record.trusted_base.as_ref().map_or(-1, |base| base.number));
                }
                if let Some(floor) = floor {
                    limit = limit.min(floor);
                }
                let current = family.current.as_ref();
                let stands = current.map_or(-1, |marker| marker.number) <= limit
                    && record.prefix_revision.as_ref() == Some(&revision)
                    && self.readable(current).await?;
                if stands {
                    return self
                        .replay(family.clone(), attempt, &revision, outcome)
                        .await;
                }
                let Some(base) = self.undo_limit(family, limit).await? else {
                    return self
                        .rebuild(
                            Reason::OrphanedLineage,
                            attempt,
                            family,
                            Some(record),
                            token,
                            outcome,
                        )
                        .await;
                };
                self.transition(family, record, Transition::Reopen(base.number))
                    .await?;
                self.undo_then_replay(attempt, base.number, token, outcome)
                    .await
            }
            State::Rebuilding | State::Complete => Err(ProjectError::data_integrity(format!(
                "chain {}'s repair record cannot resume from {:?}",
                self.chain_id, record.state
            ))),
        }
    }

    /// Undo block by block to `pending`, the last undo moving the record to replaying, then
    /// replay to the target.
    async fn undo_then_replay(
        &mut self,
        attempt: i64,
        pending: i64,
        token: &InputToken,
        outcome: &mut FamilyOutcome,
    ) -> Result<()> {
        let mut family = marker::read(self.pool, self.chain_id).await?;
        while family
            .current
            .as_ref()
            .is_some_and(|marker| marker.number > pending)
        {
            if !self.budget.take(outcome) {
                return Ok(());
            }
            let at = family.current.as_ref().map_or(0, |marker| marker.number);
            match undo::undo_block(self.pool, self.chain_id, &family, attempt)
                .await
                .map_err(|error| at_block(at, error))?
            {
                Some(restored) => {
                    outcome.undone_blocks += 1;
                    family = restored;
                }
                None => {
                    let record = repair::read(self.pool, self.chain_id).await?;
                    return self
                        .rebuild(
                            Reason::OrphanedLineage,
                            attempt,
                            &family,
                            record.as_ref(),
                            token,
                            outcome,
                        )
                        .await;
                }
            }
        }
        let record = repair::read(self.pool, self.chain_id).await?;
        let revision = match record.as_ref().map(|record| record.state) {
            Some(State::Undoing) => {
                undo::start_replay(self.pool, self.chain_id, &family, attempt).await?
            }
            _ => record
                .and_then(|record| record.prefix_revision)
                .ok_or_else(|| {
                    ProjectError::transient(format!(
                        "chain {}'s repair record lost its prefix revision",
                        self.chain_id
                    ))
                })?,
        };
        self.replay(family, attempt, &revision, outcome).await
    }

    /// Replay to the target; the block that publishes the target completes the record, or a
    /// fenced transition does when the families already stand there.
    async fn replay(
        &mut self,
        family: FamilyMarker,
        attempt: i64,
        revision: &Revision,
        outcome: &mut FamilyOutcome,
    ) -> Result<()> {
        if family.current.as_ref() == Some(self.target) {
            let record = repair::read(self.pool, self.chain_id)
                .await?
                .ok_or_else(|| {
                    ProjectError::transient(format!(
                        "chain {} lost its repair record",
                        self.chain_id
                    ))
                })?;
            return self
                .transition(&family, &record, Transition::Complete)
                .await;
        }
        self.apply_blocks(family, revision, Some(attempt), outcome)
            .await
    }
}

fn at_block(number: i64, error: ProjectError) -> ProjectError {
    reduce::in_family(&format!("block {number}"))(error)
}
