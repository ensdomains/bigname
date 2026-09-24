//! One run of the family loop. It decides whether the families follow the served marker, finish
//! an interrupted repair, undo and replay a redo, or rebuild from scratch, then does it one block
//! per transaction. The repair record says which of these is under way, so a run that stops
//! between two blocks is resumed by the next.
use sqlx::PgPool;

use super::{
    FamilyMode, FamilyOptions, FamilyOutcome, block,
    input::{self, InputToken},
    marker::{self, FamilyMarker},
    reduce,
    repair::{self, Reason, State},
    tables, undo,
};
use crate::{Marker, ProjectError, Result};

type Revision<'a> = (Option<&'a str>, Option<i64>);

pub(super) async fn run(
    pool: &PgPool,
    chain_id: &str,
    target: &Marker,
    mode: &FamilyMode,
    token: &InputToken,
    options: &FamilyOptions,
    outcome: &mut FamilyOutcome,
) -> Result<()> {
    let revision: Revision<'_> = (None, None);
    let family = marker::read(pool, chain_id).await?;
    let record = repair::read(pool, chain_id).await?;
    let attempt = token.project_redo_attempt_generation;
    let recorded = record.as_ref().map_or(0, |record| record.attempt);
    let open = record
        .as_ref()
        .filter(|record| record.state != State::Complete);

    // A retried redo whose repair already completed at this target changes nothing.
    if matches!(mode, FamilyMode::Redo { .. })
        && record.as_ref().is_some_and(|record| {
            record.state == State::Complete
                && record.attempt == attempt
                && record.completed_marker.as_ref() == Some(target)
        })
        && family.current.as_ref() == Some(target)
    {
        return Ok(());
    }

    let rebuilding = open.is_some_and(|record| record.state == State::Rebuilding);
    let rebuild = match mode {
        FamilyMode::Rebuild => Some(Reason::ContentHashRebuild),
        // A redo range the families never saw (an earlier attempt was lost) cannot be undone
        // from the journal, and neither can a redo that lands on an unfinished rebuild.
        FamilyMode::Redo { .. } if attempt > recorded + 1 || rebuilding => {
            Some(Reason::of_redo(token))
        }
        FamilyMode::Redo { from, .. } if *from < 1 => Some(Reason::of_redo(token)),
        FamilyMode::Normal if attempt > recorded => Some(Reason::OperatorRedo),
        _ if family.current.is_none() && !family.bootstrap => Some(Reason::ContentHashRebuild),
        _ => None,
    };
    if let Some(reason) = rebuild {
        return rebuild_from_scratch(pool, chain_id, target, reason, token, options, outcome).await;
    }

    if family.bootstrap {
        // An interrupted rebuild resumes above its last block while that block is still
        // readable; otherwise it starts again.
        if !readable(pool, chain_id, family.current.as_ref(), target).await? {
            return rebuild_from_scratch(
                pool,
                chain_id,
                target,
                Reason::ContentHashRebuild,
                token,
                options,
                outcome,
            )
            .await;
        }
        populate(
            pool,
            chain_id,
            target,
            family.current.clone(),
            revision,
            options,
            outcome,
        )
        .await?;
        return finish(pool, chain_id, options).await;
    }

    let resumed_undo = open
        .filter(|record| record.state == State::Undoing)
        .and_then(|record| record.pending_undo_target);
    let floor = match mode {
        FamilyMode::Redo { from, .. } => Some(resumed_undo.map_or(from - 1, |p| p.min(from - 1))),
        _ => resumed_undo,
    };
    let aligned = readable(pool, chain_id, family.current.as_ref(), target).await?;
    if floor.is_none() && aligned && open.is_none() {
        return catch_up(
            pool,
            chain_id,
            family.current,
            target,
            revision,
            options,
            outcome,
        )
        .await;
    }

    let Some(current) = family.current.clone() else {
        return rebuild_from_scratch(
            pool,
            chain_id,
            target,
            Reason::OperatorRedo,
            token,
            options,
            outcome,
        )
        .await;
    };
    match (mode, floor) {
        (FamilyMode::Redo { from, .. }, Some(floor)) => {
            let base = input::readable_hash(pool, chain_id, from - 1)
                .await?
                .map(|hash| Marker {
                    number: from - 1,
                    hash,
                });
            repair::begin_undo(
                pool,
                chain_id,
                Reason::of_redo(token),
                token,
                base.as_ref(),
                floor,
                target,
            )
            .await?;
        }
        _ if !aligned && open.is_none() => {
            let floor = (current.number - 1).min(target.number);
            repair::begin_undo(
                pool,
                chain_id,
                Reason::OrphanedLineage,
                token,
                None,
                floor,
                target,
            )
            .await?;
        }
        _ => {}
    }

    // Undo down to the floor and off any block that left the readable lineage or stands above
    // the served target. A block the journal no longer holds means a rebuild.
    let oldest = undo::oldest_journalled(pool, chain_id).await?;
    if let Some(floor) = floor
        && floor < current.number
        && oldest.is_none_or(|oldest| oldest > floor + 1)
    {
        return rebuild_from_scratch(
            pool,
            chain_id,
            target,
            Reason::of_redo(token),
            token,
            options,
            outcome,
        )
        .await;
    }
    let mut current = Some(current);
    while let Some(at) = current.clone() {
        let above_floor = floor.is_some_and(|floor| at.number > floor);
        if !above_floor && readable(pool, chain_id, Some(&at), target).await? {
            break;
        }
        match undo::undo_block(pool, chain_id, &at)
            .await
            .map_err(|error| at_block(at.number, error))?
        {
            Some(restored) => {
                outcome.undone_blocks += 1;
                current = restored.current;
            }
            None => {
                return rebuild_from_scratch(
                    pool,
                    chain_id,
                    target,
                    Reason::OrphanedLineage,
                    token,
                    options,
                    outcome,
                )
                .await;
            }
        }
    }
    let Some(base) = current else {
        return rebuild_from_scratch(
            pool,
            chain_id,
            target,
            Reason::OrphanedLineage,
            token,
            options,
            outcome,
        )
        .await;
    };
    repair::start_replay(pool, chain_id, token, &base).await?;
    catch_up(
        pool,
        chain_id,
        Some(base),
        target,
        revision,
        options,
        outcome,
    )
    .await?;
    finish(pool, chain_id, options).await
}

/// Whether the family marker stands on the readable lineage at or below the served target.
async fn readable(
    pool: &PgPool,
    chain_id: &str,
    marker: Option<&Marker>,
    target: &Marker,
) -> Result<bool> {
    let Some(marker) = marker else {
        return Ok(true);
    };
    Ok(marker.number <= target.number
        && input::readable_hash(pool, chain_id, marker.number)
            .await?
            .as_deref()
            == Some(marker.hash.as_str()))
}

async fn rebuild_from_scratch(
    pool: &PgPool,
    chain_id: &str,
    target: &Marker,
    reason: Reason,
    token: &InputToken,
    options: &FamilyOptions,
    outcome: &mut FamilyOutcome,
) -> Result<()> {
    reset(pool, chain_id).await?;
    outcome.reset = true;
    repair::begin_rebuild(pool, chain_id, reason, token, target).await?;
    populate(pool, chain_id, target, None, (None, None), options, outcome).await?;
    finish(pool, chain_id, options).await
}

async fn finish(pool: &PgPool, chain_id: &str, options: &FamilyOptions) -> Result<()> {
    let marker: FamilyMarker = marker::read(pool, chain_id).await?;
    repair::complete(pool, chain_id, &marker, &options.input_content_hash).await
}
/// Apply every block above the family marker up to the target, one transaction each.
async fn catch_up(
    pool: &PgPool,
    chain_id: &str,
    mut current: Option<Marker>,
    target: &Marker,
    revision: (Option<&str>, Option<i64>),
    options: &FamilyOptions,
    outcome: &mut FamilyOutcome,
) -> Result<()> {
    let from = current.as_ref().map_or(0, |marker| marker.number + 1);
    for number in from..=target.number {
        let plan = block::Plan {
            predecessor: current.as_ref(),
            contiguous: true,
            bootstrap: false,
        };
        let (next, stats) = block::apply(pool, chain_id, number, &plan, revision, options)
            .await
            .map_err(|error| at_block(number, error))?;
        outcome.record(stats);
        current = next.current;
    }
    Ok(())
}

/// Populate the families from `from` (a reset leaves `None`) to the target, visiting only the
/// blocks that carry events and then the target itself, so the marker ends at the target.
async fn populate(
    pool: &PgPool,
    chain_id: &str,
    target: &Marker,
    mut current: Option<Marker>,
    revision: (Option<&str>, Option<i64>),
    options: &FamilyOptions,
    outcome: &mut FamilyOutcome,
) -> Result<()> {
    let from = current.as_ref().map_or(0, |marker| marker.number + 1);
    let mut blocks = input::event_blocks(pool, chain_id, from, target.number).await?;
    if blocks.last() != Some(&target.number) {
        blocks.push(target.number);
    }
    for number in blocks {
        let plan = block::Plan {
            predecessor: current.as_ref(),
            contiguous: false,
            bootstrap: number != target.number,
        };
        let (next, stats) = block::apply(pool, chain_id, number, &plan, revision, options)
            .await
            .map_err(|error| at_block(number, error))?;
        outcome.record(stats);
        current = next.current;
    }
    Ok(())
}

/// Clear every family row, undo row and the marker of the chain.
async fn reset(pool: &PgPool, chain_id: &str) -> Result<()> {
    let mut transaction = pool
        .begin()
        .await
        .map_err(|error| ProjectError::database("failed to begin a family reset", error))?;
    marker::lock(&mut transaction, chain_id).await?;
    for table in tables::JOURNALLED
        .iter()
        .map(|table| table.name)
        .chain(tables::DERIVED)
        .chain(["project_family_undo"])
    {
        sqlx::query(&format!(
            "/* project:families.reset_{table} */ DELETE FROM {table} WHERE chain_id = $1"
        ))
        .bind(chain_id)
        .execute(&mut *transaction)
        .await
        .map_err(|error| ProjectError::database(format!("failed to reset {table}"), error))?;
    }
    let sequence = marker::read_locked_sequence(&mut transaction, chain_id).await?;
    marker::advance(
        &mut transaction,
        chain_id,
        &marker::FamilyMarker {
            sequence: sequence + 1,
            bootstrap: true,
            ..marker::FamilyMarker::default()
        },
    )
    .await?;
    transaction
        .commit()
        .await
        .map_err(|error| ProjectError::database("failed to commit a family reset", error))
}

fn at_block(number: i64, error: ProjectError) -> ProjectError {
    reduce::in_family(&format!("block {number}"))(error)
}
