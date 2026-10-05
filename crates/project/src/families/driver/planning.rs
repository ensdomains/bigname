//! The execution envelope of a requested redo, shared with the runner before it begins work.
//! The requested invalidation remains unchanged: undo may reach its journal predecessor and a
//! rebuild visits retained inputs from the beginning, then both replay to the standing target.
use super::*;

pub(super) fn resumes_rebuild(
    record: &Record,
    family: &FamilyMarker,
    token: &InputToken,
    options: &FamilyOptions,
) -> bool {
    options.resumes_interrupted_redo
        && record.attempt + 1 == token.project_redo_attempt_generation
        && token
            .revision()
            .is_some_and(|revision| record.prefix_revision.as_ref() == Some(&revision))
        && family
            .input_content_hash
            .as_deref()
            .is_none_or(|hash| hash == options.input_content_hash)
}

pub(super) fn rebuild_reason(
    mode: &FamilyMode,
    family: &FamilyMarker,
    record: Option<&Record>,
    token: &InputToken,
    session: &InputToken,
    options: &FamilyOptions,
) -> Option<Reason> {
    let recorded = record.map_or(0, |record| record.attempt);
    let rebuilding = record.is_some_and(|record| record.state == State::Rebuilding);
    match mode {
        FamilyMode::Rebuild => Some(Reason::ContentHashRebuild),
        // Required redo unions every pending invalidation. Its generation advances for each
        // stamp as well as each begin; those gaps are not unobserved completed operator work.
        FamilyMode::Redo { .. }
            if rebuilding
                || (token.project_redo_attempt_generation > recorded + 1
                    && Reason::of_redo(session) != Reason::RequiredRedoRange) =>
        {
            Some(Reason::of_redo(session))
        }
        FamilyMode::Redo { from, .. } if *from < 1 => Some(Reason::of_redo(session)),
        FamilyMode::Normal if token.project_redo_attempt_generation > recorded => {
            Some(Reason::OperatorRedo)
        }
        _ if family.current.is_some()
            && family.input_content_hash.as_deref()
                != Some(options.input_content_hash.as_str()) =>
        {
            Some(Reason::ContentHashRebuild)
        }
        _ if family.current.is_none() && !rebuilding => Some(Reason::ContentHashRebuild),
        _ => None,
    }
}

/// Plan the real range traversed by a redo before its runner attempt is committed. The runner
/// holds the Project advisory lock and the chain phase rows, so the input epoch cannot move.
/// `attempt` is the generation that begin will assign; no family or raw fact is changed here.
pub async fn redo_extent(
    pool: &PgPool,
    chain_id: &str,
    target: &Marker,
    from: i64,
    attempt: i64,
    options: &FamilyOptions,
) -> Result<(i64, i64)> {
    let family = marker::read(pool, chain_id).await?;
    let record = repair::read(pool, chain_id).await?;
    let mut token = input::input_token(pool, chain_id).await?;
    token.project_redo_attempt_generation = attempt;
    let mode = FamilyMode::Redo {
        from,
        to: target.number,
    };
    let rebuild =
        rebuild_reason(&mode, &family, record.as_ref(), &token, &token, options).is_some();
    // Both a new and a resumed rebuild can need any retained work block. A changed prefix or
    // orphaned rebuild marker restarts population, so its envelope still starts at zero.
    let lower = if rebuild
        || record
            .as_ref()
            .is_some_and(|record| record.state == State::Rebuilding)
    {
        0
    } else {
        let run = Run {
            pool,
            chain_id,
            target,
            options,
            budget: Budget { left: 0 },
            head: None,
        };
        let base = run
            .undo_limit(&family, (from - 1).min(target.number))
            .await?;
        base.map_or(0, |base| base.number)
    };
    Ok((
        lower.min(from),
        target
            .number
            .max(family.current.map_or(target.number, |marker| marker.number)),
    ))
}
