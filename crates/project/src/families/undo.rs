//! Undo of one family block in its own transaction. The locked marker must be the block the
//! driver planned to undo at the generation it planned from, and the repair record must be
//! undoing under the driver's attempt. Every journalled before-image of the block is restored (a
//! missing image deletes the row the block created), the index rows are derived again, the
//! marker returns to the block's predecessor one sequence up, and the block's journal rows are
//! deleted. The undo that reaches the repair's pending undo target also moves the record to
//! replaying in the same commit, with the input revision it read inside the transaction. Undo
//! never rewrites the repair record's facts and never touches `chain_phase_state`.
use std::collections::BTreeMap;

use serde_json::Value;
use sqlx::PgPool;

use super::{
    derived, input,
    marker::{self, FamilyMarker},
    repair, store, tables,
};
use crate::{ProjectError, Result};

/// Undo the block `expected` stands on under the repair `attempt`. Returns the restored marker,
/// or `None` when the journal no longer holds the block (it was pruned or never written), which
/// leaves everything untouched.
pub(crate) async fn undo_block(
    pool: &PgPool,
    chain_id: &str,
    expected: &FamilyMarker,
    attempt: i64,
) -> Result<Option<FamilyMarker>> {
    let Some(current) = expected.current.as_ref() else {
        return Ok(None);
    };
    let mut transaction = pool
        .begin()
        .await
        .map_err(|error| ProjectError::database("failed to begin a family undo", error))?;
    let locked = marker::lock(&mut transaction, chain_id).await?;
    marker::require(chain_id, &locked, Some(current), expected.sequence)?;
    let record = repair::lock(&mut transaction, chain_id).await?;
    let record = repair::require_state(chain_id, record.as_ref(), attempt, repair::State::Undoing)?;
    let journal: Vec<(String, String, Option<Value>)> = sqlx::query_as(
        "/* project:families.undo.journal */ SELECT family, key, before_image
         FROM project_family_undo
         WHERE chain_id = $1 AND block_number = $2 AND block_hash = $3
         ORDER BY family, key",
    )
    .bind(chain_id)
    .bind(current.number)
    .bind(&current.hash)
    .fetch_all(&mut *transaction)
    .await
    .map_err(|error| ProjectError::database("failed to read the family undo record", error))?;
    let Some(prior) = journal
        .iter()
        .find(|(family, _, _)| family == "marker")
        .map(|(_, _, image)| image.clone().unwrap_or(Value::Null))
    else {
        return Ok(None);
    };

    let mut by_table: BTreeMap<&str, (Vec<Value>, Vec<Value>)> = BTreeMap::new();
    for (family, key, image) in &journal {
        if family == "marker" {
            continue;
        }
        let table = tables::find(family).ok_or_else(|| {
            ProjectError::data_integrity(format!(
                "family undo record of chain {chain_id} names unknown table {family}"
            ))
        })?;
        let (keys, images) = by_table.entry(table.name).or_default();
        keys.push(Value::Object(store::key_object(table, key)?));
        if let Some(image) = image {
            images.push(image.clone());
        }
    }
    // The index rows follow the base rows: take the keys while the block's rows still stand.
    let touched = derived::touched(&mut transaction, chain_id, current.number).await?;
    for (name, (keys, images)) in by_table {
        store::replace(&mut transaction, tables::spec(name), keys, images).await?;
    }
    derived::refresh(&mut transaction, chain_id, &touched).await?;

    let restored = FamilyMarker::from_journal_image(&prior, locked.sequence + 1);
    marker::advance(&mut transaction, chain_id, &restored).await?;
    sqlx::query(
        "/* project:families.undo.forget */ DELETE FROM project_family_undo
         WHERE chain_id = $1 AND block_number = $2",
    )
    .bind(chain_id)
    .bind(current.number)
    .execute(&mut *transaction)
    .await
    .map_err(|error| ProjectError::database("failed to clear the undone block's journal", error))?;

    let reached = restored
        .current
        .as_ref()
        .is_some_and(|marker| Some(marker.number) <= record.pending_undo_target);
    if reached {
        let base = restored.current.as_ref().expect("checked above");
        let token = input::token_in(&mut transaction, chain_id).await?;
        let Some(revision) = token.revision() else {
            return Err(ProjectError::transient(format!(
                "the last undo of chain {chain_id}'s families waits: Interpret is in redo"
            )));
        };
        repair::start_replay(&mut transaction, chain_id, base, &revision).await?;
    }
    transaction
        .commit()
        .await
        .map_err(|error| ProjectError::database("failed to commit a family undo", error))?;
    Ok(Some(restored))
}

/// Move an undoing repair whose marker already stands at or below its pending undo target to
/// replaying, when there was nothing left to undo.
pub(crate) async fn start_replay(
    pool: &PgPool,
    chain_id: &str,
    expected: &FamilyMarker,
    attempt: i64,
) -> Result<input::Revision> {
    let mut transaction = pool
        .begin()
        .await
        .map_err(|error| ProjectError::database("failed to begin a replay start", error))?;
    let locked = marker::lock(&mut transaction, chain_id).await?;
    marker::require(
        chain_id,
        &locked,
        expected.current.as_ref(),
        expected.sequence,
    )?;
    let record = repair::lock(&mut transaction, chain_id).await?;
    let record = repair::require_state(chain_id, record.as_ref(), attempt, repair::State::Undoing)?;
    let base = locked
        .current
        .as_ref()
        .filter(|marker| Some(marker.number) <= record.pending_undo_target)
        .ok_or_else(|| {
            ProjectError::transient(format!(
                "chain {chain_id}'s families are above their pending undo target"
            ))
        })?;
    let token = input::token_in(&mut transaction, chain_id).await?;
    let revision = token.revision().ok_or_else(|| {
        ProjectError::transient(format!(
            "chain {chain_id}'s family replay waits: Interpret is in redo"
        ))
    })?;
    repair::start_replay(&mut transaction, chain_id, base, &revision).await?;
    transaction
        .commit()
        .await
        .map_err(|error| ProjectError::database("failed to commit a replay start", error))?;
    Ok(revision)
}

/// Where undo can take the families: from `current` down the chain of journalled prior markers
/// to the highest marker at or below `limit` that stands on the readable lineage. `None` when the
/// journal runs out first (pruned, or the families began there), which means a rebuild.
pub(crate) async fn undo_target(
    pool: &PgPool,
    chain_id: &str,
    current: &crate::Marker,
    limit: i64,
) -> Result<Option<crate::Marker>> {
    type Step = (i64, String, Option<i64>, Option<String>, bool, bool);
    // Walk the journalled prior markers down from `current` in SQL, stopping below the first
    // readable marker at or under `limit`, so a shallow undo reads a few rows however much
    // journal a chain without finality heads has kept.
    let steps: Vec<Step> = sqlx::query_as(
        "/* project:families.undo.path */ WITH RECURSIVE step AS (
             SELECT journal.block_number, journal.block_hash,
                    (journal.before_image ->> 'current_block_number')::bigint AS prior_number,
                    journal.before_image ->> 'current_block_hash' AS prior_hash
             FROM project_family_undo journal
             WHERE journal.chain_id = $1 AND journal.family = 'marker'
               AND journal.block_number = $2 AND journal.block_hash = $3
             UNION
             SELECT journal.block_number, journal.block_hash,
                    (journal.before_image ->> 'current_block_number')::bigint,
                    journal.before_image ->> 'current_block_hash'
             FROM step
             JOIN project_family_undo journal
               ON journal.chain_id = $1 AND journal.family = 'marker'
              AND journal.block_number = step.prior_number
              AND journal.block_hash = step.prior_hash
             WHERE NOT (step.block_number <= $4 AND EXISTS (
                 SELECT 1 FROM chain_lineage lineage
                 WHERE lineage.chain_id = $1
                   AND lineage.block_number = step.block_number
                   AND lineage.block_hash = step.block_hash
                   AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')))
         )
         SELECT step.block_number, step.block_hash, step.prior_number, step.prior_hash,
                EXISTS (
                    SELECT 1 FROM chain_lineage lineage
                    WHERE lineage.chain_id = $1
                      AND lineage.block_number = step.block_number
                      AND lineage.block_hash = step.block_hash
                      AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
                ),
                EXISTS (
                    SELECT 1 FROM chain_lineage lineage
                    WHERE lineage.chain_id = $1
                      AND lineage.block_number = step.prior_number
                      AND lineage.block_hash = step.prior_hash
                      AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
                )
         FROM step",
    )
    .bind(chain_id)
    .bind(current.number)
    .bind(&current.hash)
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(|error| ProjectError::database("failed to read the family undo path", error))?;
    let mut path = BTreeMap::new();
    let mut readable = BTreeMap::new();
    for (number, hash, prior_number, prior_hash, block_readable, prior_readable) in steps {
        let prior = prior_number
            .zip(prior_hash)
            .map(|(number, hash)| crate::Marker { number, hash });
        if let Some(prior) = &prior {
            readable.insert((prior.number, prior.hash.clone()), prior_readable);
        }
        readable.insert((number, hash.clone()), block_readable);
        path.insert((number, hash), prior);
    }
    // A well-formed journal only steps down, but a malformed one can name a marker already
    // visited; refuse it rather than follow it forever.
    let mut visited = std::collections::BTreeSet::new();
    let mut at = current.clone();
    loop {
        let key = (at.number, at.hash.clone());
        if at.number <= limit && readable.get(&key).copied().unwrap_or(false) {
            return Ok(Some(at));
        }
        if !visited.insert(key.clone()) {
            return Err(ProjectError::data_integrity(format!(
                "the family undo journal of chain {chain_id} has a cycle at block {} ({})",
                at.number, at.hash
            )));
        }
        match path.get(&key) {
            Some(Some(prior)) => at = prior.clone(),
            _ => return Ok(None),
        }
    }
}
