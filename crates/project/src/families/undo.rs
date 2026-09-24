//! Undo of one family block in its own transaction: the locked marker must be the block, every
//! journalled before-image of the block is restored (a missing image deletes the row the block
//! created), the marker returns to the block's predecessor, one sequence up, and the block's
//! journal rows are deleted. Undo never touches the repair record or `chain_phase_state`.
use std::collections::BTreeMap;

use serde_json::Value;
use sqlx::PgPool;

use super::{
    marker::{self, FamilyMarker},
    store, tables,
};
use crate::{Marker, ProjectError, Result};

/// Undo the block the marker stands on. Returns the restored marker, or `None` when the journal
/// no longer holds the block (it was pruned or never written), which leaves everything untouched.
pub(crate) async fn undo_block(
    pool: &PgPool,
    chain_id: &str,
    expected: &Marker,
) -> Result<Option<FamilyMarker>> {
    let mut transaction = pool
        .begin()
        .await
        .map_err(|error| ProjectError::database("failed to begin a family undo", error))?;
    let locked = marker::lock(&mut transaction, chain_id).await?;
    if locked.current.as_ref() != Some(expected) {
        return Err(ProjectError::transient(format!(
            "family marker for chain {chain_id} is {:?}, expected {expected:?} to undo",
            locked.current
        )));
    }
    let journal: Vec<(String, String, Option<Value>)> = sqlx::query_as(
        "/* project:families.undo.journal */ SELECT family, key, before_image
         FROM project_family_undo
         WHERE chain_id = $1 AND block_number = $2 AND block_hash = $3
         ORDER BY family, key",
    )
    .bind(chain_id)
    .bind(expected.number)
    .bind(&expected.hash)
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
    for (name, (keys, images)) in by_table {
        store::replace(&mut transaction, tables::spec(name), keys, images).await?;
    }

    let restored = FamilyMarker::from_journal_image(&prior, locked.sequence + 1);
    marker::advance(&mut transaction, chain_id, &restored).await?;
    sqlx::query(
        "/* project:families.undo.forget */ DELETE FROM project_family_undo
         WHERE chain_id = $1 AND block_number = $2",
    )
    .bind(chain_id)
    .bind(expected.number)
    .execute(&mut *transaction)
    .await
    .map_err(|error| ProjectError::database("failed to clear the undone block's journal", error))?;
    transaction
        .commit()
        .await
        .map_err(|error| ProjectError::database("failed to commit a family undo", error))?;
    Ok(Some(restored))
}

/// The lowest block the journal can still undo, if any.
pub(crate) async fn oldest_journalled(pool: &PgPool, chain_id: &str) -> Result<Option<i64>> {
    sqlx::query_scalar(
        "/* project:families.undo.oldest */ SELECT min(block_number)
         FROM project_family_undo WHERE chain_id = $1 AND family = 'marker'",
    )
    .bind(chain_id)
    .fetch_one(pool)
    .await
    .map_err(|error| ProjectError::database("failed to read the family undo depth", error))
}
