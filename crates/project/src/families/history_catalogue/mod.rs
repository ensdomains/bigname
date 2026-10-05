//! Compact Project-owned address history catalogue. Facts and pruning columns belong to
//! their chain and use its ordinary family transaction and before-image journal.
mod edges;
mod envelopes;
mod membership;
mod prepare;
mod sources;
mod write;

use bigname_storage::{
    families::records::CurrentHistoryRelation, history_catalogue_contract::CATALOGUE_VERSION,
};
use serde_json::{Value, json};
use sqlx::{Postgres, Transaction};
use std::collections::BTreeSet;

use super::{
    block::BlockStats,
    input::BlockHeader,
    marker::FamilyMarker,
    tables::{HISTORY_ANCHOR, HISTORY_EDGE, HISTORY_MARKER, HISTORY_SOURCE},
};
use crate::{ProjectError, Result};

pub(crate) async fn refresh(
    transaction: &mut Transaction<'_, Postgres>,
    chain: &str,
    block: &BlockHeader,
    after: i64,
    names: &[String],
    current: &[CurrentHistoryRelation],
    stats: &mut BlockStats,
) -> Result<()> {
    let work = prepare::apply(transaction, chain, block.number, after, names).await?;
    let mut sources = sources::incoming(
        transaction,
        chain,
        after,
        block.number,
        &work.names,
        &work.resources,
    )
    .await?;
    let edges = edges::discover(transaction, chain, block.number, &work.edge_resources).await?;
    sources.extend(edges::source_keys(&edges));
    let changed = sources::refresh(transaction, chain, block, after, &sources).await?;
    stats.rows.insert(HISTORY_SOURCE.name, changed.rows);
    stats.undo_rows += changed.undo_rows;
    let (edge_resources, rows, undo) = edges::refresh(
        transaction,
        chain,
        block,
        edges,
        &work.edge_resources,
        &changed.keys,
    )
    .await?;
    stats.rows.insert(HISTORY_EDGE.name, rows);
    stats.undo_rows += undo;
    let (rows, undo) =
        membership::refresh(transaction, chain, block, &work, names, current).await?;
    stats.rows.insert(HISTORY_ANCHOR.name, rows);
    stats.undo_rows += undo;
    let resources: Vec<_> = work
        .resources
        .into_iter()
        .chain(edge_resources)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let (rows, undo) =
        envelopes::refresh(transaction, chain, block, &work.names, &resources).await?;
    *stats.rows.entry(HISTORY_ANCHOR.name).or_default() += rows;
    stats.undo_rows += undo;
    Ok(())
}

pub(crate) async fn stamp(
    transaction: &mut Transaction<'_, Postgres>,
    chain: &str,
    block: &BlockHeader,
    marker: &FamilyMarker,
    stats: &mut BlockStats,
) -> Result<()> {
    let before: Vec<Value> = sqlx::query_scalar(
        "/* project:history.stamp_before */ SELECT to_jsonb(marker)
         FROM project_history_catalogue_marker marker WHERE chain_id=$1",
    )
    .bind(chain)
    .fetch_all(&mut **transaction)
    .await
    .map_err(|e| ProjectError::database("failed to read history completeness stamp", e))?;
    let fresh = json!({"chain_id":chain,"block_number":block.number,"block_hash":block.hash,
        "publication_sequence":marker.sequence,"input_content_hash":marker.input_content_hash,
        "catalogue_version":CATALOGUE_VERSION});
    let (rows, undo) = write::replace(
        transaction,
        chain,
        block,
        &HISTORY_MARKER,
        before,
        vec![fresh],
    )
    .await?;
    stats.rows.insert(HISTORY_MARKER.name, rows);
    stats.undo_rows += undo;
    Ok(())
}

pub(crate) async fn restored(
    transaction: &mut Transaction<'_, Postgres>,
    chain: &str,
    marker: &FamilyMarker,
) -> Result<()> {
    // Undo advances the sequence while restoring the old facts. Re-stamp only an already
    // restored completeness row: a missing prior catalogue must never be manufactured.
    sqlx::query(
        "/* project:history.restored_stamp */ UPDATE project_history_catalogue_marker
        SET publication_sequence=$2 WHERE chain_id=$1",
    )
    .bind(chain)
    .bind(marker.sequence)
    .execute(&mut **transaction)
    .await
    .map_err(|e| ProjectError::database("failed to re-stamp restored history catalogue", e))?;
    Ok(())
}
