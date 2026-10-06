//! Transaction-local affected names for ENSv2 resolution paths. Shared by the summary writer
//! and resolver membership publication; selection happens before due summary clocks advance.
//! Ordinary record values and links never expand descendants.
mod selection;

use super::input::BlockHeader;
use crate::{ProjectError, Result};
use bigname_storage::families::name::{FamilyPublication, compose_name_resolution_summaries};
use sqlx::{Postgres, Transaction, types::time::OffsetDateTime};

pub(super) async fn prepare(
    transaction: &mut Transaction<'_, Postgres>,
    chain: &str,
    block: &BlockHeader,
    after: i64,
) -> Result<()> {
    sqlx::query(
        "/* project:families.resolution_paths.work_table */ CREATE TEMP TABLE IF NOT EXISTS bigname_resolution_path_work (
        logical_name_id text PRIMARY KEY) ON COMMIT DROP",
    )
    .execute(&mut **transaction)
    .await
    .map_err(|e| ProjectError::database("failed to prepare resolution path work", e))?;
    // Repeated preparation in one publication preserves work already captured before clocks
    // were consumed. The table is discarded with the publication transaction.
    let statement = selection::statement(super::derived::SUMMARY_WORK_LIST);
    sqlx::query(&statement)
        .bind(chain)
        .bind(block.number)
        .bind(block.timestamp_seconds)
        .bind(after)
        .execute(&mut **transaction)
        .await
        .map_err(|e| ProjectError::database("failed to select affected resolution paths", e))?;
    Ok(())
}

pub(super) async fn refresh(
    transaction: &mut Transaction<'_, Postgres>,
    chain: &str,
    block: &BlockHeader,
) -> Result<(u64, u64)> {
    let publication = FamilyPublication {
        chain_id: chain.into(),
        block_number: block.number,
        block_hash: block.hash.clone(),
        block_timestamp: OffsetDateTime::from_unix_timestamp(block.timestamp_seconds).map_err(
            |e| ProjectError::data_integrity(format!("resolution publication time: {e}")),
        )?,
        block_timestamp_json: block.timestamp.clone(),
    };
    let mut after = String::new();
    let mut written = (0, 0);
    loop {
        let names: Vec<String> = sqlx::query_scalar(
            "/* project:families.resolution_paths.page */
            SELECT logical_name_id FROM pg_temp.bigname_resolution_path_work
            WHERE logical_name_id > $1 ORDER BY logical_name_id LIMIT 1000",
        )
        .bind(&after)
        .fetch_all(&mut **transaction)
        .await
        .map_err(|e| ProjectError::database("failed to page resolution path work", e))?;
        let Some(last) = names.last() else {
            break;
        };
        after = last.clone();
        let fresh = compose_name_resolution_summaries(transaction, &publication, &names)
            .await
            .map_err(|e| {
                ProjectError::data_integrity(format!(
                    "failed to compose resolution path summaries: {e:#}"
                ))
            })?;
        super::derived::retire_null_resolver_divergences(
            transaction,
            chain,
            &fresh.null_resolver_names,
        )
        .await?;
        let (rows, undo) =
            super::derived::replace_summary_chunk(transaction, chain, block, &names, &fresh.rows)
                .await?;
        written.0 += rows;
        written.1 += undo;
    }
    Ok(written)
}
