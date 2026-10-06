//! The bounded summary-only pass after an actual Universal Resolver selection change.
use crate::{ProjectError, Result, families::input::BlockHeader};
use bigname_storage::families::name::{FamilyPublication, compose_name_summaries};
use sqlx::{Postgres, Transaction, types::time::OffsetDateTime};

pub(crate) async fn refresh(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    block: &BlockHeader,
) -> Result<(u64, u64)> {
    let changed: bool = sqlx::query_scalar(
        "/* project:families.derived.search_cutover_changed */
         SELECT EXISTS (SELECT 1 FROM project_family_undo
          WHERE chain_id=$1 AND block_number=$2 AND family='project_universal_resolver_proxy')",
    )
    .bind(chain_id)
    .bind(block.number)
    .fetch_one(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to read search cutover changes", error))?;
    if !changed {
        return Ok((0, 0));
    }
    let publication = FamilyPublication {
        chain_id: chain_id.to_owned(),
        block_number: block.number,
        block_hash: block.hash.clone(),
        block_timestamp: OffsetDateTime::from_unix_timestamp(block.timestamp_seconds).map_err(
            |error| ProjectError::data_integrity(format!("cutover block time: {error}")),
        )?,
        block_timestamp_json: block.timestamp.clone(),
    };
    let mut after = String::new();
    let mut written = (0, 0);
    loop {
        let names: Vec<String> = sqlx::query_scalar(
            "/* project:families.derived.search_cutover_names */
             SELECT surface.logical_name_id FROM name_surfaces surface
             JOIN chain_lineage lineage ON lineage.chain_id=surface.chain_id
              AND lineage.block_hash=surface.block_hash
             WHERE surface.chain_id=$1 AND surface.block_number <= $2
               AND surface.logical_name_id > $3
               AND surface.namespace='ens' AND cardinality(surface.labelhashes)=2
               AND lower(surface.labelhashes[2])=$4
               AND surface.visibility_state='active' AND surface.raw_name IS DISTINCT FROM ''
               AND surface.canonicality_state IN ('canonical','safe','finalized')
               AND lineage.canonicality_state IN ('canonical','safe','finalized')
             ORDER BY surface.logical_name_id LIMIT 1000",
        )
        .bind(chain_id)
        .bind(block.number)
        .bind(&after)
        .bind(bigname_storage::families::control::lifecycle::ETH_LABELHASH)
        .fetch_all(&mut **transaction)
        .await
        .map_err(|error| ProjectError::database("failed to select search cutover names", error))?;
        let Some(last) = names.last() else {
            break;
        };
        after = last.clone();
        let rows = compose_name_summaries(transaction, &publication, &names)
            .await
            .map_err(|error| {
                ProjectError::data_integrity(format!(
                    "failed to shape cutover search summaries: {error:#}"
                ))
            })?;
        let (rows, undo) =
            super::summary::replace_chunk(transaction, chain_id, block, &names, &rows).await?;
        written.0 += rows;
        written.1 += undo;
    }
    Ok(written)
}
