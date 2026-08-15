use bigname_adapters::schema_v2::BatchOutput;
use sqlx::{Postgres, QueryBuilder, Transaction, types::Uuid};
use std::collections::HashSet;

use crate::{InterpretError, Result};

use super::super::batching::{batch_row_context, conflict_free_batches};

pub(super) async fn write(
    transaction: &mut Transaction<'_, Postgres>,
    output: &BatchOutput,
) -> Result<()> {
    write_token_lineages(transaction, output).await?;
    write_resources(transaction, output).await
}

async fn write_token_lineages(
    transaction: &mut Transaction<'_, Postgres>,
    output: &BatchOutput,
) -> Result<()> {
    for (start, batch) in
        conflict_free_batches(&output.token_lineages, |lineage| lineage.token_lineage_id)
    {
        let mut query = QueryBuilder::<Postgres>::new(
            "
            INSERT INTO token_lineages (
                token_lineage_id, chain_id, block_hash, block_number,
                provenance, canonicality_state
            )
            ",
        );
        query.push_values(batch, |mut row, lineage| {
            row.push_bind(lineage.token_lineage_id)
                .push_bind(&lineage.chain_id)
                .push_bind(&lineage.block_hash)
                .push_bind(lineage.block_number)
                .push_bind(&lineage.provenance)
                .push_bind(&lineage.canonicality_state)
                .push_unseparated("::canonicality_state");
        });
        query.push(
            "
            ON CONFLICT (token_lineage_id) DO UPDATE
            SET block_hash = CASE
                    WHEN token_lineages.canonicality_state = 'orphaned'
                        THEN EXCLUDED.block_hash
                    ELSE token_lineages.block_hash
                END,
                block_number = CASE
                    WHEN token_lineages.canonicality_state = 'orphaned'
                        THEN EXCLUDED.block_number
                    ELSE token_lineages.block_number
                END,
                provenance = CASE
                    WHEN token_lineages.canonicality_state = 'orphaned'
                        THEN EXCLUDED.provenance
                    ELSE token_lineages.provenance
                END,
                canonicality_state = CASE
                    WHEN token_lineages.canonicality_state = 'orphaned'
                      OR (
                          EXCLUDED.block_number = token_lineages.block_number
                          AND EXCLUDED.block_hash = token_lineages.block_hash
                      )
                        THEN EXCLUDED.canonicality_state
                    ELSE token_lineages.canonicality_state
                END,
                observed_at = CASE
                    WHEN token_lineages.canonicality_state = 'orphaned'
                        THEN now()
                    ELSE token_lineages.observed_at
                END
            WHERE token_lineages.chain_id = EXCLUDED.chain_id
              AND (
                  token_lineages.canonicality_state = 'orphaned'
                  OR (
                      token_lineages.block_hash = EXCLUDED.block_hash
                      AND token_lineages.block_number = EXCLUDED.block_number
                      AND token_lineages.provenance = EXCLUDED.provenance
                  )
              )
            RETURNING token_lineage_id
            ",
        );
        let written = query
            .build_query_scalar::<Uuid>()
            .fetch_all(&mut **transaction)
            .await
            .map_err(|error| {
                let context =
                    batch_row_context(start, batch.iter().map(|lineage| lineage.token_lineage_id));
                InterpretError::database(
                    format!("failed to write token-lineage batch; {context}"),
                    error,
                )
            })?
            .into_iter()
            .collect::<HashSet<_>>();
        let conflicting = batch
            .iter()
            .enumerate()
            .filter(|(_, lineage)| !written.contains(&lineage.token_lineage_id))
            .map(|(offset, lineage)| format!("{}={}", start + offset, lineage.token_lineage_id))
            .collect::<Vec<_>>();
        if !conflicting.is_empty() {
            return Err(InterpretError::data_integrity(format!(
                "token lineages are already bound to a different chain or different lineage data; conflicting batch rows [{}]",
                conflicting.join(", ")
            )));
        }
    }
    Ok(())
}

async fn write_resources(
    transaction: &mut Transaction<'_, Postgres>,
    output: &BatchOutput,
) -> Result<()> {
    for (start, batch) in conflict_free_batches(&output.resources, |resource| resource.resource_id)
    {
        let mut query = QueryBuilder::<Postgres>::new(
            "
            INSERT INTO resources (
                resource_id, token_lineage_id, chain_id, block_hash,
                block_number, provenance, canonicality_state
            )
            ",
        );
        query.push_values(batch, |mut row, resource| {
            row.push_bind(resource.resource_id)
                .push_bind(resource.token_lineage_id)
                .push_bind(&resource.chain_id)
                .push_bind(&resource.block_hash)
                .push_bind(resource.block_number)
                .push_bind(&resource.provenance)
                .push_bind(&resource.canonicality_state)
                .push_unseparated("::canonicality_state");
        });
        query.push(
            "
            ON CONFLICT (resource_id) DO UPDATE
            SET block_hash = CASE
                    WHEN resources.canonicality_state = 'orphaned'
                        THEN EXCLUDED.block_hash
                    ELSE resources.block_hash
                END,
                block_number = CASE
                    WHEN resources.canonicality_state = 'orphaned'
                        THEN EXCLUDED.block_number
                    ELSE resources.block_number
                END,
                provenance = CASE
                    WHEN resources.canonicality_state = 'orphaned'
                        THEN EXCLUDED.provenance
                    ELSE resources.provenance
                END,
                canonicality_state = CASE
                    WHEN resources.canonicality_state = 'orphaned'
                      OR (
                          EXCLUDED.block_number = resources.block_number
                          AND EXCLUDED.block_hash = resources.block_hash
                      )
                        THEN EXCLUDED.canonicality_state
                    ELSE resources.canonicality_state
                END,
                observed_at = CASE
                    WHEN resources.canonicality_state = 'orphaned'
                        THEN now()
                    ELSE resources.observed_at
                END
            WHERE resources.chain_id = EXCLUDED.chain_id
              AND resources.token_lineage_id IS NOT DISTINCT FROM EXCLUDED.token_lineage_id
            RETURNING resource_id
            ",
        );
        let written = query
            .build_query_scalar::<Uuid>()
            .fetch_all(&mut **transaction)
            .await
            .map_err(|error| {
                let context =
                    batch_row_context(start, batch.iter().map(|resource| resource.resource_id));
                InterpretError::database(
                    format!("failed to write resource batch; {context}"),
                    error,
                )
            })?
            .into_iter()
            .collect::<HashSet<_>>();
        let conflicting = batch
            .iter()
            .enumerate()
            .filter(|(_, resource)| !written.contains(&resource.resource_id))
            .map(|(offset, resource)| format!("{}={}", start + offset, resource.resource_id))
            .collect::<Vec<_>>();
        if !conflicting.is_empty() {
            return Err(InterpretError::data_integrity(format!(
                "resources are already bound to different lineage data; conflicting batch rows [{}]",
                conflicting.join(", ")
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
