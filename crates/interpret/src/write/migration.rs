use bigname_adapters::schema_v2::{BatchOutput, MigrationCandidateEffect};
use sqlx::{Postgres, QueryBuilder, Transaction};
use std::collections::HashSet;

use crate::{InterpretError, Result};

use super::batching::{batch_row_context, conflict_free_batches};

pub(super) async fn write(
    transaction: &mut Transaction<'_, Postgres>,
    output: &BatchOutput,
) -> Result<()> {
    if output.migration_event_associations.is_empty()
        && output.migration_discovery_associations.is_empty()
        && output.migration_candidate_identity_effects.is_empty()
        && output.migration_candidate_discovery_effects.is_empty()
    {
        return Ok(());
    }
    let content_hash: Option<String> =
        sqlx::query_scalar("SELECT current_setting('bigname.interpreter_content_hash', true)")
            .fetch_one(&mut **transaction)
            .await
            .map_err(|error| {
                InterpretError::database("failed to read the interpreter content hash", error)
            })?;
    let content_hash = content_hash
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            InterpretError::configuration(
                "migration correlation writes require bigname.interpreter_content_hash",
            )
        })?;

    for (start, batch) in
        conflict_free_batches(&output.migration_event_associations, |association| {
            (
                association.event_identity.clone(),
                association.migration_correlation_id.clone(),
            )
        })
    {
        let mut query = QueryBuilder::<Postgres>::new(
            "INSERT INTO migration_event_associations (
                 event_identity, migration_correlation_id, correlation_kind, evidence_refs,
                 chain_id, block_number, block_hash, transaction_hash, transaction_index,
                 log_index, canonicality_state, consumer_visibility, interpreter_content_hash
             ) ",
        );
        query.push_values(batch, |mut row, association| {
            row.push_bind(&association.event_identity)
                .push_bind(&association.migration_correlation_id)
                .push_bind(&association.correlation_kind)
                .push_bind(&association.evidence_refs)
                .push_bind(&association.chain_id)
                .push_bind(association.block_number)
                .push_bind(&association.block_hash)
                .push_bind(&association.transaction_hash)
                .push_bind(association.transaction_index)
                .push_bind(association.log_index)
                .push_bind(&association.canonicality_state)
                .push_unseparated("::canonicality_state")
                .push_bind(&association.consumer_visibility)
                .push_bind(&content_hash);
        });
        query.push(
            "
             ON CONFLICT (event_identity, migration_correlation_id) DO UPDATE
             SET canonicality_state = EXCLUDED.canonicality_state,
                 consumer_visibility = EXCLUDED.consumer_visibility,
                 interpreter_content_hash = EXCLUDED.interpreter_content_hash,
                 observed_at = now()
             WHERE ROW(
                 migration_event_associations.correlation_kind,
                 migration_event_associations.evidence_refs,
                 migration_event_associations.chain_id,
                 migration_event_associations.block_number,
                 migration_event_associations.block_hash,
                 migration_event_associations.transaction_hash,
                 migration_event_associations.transaction_index,
                 migration_event_associations.log_index
             ) IS NOT DISTINCT FROM ROW(
                 EXCLUDED.correlation_kind, EXCLUDED.evidence_refs, EXCLUDED.chain_id,
                 EXCLUDED.block_number, EXCLUDED.block_hash, EXCLUDED.transaction_hash,
                 EXCLUDED.transaction_index, EXCLUDED.log_index
             )
             RETURNING event_identity, migration_correlation_id",
        );
        let written = query
            .build_query_as::<(String, String)>()
            .fetch_all(&mut **transaction)
            .await
            .map_err(|error| {
                let identities = batch.iter().map(|association| {
                    format!(
                        "({}, {})",
                        association.event_identity, association.migration_correlation_id
                    )
                });
                let context = batch_row_context(start, identities);
                InterpretError::database(
                    format!("failed to write migration-event-association batch; {context}"),
                    error,
                )
            })?
            .into_iter()
            .collect::<HashSet<_>>();
        let conflicting = batch
            .iter()
            .enumerate()
            .filter(|(_, association)| {
                !written.contains(&(
                    association.event_identity.clone(),
                    association.migration_correlation_id.clone(),
                ))
            })
            .map(|(offset, association)| {
                format!(
                    "{}=({}, {})",
                    start + offset,
                    association.event_identity,
                    association.migration_correlation_id
                )
            })
            .collect::<Vec<_>>();
        if !conflicting.is_empty() {
            return Err(InterpretError::data_integrity(format!(
                "migration event associations are already bound to different evidence; conflicting batch rows [{}]",
                conflicting.join(", ")
            )));
        }
    }

    for (start, batch) in
        conflict_free_batches(&output.migration_discovery_associations, |association| {
            (
                association.logical_edge_identity.clone(),
                association.migration_correlation_id.clone(),
            )
        })
    {
        let mut query = QueryBuilder::<Postgres>::new(
            "INSERT INTO migration_discovery_associations (
                 logical_edge_identity, migration_correlation_id, correlation_kind,
                 registry_contract_instance_id, registry_address, source_manifest_id,
                 evidence_refs, chain_id, block_number, block_hash, transaction_hash,
                 transaction_index, log_index, canonicality_state, consumer_visibility,
                 interpreter_content_hash
             ) ",
        );
        query.push_values(batch, |mut row, association| {
            row.push_bind(&association.logical_edge_identity)
                .push_bind(&association.migration_correlation_id)
                .push("'migration_registry_creation'")
                .push_bind(association.registry_contract_instance_id)
                .push("lower(")
                .push_bind_unseparated(&association.registry_address)
                .push_unseparated(")")
                .push_bind(association.source_manifest_id)
                .push_bind(&association.evidence_refs)
                .push_bind(&association.chain_id)
                .push_bind(association.block_number)
                .push_bind(&association.block_hash)
                .push_bind(&association.transaction_hash)
                .push_bind(association.transaction_index)
                .push_bind(association.log_index)
                .push_bind(&association.canonicality_state)
                .push_unseparated("::canonicality_state")
                .push_bind(&association.consumer_visibility)
                .push_bind(&content_hash);
        });
        query.push(
            "
             ON CONFLICT (logical_edge_identity, migration_correlation_id) DO UPDATE
             SET canonicality_state = EXCLUDED.canonicality_state,
                 consumer_visibility = EXCLUDED.consumer_visibility,
                 interpreter_content_hash = EXCLUDED.interpreter_content_hash,
                 observed_at = now()
             WHERE ROW(
                 migration_discovery_associations.registry_contract_instance_id,
                 migration_discovery_associations.registry_address,
                 migration_discovery_associations.source_manifest_id,
                 migration_discovery_associations.evidence_refs,
                 migration_discovery_associations.chain_id,
                 migration_discovery_associations.block_number,
                 migration_discovery_associations.block_hash,
                 migration_discovery_associations.transaction_hash,
                 migration_discovery_associations.transaction_index,
                 migration_discovery_associations.log_index
             ) IS NOT DISTINCT FROM ROW(
                 EXCLUDED.registry_contract_instance_id, EXCLUDED.registry_address,
                 EXCLUDED.source_manifest_id, EXCLUDED.evidence_refs, EXCLUDED.chain_id,
                 EXCLUDED.block_number, EXCLUDED.block_hash, EXCLUDED.transaction_hash,
                 EXCLUDED.transaction_index, EXCLUDED.log_index
             )
             RETURNING logical_edge_identity, migration_correlation_id",
        );
        let written = query
            .build_query_as::<(String, String)>()
            .fetch_all(&mut **transaction)
            .await
            .map_err(|error| {
                let identities = batch.iter().map(|association| {
                    format!(
                        "({}, {})",
                        association.logical_edge_identity, association.migration_correlation_id
                    )
                });
                let context = batch_row_context(start, identities);
                InterpretError::database(
                    format!("failed to write migration-discovery-association batch; {context}"),
                    error,
                )
            })?
            .into_iter()
            .collect::<HashSet<_>>();
        let conflicting = batch
            .iter()
            .enumerate()
            .filter(|(_, association)| {
                !written.contains(&(
                    association.logical_edge_identity.clone(),
                    association.migration_correlation_id.clone(),
                ))
            })
            .map(|(offset, association)| {
                format!(
                    "{}=({}, {})",
                    start + offset,
                    association.logical_edge_identity,
                    association.migration_correlation_id
                )
            })
            .collect::<Vec<_>>();
        if !conflicting.is_empty() {
            return Err(InterpretError::data_integrity(format!(
                "migration discovery associations are already bound to different evidence; conflicting batch rows [{}]",
                conflicting.join(", ")
            )));
        }
    }
    write_effects(
        transaction,
        "migration_candidate_identity_effects",
        &output.migration_candidate_identity_effects,
        &content_hash,
    )
    .await?;
    write_effects(
        transaction,
        "migration_candidate_discovery_effects",
        &output.migration_candidate_discovery_effects,
        &content_hash,
    )
    .await?;
    Ok(())
}

async fn write_effects(
    transaction: &mut Transaction<'_, Postgres>,
    table: &str,
    effects: &[MigrationCandidateEffect],
    content_hash: &str,
) -> Result<()> {
    for (start, batch) in conflict_free_batches(effects, |effect| effect.effect_identity.clone()) {
        let mut query = QueryBuilder::<Postgres>::new(format!(
            "INSERT INTO {table} (
             effect_identity, migration_correlation_ids, correlation_kind, effect_kind,
             proposed_effect, evidence_refs, chain_id, block_number, block_hash,
             transaction_hash, transaction_index, log_index, canonicality_state,
             consumer_visibility, interpreter_content_hash
         ) "
        ));
        query.push_values(batch, |mut row, effect| {
            row.push_bind(&effect.effect_identity)
                .push_bind(&effect.migration_correlation_ids)
                .push_bind(&effect.correlation_kind)
                .push_bind(&effect.effect_kind)
                .push_bind(&effect.proposed_effect)
                .push_bind(&effect.evidence_refs)
                .push_bind(&effect.chain_id)
                .push_bind(effect.block_number)
                .push_bind(&effect.block_hash)
                .push_bind(&effect.transaction_hash)
                .push_bind(effect.transaction_index)
                .push_bind(effect.log_index)
                .push_bind(&effect.canonicality_state)
                .push_unseparated("::canonicality_state")
                .push_bind(&effect.consumer_visibility)
                .push_bind(content_hash);
        });
        query.push(format!(
            "
         ON CONFLICT (effect_identity) DO UPDATE
         SET canonicality_state = EXCLUDED.canonicality_state,
             consumer_visibility = EXCLUDED.consumer_visibility,
             interpreter_content_hash = EXCLUDED.interpreter_content_hash,
             observed_at = now()
         WHERE ROW(
             {table}.migration_correlation_ids, {table}.correlation_kind,
             {table}.effect_kind, {table}.proposed_effect, {table}.evidence_refs,
             {table}.chain_id, {table}.block_number, {table}.block_hash,
             {table}.transaction_hash, {table}.transaction_index, {table}.log_index
         ) IS NOT DISTINCT FROM ROW(
             EXCLUDED.migration_correlation_ids, EXCLUDED.correlation_kind,
             EXCLUDED.effect_kind, EXCLUDED.proposed_effect, EXCLUDED.evidence_refs,
             EXCLUDED.chain_id, EXCLUDED.block_number, EXCLUDED.block_hash,
             EXCLUDED.transaction_hash, EXCLUDED.transaction_index, EXCLUDED.log_index
         )
         RETURNING effect_identity"
        ));
        let written = query
            .build_query_scalar::<String>()
            .fetch_all(&mut **transaction)
            .await
            .map_err(|error| {
                let context =
                    batch_row_context(start, batch.iter().map(|effect| &effect.effect_identity));
                InterpretError::database(format!("failed to write {table} batch; {context}"), error)
            })?
            .into_iter()
            .collect::<HashSet<_>>();
        let conflicting = batch
            .iter()
            .enumerate()
            .filter(|(_, effect)| !written.contains(&effect.effect_identity))
            .map(|(offset, effect)| format!("{}={}", start + offset, effect.effect_identity))
            .collect::<Vec<_>>();
        if !conflicting.is_empty() {
            return Err(InterpretError::data_integrity(format!(
                "migration candidate effects in {table} are already bound to different evidence; conflicting batch rows [{}]",
                conflicting.join(", ")
            )));
        }
    }
    Ok(())
}

pub(super) async fn clear_redo_range(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    from_block: i64,
    to_block: i64,
) -> Result<()> {
    for table in [
        "migration_event_associations",
        "migration_discovery_associations",
        "migration_candidate_identity_effects",
        "migration_candidate_discovery_effects",
    ] {
        let statement = format!(
            "DELETE FROM {table} AS migration_row
             WHERE migration_row.chain_id = $1
               AND migration_row.block_number BETWEEN $2 AND $3
               AND EXISTS (
                   SELECT 1
                   FROM chain_lineage lineage
                   WHERE lineage.chain_id = migration_row.chain_id
                     AND lineage.block_hash = migration_row.block_hash
                     AND lineage.block_number = migration_row.block_number
                     AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
               )"
        );
        sqlx::query(&statement)
            .bind(chain_id)
            .bind(from_block)
            .bind(to_block)
            .execute(&mut **transaction)
            .await
            .map_err(|error| {
                InterpretError::database(format!("failed to clear {table} redo range"), error)
            })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
