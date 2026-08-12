use bigname_adapters::schema_v2::{BatchOutput, MigrationCandidateEffect};
use sqlx::{Postgres, Transaction};

use crate::{InterpretError, Result};

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

    for association in &output.migration_event_associations {
        let written: Option<String> = sqlx::query_scalar(
            "INSERT INTO migration_event_associations (
                 event_identity, migration_correlation_id, correlation_kind, evidence_refs,
                 chain_id, block_number, block_hash, transaction_hash, transaction_index,
                 log_index, canonicality_state, consumer_visibility, interpreter_content_hash
             ) VALUES (
                 $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                 $11::canonicality_state, $12, $13
             )
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
             RETURNING event_identity",
        )
        .bind(&association.event_identity)
        .bind(&association.migration_correlation_id)
        .bind(&association.correlation_kind)
        .bind(&association.evidence_refs)
        .bind(&association.chain_id)
        .bind(association.block_number)
        .bind(&association.block_hash)
        .bind(&association.transaction_hash)
        .bind(association.transaction_index)
        .bind(association.log_index)
        .bind(&association.canonicality_state)
        .bind(&association.consumer_visibility)
        .bind(&content_hash)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|error| {
            InterpretError::database("failed to write migration event association", error)
        })?;
        if written.is_none() {
            return Err(InterpretError::data_integrity(format!(
                "migration event association ({}, {}) is already bound to different evidence",
                association.event_identity, association.migration_correlation_id
            )));
        }
    }

    for association in &output.migration_discovery_associations {
        let written: Option<String> = sqlx::query_scalar(
            "INSERT INTO migration_discovery_associations (
                 logical_edge_identity, migration_correlation_id, correlation_kind,
                 registry_contract_instance_id, registry_address, source_manifest_id,
                 evidence_refs, chain_id, block_number, block_hash, transaction_hash,
                 transaction_index, log_index, canonicality_state, consumer_visibility,
                 interpreter_content_hash
             ) VALUES (
                 $1, $2, 'migration_registry_creation', $3, lower($4), $5, $6, $7,
                 $8, $9, $10, $11, $12, $13::canonicality_state, $14, $15
             )
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
             RETURNING logical_edge_identity",
        )
        .bind(&association.logical_edge_identity)
        .bind(&association.migration_correlation_id)
        .bind(association.registry_contract_instance_id)
        .bind(&association.registry_address)
        .bind(association.source_manifest_id)
        .bind(&association.evidence_refs)
        .bind(&association.chain_id)
        .bind(association.block_number)
        .bind(&association.block_hash)
        .bind(&association.transaction_hash)
        .bind(association.transaction_index)
        .bind(association.log_index)
        .bind(&association.canonicality_state)
        .bind(&association.consumer_visibility)
        .bind(&content_hash)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|error| {
            InterpretError::database("failed to write migration discovery association", error)
        })?;
        if written.is_none() {
            return Err(InterpretError::data_integrity(format!(
                "migration discovery association ({}, {}) is already bound to different evidence",
                association.logical_edge_identity, association.migration_correlation_id
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
    let statement = format!(
        "INSERT INTO {table} (
             effect_identity, migration_correlation_ids, correlation_kind, effect_kind,
             proposed_effect, evidence_refs, chain_id, block_number, block_hash,
             transaction_hash, transaction_index, log_index, canonicality_state,
             consumer_visibility, interpreter_content_hash
         ) VALUES (
             $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
             $13::canonicality_state, $14, $15
         )
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
    );
    for effect in effects {
        let written: Option<String> = sqlx::query_scalar(&statement)
            .bind(&effect.effect_identity)
            .bind(&effect.migration_correlation_ids)
            .bind(&effect.correlation_kind)
            .bind(&effect.effect_kind)
            .bind(&effect.proposed_effect)
            .bind(&effect.evidence_refs)
            .bind(&effect.chain_id)
            .bind(effect.block_number)
            .bind(&effect.block_hash)
            .bind(&effect.transaction_hash)
            .bind(effect.transaction_index)
            .bind(effect.log_index)
            .bind(&effect.canonicality_state)
            .bind(&effect.consumer_visibility)
            .bind(content_hash)
            .fetch_optional(&mut **transaction)
            .await
            .map_err(|error| InterpretError::database(format!("failed to write {table}"), error))?;
        if written.is_none() {
            return Err(InterpretError::data_integrity(format!(
                "migration candidate effect {} in {table} is already bound to different evidence",
                effect.effect_identity
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
