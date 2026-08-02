use bigname_adapters::schema_v2::NormalizedEvent;
use sqlx::{Postgres, Transaction};

use crate::{InterpretError, Result};

pub(super) async fn events(
    transaction: &mut Transaction<'_, Postgres>,
    events: &[NormalizedEvent],
) -> Result<()> {
    for event in events {
        sqlx::query(
            "
            INSERT INTO normalized_events (
                event_identity,
                namespace,
                logical_name_id,
                resource_id,
                event_kind,
                source_family,
                manifest_version,
                source_manifest_id,
                chain_id,
                block_number,
                block_hash,
                transaction_hash,
                transaction_index,
                log_index,
                raw_fact_ref,
                derivation_kind,
                canonicality_state,
                before_state,
                after_state
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15, $16, $17::canonicality_state, $18, $19
            )
            ON CONFLICT (event_identity) DO UPDATE
            SET namespace = EXCLUDED.namespace,
                logical_name_id = EXCLUDED.logical_name_id,
                resource_id = EXCLUDED.resource_id,
                event_kind = EXCLUDED.event_kind,
                source_family = EXCLUDED.source_family,
                manifest_version = EXCLUDED.manifest_version,
                source_manifest_id = EXCLUDED.source_manifest_id,
                chain_id = EXCLUDED.chain_id,
                block_number = EXCLUDED.block_number,
                block_hash = EXCLUDED.block_hash,
                transaction_hash = EXCLUDED.transaction_hash,
                transaction_index = EXCLUDED.transaction_index,
                log_index = EXCLUDED.log_index,
                raw_fact_ref = EXCLUDED.raw_fact_ref,
                derivation_kind = EXCLUDED.derivation_kind,
                canonicality_state = EXCLUDED.canonicality_state,
                before_state = EXCLUDED.before_state,
                after_state = EXCLUDED.after_state,
                observed_at = now()
            ",
        )
        .bind(&event.event_identity)
        .bind(&event.namespace)
        .bind(&event.logical_name_id)
        .bind(event.resource_id)
        .bind(&event.event_kind)
        .bind(&event.source_family)
        .bind(event.manifest_version)
        .bind(event.source_manifest_id)
        .bind(&event.chain_id)
        .bind(event.block_number)
        .bind(&event.block_hash)
        .bind(&event.transaction_hash)
        .bind(event.transaction_index)
        .bind(event.log_index)
        .bind(&event.raw_fact_ref)
        .bind(&event.derivation_kind)
        .bind(&event.canonicality_state)
        .bind(&event.before_state)
        .bind(&event.after_state)
        .execute(&mut **transaction)
        .await
        .map_err(|error| InterpretError::database("failed to write normalized event", error))?;
    }
    Ok(())
}
