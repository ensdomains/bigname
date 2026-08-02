use bigname_adapters::schema_v2::NormalizedEvent;
use sqlx::{Postgres, Transaction};

use crate::{InterpretError, Result};

pub(super) async fn events(
    transaction: &mut Transaction<'_, Postgres>,
    events: &[NormalizedEvent],
) -> Result<()> {
    for event in events {
        let written: Option<String> = sqlx::query_scalar(
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
            SET canonicality_state = EXCLUDED.canonicality_state,
                observed_at = now()
            WHERE ROW(
                normalized_events.namespace,
                normalized_events.logical_name_id,
                normalized_events.resource_id,
                normalized_events.event_kind,
                normalized_events.source_family,
                normalized_events.manifest_version,
                normalized_events.source_manifest_id,
                normalized_events.chain_id,
                normalized_events.block_number,
                normalized_events.block_hash,
                normalized_events.transaction_hash,
                normalized_events.transaction_index,
                normalized_events.log_index,
                normalized_events.raw_fact_ref,
                normalized_events.derivation_kind,
                normalized_events.before_state,
                normalized_events.after_state
            ) IS NOT DISTINCT FROM ROW(
                EXCLUDED.namespace,
                EXCLUDED.logical_name_id,
                EXCLUDED.resource_id,
                EXCLUDED.event_kind,
                EXCLUDED.source_family,
                EXCLUDED.manifest_version,
                EXCLUDED.source_manifest_id,
                EXCLUDED.chain_id,
                EXCLUDED.block_number,
                EXCLUDED.block_hash,
                EXCLUDED.transaction_hash,
                EXCLUDED.transaction_index,
                EXCLUDED.log_index,
                EXCLUDED.raw_fact_ref,
                EXCLUDED.derivation_kind,
                EXCLUDED.before_state,
                EXCLUDED.after_state
            )
            RETURNING event_identity
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
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|error| InterpretError::database("failed to write normalized event", error))?;
        if written.is_none() {
            return Err(InterpretError::data_integrity(format!(
                "normalized event identity {} is already bound to different event data",
                event.event_identity
            )));
        }
    }
    Ok(())
}
