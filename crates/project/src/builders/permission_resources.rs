use sqlx::{Postgres, Transaction};

use crate::{ProjectError, Result};

pub(super) async fn build_registry_binding(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<()> {
    sqlx::query(
        r#"
        WITH latest_registry_observation AS (
            SELECT DISTINCT ON (event.resource_id)
                   event.resource_id,
                   event.normalized_event_id,
                   CASE WHEN event.event_kind = 'SurfaceUnbound' THEN NULL
                        ELSE lower(event.after_state ->> 'owner_getter') END AS registry_owner,
                   lower(COALESCE(event.raw_fact_ref ->> 'emitting_address',
                                  event.after_state ->> 'registry_contract')) AS registry_contract,
                   jsonb_build_object(
                       'normalized_event_ids', jsonb_build_array(event.normalized_event_id),
                       'raw_fact_ref', event.raw_fact_ref,
                       'chain_id', event.chain_id,
                       'derivation_kind', 'registry_owner_binding_rebuild'
                   ) AS provenance,
                   jsonb_build_object(
                       'block_number', event.block_number,
                       'block_hash', event.block_hash,
                       'transaction_index', event.transaction_index,
                       'log_index', event.log_index
                   ) AS chain_positions,
                   event.manifest_version
            FROM project_events event
            WHERE event.event_kind IN (
                      'AuthorityTransferred', 'SubregistryChanged', 'SurfaceBound', 'SurfaceUnbound'
                  )
              AND event.source_family IN ('ens_v1_registry_l1', 'basenames_base_registry')
              AND event.resource_id IS NOT NULL
            ORDER BY event.resource_id, event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST, event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
        ), classified AS (
            SELECT observation.*,
                   observation.registry_owner ~ '^0x[0-9a-f]{40}$'
                   AND observation.registry_owner <>
                       '0x0000000000000000000000000000000000000000'
                   AND observation.registry_contract ~ '^0x[0-9a-f]{40}$'
                       AS applicable
            FROM latest_registry_observation observation
        )
        UPDATE project_stage_permissions_current_resource_summary summary
        SET registry_owner = CASE WHEN binding.applicable THEN binding.registry_owner END,
            registry_contract = CASE WHEN binding.applicable THEN binding.registry_contract END,
            registry_binding_provenance = CASE WHEN binding.applicable THEN binding.provenance END,
            registry_binding_chain_positions = CASE WHEN binding.applicable THEN binding.chain_positions END,
            provenance = (summary.provenance - 'registry_binding_clear_event_id') ||
                CASE WHEN binding.applicable THEN '{}'::jsonb ELSE jsonb_build_object(
                    'registry_binding_clear_event_id', binding.normalized_event_id) END,
            manifest_version = greatest(summary.manifest_version, binding.manifest_version)
        FROM classified binding
        WHERE summary.resource_id = binding.resource_id
        "#,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to build registry-owner bindings", error))?;
    Ok(())
}
