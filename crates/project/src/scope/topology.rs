use sqlx::{Postgres, Transaction};

use crate::{ProjectError, Result};

pub(super) async fn include_referenced_registrations(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target_block: i64,
) -> Result<()> {
    // The child builder joins a name's latest staged subregistry address to staged registration
    // histories. Scope every registration family for registry instances referenced by that
    // name's topology history, including the pointer being replaced by the changed event.
    sqlx::query(
        "WITH referenced_registry_instances AS (
             SELECT DISTINCT address.contract_instance_id
             FROM (
                 SELECT logical_name_id FROM project_scope_names
                 UNION
                 SELECT logical_name_id FROM project_scope_children
             ) scope
             JOIN normalized_events topology USING (logical_name_id)
             CROSS JOIN LATERAL (
                 VALUES (topology.after_state ->> 'subregistry'),
                        (topology.before_state ->> 'subregistry')
             ) pointer(address)
             JOIN contract_instance_addresses address
               ON address.chain_id = topology.chain_id
              AND lower(address.address) = lower(pointer.address)
              AND (address.active_from_block_number IS NULL
                   OR address.active_from_block_number <= $2)
              AND (address.active_to_block_number IS NULL
                   OR address.active_to_block_number > $2)
              AND address.deactivated_at IS NULL
             WHERE topology.chain_id = $1
               AND topology.block_number <= $2
               AND topology.event_kind = 'SubregistryChanged'
               AND topology.source_family IN ('ens_v2_root_l1', 'ens_v2_registry_l1')
               AND topology.canonicality_state IN ('canonical', 'safe', 'finalized')
               AND pointer.address IS NOT NULL
               AND btrim(pointer.address) <> ''
         )
         INSERT INTO project_scope_children
         SELECT registration.logical_name_id
         FROM referenced_registry_instances registry
         JOIN normalized_events registration
           ON registration.chain_id = $1
          AND registration.after_state ->> 'registry_contract_instance_id' =
              registry.contract_instance_id::text
         WHERE registration.block_number <= $2
           AND registration.event_kind IN (
               'RegistrationGranted', 'RegistrationRenewed', 'RegistrationReleased'
           )
           AND registration.source_family IN ('ens_v2_root_l1', 'ens_v2_registry_l1')
           AND registration.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND registration.logical_name_id IS NOT NULL
         ON CONFLICT DO NOTHING",
    )
    .bind(chain_id)
    .bind(target_block)
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        ProjectError::database(
            "failed to scope referenced subregistry registrations",
            error,
        )
    })?;
    Ok(())
}
