use sqlx::{Postgres, Transaction};

use crate::{ProjectError, Result};

pub(super) async fn include_names_for_scoped_registrars(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target_block: i64,
) -> Result<()> {
    sqlx::query(
        "WITH scoped_registrars AS (
             SELECT DISTINCT registrar.resource_id,
                    registrar.namespace || ':' ||
                        lower(registrar.after_state ->> 'namehash') AS logical_name_id
             FROM project_scope_resources scope
             JOIN normalized_events registrar USING (resource_id)
             JOIN chain_lineage registrar_lineage
               ON registrar_lineage.chain_id = registrar.chain_id
              AND registrar_lineage.block_hash = registrar.block_hash
              AND registrar_lineage.block_number = registrar.block_number
             WHERE registrar.chain_id = $1
               AND registrar.block_number <= $2
               AND registrar.source_family = 'ens_v1_registrar_l1'
               AND registrar.consumer_visibility = 'activated'
               AND registrar.canonicality_state IN ('canonical', 'safe', 'finalized')
               AND registrar_lineage.canonicality_state IN (
                   'canonical', 'safe', 'finalized'
               )
               AND registrar.after_state ->> 'namehash' IS NOT NULL
               AND btrim(registrar.after_state ->> 'namehash') <> ''
         )
         INSERT INTO project_scope_names
         SELECT DISTINCT wrapper.logical_name_id
         FROM scoped_registrars registrar
         JOIN normalized_events wrapper
           ON wrapper.logical_name_id = registrar.logical_name_id
          AND wrapper.after_state ->> 'wrapped_registrar_resource_id' =
              registrar.resource_id::text
         JOIN chain_lineage wrapper_lineage
           ON wrapper_lineage.chain_id = wrapper.chain_id
          AND wrapper_lineage.block_hash = wrapper.block_hash
          AND wrapper_lineage.block_number = wrapper.block_number
         WHERE wrapper.chain_id = $1
           AND wrapper.block_number <= $2
           AND wrapper.source_family = 'ens_v1_wrapper_l1'
           AND wrapper.event_kind = 'SurfaceBound'
           AND wrapper.consumer_visibility = 'activated'
           AND wrapper.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND wrapper_lineage.canonicality_state IN (
               'canonical', 'safe', 'finalized'
           )
         ON CONFLICT DO NOTHING",
    )
    .bind(chain_id)
    .bind(target_block)
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        ProjectError::database(
            "failed to scope later-wrapped names from registrar resources",
            error,
        )
    })?;
    Ok(())
}

pub(super) async fn include_registrars_for_scoped_wrappers(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target_block: i64,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO project_scope_resources
         SELECT DISTINCT registrar.resource_id
         FROM project_scope_resources scope
         JOIN normalized_events wrapper
           ON wrapper.resource_id = scope.resource_id
         JOIN chain_lineage wrapper_lineage
           ON wrapper_lineage.chain_id = wrapper.chain_id
          AND wrapper_lineage.block_hash = wrapper.block_hash
          AND wrapper_lineage.block_number = wrapper.block_number
         JOIN resources registrar
           ON registrar.chain_id = wrapper.chain_id
          AND registrar.resource_id::text =
              wrapper.after_state ->> 'wrapped_registrar_resource_id'
         WHERE wrapper.chain_id = $1
           AND wrapper.block_number <= $2
           AND wrapper.source_family = 'ens_v1_wrapper_l1'
           AND wrapper.event_kind = 'SurfaceBound'
           AND wrapper.consumer_visibility = 'activated'
           AND wrapper.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND wrapper_lineage.canonicality_state IN (
               'canonical', 'safe', 'finalized'
           )
         ON CONFLICT DO NOTHING",
    )
    .bind(chain_id)
    .bind(target_block)
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        ProjectError::database(
            "failed to scope wrapped registrar resources from wrapper bindings",
            error,
        )
    })?;
    Ok(())
}
