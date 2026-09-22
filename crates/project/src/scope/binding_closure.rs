use sqlx::{Postgres, Transaction};

use crate::{Marker, ProjectError, Result};

pub(super) async fn close_binding_scope(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target: &Marker,
) -> Result<()> {
    #[cfg(test)]
    if crate::reference::enabled(transaction).await? {
        return crate::reference::execute(
            transaction,
            chain_id,
            target.number,
            Some(&target.hash),
            include_str!("binding_previous.sql"),
        )
        .await;
    }

    super::authority::include_latest_arm_resources(transaction, chain_id, target.number).await?;
    super::resolver::include_registry_read_anchors(transaction, chain_id, target.number).await?;
    super::wrapper_registrar::include_names_for_scoped_registrars(
        transaction,
        chain_id,
        target.number,
    )
    .await?;
    super::registrar_bindings::include_names_for_scoped_unnamed_lease_rows(
        transaction,
        chain_id,
        target.number,
    )
    .await?;
    sqlx::query(
        &super::frontier::query(
            transaction,
            "active_bindings_from_names",
            "INSERT INTO project_scope_resources
         SELECT binding.resource_id
         FROM project_scope_names scope
         JOIN LATERAL (
             SELECT * FROM surface_bindings WHERE logical_name_id = scope.logical_name_id
               AND chain_id = $1 AND block_number <= $2
               AND canonicality_state IN ('canonical', 'safe', 'finalized') OFFSET 0
         ) binding ON TRUE
         JOIN chain_lineage lineage
           ON lineage.chain_id = binding.chain_id
          AND lineage.block_hash = binding.block_hash
          AND lineage.block_number = binding.block_number
         WHERE binding.chain_id = $1
           AND binding.block_number <= $2
           AND binding.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND binding.active_from < (
               SELECT block_timestamp + interval '1 second' FROM chain_lineage
               WHERE chain_id = $1 AND block_hash = $3 AND block_number = $2
           )
           AND (
               binding.active_to IS NULL OR binding.active_to >= (
                   SELECT block_timestamp + interval '1 second' FROM chain_lineage
                   WHERE chain_id = $1 AND block_hash = $3 AND block_number = $2
               )
           )
         ON CONFLICT DO NOTHING",
        )
        .await?,
    )
    .bind(chain_id)
    .bind(target.number)
    .bind(&target.hash)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to close resource binding scope", error))?;

    super::wrapper_registrar::include_registrars_for_scoped_wrappers(
        transaction,
        chain_id,
        target.number,
    )
    .await?;
    super::registrar_bindings::include_unnamed_lease_resources_for_scoped_names(
        transaction,
        chain_id,
        target.number,
    )
    .await?;

    sqlx::query(
        &super::frontier::query(
            transaction,
            "active_bindings_from_resources",
            "INSERT INTO project_scope_names
         SELECT binding.logical_name_id
         FROM project_scope_resources scope
         JOIN LATERAL (
             SELECT * FROM surface_bindings WHERE resource_id = scope.resource_id
               AND chain_id = $1 AND block_number <= $2
               AND canonicality_state IN ('canonical', 'safe', 'finalized') OFFSET 0
         ) binding ON TRUE
         JOIN chain_lineage lineage
           ON lineage.chain_id = binding.chain_id
          AND lineage.block_hash = binding.block_hash
          AND lineage.block_number = binding.block_number
         WHERE binding.chain_id = $1
           AND binding.block_number <= $2
           AND binding.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND binding.active_from < (
               SELECT block_timestamp + interval '1 second' FROM chain_lineage
               WHERE chain_id = $1 AND block_hash = $3 AND block_number = $2
           )
           AND (
               binding.active_to IS NULL OR binding.active_to >= (
                   SELECT block_timestamp + interval '1 second' FROM chain_lineage
                   WHERE chain_id = $1 AND block_hash = $3 AND block_number = $2
               )
           )
         ON CONFLICT DO NOTHING",
        )
        .await?,
    )
    .bind(chain_id)
    .bind(target.number)
    .bind(&target.hash)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to close name binding scope", error))?;
    Ok(())
}

#[cfg(test)]
#[path = "binding_tests.rs"]
mod tests;
