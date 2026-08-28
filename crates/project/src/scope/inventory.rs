use sqlx::{Postgres, Transaction};

use crate::{Marker, ProjectError, Result};

pub(super) async fn include_changed_record_consumers(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target_block: i64,
) -> Result<()> {
    // ENSv1 resolver writes may carry only the node and resolver emitter. Match those facts to
    // the previously published inventory's exact name surface so a record-only live window
    // rebuilds the consuming name and resource without expanding every name on a shared resolver.
    sqlx::query(
        "WITH matched AS MATERIALIZED (
             SELECT DISTINCT inventory.resource_id,
                    inventory.provenance ->> 'logical_name_id' AS logical_name_id
             FROM project_changed_events event
             JOIN record_inventory_current inventory
               ON inventory.provenance ->> 'chain_id' = $1
              AND lower(inventory.provenance ->> 'resolver_address') =
                  lower(event.raw_fact_ref ->> 'emitting_address')
             JOIN name_surfaces surface
               ON surface.logical_name_id =
                  inventory.provenance ->> 'logical_name_id'
              AND surface.chain_id = $1
             JOIN chain_lineage lineage
               ON lineage.chain_id = surface.chain_id
              AND lineage.block_number = surface.block_number
              AND lineage.block_hash = surface.block_hash
             WHERE event.event_kind IN ('RecordChanged', 'RecordVersionChanged')
               AND event.source_family IN (
                   'ens_v1_resolver_l1', 'ens_v2_resolver_l1',
                   'basenames_base_resolver'
               )
               AND event.raw_fact_ref ->> 'emitting_address' IS NOT NULL
               AND (
                   event.logical_name_id = surface.logical_name_id
                   OR (
                       event.logical_name_id IS NULL
                       AND event.source_family = 'ens_v1_resolver_l1'
                       AND lower(event.after_state ->> 'node') =
                           lower(surface.namehash)
                   )
               )
               AND surface.block_number <= $2
               AND surface.canonicality_state IN (
                   'canonical', 'safe', 'finalized'
               )
               AND lineage.canonicality_state IN (
                   'canonical', 'safe', 'finalized'
               )
         ), inserted_resources AS (
             INSERT INTO project_scope_resources
             SELECT resource_id FROM matched
             ON CONFLICT DO NOTHING
             RETURNING resource_id
         )
         INSERT INTO project_scope_names
         SELECT logical_name_id FROM matched
         WHERE logical_name_id IS NOT NULL
         ON CONFLICT DO NOTHING",
    )
    .bind(chain_id)
    .bind(target_block)
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        ProjectError::database("failed to scope changed record inventory consumers", error)
    })?;
    Ok(())
}

pub(super) async fn close(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target: &Marker,
) -> Result<()> {
    // A pointer-derived name can be bound to another resource, whose latest pointer can name a
    // further surface. Reach the finite name/resource fixed point before staging and publication.
    loop {
        let before = scope_size(transaction).await?;
        include_pointer_names(transaction, chain_id, target.number).await?;
        super::close_binding_scope(transaction, chain_id, target).await?;
        if scope_size(transaction).await? == before {
            return Ok(());
        }
    }
}

async fn scope_size(transaction: &mut Transaction<'_, Postgres>) -> Result<(i64, i64)> {
    sqlx::query_as(
        "SELECT (SELECT count(*) FROM project_scope_names),
                (SELECT count(*) FROM project_scope_resources)",
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to measure inventory scope", error))
}

async fn include_pointer_names(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target_block: i64,
) -> Result<()> {
    // Publication deletes every scoped resource before inserting its replacement. Stage every
    // readable linked pointer name so the inventory builder can fall back to an earlier pointer
    // when a later pointer's name surface is not visible at the target.
    sqlx::query("ANALYZE project_scope_resources")
        .execute(&mut **transaction)
        .await
        .map_err(|error| ProjectError::database("failed to analyze inventory scope", error))?;
    sqlx::query(
        "INSERT INTO project_scope_names
         SELECT DISTINCT event.logical_name_id
         FROM project_scope_resources scope
         JOIN normalized_events event USING (resource_id)
         JOIN chain_lineage lineage
           ON lineage.chain_id = event.chain_id
          AND lineage.block_hash = event.block_hash
          AND lineage.block_number = event.block_number
         WHERE event.chain_id = $1
           AND event.block_number <= $2
           AND event.event_kind = 'ResolverChanged'
           AND event.logical_name_id IS NOT NULL
           AND event.consumer_visibility = 'activated'
           AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
         ON CONFLICT DO NOTHING",
    )
    .bind(chain_id)
    .bind(target_block)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to scope inventory pointer names", error))?;
    Ok(())
}
