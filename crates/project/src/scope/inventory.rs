use sqlx::{Postgres, Transaction};

use crate::{Marker, ProjectError, Result};

pub(super) async fn include_changed_node_record_dependents(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
) -> Result<()> {
    // Start from this window's node-only v1 record changes and follow only the pointer ID cited by
    // the published inventory; redo expands retracted pointer dependents independently. The plain
    // namehash equality keeps the targeted index lookup while the lowercase equality, declaration,
    // and namespace joins mirror the guarded arm in builders/record_inventory.rs.
    sqlx::query(
        "CREATE TEMP TABLE project_changed_node_record_dependents ON COMMIT DROP AS
         SELECT DISTINCT pointer.logical_name_id, inventory.resource_id
         FROM project_changed_events record
         JOIN name_surfaces surface
           ON surface.chain_id = record.chain_id
          AND surface.namehash = lower(record.after_state ->> 'node')
          AND lower(surface.namehash) = lower(record.after_state ->> 'node')
          AND surface.canonicality_state IN ('canonical', 'safe', 'finalized')
         JOIN normalized_events pointer
           ON pointer.chain_id = record.chain_id
          AND pointer.logical_name_id = surface.logical_name_id
          AND pointer.resource_id IS NOT NULL
          AND pointer.event_kind = 'ResolverChanged'
          AND pointer.source_family IN ('ens_v2_registry_l1', 'ens_v2_root_l1')
          AND pointer.canonicality_state IN ('canonical', 'safe', 'finalized')
         JOIN record_inventory_current inventory
           ON inventory.resource_id = pointer.resource_id
          AND (inventory.provenance ->> 'resolver_pointer_event_id')::bigint =
              pointer.normalized_event_id
          AND inventory.provenance ->> 'chain_id' = record.chain_id
          AND inventory.support_status = 'supported'
         JOIN resolver_current resolver
           ON resolver.chain_id = record.chain_id
          AND lower(resolver.resolver_address) =
              lower(pointer.after_state ->> 'resolver')
          AND resolver.support_status = 'supported'
          AND resolver.declared_summary #>> '{classification,source_family}' =
              'ens_v1_resolver_l1'
          AND resolver.declared_summary #>> '{classification,basis}' =
              'manifest_declared_address'
         JOIN project_declared_resolver_addresses declaration
           ON declaration.manifest_id =
              (resolver.provenance ->> 'manifest_id')::bigint
          AND declaration.namespace = pointer.namespace
          AND declaration.resolver_address =
              lower(pointer.after_state ->> 'resolver')
         WHERE record.chain_id = $1
           AND record.event_kind IN ('RecordChanged', 'RecordVersionChanged')
           AND record.source_family = 'ens_v1_resolver_l1'
           AND record.logical_name_id IS NULL
           AND lower(COALESCE(
                   NULLIF(record.after_state ->> 'resolver', ''),
                   NULLIF(record.raw_fact_ref ->> 'emitting_address', '')
               )) = lower(pointer.after_state ->> 'resolver')",
    )
    .bind(chain_id)
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        ProjectError::database("failed to match changed node-only record dependents", error)
    })?;

    sqlx::query(
        "INSERT INTO project_scope_names
         SELECT logical_name_id FROM project_changed_node_record_dependents
         ON CONFLICT DO NOTHING",
    )
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        ProjectError::database("failed to scope changed node-only record names", error)
    })?;
    sqlx::query(
        "INSERT INTO project_scope_resources
         SELECT resource_id FROM project_changed_node_record_dependents
         ON CONFLICT DO NOTHING",
    )
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        ProjectError::database("failed to scope changed node-only record resources", error)
    })?;
    Ok(())
}

pub(super) async fn include_changed_record_consumers(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target_block: i64,
) -> Result<()> {
    // ENSv1 resolver writes may carry only the node and resolver emitter: the record events
    // identify the name solely by its node hash, with the resolver as the emitting address.
    // (upstream: .refs/ens_v1/contracts/resolvers/profiles/ITextResolver.sol:L5-L10 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/resolvers/profiles/IAddrResolver.sol:L6 @ ens_v1@91c966f)
    // Match those facts to
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
                       AND (
                           event.source_family = 'ens_v1_resolver_l1'
                           OR (
                               event.source_family = 'basenames_base_resolver'
                               AND EXISTS (
                                   SELECT 1
                                   FROM normalized_events pointer
                                   WHERE pointer.normalized_event_id =
                                       (inventory.provenance ->>
                                           'resolver_pointer_event_id')::bigint
                                     AND pointer.source_family =
                                         'basenames_base_registry'
                               )
                           )
                       )
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
