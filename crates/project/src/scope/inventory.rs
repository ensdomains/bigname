use sqlx::{Postgres, Transaction};

use crate::{Marker, ProjectError, Result};

#[path = "mirror.rs"]
mod mirror;

pub(super) async fn include_changed_node_record_dependents(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
) -> Result<()> {
    // Collapse repeated record writes and resolve their surfaces before shared-resolver joins.
    // Keep namespace and manifest in the key: only the v2 arm requires their declaration match.
    sqlx::query(
        "CREATE TEMP TABLE project_changed_node_record_keys ON COMMIT DROP AS
         WITH records AS MATERIALIZED (
             SELECT DISTINCT chain_id, namespace, source_manifest_id, source_family,
                    lower(after_state ->> 'node') AS node,
                    lower(COALESCE(NULLIF(after_state ->> 'resolver', ''),
                                   NULLIF(raw_fact_ref ->> 'emitting_address', ''))) AS resolver_address
             FROM project_changed_events
             WHERE chain_id = $1
               AND event_kind IN ('RecordChanged', 'RecordVersionChanged')
               AND logical_name_id IS NULL
         )
         SELECT DISTINCT record.*, surface.logical_name_id
         FROM records record
         JOIN name_surfaces surface
           ON surface.chain_id = record.chain_id
          AND surface.namehash = record.node
          AND lower(surface.namehash) = record.node
          AND surface.canonicality_state IN ('canonical', 'safe', 'finalized')",
    )
    .bind(chain_id)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to stage changed node record keys", error))?;
    sqlx::query("ANALYZE project_changed_node_record_keys")
        .execute(&mut **transaction)
        .await
        .map_err(|error| {
            ProjectError::database("failed to analyze changed node record keys", error)
        })?;

    // Materialize eligible name/resource/pointer candidates before casting inventory provenance.
    // A broad inventory-first stage would evaluate malformed IDs from unrelated inventories.
    // Retain every historical pointer here; the final equality selects the exact published ID.
    sqlx::query(
        "CREATE TEMP TABLE project_changed_node_record_dependents ON COMMIT DROP AS
         WITH candidates AS MATERIALIZED (
             SELECT pointer.logical_name_id, inventory.resource_id,
                    pointer.normalized_event_id,
                    inventory.provenance ->> 'resolver_pointer_event_id' AS inventory_pointer_id
             FROM project_changed_node_record_keys record
             JOIN normalized_events pointer
               ON pointer.chain_id = record.chain_id
              AND pointer.logical_name_id = record.logical_name_id
              AND pointer.resource_id IS NOT NULL
              AND pointer.event_kind = 'ResolverChanged'
              AND pointer.source_family IN ('ens_v2_registry_l1', 'ens_v2_root_l1')
              AND pointer.canonicality_state IN ('canonical', 'safe', 'finalized')
             JOIN record_inventory_current inventory
               ON inventory.resource_id = pointer.resource_id
              AND inventory.provenance ->> 'chain_id' = record.chain_id
              AND inventory.support_status = 'supported'
             JOIN resolver_current resolver
               ON resolver.chain_id = record.chain_id
              AND lower(resolver.resolver_address) =
                  lower(pointer.after_state ->> 'resolver')
              AND resolver.support_status = 'supported'
              AND (resolver.declared_summary #>> '{classification,source_family}' =
                  'ens_v1_resolver_l1'
               OR (resolver.declared_summary #>> '{classification,source_family}' =
                       'ens_v2_resolver_l1'
                   AND resolver.declared_summary #>> '{classification,role}' =
                       'public_resolver_v2'))
              AND resolver.declared_summary #>> '{classification,basis}' =
                  'manifest_declared_address'
             JOIN project_declared_resolver_addresses declaration
               ON declaration.manifest_id =
                  (resolver.provenance ->> 'manifest_id')::bigint
              AND declaration.namespace = pointer.namespace
              AND declaration.resolver_address =
                  lower(pointer.after_state ->> 'resolver')
             WHERE record.chain_id = $1
               AND record.source_family =
                   resolver.declared_summary #>> '{classification,source_family}'
               AND (record.source_family <> 'ens_v2_resolver_l1'
                    OR (record.namespace = pointer.namespace
                        AND record.source_manifest_id = declaration.manifest_id))
               AND record.resolver_address = lower(pointer.after_state ->> 'resolver')
         )
         SELECT DISTINCT logical_name_id, resource_id
         FROM candidates
         WHERE inventory_pointer_id::bigint = normalized_event_id",
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
    // Temporary tables have no autovacuum statistics; replay windows can contain many records.
    sqlx::query("ANALYZE project_changed_events")
        .execute(&mut **transaction)
        .await
        .map_err(|error| {
            ProjectError::database("failed to analyze changed project inputs", error)
        })?;

    // ENSv1 resolver writes may carry only the node and resolver emitter: the record events
    // identify the name solely by its node hash, with the resolver as the emitting address.
    // (upstream: .refs/ens_v1/contracts/resolvers/profiles/ITextResolver.sol:L5-L10 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/resolvers/profiles/IAddrResolver.sol:L6 @ ens_v1@91c966f)
    // Match those facts to
    // the previously published inventory's exact name surface so a record-only live window
    // rebuilds the consuming name and resource without expanding every name on a shared resolver.
    // Filter each record family before materializing its distinct lookup keys, so PostgreSQL
    // can estimate each arm from the analyzed event table. Materialize canonical inventory
    // surfaces before matching.
    // Each arm can then hash both the resolver and the name/node, avoiding the cross-product
    // of changed records and inventory rows that share a public resolver. Keep pointer IDs
    // distinct across inventory versions so the Basenames node-only guard retains its evidence.
    // Match Basenames candidates before checking pointer evidence: the correlated guard must
    // not prevent a composite hash join or cast pointer IDs from unrelated inventory rows.
    sqlx::query(
        "WITH attributed_records AS MATERIALIZED (
             SELECT DISTINCT event.logical_name_id,
                    lower(event.raw_fact_ref ->> 'emitting_address') AS address
             FROM project_changed_events event
             WHERE event.event_kind IN ('RecordChanged', 'RecordVersionChanged')
               AND event.source_family IN (
                   'ens_v1_resolver_l1', 'ens_v2_resolver_l1',
                   'basenames_base_resolver'
               )
               AND event.logical_name_id IS NOT NULL
               AND event.raw_fact_ref ->> 'emitting_address' IS NOT NULL
         ), node_records AS MATERIALIZED (
             SELECT DISTINCT lower(event.after_state ->> 'node') AS node,
                    lower(event.raw_fact_ref ->> 'emitting_address') AS address
             FROM project_changed_events event
             WHERE event.event_kind IN ('RecordChanged', 'RecordVersionChanged')
               AND event.source_family = 'ens_v1_resolver_l1'
               AND event.logical_name_id IS NULL
               AND event.raw_fact_ref ->> 'emitting_address' IS NOT NULL
         ), basenames_records AS MATERIALIZED (
             SELECT DISTINCT lower(event.after_state ->> 'node') AS node,
                    lower(event.raw_fact_ref ->> 'emitting_address') AS address
             FROM project_changed_events event
             WHERE event.event_kind IN ('RecordChanged', 'RecordVersionChanged')
               AND event.source_family = 'basenames_base_resolver'
               AND event.logical_name_id IS NULL
               AND event.raw_fact_ref ->> 'emitting_address' IS NOT NULL
         ), inventory_resolvers AS MATERIALIZED (
             SELECT DISTINCT inventory.resource_id,
                    inventory.provenance ->> 'logical_name_id' AS logical_name_id,
                    inventory.provenance ->> 'resolver_pointer_event_id' AS pointer_event_id,
                    resolver.address
             FROM record_inventory_current inventory
             CROSS JOIN LATERAL (VALUES
                 (lower(inventory.provenance ->> 'resolver_address')),
                 (lower(inventory.provenance #>> '{mirror,mirrored_resolver_address}'))
             ) resolver(address)
             WHERE inventory.provenance ->> 'chain_id' = $1
               AND resolver.address IS NOT NULL
         ), inventory_surfaces AS MATERIALIZED (
             SELECT DISTINCT inventory.resource_id, inventory.logical_name_id,
                    inventory.address, inventory.pointer_event_id,
                    lower(surface.namehash) AS node
             FROM inventory_resolvers inventory
             JOIN name_surfaces surface
               ON surface.logical_name_id = inventory.logical_name_id
              AND surface.chain_id = $1
             JOIN chain_lineage lineage
               ON lineage.chain_id = surface.chain_id
              AND lineage.block_number = surface.block_number
              AND lineage.block_hash = surface.block_hash
             WHERE surface.block_number <= $2
               AND surface.canonicality_state IN (
                   'canonical', 'safe', 'finalized'
               )
               AND lineage.canonicality_state IN (
                   'canonical', 'safe', 'finalized'
               )
         ), basenames_candidates AS MATERIALIZED (
             SELECT inventory.resource_id, inventory.logical_name_id,
                    inventory.pointer_event_id
             FROM basenames_records event
             JOIN inventory_surfaces inventory
               ON event.address = inventory.address
              AND event.node = inventory.node
         ), matched AS MATERIALIZED (
             SELECT inventory.resource_id, inventory.logical_name_id
             FROM attributed_records event
             JOIN inventory_surfaces inventory
               ON event.address = inventory.address
              AND event.logical_name_id = inventory.logical_name_id
             UNION
             SELECT inventory.resource_id, inventory.logical_name_id
             FROM node_records event
             JOIN inventory_surfaces inventory
               ON event.address = inventory.address
              AND event.node = inventory.node
             UNION
             SELECT inventory.resource_id, inventory.logical_name_id
             FROM basenames_candidates inventory
             WHERE EXISTS (
                 SELECT 1
                 FROM normalized_events pointer
                 WHERE pointer.normalized_event_id = inventory.pointer_event_id::bigint
                   AND pointer.source_family = 'basenames_base_registry'
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
    mirror::stage(transaction, chain_id, target.number).await?;
    // A pointer-derived name can be bound to another resource, whose latest pointer can name a
    // further surface. Reach the finite name/resource fixed point before staging and publication.
    loop {
        let before = scope_size(transaction).await?;
        include_pointer_names(transaction, chain_id, target.number).await?;
        mirror::include(transaction).await?;
        super::close_binding_scope(transaction, chain_id, target).await?;
        if scope_size(transaction).await? == before {
            sqlx::query("DROP TABLE project_mirror_pairs")
                .execute(&mut **transaction)
                .await
                .map_err(|error| {
                    ProjectError::database("failed to drop mirror scope pairs", error)
                })?;
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

#[cfg(test)]
#[path = "inventory_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "changed_node_tests.rs"]
mod changed_node_tests;
