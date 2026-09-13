use sqlx::{Postgres, Transaction};

use crate::{Marker, ProjectError, Result};

pub(super) async fn include_changed_node_record_dependents(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
) -> Result<()> {
    // Start from this window's node-only record changes and follow only the pointer ID cited by
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
           AND record.event_kind IN ('RecordChanged', 'RecordVersionChanged')
           AND record.source_family =
               resolver.declared_summary #>> '{classification,source_family}'
           AND (record.source_family <> 'ens_v2_resolver_l1'
                OR (record.namespace = pointer.namespace
                    AND record.source_manifest_id = declaration.manifest_id))
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
              AND lower(event.raw_fact_ref ->> 'emitting_address') IN (
                  -- A mirrored row serves the writes of the ENSv1 resolver it was derived
                  -- from (builders/record_inventory/mirror.rs), not of the mirror itself.
                  lower(inventory.provenance ->> 'resolver_address'),
                  lower(inventory.provenance #>> '{mirror,mirrored_resolver_address}')
              )
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
        include_mirror_pairs(transaction, chain_id, target.number).await?;
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

async fn include_mirror_pairs(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target_block: i64,
) -> Result<()> {
    // A resource whose readable pointer targets a declared ENSv1 mirror resolver is served through
    // the ENSv1 resolver the mirror finds for the name: the exact node's, else the nearest
    // ancestor's (builders/record_inventory/mirror.rs). The ENSv1 registry side is keyed by node
    // and may have no resource or surface link of its own. The sides rebuild together: a scoped
    // mirror-pointer resource scopes the names (and any pointer resources) of every node its walk
    // consults; a scoped consulted name, pointer resource, or a changed node-keyed ENSv1 pointer
    // scopes the mirror-pointer resources of every name whose walk consults that node.
    sqlx::query(
        "WITH mirror_pointers AS (
             SELECT event.resource_id, event.logical_name_id
             FROM normalized_events event
             JOIN chain_lineage lineage
               ON lineage.chain_id = event.chain_id
              AND lineage.block_hash = event.block_hash
              AND lineage.block_number = event.block_number
             JOIN project_declared_resolver_addresses declaration
               ON declaration.resolver_address = lower(event.after_state ->> 'resolver')
              AND declaration.classification_role = 'ensv1_mirror_resolver'
             WHERE event.chain_id = $1
               AND event.block_number <= $2
               AND event.event_kind = 'ResolverChanged'
               AND event.source_family IN ('ens_v2_registry_l1', 'ens_v2_root_l1')
               AND event.resource_id IS NOT NULL
               AND event.logical_name_id IS NOT NULL
               AND event.consumer_visibility = 'activated'
               AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
               AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
         ),
         v1_nodes AS (
             SELECT event.namespace, lower(event.after_state ->> 'node') AS namehash,
                    event.resource_id,
                    bool_or(changed.normalized_event_id IS NOT NULL) AS changed
             FROM normalized_events event
             JOIN chain_lineage lineage
               ON lineage.chain_id = event.chain_id
              AND lineage.block_hash = event.block_hash
              AND lineage.block_number = event.block_number
             LEFT JOIN project_changed_events changed USING (normalized_event_id)
             WHERE event.chain_id = $1
               AND event.block_number <= $2
               AND event.event_kind = 'ResolverChanged'
               AND event.source_family IN (
                   'ens_v1_registry_l1', 'ens_v1_registrar_l1', 'ens_v1_wrapper_l1'
               )
               AND event.after_state ->> 'node' IS NOT NULL
               AND event.consumer_visibility = 'activated'
               AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
               AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
             GROUP BY event.namespace, lower(event.after_state ->> 'node'), event.resource_id
         ),
         surfaces AS (
             SELECT DISTINCT surface.logical_name_id, surface.namespace, surface.namehash,
                    surface.raw_labels
             FROM name_surfaces surface
             JOIN chain_lineage lineage
               ON lineage.chain_id = surface.chain_id
              AND lineage.block_hash = surface.block_hash
              AND lineage.block_number = surface.block_number
             WHERE surface.chain_id = $1
               AND surface.block_number <= $2
               AND surface.canonicality_state IN ('canonical', 'safe', 'finalized')
               AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
               AND (
                   EXISTS (
                       SELECT 1 FROM mirror_pointers mirror
                       WHERE mirror.logical_name_id = surface.logical_name_id
                   )
                   OR EXISTS (
                       SELECT 1 FROM v1_nodes node
                       WHERE node.namespace = surface.namespace
                         AND node.namehash = lower(surface.namehash)
                   )
               )
         ),
         -- Every registry node the mirror's resolver walk consults for a mirrored name: the name
         -- itself and each proper ancestor below the root.
         walk AS (
             SELECT mirror.resource_id AS mirror_resource_id,
                    queried.namespace,
                    queried.raw_labels[position : cardinality(queried.raw_labels)] AS suffix
             FROM mirror_pointers mirror
             JOIN surfaces queried ON queried.logical_name_id = mirror.logical_name_id
             CROSS JOIN generate_series(1, cardinality(queried.raw_labels)) AS position
         ),
         pairs AS (
             SELECT walk.mirror_resource_id,
                    consulted.logical_name_id AS consulted_logical_name_id,
                    node.resource_id AS consulted_resource_id,
                    node.changed
             FROM walk
             JOIN surfaces consulted
               ON consulted.namespace = walk.namespace
              AND consulted.raw_labels = walk.suffix
             JOIN v1_nodes node
               ON node.namespace = consulted.namespace
              AND node.namehash = lower(consulted.namehash)
         ),
         rebuilt AS (
             SELECT DISTINCT pair.*
             FROM pairs pair
             WHERE EXISTS (
                     SELECT 1 FROM project_scope_resources scope
                     WHERE scope.resource_id = pair.mirror_resource_id
                 )
                OR EXISTS (
                     SELECT 1 FROM project_scope_resources scope
                     WHERE scope.resource_id = pair.consulted_resource_id
                 )
                OR EXISTS (
                     SELECT 1 FROM project_scope_names scope
                     WHERE scope.logical_name_id = pair.consulted_logical_name_id
                 )
                OR pair.changed
         ),
         scoped_names AS (
             INSERT INTO project_scope_names
             SELECT DISTINCT consulted_logical_name_id FROM rebuilt
             ON CONFLICT DO NOTHING
         )
         INSERT INTO project_scope_resources
         SELECT mirror_resource_id FROM rebuilt
         UNION
         SELECT consulted_resource_id FROM rebuilt WHERE consulted_resource_id IS NOT NULL
         ON CONFLICT DO NOTHING",
    )
    .bind(chain_id)
    .bind(target_block)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to scope mirror resolver pairs", error))?;
    Ok(())
}
