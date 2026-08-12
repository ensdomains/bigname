use sqlx::{Postgres, Transaction};

use crate::{Marker, ProjectError, Result};

mod primary;
mod retracted;
mod wrapper;

pub(crate) struct Window<'a> {
    pub(crate) previous: Option<&'a Marker>,
    pub(crate) from_block: i64,
    pub(crate) to_block: i64,
    pub(crate) full_rebuild: bool,
    pub(crate) retain_retracted: bool,
}

pub(crate) async fn initialize(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target: &Marker,
    window: Window<'_>,
) -> Result<()> {
    create_scope_tables(transaction).await?;
    if window.full_rebuild {
        return Ok(());
    }

    stage_changed_events(transaction, chain_id, window.from_block, window.to_block).await?;
    seed_direct_scope(transaction, chain_id, window.from_block, window.to_block).await?;
    wrapper::include_time_boundaries(transaction, chain_id, window.previous, target).await?;
    if window.retain_retracted {
        retracted::seed(transaction, chain_id, window.from_block, window.to_block).await?;
    }
    include_topology_scope(transaction, chain_id, target.number).await?;
    include_classification_scope(transaction, chain_id, target.number).await?;
    include_resolver_dependents(transaction, chain_id, target.number).await?;
    close_binding_scope(transaction, chain_id, target).await?;
    include_alias_and_wildcard_scope(transaction, chain_id, target).await?;
    close_binding_scope(transaction, chain_id, target).await?;
    Ok(())
}

async fn create_scope_tables(transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
    for statement in [
        "CREATE TEMP TABLE project_scope_names (logical_name_id text PRIMARY KEY) ON COMMIT DROP",
        "CREATE TEMP TABLE project_scope_children (logical_name_id text PRIMARY KEY) ON COMMIT DROP",
        "CREATE TEMP TABLE project_scope_resources (resource_id uuid PRIMARY KEY) ON COMMIT DROP",
        "CREATE TEMP TABLE project_scope_resolvers (resolver_address text PRIMARY KEY) ON COMMIT DROP",
        "CREATE TEMP TABLE project_scope_primary (address text, coin_type text, namespace text, PRIMARY KEY (address, coin_type, namespace)) ON COMMIT DROP",
    ] {
        sqlx::query(statement)
            .execute(&mut **transaction)
            .await
            .map_err(|error| ProjectError::database("failed to create project scope", error))?;
    }
    Ok(())
}

async fn stage_changed_events(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    from_block: i64,
    to_block: i64,
) -> Result<()> {
    sqlx::query(
        "CREATE TEMP TABLE project_changed_events ON COMMIT DROP AS
         SELECT event.*
         FROM normalized_events event
         WHERE event.chain_id = $1
           AND event.consumer_visibility = 'activated'
           AND event.block_number BETWEEN $2 AND $3",
    )
    .bind(chain_id)
    .bind(from_block)
    .bind(to_block)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to stage changed project inputs", error))?;
    Ok(())
}

async fn seed_direct_scope(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    from_block: i64,
    to_block: i64,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO project_scope_names
         SELECT logical_name_id FROM project_changed_events
         WHERE logical_name_id IS NOT NULL
         UNION
         SELECT logical_name_id FROM name_surfaces
         WHERE chain_id = $1 AND block_number BETWEEN $2 AND $3
         UNION
         SELECT logical_name_id FROM surface_bindings
         WHERE chain_id = $1 AND block_number BETWEEN $2 AND $3
         ON CONFLICT DO NOTHING",
    )
    .bind(chain_id)
    .bind(from_block)
    .bind(to_block)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to derive direct name scope", error))?;

    sqlx::query(
        "INSERT INTO project_scope_children
         SELECT event.namespace || ':' || lower(candidate.node)
         FROM project_changed_events event
         CROSS JOIN LATERAL (
             VALUES (event.after_state ->> 'node'),
                    (event.after_state ->> 'child_node'),
                    (event.before_state ->> 'node'),
                    (event.before_state ->> 'child_node')
         ) candidate(node)
         WHERE event.event_kind = 'SubregistryChanged'
           AND candidate.node IS NOT NULL AND btrim(candidate.node) <> ''
         ON CONFLICT DO NOTHING",
    )
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to derive direct child scope", error))?;

    sqlx::query(
        "INSERT INTO project_scope_resources
         SELECT resource_id FROM project_changed_events
         WHERE resource_id IS NOT NULL
         UNION
         SELECT resource_id FROM resources
         WHERE chain_id = $1 AND block_number BETWEEN $2 AND $3
         UNION
         SELECT resource_id FROM surface_bindings
         WHERE chain_id = $1 AND block_number BETWEEN $2 AND $3
         ON CONFLICT DO NOTHING",
    )
    .bind(chain_id)
    .bind(from_block)
    .bind(to_block)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to derive direct resource scope", error))?;

    sqlx::query(
        "INSERT INTO project_scope_resolvers
         SELECT lower(address)
         FROM (
             SELECT after_state ->> 'resolver' AS address FROM project_changed_events
             UNION ALL
             SELECT before_state ->> 'resolver' FROM project_changed_events
             UNION ALL
             SELECT after_state ->> 'proxy_address' FROM project_changed_events
             WHERE event_kind = 'Upgraded'
             UNION ALL
             SELECT before_state ->> 'proxy_address' FROM project_changed_events
             WHERE event_kind = 'Upgraded'
             UNION ALL
             SELECT raw_fact_ref ->> 'emitting_address' FROM project_changed_events
             WHERE event_kind IN (
                 'RecordChanged', 'RecordVersionChanged', 'PermissionChanged',
                 'AliasChanged'
             )
         ) candidate
         WHERE address IS NOT NULL AND btrim(address) <> ''
           AND lower(address) <>
               '0x0000000000000000000000000000000000000000'
         ON CONFLICT DO NOTHING",
    )
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to derive direct resolver scope", error))?;

    primary::seed(transaction).await?;
    Ok(())
}

async fn include_topology_scope(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target_block: i64,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO project_scope_children
         SELECT event.namespace || ':' || lower(candidate.node)
         FROM normalized_events event
         JOIN chain_lineage lineage
           ON lineage.chain_id = event.chain_id
          AND lineage.block_hash = event.block_hash
          AND lineage.block_number = event.block_number
         CROSS JOIN LATERAL (
             VALUES (event.after_state ->> 'node'),
                    (event.after_state ->> 'child_node')
         ) candidate(node)
         WHERE event.chain_id = $1
           AND event.block_number <= $2
           AND event.event_kind = 'SubregistryChanged'
           AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND candidate.node IS NOT NULL
           AND (
               EXISTS (
                   SELECT 1 FROM project_scope_names scope
                   WHERE scope.logical_name_id IN (
                       event.namespace || ':' ||
                           lower(event.after_state ->> 'node'),
                       event.namespace || ':' ||
                           lower(event.after_state ->> 'child_node')
                   )
               )
               OR EXISTS (
                   SELECT 1 FROM project_scope_children scope
                   WHERE scope.logical_name_id IN (
                       event.namespace || ':' || lower(event.after_state ->> 'node'),
                       event.namespace || ':' || lower(event.after_state ->> 'child_node')
                   )
               )
               OR EXISTS (
                   SELECT 1 FROM project_changed_events changed
                   WHERE changed.after_state ->> 'labelhash' =
                         event.after_state ->> 'labelhash'
               )
           )
         ON CONFLICT DO NOTHING",
    )
    .bind(chain_id)
    .bind(target_block)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to close child topology scope", error))?;
    Ok(())
}

async fn include_classification_scope(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target_block: i64,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO project_scope_resolvers
         SELECT lower(live.resolver_address)
         FROM resolver_current live
         LEFT JOIN project_manifests manifest
           ON manifest.source_family =
              live.declared_summary -> 'classification' ->> 'source_family'
         WHERE live.chain_id = $1
           AND (
               manifest.manifest_id IS NULL
               OR live.provenance ->> 'manifest_event_id' IS DISTINCT FROM
                  manifest.manifest_event_id::text
           )
         UNION
         SELECT lower(declaration ->> 'address')
         FROM project_manifests manifest
         CROSS JOIN LATERAL jsonb_array_elements(COALESCE(
             manifest.manifest_payload -> 'contracts', '[]'::jsonb
         )) declaration
         LEFT JOIN resolver_current live
           ON live.chain_id = $1
          AND lower(live.resolver_address) = lower(declaration ->> 'address')
         WHERE manifest.source_family IN (
             'ens_v1_resolver_l1', 'basenames_base_resolver'
         )
           AND declaration ->> 'address' IS NOT NULL
           AND (
               live.resolver_address IS NULL
               OR live.provenance ->> 'manifest_event_id' IS DISTINCT FROM
                  manifest.manifest_event_id::text
           )
         UNION
         SELECT lower(upgrade.after_state ->> 'proxy_address')
         FROM (
             SELECT DISTINCT ON (event.after_state ->> 'proxy_address') event.*
             FROM normalized_events event
             JOIN chain_lineage lineage
               ON lineage.chain_id = event.chain_id
              AND lineage.block_hash = event.block_hash
              AND lineage.block_number = event.block_number
             WHERE event.chain_id = $1
               AND event.block_number <= $2
               AND event.event_kind = 'Upgraded'
               AND event.source_family = 'ens_v2_resolver_l1'
               AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
               AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
             ORDER BY event.after_state ->> 'proxy_address',
                      event.block_number DESC, event.transaction_index DESC NULLS LAST,
                      event.log_index DESC NULLS LAST, event.normalized_event_id DESC
         ) upgrade
         JOIN project_manifests manifest
           ON manifest.source_family = upgrade.source_family
         LEFT JOIN resolver_current live
           ON live.chain_id = $1
          AND lower(live.resolver_address) =
              lower(upgrade.after_state ->> 'proxy_address')
         WHERE live.resolver_address IS NULL
            OR live.provenance ->> 'manifest_event_id' IS DISTINCT FROM
               manifest.manifest_event_id::text
            OR live.provenance ->> 'upgrade_event_id' IS DISTINCT FROM
               upgrade.normalized_event_id::text
         ON CONFLICT DO NOTHING",
    )
    .bind(chain_id)
    .bind(target_block)
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        ProjectError::database("failed to scope resolver classification changes", error)
    })?;
    Ok(())
}

async fn include_resolver_dependents(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target_block: i64,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO project_scope_resources
         SELECT event.resource_id
         FROM normalized_events event
         JOIN chain_lineage lineage
           ON lineage.chain_id = event.chain_id
          AND lineage.block_hash = event.block_hash
          AND lineage.block_number = event.block_number
         JOIN project_scope_resolvers scope
           ON lower(scope.resolver_address) IN (
              lower(event.after_state ->> 'resolver'),
              lower(event.before_state ->> 'resolver')
           )
         WHERE event.chain_id = $1
           AND event.block_number <= $2
           AND event.event_kind = 'ResolverChanged'
           AND event.resource_id IS NOT NULL
           AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
         UNION
         SELECT inventory.resource_id
         FROM record_inventory_current inventory
         JOIN project_scope_resolvers scope
           ON lower(scope.resolver_address) = lower(
               inventory.record_version_boundary ->> 'resolver_address'
           )
         ON CONFLICT DO NOTHING",
    )
    .bind(chain_id)
    .bind(target_block)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to scope resolver resources", error))?;

    sqlx::query(
        "INSERT INTO project_scope_names
         SELECT event.logical_name_id
         FROM normalized_events event
         JOIN chain_lineage lineage
           ON lineage.chain_id = event.chain_id
          AND lineage.block_hash = event.block_hash
          AND lineage.block_number = event.block_number
         JOIN project_scope_resolvers scope
           ON lower(scope.resolver_address) IN (
              lower(event.after_state ->> 'resolver'),
              lower(event.before_state ->> 'resolver')
           )
         WHERE event.chain_id = $1
           AND event.block_number <= $2
           AND event.event_kind = 'ResolverChanged'
           AND event.logical_name_id IS NOT NULL
           AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
         ON CONFLICT DO NOTHING",
    )
    .bind(chain_id)
    .bind(target_block)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to scope resolver names", error))?;
    Ok(())
}

async fn close_binding_scope(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target: &Marker,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO project_scope_resources
         SELECT binding.resource_id
         FROM surface_bindings binding
         JOIN project_scope_names scope USING (logical_name_id)
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
    .bind(chain_id)
    .bind(target.number)
    .bind(&target.hash)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to close resource binding scope", error))?;

    sqlx::query(
        "INSERT INTO project_scope_names
         SELECT binding.logical_name_id
         FROM surface_bindings binding
         JOIN project_scope_resources scope USING (resource_id)
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
    .bind(chain_id)
    .bind(target.number)
    .bind(&target.hash)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to close name binding scope", error))?;
    Ok(())
}

async fn include_alias_and_wildcard_scope(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target: &Marker,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO project_scope_names
         SELECT event.after_state ->> 'to_logical_name_id'
         FROM normalized_events event
         JOIN project_scope_names scope
           ON scope.logical_name_id = event.logical_name_id
         JOIN chain_lineage lineage
           ON lineage.chain_id = event.chain_id
          AND lineage.block_hash = event.block_hash
          AND lineage.block_number = event.block_number
         WHERE event.chain_id = $1
           AND event.block_number <= $2
           AND event.event_kind = 'AliasChanged'
           AND event.after_state ->> 'to_logical_name_id' IS NOT NULL
           AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
         UNION
         SELECT ancestor.logical_name_id
         FROM name_surfaces scoped
         JOIN project_scope_names scope
           ON scope.logical_name_id = scoped.logical_name_id
         JOIN chain_lineage scoped_lineage
           ON scoped_lineage.chain_id = scoped.chain_id
          AND scoped_lineage.block_hash = scoped.block_hash
          AND scoped_lineage.block_number = scoped.block_number
         JOIN surface_bindings binding
           ON binding.logical_name_id = scoped.logical_name_id
          AND binding.binding_kind = 'observed_wildcard_path'
         JOIN chain_lineage binding_lineage
           ON binding_lineage.chain_id = binding.chain_id
          AND binding_lineage.block_hash = binding.block_hash
          AND binding_lineage.block_number = binding.block_number
         JOIN name_surfaces ancestor
           ON ancestor.chain_id = scoped.chain_id
          AND ancestor.namespace = scoped.namespace
          AND scoped.raw_name LIKE '%.' || ancestor.raw_name
          AND ancestor.raw_name <> ''
         JOIN chain_lineage ancestor_lineage
           ON ancestor_lineage.chain_id = ancestor.chain_id
          AND ancestor_lineage.block_hash = ancestor.block_hash
          AND ancestor_lineage.block_number = ancestor.block_number
         WHERE scoped.chain_id = $1
           AND binding.block_number <= $2
           AND scoped.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND scoped_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND binding.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND binding_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND ancestor.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND ancestor_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
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
    .bind(chain_id)
    .bind(target.number)
    .bind(&target.hash)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to scope resolution topology", error))?;
    Ok(())
}
