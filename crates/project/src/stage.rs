use sqlx::{Postgres, Transaction};

use crate::{Marker, ProjectError, Result};

const PROJECTION_TABLES: &[&str] = &[
    "name_current",
    "children_current",
    "permissions_current",
    "permissions_current_resource_summary",
    "record_inventory_current",
    "resolver_current",
    "address_names_current",
    "primary_names_current",
];

pub(crate) async fn prepare(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target: &Marker,
) -> Result<()> {
    for table in PROJECTION_TABLES {
        let statement = format!(
            "CREATE TEMP TABLE project_stage_{table}
             (LIKE {table} INCLUDING DEFAULTS) ON COMMIT DROP"
        );
        sqlx::query(&statement)
            .execute(&mut **transaction)
            .await
            .map_err(|error| {
                ProjectError::database(format!("failed to create {table} stage"), error)
            })?;
    }
    create_manifests(transaction, chain_id, target.number).await?;
    Ok(())
}

pub(crate) async fn inputs(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target: &Marker,
    full_rebuild: bool,
) -> Result<()> {
    create_events(transaction, chain_id, target.number, full_rebuild).await?;
    create_identity_views(transaction, chain_id, target, full_rebuild).await?;
    Ok(())
}

async fn create_manifests(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target_block: i64,
) -> Result<()> {
    sqlx::query(
        "CREATE TEMP TABLE project_manifests ON COMMIT DROP AS
         WITH latest AS (
             SELECT DISTINCT ON (event.source_manifest_id)
                    event.source_manifest_id AS manifest_id,
                    event.manifest_version,
                    event.namespace,
                    event.source_family,
                    event.chain_id,
                    COALESCE(
                        event.after_state -> 'manifest_payload' ->> 'deployment_epoch',
                        event.raw_fact_ref ->> 'deployment_epoch'
                    ) AS deployment_label,
                    event.after_state ->> 'rollout_status' AS rollout_status,
                    event.after_state ->> 'normalizer_version' AS normalizer_version,
                    event.after_state -> 'manifest_payload' AS manifest_payload,
                    event.normalized_event_id AS manifest_event_id
             FROM normalized_events event
             WHERE (
                       event.chain_id = $1
                       OR (
                           $1 = 'base-mainnet'
                           AND event.namespace = 'basenames'
                           AND event.source_family = 'basenames_execution'
                           AND event.chain_id = 'ethereum-mainnet'
                       )
                   )
               AND event.event_kind = 'SourceManifestUpdated'
               AND event.source_manifest_id IS NOT NULL
               AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
               AND (event.block_number IS NULL OR event.block_number <= $2)
             ORDER BY event.source_manifest_id, event.normalized_event_id DESC
         )
         SELECT * FROM latest
         WHERE rollout_status = 'active' AND manifest_payload IS NOT NULL",
    )
    .bind(chain_id)
    .bind(target_block)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to stage admitted manifest events", error))?;
    Ok(())
}

async fn create_events(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target_block: i64,
    full_rebuild: bool,
) -> Result<()> {
    sqlx::query(
        "CREATE TEMP TABLE project_events ON COMMIT DROP AS
         SELECT event.*
         FROM normalized_events event
         LEFT JOIN chain_lineage lineage
           ON lineage.chain_id = event.chain_id
          AND lineage.block_hash = event.block_hash
          AND lineage.block_number = event.block_number
         WHERE event.chain_id = $1
           AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND (
               (event.block_number IS NULL AND event.block_hash IS NULL)
               OR (
                   event.block_number <= $2
                   AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
               )
           )
           AND (
               $3
               OR event.event_kind = 'SourceManifestUpdated'
               OR EXISTS (
                   SELECT 1 FROM project_scope_names scope
                   WHERE scope.logical_name_id = event.logical_name_id
                      OR scope.logical_name_id =
                         event.namespace || ':' || lower(event.after_state ->> 'node')
                      OR scope.logical_name_id =
                         event.namespace || ':' || lower(event.after_state ->> 'child_node')
                      OR scope.logical_name_id =
                         event.after_state ->> 'to_logical_name_id'
                      OR scope.logical_name_id =
                         event.before_state ->> 'to_logical_name_id'
               )
               OR EXISTS (
                   SELECT 1 FROM project_scope_children scope
                   WHERE scope.logical_name_id = event.logical_name_id
                      OR scope.logical_name_id =
                         event.namespace || ':' || lower(event.after_state ->> 'node')
                      OR scope.logical_name_id =
                         event.namespace || ':' || lower(event.after_state ->> 'child_node')
               )
               OR EXISTS (
                   SELECT 1 FROM project_scope_resources scope
                   WHERE scope.resource_id = event.resource_id
                      OR scope.resource_id::text =
                         event.after_state ->> 'to_resource_id'
                      OR scope.resource_id::text =
                         event.before_state ->> 'to_resource_id'
               )
               OR EXISTS (
                   SELECT 1 FROM project_scope_resolvers scope
                   WHERE lower(scope.resolver_address) IN (
                       lower(event.after_state ->> 'resolver'),
                       lower(event.before_state ->> 'resolver'),
                       lower(event.after_state ->> 'proxy_address'),
                       lower(event.before_state ->> 'proxy_address'),
                       lower(event.raw_fact_ref ->> 'emitting_address')
                   )
               )
               OR EXISTS (
                   SELECT 1 FROM project_scope_primary scope
                   WHERE (
                       lower(scope.address) = lower(event.after_state ->> 'address')
                       AND scope.coin_type = event.after_state ->> 'coin_type'
                       AND scope.namespace = event.after_state ->> 'namespace'
                   ) OR (
                       lower(scope.address) = lower(event.before_state ->> 'address')
                       AND scope.coin_type = event.before_state ->> 'coin_type'
                       AND scope.namespace = event.before_state ->> 'namespace'
                   ) OR (
                       lower(scope.address) = lower(
                           event.after_state -> 'primary_claim_source' ->> 'address'
                       )
                       AND scope.coin_type = event.after_state
                           -> 'primary_claim_source' ->> 'coin_type'
                       AND scope.namespace = event.after_state
                           -> 'primary_claim_source' ->> 'namespace'
                   ) OR (
                       lower(scope.address) = lower(
                           event.before_state -> 'primary_claim_source' ->> 'address'
                       )
                       AND scope.coin_type = event.before_state
                           -> 'primary_claim_source' ->> 'coin_type'
                       AND scope.namespace = event.before_state
                           -> 'primary_claim_source' ->> 'namespace'
                   )
               )
           )",
    )
    .bind(chain_id)
    .bind(target_block)
    .bind(full_rebuild)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to stage canonical events", error))?;
    for statement in [
        "CREATE INDEX ON project_events (logical_name_id, normalized_event_id)",
        "CREATE INDEX ON project_events (resource_id, normalized_event_id)",
        "CREATE INDEX ON project_events (event_kind, normalized_event_id)",
    ] {
        sqlx::query(statement)
            .execute(&mut **transaction)
            .await
            .map_err(|error| ProjectError::database("failed to index staged events", error))?;
    }
    Ok(())
}

async fn create_identity_views(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target: &Marker,
    full_rebuild: bool,
) -> Result<()> {
    sqlx::query(
        "CREATE TEMP TABLE project_surfaces ON COMMIT DROP AS
         SELECT surface.*
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
               $3 OR EXISTS (
                   SELECT 1 FROM project_scope_names scope
                   WHERE scope.logical_name_id = surface.logical_name_id
               ) OR EXISTS (
                   SELECT 1 FROM project_scope_children scope
                   WHERE scope.logical_name_id = surface.logical_name_id
               )
           )",
    )
    .bind(chain_id)
    .bind(target.number)
    .bind(full_rebuild)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to stage name identities", error))?;

    sqlx::query(
        "CREATE TEMP TABLE project_resources ON COMMIT DROP AS
         SELECT resource.*
         FROM resources resource
         JOIN chain_lineage lineage
           ON lineage.chain_id = resource.chain_id
          AND lineage.block_hash = resource.block_hash
          AND lineage.block_number = resource.block_number
         WHERE resource.chain_id = $1
           AND resource.block_number <= $2
           AND resource.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND (
               $3 OR EXISTS (
                   SELECT 1 FROM project_scope_resources scope
                   WHERE scope.resource_id = resource.resource_id
               )
           )",
    )
    .bind(chain_id)
    .bind(target.number)
    .bind(full_rebuild)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to stage resource identities", error))?;

    sqlx::query(
        "CREATE TEMP TABLE project_bindings ON COMMIT DROP AS
         SELECT DISTINCT ON (binding.logical_name_id)
                binding.*
         FROM surface_bindings binding
         JOIN project_resources resource
           ON resource.resource_id = binding.resource_id
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
               binding.active_to IS NULL
               OR binding.active_to >= (
                   SELECT block_timestamp + interval '1 second' FROM chain_lineage
                   WHERE chain_id = $1 AND block_hash = $3 AND block_number = $2
               )
           )
         ORDER BY binding.logical_name_id, binding.active_from DESC,
                  binding.surface_binding_id DESC",
    )
    .bind(chain_id)
    .bind(target.number)
    .bind(&target.hash)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to stage current bindings", error))?;

    Ok(())
}
