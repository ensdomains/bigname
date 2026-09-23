use sqlx::{Postgres, Transaction};

use crate::{Marker, ProjectError, Result};

mod event_ids;
mod events;
mod history;
mod linked_records;
pub(crate) mod mirror_evidence;
pub(crate) mod node_record_events;

const PROJECTION_TABLES: &[&str] = &[
    "name_current",
    "children_current",
    "permissions_current",
    "account_permission_state_current",
    "permissions_current_resource_summary",
    "record_inventory_current",
    "resolver_current",
    "address_names_current",
    "address_records_current",
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
    create_declared_resolver_addresses(transaction, target.number).await?;
    Ok(())
}

pub(crate) async fn inputs(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target: &Marker,
    full_rebuild: bool,
) -> Result<()> {
    mirror_evidence::create(transaction).await?;
    create_events(transaction, chain_id, target.number, full_rebuild).await?;
    linked_records::include(transaction, chain_id, target.number, full_rebuild).await?;
    // Collect statistics after all history is staged so builders can plan joins
    // against the actual event mix, including linked resolver records.
    sqlx::query("ANALYZE project_events")
        .execute(&mut **transaction)
        .await
        .map_err(|error| ProjectError::database("failed to analyze staged events", error))?;
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
             LEFT JOIN chain_lineage lineage
               ON lineage.chain_id = event.chain_id
              AND lineage.block_hash = event.block_hash
              AND lineage.block_number = event.block_number
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
               AND (
                   event.block_hash IS NULL
                   OR lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
               )
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

async fn create_declared_resolver_addresses(
    transaction: &mut Transaction<'_, Postgres>,
    target_block: i64,
) -> Result<()> {
    sqlx::query(
        "CREATE TEMP TABLE project_declared_resolver_addresses ON COMMIT DROP AS
         SELECT manifest.namespace,
                manifest.source_family,
                lower(declaration ->> 'address') AS resolver_address,
                declaration ->> 'role' AS classification_role,
                (declaration ->> 'start_block')::bigint AS declaration_start_block,
                declaration_ordinality AS classification_declaration_ordinality,
                manifest.manifest_id,
                manifest.manifest_version,
                manifest.manifest_event_id
         FROM project_manifests manifest
         CROSS JOIN LATERAL jsonb_array_elements(COALESCE(
             manifest.manifest_payload -> 'contracts', '[]'::jsonb
         )) WITH ORDINALITY declarations(declaration, declaration_ordinality)
         WHERE (manifest.source_family = 'ens_v1_resolver_l1'
                OR (manifest.source_family = 'ens_v2_resolver_l1'
                    AND declaration ->> 'role' IN ('public_resolver_v2', 'ensv1_mirror_resolver')
                    AND declaration ->> 'proxy_kind' = 'none'))
           AND declaration ->> 'address' IS NOT NULL
           AND btrim(declaration ->> 'address') <> ''
           AND lower(declaration ->> 'address') <>
               '0x0000000000000000000000000000000000000000'
           AND (
               declaration ->> 'start_block' IS NULL
               OR (declaration ->> 'start_block')::bigint <= $1
           )
           AND (manifest.source_family <> 'ens_v2_resolver_l1' OR NOT EXISTS (
               SELECT 1 FROM jsonb_array_elements(
                   manifest.manifest_payload -> 'contracts'
               ) WITH ORDINALITY later(item, ordinal)
               WHERE lower(item ->> 'address') = lower(declaration ->> 'address')
                 AND COALESCE((item ->> 'start_block')::bigint, 0) <= $1
                 AND (COALESCE((item ->> 'start_block')::bigint, 0), ordinal) >
                     (COALESCE((declaration ->> 'start_block')::bigint, 0),
                      declaration_ordinality)
           ))",
    )
    .bind(target_block)
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        ProjectError::database("failed to stage declared resolver addresses", error)
    })?;
    sqlx::query(
        "CREATE INDEX ON project_declared_resolver_addresses (
             namespace, resolver_address, manifest_id
         )",
    )
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        ProjectError::database("failed to index declared resolver addresses", error)
    })?;
    Ok(())
}

async fn create_events(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target_block: i64,
    full_rebuild: bool,
) -> Result<()> {
    if !full_rebuild {
        event_ids::create(transaction, chain_id, target_block).await?;
    }

    events::create(transaction, chain_id, target_block, full_rebuild).await
}

async fn create_identity_views(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target: &Marker,
    full_rebuild: bool,
) -> Result<()> {
    let surface_scope_join = if full_rebuild {
        ""
    } else {
        "JOIN (
             SELECT logical_name_id FROM project_mirror_evidence_names
             UNION
             SELECT logical_name_id FROM project_scope_names
             UNION
             SELECT logical_name_id FROM project_scope_children
             UNION
             SELECT logical_name_id FROM project_scope_ancestors
         ) scope USING (logical_name_id)"
    };
    let surface_statement = format!(
        "CREATE TEMP TABLE project_surfaces ON COMMIT DROP AS
         SELECT surface.*
         FROM name_surfaces surface
         {surface_scope_join}
         JOIN chain_lineage lineage
           ON lineage.chain_id = surface.chain_id
          AND lineage.block_hash = surface.block_hash
          AND lineage.block_number = surface.block_number
         WHERE surface.chain_id = $1
           AND surface.block_number <= $2
           AND surface.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')"
    );
    sqlx::query(&surface_statement)
        .bind(chain_id)
        .bind(target.number)
        .execute(&mut **transaction)
        .await
        .map_err(|error| ProjectError::database("failed to stage name identities", error))?;

    let resource_scope_join = if full_rebuild {
        ""
    } else {
        "JOIN project_scope_resources scope USING (resource_id)"
    };
    let resource_statement = format!(
        "CREATE TEMP TABLE project_resources ON COMMIT DROP AS
         SELECT resource.*
         FROM resources resource
         {resource_scope_join}
         JOIN chain_lineage lineage
           ON lineage.chain_id = resource.chain_id
          AND lineage.block_hash = resource.block_hash
          AND lineage.block_number = resource.block_number
         WHERE resource.chain_id = $1
           AND resource.block_number <= $2
           AND resource.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')"
    );
    sqlx::query(&resource_statement)
        .bind(chain_id)
        .bind(target.number)
        .execute(&mut **transaction)
        .await
        .map_err(|error| ProjectError::database("failed to stage resource identities", error))?;

    sqlx::query(
        "CREATE TEMP TABLE project_binding_candidates ON COMMIT DROP AS
         SELECT binding.*
         FROM surface_bindings binding
         JOIN project_surfaces surface
           ON surface.logical_name_id = binding.logical_name_id
         JOIN resources resource
           ON resource.resource_id = binding.resource_id
         JOIN chain_lineage lineage
           ON lineage.chain_id = binding.chain_id
          AND lineage.block_hash = binding.block_hash
          AND lineage.block_number = binding.block_number
         JOIN chain_lineage resource_lineage
           ON resource_lineage.chain_id = resource.chain_id
          AND resource_lineage.block_hash = resource.block_hash
          AND resource_lineage.block_number = resource.block_number
         WHERE binding.chain_id = $1
           AND binding.block_number <= $2
           AND resource.chain_id = $1
           AND resource.block_number <= $2
           AND binding.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND resource.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND resource_lineage.canonicality_state IN (
               'canonical', 'safe', 'finalized'
           )
         ORDER BY binding.logical_name_id, binding.block_number,
                  COALESCE((binding.provenance ->> 'transaction_index')::bigint, -1),
                  COALESCE((binding.provenance ->> 'log_index')::bigint, -1),
                  binding.surface_binding_id",
    )
    .bind(chain_id)
    .bind(target.number)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to stage binding candidates", error))?;

    // The builders look these tables up once per name or per resource, so each gets its key and
    // statistics. Temporary tables are never analyzed automatically.
    for statement in [
        "ALTER TABLE project_surfaces ADD PRIMARY KEY (logical_name_id)",
        "ALTER TABLE project_resources ADD PRIMARY KEY (resource_id)",
        "CREATE INDEX ON project_binding_candidates (logical_name_id, authority_arm, block_number)",
        "ANALYZE project_surfaces",
        "ANALYZE project_resources",
        "ANALYZE project_binding_candidates",
    ] {
        sqlx::query(statement)
            .execute(&mut **transaction)
            .await
            .map_err(|error| ProjectError::database("failed to index identity stages", error))?;
    }
    Ok(())
}
