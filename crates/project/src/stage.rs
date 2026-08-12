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
    if !full_rebuild {
        close_staged_topology_scope(transaction).await?;
    }
    create_identity_views(transaction, chain_id, target, full_rebuild).await?;
    Ok(())
}

async fn close_staged_topology_scope(transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
    sqlx::query(
        "INSERT INTO project_scope_children
         SELECT event.namespace || ':' || lower(candidate.node)
         FROM project_events event
         CROSS JOIN LATERAL (
             VALUES (event.after_state ->> 'node'),
                    (event.after_state ->> 'child_node')
         ) candidate(node)
         WHERE event.event_kind = 'SubregistryChanged'
           AND candidate.node IS NOT NULL
           AND EXISTS (
               SELECT 1
               FROM (
                   SELECT logical_name_id FROM project_scope_names
                   UNION
                   SELECT logical_name_id FROM project_scope_children
               ) scope
               WHERE scope.logical_name_id IN (
                   event.namespace || ':' || lower(event.after_state ->> 'node'),
                   event.namespace || ':' || lower(event.after_state ->> 'child_node')
               )
           )
         ON CONFLICT DO NOTHING",
    )
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to close staged topology scope", error))?;
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

async fn create_events(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target_block: i64,
    full_rebuild: bool,
) -> Result<()> {
    if !full_rebuild {
        create_scoped_event_ids(transaction, chain_id, target_block).await?;
    }

    let scope_join = if full_rebuild {
        ""
    } else {
        "JOIN project_event_ids scope
           ON scope.normalized_event_id = event.normalized_event_id"
    };
    let statement = format!(
        "CREATE TEMP TABLE project_events ON COMMIT DROP AS
         SELECT event.*
         FROM normalized_events event
         {scope_join}
         LEFT JOIN chain_lineage lineage
           ON lineage.chain_id = event.chain_id
          AND lineage.block_hash = event.block_hash
          AND lineage.block_number = event.block_number
         WHERE event.chain_id = $1
           AND event.consumer_visibility = 'activated'
           AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND (
               (event.block_number IS NULL AND event.block_hash IS NULL)
               OR (
                   event.block_number <= $2
                   AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
               )
           )"
    );
    sqlx::query(&statement)
        .bind(chain_id)
        .bind(target_block)
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

async fn create_scoped_event_ids(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target_block: i64,
) -> Result<()> {
    sqlx::query(
        "CREATE TEMP TABLE project_event_ids (
             normalized_event_id bigint PRIMARY KEY
         ) ON COMMIT DROP",
    )
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to create event identity stage", error))?;

    sqlx::query(
        r#"
        INSERT INTO project_event_ids
        SELECT event.normalized_event_id
        FROM normalized_events event
        WHERE event.chain_id = $1
          AND event.event_kind = 'SourceManifestUpdated'
          AND (event.block_number IS NULL OR event.block_number <= $2)
        UNION
        SELECT event.normalized_event_id
        FROM project_scope_names scope
        JOIN normalized_events event USING (logical_name_id)
        WHERE event.chain_id = $1 AND event.block_number <= $2
          AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
        UNION
        SELECT event.normalized_event_id
        FROM project_scope_children scope
        JOIN normalized_events event USING (logical_name_id)
        WHERE event.chain_id = $1 AND event.block_number <= $2
          AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
        UNION
        -- ENSv2 registration stores the entry's subregistry and emits the label registration
        -- separately. (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L459 @ ens_v2@ccaeb58)
        -- (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L464 @ ens_v2@ccaeb58)
        -- The child-edge projection combines those inputs, so rebuilding a scoped parent's row
        -- family stages each current sibling's registrations without widening projection scope.
        SELECT event.normalized_event_id
        FROM project_scope_children scope
        JOIN children_current child
          ON child.parent_logical_name_id = scope.logical_name_id
         AND child.provenance ->> 'chain_id' = $1
        JOIN normalized_events event
          ON event.logical_name_id = child.child_logical_name_id
        WHERE event.chain_id = $1 AND event.block_number <= $2
          AND event.event_kind IN (
              'RegistrationGranted', 'RegistrationRenewed', 'RegistrationReleased'
          )
          AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
        UNION
        SELECT event.normalized_event_id
        FROM project_scope_resources scope
        JOIN normalized_events event USING (resource_id)
        WHERE event.chain_id = $1 AND event.block_number <= $2
          AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
        UNION
        SELECT event.normalized_event_id
        FROM normalized_events event
        CROSS JOIN LATERAL (
            VALUES
                (event.namespace || ':' || lower(event.after_state ->> 'node')),
                (event.namespace || ':' || lower(event.after_state ->> 'child_node')),
                (event.after_state ->> 'to_logical_name_id'),
                (event.before_state ->> 'to_logical_name_id')
        ) candidate(logical_name_id)
        JOIN (
            SELECT logical_name_id FROM project_scope_names
            UNION
            SELECT logical_name_id FROM project_scope_children
        ) scope
          ON scope.logical_name_id = candidate.logical_name_id
        WHERE event.chain_id = $1 AND event.block_number <= $2
          AND event.event_kind IN ('SubregistryChanged', 'AliasChanged')
          AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
        UNION
        SELECT event.normalized_event_id
        FROM normalized_events event
        CROSS JOIN LATERAL (
            VALUES (event.after_state ->> 'to_resource_id'),
                   (event.before_state ->> 'to_resource_id')
        ) candidate(resource_id)
        JOIN project_scope_resources scope
          ON scope.resource_id::text = candidate.resource_id
        WHERE event.chain_id = $1 AND event.block_number <= $2
          AND event.event_kind = 'AliasChanged'
          AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
        UNION
        SELECT event.normalized_event_id
        FROM project_scope_resolvers scope
        JOIN normalized_events event
          ON lower(COALESCE(
                 event.after_state ->> 'resolver',
                 event.before_state ->> 'resolver',
                 event.raw_fact_ref ->> 'emitting_address'
             )) = lower(scope.resolver_address)
        WHERE event.chain_id = $1 AND event.block_number <= $2
          AND event.event_kind = 'AliasChanged'
          AND NOT EXISTS (
              SELECT 1 FROM project_scope_resolver_passthrough passthrough
              WHERE lower(passthrough.resolver_address) =
                    lower(scope.resolver_address)
          )
          AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
        UNION
        SELECT event.normalized_event_id
        FROM project_scope_resolvers scope
        JOIN normalized_events event
          ON lower(event.after_state ->> 'proxy_address') =
             lower(scope.resolver_address)
        WHERE event.chain_id = $1 AND event.block_number <= $2
          AND event.event_kind = 'Upgraded'
          AND NOT EXISTS (
              SELECT 1 FROM project_scope_resolver_passthrough passthrough
              WHERE lower(passthrough.resolver_address) =
                    lower(scope.resolver_address)
          )
          AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
        UNION
        SELECT event.normalized_event_id
        FROM normalized_events event
        CROSS JOIN LATERAL (
            VALUES
                (lower(event.after_state ->> 'address'),
                 event.after_state ->> 'coin_type',
                 event.after_state ->> 'namespace'),
                (lower(event.before_state ->> 'address'),
                 event.before_state ->> 'coin_type',
                 event.before_state ->> 'namespace'),
                (lower(event.after_state -> 'primary_claim_source' ->> 'address'),
                 event.after_state -> 'primary_claim_source' ->> 'coin_type',
                 event.after_state -> 'primary_claim_source' ->> 'namespace'),
                (lower(event.before_state -> 'primary_claim_source' ->> 'address'),
                 event.before_state -> 'primary_claim_source' ->> 'coin_type',
                 event.before_state -> 'primary_claim_source' ->> 'namespace')
        ) candidate(address, coin_type, namespace)
        JOIN project_scope_primary scope
          ON scope.address = candidate.address
         AND scope.coin_type = candidate.coin_type
         AND scope.namespace = candidate.namespace
        WHERE event.chain_id = $1 AND event.block_number <= $2
          AND event.event_kind IN ('ReverseChanged', 'RecordChanged')
          AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
        UNION
        SELECT resolver.normalized_event_id
        FROM project_scope_primary scope
        JOIN normalized_events reverse
          ON reverse.chain_id = $1
         AND reverse.block_number <= $2
         AND reverse.event_kind = 'ReverseChanged'
         AND reverse.canonicality_state IN ('canonical', 'safe', 'finalized')
         AND lower(reverse.after_state ->> 'address') = scope.address
         AND reverse.after_state ->> 'coin_type' = scope.coin_type
         AND reverse.after_state ->> 'namespace' = scope.namespace
        JOIN normalized_events resolver
          ON resolver.chain_id = $1
         AND resolver.block_number <= $2
         AND resolver.event_kind = 'ResolverChanged'
         AND resolver.canonicality_state IN ('canonical', 'safe', 'finalized')
         AND lower(resolver.after_state ->> 'node') =
             lower(reverse.after_state ->> 'reverse_node')
        ON CONFLICT DO NOTHING
        "#,
    )
    .bind(chain_id)
    .bind(target_block)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to stage scoped event identities", error))?;
    Ok(())
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
             SELECT logical_name_id FROM project_scope_names
             UNION
             SELECT logical_name_id FROM project_scope_children
             UNION
             SELECT child.child_logical_name_id
             FROM project_scope_children parent
             JOIN children_current child
               ON child.parent_logical_name_id = parent.logical_name_id
              AND child.provenance ->> 'chain_id' = $1
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
