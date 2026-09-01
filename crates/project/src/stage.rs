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
    create_declared_resolver_addresses(transaction, target.number).await?;
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
         WHERE manifest.source_family = 'ens_v1_resolver_l1'
           AND declaration ->> 'address' IS NOT NULL
           AND btrim(declaration ->> 'address') <> ''
           AND lower(declaration ->> 'address') <>
               '0x0000000000000000000000000000000000000000'
           AND (
               declaration ->> 'start_block' IS NULL
               OR (declaration ->> 'start_block')::bigint <= $1
           )",
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

    let scoped_event_ids = r#"
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
        -- separately. (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L462 @ ens_v2@a971bd64)
        -- (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L467 @ ens_v2@a971bd64)
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
        -- Defensive symmetry with create_identity_views; inventory closure guarantees the names.
        SELECT record.normalized_event_id FROM (
            SELECT logical_name_id FROM project_scope_names
            UNION
            SELECT logical_name_id FROM project_scope_children
        ) scope
        JOIN name_surfaces surface USING (logical_name_id)
        JOIN chain_lineage surface_lineage ON surface_lineage.chain_id = surface.chain_id
         AND (surface_lineage.block_number, surface_lineage.block_hash) = (surface.block_number, surface.block_hash)
        JOIN (
            SELECT DISTINCT event.resource_id, event.logical_name_id,
                   lower(event.after_state ->> 'resolver') AS resolver_address
            FROM project_scope_resources resource_scope
            JOIN normalized_events event USING (resource_id)
            JOIN chain_lineage lineage USING (chain_id, block_number, block_hash)
            WHERE event.chain_id = $1 AND event.block_number <= $2
              AND event.resource_id IS NOT NULL AND event.logical_name_id IS NOT NULL
              AND event.event_kind = 'ResolverChanged' AND event.consumer_visibility = 'activated'
              AND (
                  event.source_family IN (
                      'ens_v1_registry_l1',
                      'ens_v1_registrar_l1',
                      'ens_v1_wrapper_l1'
                  ) OR (
                      event.source_family IN (
                          'ens_v2_registry_l1', 'ens_v2_root_l1'
                      )
                      AND EXISTS (
                          SELECT 1
                          FROM project_declared_resolver_addresses declaration
                          WHERE declaration.namespace = event.namespace
                            AND declaration.resolver_address =
                                lower(event.after_state ->> 'resolver')
                      )
                  )
              )
              AND event.canonicality_state IN ('canonical', 'safe', 'finalized') AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
        ) pointer USING (logical_name_id)
        JOIN normalized_events record ON record.chain_id = $1 AND record.logical_name_id IS NULL
         AND record.source_family = 'ens_v1_resolver_l1' AND lower(record.after_state ->> 'node') = lower(surface.namehash)
         AND lower(COALESCE(NULLIF(record.after_state ->> 'resolver', ''),
             NULLIF(record.raw_fact_ref ->> 'emitting_address', ''))) = pointer.resolver_address
        JOIN chain_lineage record_lineage ON record_lineage.chain_id = record.chain_id
         AND (record_lineage.block_number, record_lineage.block_hash) = (record.block_number, record.block_hash)
        WHERE surface.chain_id = $1 AND surface.block_number <= $2
          AND surface.canonicality_state IN ('canonical', 'safe', 'finalized') AND surface_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
          AND pointer.resolver_address NOT IN ('0x0000000000000000000000000000000000000000', '')
          AND record.block_number <= $2 AND record.consumer_visibility = 'activated' AND record.event_kind IN ('RecordChanged', 'RecordVersionChanged')
          AND record.canonicality_state IN ('canonical', 'safe', 'finalized') AND record_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
        UNION
        -- Candidate-only resources supply just the resolver evidence consumed by this build.
        -- They remain outside delete-and-publish resource scope.
        SELECT normalized_event_id
        FROM project_scope_resolver_candidate_events
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
          AND (
              event.event_kind IN ('SubregistryChanged', 'AliasChanged')
              OR (
                  event.event_kind = 'AuthorityTransferred'
                  AND event.source_family IN (
                      'ens_v1_registry_l1', 'basenames_base_registry'
                  )
              )
          )
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
        FROM project_changed_events event
        CROSS JOIN LATERAL (VALUES
            (CASE WHEN event.event_kind = 'ResolverChanged'
                  THEN event.after_state ->> 'resolver' END),
            (CASE WHEN event.event_kind = 'ResolverChanged'
                  THEN event.before_state ->> 'resolver' END),
            (CASE WHEN event.event_kind = 'PermissionChanged'
                       AND event.after_state #>> '{scope,kind}' = 'resolver'
                  THEN event.after_state #>> '{scope,resolver_address}' END),
            (CASE WHEN event.event_kind = 'PermissionChanged'
                       AND event.before_state #>> '{scope,kind}' = 'resolver'
                  THEN event.before_state #>> '{scope,resolver_address}' END)
        ) candidate(resolver_address)
        JOIN project_scope_resolvers scope
          ON lower(candidate.resolver_address) = lower(scope.resolver_address)
        WHERE event.event_kind IN ('ResolverChanged', 'PermissionChanged')
          AND NOT EXISTS (
              SELECT 1 FROM project_scope_resolver_passthrough passthrough
              WHERE lower(passthrough.resolver_address) =
                    lower(scope.resolver_address)
          )
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
        "#;
    sqlx::query(scoped_event_ids)
        .bind(chain_id)
        .bind(target_block)
        .execute(&mut **transaction)
        .await
        .map_err(|error| {
            ProjectError::database("failed to stage scoped event identities", error)
        })?;
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

    sqlx::query(
        "CREATE INDEX ON project_binding_candidates (
             logical_name_id, authority_arm, block_number
         )",
    )
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to index binding candidates", error))?;

    Ok(())
}
