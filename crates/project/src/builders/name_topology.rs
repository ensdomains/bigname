use sqlx::{Postgres, Transaction};

use crate::{Marker, ProjectError, Result};

mod serialization;

pub(super) async fn build(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target: &Marker,
) -> Result<()> {
    project_alias_topology(transaction).await?;
    project_wildcard_topology(transaction).await?;
    project_basenames_transport(transaction, chain_id, target).await?;
    serialization::serialize_projected_topologies(transaction).await?;
    Ok(())
}

async fn project_alias_topology(transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE project_stage_name_current name
        SET declared_summary = jsonb_set(
            name.declared_summary,
            '{topology}',
            jsonb_build_object(
                'registry_path', jsonb_build_array(jsonb_build_object(
                    'logical_name_id', surface.logical_name_id,
                    'namespace', surface.namespace,
                    'normalized_name', surface.raw_name,
                    'canonical_display_name', surface.raw_name,
                    'namehash', surface.namehash,
                    'resource_id', binding.resource_id,
                    'binding_kind', binding.binding_kind
                )),
                'subregistry_path', '[]'::jsonb,
                'resolver_path', jsonb_build_array(jsonb_build_object(
                    'logical_name_id', surface.logical_name_id,
                    'namespace', surface.namespace,
                    'normalized_name', surface.raw_name,
                    'canonical_display_name', surface.raw_name,
                    'resource_id', binding.resource_id,
                    'chain_id', resolver.chain_id,
                    'address', resolver.resolver_address,
                    'latest_event_kind', resolver.event_kind
                )),
                'wildcard', jsonb_build_object(
                    'source', NULL,
                    'matched_labels', '[]'::jsonb
                ),
                'alias', jsonb_build_object(
                    'final_target', alias.final_target,
                    'hops', jsonb_build_array(alias.final_target)
                ),
                'version_boundaries', jsonb_build_object(
                    'topology_version_boundary', boundary.value,
                    'record_version_boundary', boundary.value
                ),
                'transport', jsonb_build_object(
                    'source_chain_id', NULL,
                    'target_chain_id', NULL,
                    'contract_address', NULL,
                    'latest_event_kind', NULL
                )
            ),
            true
        )
        FROM project_surfaces surface
        JOIN project_bindings binding
          ON binding.logical_name_id = surface.logical_name_id
         AND binding.binding_kind = 'resolver_alias_path'
        JOIN chain_lineage binding_lineage
          ON binding_lineage.chain_id = binding.chain_id
         AND binding_lineage.block_hash = binding.block_hash
         AND binding_lineage.block_number = binding.block_number
        JOIN LATERAL (
            SELECT event.chain_id,
                   event.event_kind,
                   lower(event.after_state ->> 'resolver') AS resolver_address
            FROM project_events event
            WHERE event.logical_name_id = surface.logical_name_id
              AND event.event_kind = 'ResolverChanged'
              AND event.resource_id = binding.resource_id
            ORDER BY event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST,
                     event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
            LIMIT 1
        ) resolver ON resolver.resolver_address IS NOT NULL
                     AND resolver.resolver_address <>
                         '0x0000000000000000000000000000000000000000'
        JOIN LATERAL (
            SELECT jsonb_strip_nulls(jsonb_build_object(
                       'logical_name_id', event.after_state ->> 'to_logical_name_id',
                       'namespace', event.namespace,
                       'normalized_name', COALESCE(
                           event.after_state ->> 'to_normalized_name',
                           event.after_state ->> 'to_name'
                       ),
                       'canonical_display_name', COALESCE(
                           event.after_state ->> 'to_canonical_display_name',
                           event.after_state ->> 'to_name'
                       ),
                       'namehash', event.after_state ->> 'to_namehash',
                       'resource_id', event.after_state ->> 'to_resource_id',
                       'binding_kind', 'resolver_alias_path'
                   )) AS final_target
            FROM (
                SELECT candidate.*
                FROM project_events candidate
                WHERE candidate.logical_name_id = surface.logical_name_id
                  AND candidate.event_kind = 'AliasChanged'
                ORDER BY candidate.block_number DESC NULLS LAST,
                         candidate.transaction_index DESC NULLS LAST,
                         candidate.log_index DESC NULLS LAST,
                         candidate.normalized_event_id DESC
                LIMIT 1
            ) event
            WHERE COALESCE((event.after_state ->> 'active')::boolean, true)
              AND event.after_state ->> 'to_logical_name_id' IS NOT NULL
        ) alias ON TRUE
        CROSS JOIN LATERAL (
            SELECT jsonb_build_object(
                       'logical_name_id', surface.logical_name_id,
                       'resource_id', binding.resource_id,
                       'normalized_event_id', NULL,
                       'event_kind', NULL,
                       'chain_position', jsonb_build_object(
                           'chain_id', binding.chain_id,
                           'block_number', binding.block_number,
                           'block_hash', binding.block_hash,
                           'timestamp', binding_lineage.block_timestamp
                       )
                   ) AS value
        ) boundary
        WHERE name.logical_name_id = surface.logical_name_id
        "#,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to build alias name topology", error))?;
    Ok(())
}

async fn project_wildcard_topology(transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE project_stage_name_current name
        SET declared_summary = jsonb_set(
            name.declared_summary,
            '{topology}',
            jsonb_build_object(
                'registry_path', jsonb_build_array(jsonb_build_object(
                    'logical_name_id', surface.logical_name_id,
                    'namespace', surface.namespace,
                    'normalized_name', surface.raw_name,
                    'canonical_display_name', surface.raw_name,
                    'namehash', surface.namehash,
                    'resource_id', binding.resource_id,
                    'binding_kind', binding.binding_kind
                )),
                'subregistry_path', '[]'::jsonb,
                'resolver_path', jsonb_build_array(source.resolver_hop),
                'wildcard', jsonb_build_object(
                    'source', source.name_ref,
                    'matched_labels', to_jsonb(
                        (string_to_array(surface.raw_name, '.'))[
                            1:cardinality(string_to_array(surface.raw_name, '.')) -
                              cardinality(string_to_array(source.raw_name, '.'))
                        ]
                    )
                ),
                'alias', jsonb_build_object(
                    'final_target', NULL,
                    'hops', '[]'::jsonb
                ),
                'version_boundaries', jsonb_build_object(
                    'topology_version_boundary', source.boundary,
                    'record_version_boundary', source.boundary
                ),
                'transport', jsonb_build_object(
                    'source_chain_id', NULL,
                    'target_chain_id', NULL,
                    'contract_address', NULL,
                    'latest_event_kind', NULL
                )
            ),
            true
        )
        FROM project_surfaces surface
        JOIN project_bindings binding
          ON binding.logical_name_id = surface.logical_name_id
         AND binding.binding_kind = 'observed_wildcard_path'
        JOIN LATERAL (
            SELECT ancestor.raw_name,
                   jsonb_build_object(
                       'logical_name_id', ancestor.logical_name_id,
                       'namespace', ancestor.namespace,
                       'normalized_name', ancestor.raw_name,
                       'canonical_display_name', ancestor.raw_name,
                       'namehash', ancestor.namehash,
                       'resource_id', ancestor_binding.resource_id,
                       'binding_kind', 'observed_wildcard_path'
                   ) AS name_ref,
                   jsonb_build_object(
                       'logical_name_id', ancestor.logical_name_id,
                       'namespace', ancestor.namespace,
                       'normalized_name', ancestor.raw_name,
                       'canonical_display_name', ancestor.raw_name,
                       'resource_id', ancestor_binding.resource_id,
                       'chain_id', resolver.chain_id,
                       'address', resolver.after_state ->> 'resolver',
                       'latest_event_kind', resolver.event_kind
                   ) AS resolver_hop,
                   jsonb_build_object(
                       'logical_name_id', ancestor.logical_name_id,
                       'resource_id', ancestor_binding.resource_id,
                       'normalized_event_id', CASE
                           WHEN boundary.event_kind = 'RecordVersionChanged'
                               THEN boundary.normalized_event_id
                           ELSE NULL
                       END,
                       'event_kind', CASE
                           WHEN boundary.event_kind = 'RecordVersionChanged'
                               THEN boundary.event_kind
                           ELSE NULL
                       END,
                       'chain_position', jsonb_build_object(
                           'chain_id', boundary.chain_id,
                           'block_number', boundary.block_number,
                           'block_hash', boundary.block_hash,
                           'timestamp', boundary.block_timestamp
                       )
                   ) AS boundary
            FROM project_surfaces ancestor
            JOIN project_bindings ancestor_binding
              ON ancestor_binding.logical_name_id = ancestor.logical_name_id
            JOIN LATERAL (
                SELECT event.*
                FROM project_events event
                WHERE event.logical_name_id = ancestor.logical_name_id
                  AND event.resource_id = ancestor_binding.resource_id
                  AND event.event_kind = 'ResolverChanged'
                  AND lower(COALESCE(event.after_state ->> 'resolver', '')) <>
                      '0x0000000000000000000000000000000000000000'
                ORDER BY event.block_number DESC NULLS LAST,
                         event.transaction_index DESC NULLS LAST,
                         event.log_index DESC NULLS LAST,
                         event.normalized_event_id DESC
                LIMIT 1
            ) resolver ON TRUE
            JOIN LATERAL (
                SELECT event.*, lineage.block_timestamp
                FROM project_events event
                JOIN chain_lineage lineage
                  ON lineage.chain_id = event.chain_id
                 AND lineage.block_hash = event.block_hash
                 AND lineage.block_number = event.block_number
                WHERE event.logical_name_id = ancestor.logical_name_id
                  AND event.resource_id = ancestor_binding.resource_id
                  AND event.event_kind IN (
                      'RecordVersionChanged', 'ResolverChanged'
                  )
                ORDER BY event.block_number DESC NULLS LAST,
                         event.transaction_index DESC NULLS LAST,
                         event.log_index DESC NULLS LAST,
                         event.normalized_event_id DESC
                LIMIT 1
            ) boundary ON TRUE
            WHERE ancestor.namespace = surface.namespace
              AND surface.raw_name LIKE '%.' || ancestor.raw_name
              AND ancestor.raw_name <> ''
            ORDER BY cardinality(string_to_array(ancestor.raw_name, '.')) DESC,
                     ancestor.logical_name_id
            LIMIT 1
        ) source ON TRUE
        WHERE name.logical_name_id = surface.logical_name_id
        "#,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to build wildcard name topology", error))?;
    Ok(())
}

async fn project_basenames_transport(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target: &Marker,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE project_stage_name_current name
        SET declared_summary = jsonb_set(
            name.declared_summary,
            '{topology}',
            jsonb_build_object(
                'registry_path', jsonb_build_array(jsonb_build_object(
                    'logical_name_id', surface.logical_name_id,
                    'namespace', surface.namespace,
                    'normalized_name', surface.raw_name,
                    'canonical_display_name', surface.raw_name,
                    'namehash', surface.namehash,
                    'resource_id', binding.resource_id,
                    'binding_kind', binding.binding_kind
                )),
                'subregistry_path', '[]'::jsonb,
                'resolver_path', jsonb_build_array(jsonb_build_object(
                    'logical_name_id', surface.logical_name_id,
                    'namespace', surface.namespace,
                    'normalized_name', surface.raw_name,
                    'canonical_display_name', surface.raw_name,
                    'resource_id', binding.resource_id,
                    'chain_id', resolver.chain_id,
                    'address', resolver.after_state ->> 'resolver',
                    'latest_event_kind', resolver.event_kind
                )),
                'wildcard', jsonb_build_object(
                    'source', NULL,
                    'matched_labels', '[]'::jsonb
                ),
                'alias', jsonb_build_object(
                    'final_target', NULL,
                    'hops', '[]'::jsonb
                ),
                'version_boundaries', jsonb_build_object(
                    'topology_version_boundary', boundary.value,
                    'record_version_boundary', boundary.value
                ),
                'transport', jsonb_build_object(
                    'source_chain_id', 'base-mainnet',
                    'target_chain_id', 'ethereum-mainnet',
                    'contract_address',
                        '0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31',
                    'latest_event_kind', NULL
                )
            ),
            true
        ),
        provenance = jsonb_set(
            name.provenance,
            '{manifest_versions}',
            COALESCE(name.provenance -> 'manifest_versions', '[]'::jsonb) ||
                jsonb_build_array(jsonb_build_object(
                    'source_family', execution_manifest.source_family,
                    'manifest_version', execution_manifest.manifest_version,
                    'chain', execution_manifest.chain_id,
                    'deployment_epoch', execution_manifest.deployment_label
                )),
            true
        ),
        chain_positions = name.chain_positions || jsonb_build_object(
            'ethereum', jsonb_build_object(
                'chain_id', execution_lineage.chain_id,
                'block_number', execution_lineage.block_number,
                'block_hash', execution_lineage.block_hash,
                'timestamp', execution_lineage.block_timestamp
            )
        ),
        manifest_version = GREATEST(
            name.manifest_version, execution_manifest.manifest_version
        )
        FROM project_surfaces surface
        JOIN project_bindings binding
          ON binding.logical_name_id = surface.logical_name_id
         AND binding.binding_kind = 'declared_registry_path'
        JOIN project_manifests execution_manifest
          ON execution_manifest.namespace = 'basenames'
         AND execution_manifest.source_family = 'basenames_execution'
         AND execution_manifest.chain_id = 'ethereum-mainnet'
         AND execution_manifest.manifest_version = 2
         AND execution_manifest.deployment_label = 'basenames_v1'
         AND execution_manifest.manifest_payload
             -> 'capability_flags' -> 'verified_resolution' ->> 'status' =
                 'supported'
         AND EXISTS (
             SELECT 1
             FROM jsonb_array_elements(COALESCE(
                 execution_manifest.manifest_payload -> 'contracts', '[]'::jsonb
             )) declaration
             WHERE declaration ->> 'role' = 'l1_resolver'
               AND lower(declaration ->> 'address') =
                   '0xde9049636f4a1dfe0a64d1bfe3155c0a14c54f31'
         )
        JOIN chain_lineage source_lineage
          ON source_lineage.chain_id = $1
         AND source_lineage.block_number = $2
         AND source_lineage.block_hash = $3
         AND source_lineage.canonicality_state IN (
             'canonical', 'safe', 'finalized'
         )
        JOIN LATERAL (
            SELECT lineage.*
            FROM chain_lineage lineage
            WHERE lineage.chain_id = 'ethereum-mainnet'
              AND lineage.block_timestamp <= source_lineage.block_timestamp
              AND lineage.canonicality_state IN (
                  'canonical', 'safe', 'finalized'
              )
            ORDER BY lineage.block_timestamp DESC,
                     lineage.block_number DESC,
                     lineage.block_hash DESC
            LIMIT 1
        ) execution_lineage ON TRUE
        JOIN LATERAL (
            SELECT event.*
            FROM project_events event
            WHERE event.logical_name_id = surface.logical_name_id
              AND event.resource_id = binding.resource_id
              AND event.event_kind = 'ResolverChanged'
              AND lower(COALESCE(event.after_state ->> 'resolver', '')) <>
                  '0x0000000000000000000000000000000000000000'
            ORDER BY event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST,
                     event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
            LIMIT 1
        ) resolver ON TRUE
        JOIN LATERAL (
            SELECT jsonb_build_object(
                       'logical_name_id', surface.logical_name_id,
                       'resource_id', binding.resource_id,
                       'normalized_event_id', CASE
                           WHEN event.event_kind = 'RecordVersionChanged'
                               THEN event.normalized_event_id
                           ELSE NULL
                       END,
                       'event_kind', CASE
                           WHEN event.event_kind = 'RecordVersionChanged'
                               THEN event.event_kind
                           ELSE NULL
                       END,
                       'chain_position', jsonb_build_object(
                           'chain_id', event.chain_id,
                           'block_number', event.block_number,
                           'block_hash', event.block_hash,
                           'timestamp', lineage.block_timestamp
                       )
                   ) AS value
            FROM project_events event
            JOIN chain_lineage lineage
              ON lineage.chain_id = event.chain_id
             AND lineage.block_hash = event.block_hash
             AND lineage.block_number = event.block_number
            WHERE event.logical_name_id = surface.logical_name_id
              AND event.resource_id = binding.resource_id
              AND event.event_kind IN (
                  'RecordVersionChanged', 'ResolverChanged'
              )
            ORDER BY event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST,
                     event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
            LIMIT 1
        ) boundary ON TRUE
        WHERE name.logical_name_id = surface.logical_name_id
          AND surface.namespace = 'basenames'
          AND $1 = 'base-mainnet'
        "#,
    )
    .bind(chain_id)
    .bind(target.number)
    .bind(&target.hash)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to build Basenames name topology", error))?;
    Ok(())
}
