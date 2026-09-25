use sqlx::{Postgres, Transaction};

use crate::{ProjectError, Result};

pub(super) async fn build(transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
    sqlx::query(PROJECT_DIRECT_TOPOLOGY)
        .execute(&mut **transaction)
        .await
        .map_err(|error| {
            ProjectError::database("failed to build direct ENS name topology", error)
        })?;
    Ok(())
}

pub(in crate::builders) const PROJECT_DIRECT_TOPOLOGY: &str = r#"/* project:builders.name_topology.direct */
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
                    'chain_id', name.declared_summary -> 'resolver' ->> 'chain_id',
                    'address', name.declared_summary -> 'resolver' ->> 'address',
                    'latest_event_kind',
                        name.declared_summary -> 'resolver' ->> 'latest_event_kind'
                )),
                'wildcard', jsonb_build_object(
                    'source', NULL, 'matched_labels', '[]'::jsonb
                ),
                'alias', jsonb_build_object(
                    'final_target', NULL, 'hops', '[]'::jsonb
                ),
                'version_boundaries', jsonb_build_object(
                    'topology_version_boundary', inventory.record_version_boundary,
                    'record_version_boundary', inventory.record_version_boundary
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
         AND binding.binding_kind = 'declared_registry_path'
         AND binding.active_to IS NULL
         AND binding.authority_arm IN ('ens_v1', 'ens_v2')
        JOIN project_stage_record_inventory_current inventory
          ON inventory.resource_id = binding.resource_id
         AND inventory.provenance ->> 'record_serving' IS DISTINCT FROM 'false'
        WHERE name.logical_name_id = surface.logical_name_id
          AND name.surface_binding_id = binding.surface_binding_id
          AND name.resource_id = binding.resource_id
          AND surface.namespace = 'ens'
          AND jsonb_typeof(name.declared_summary -> 'topology') IS DISTINCT FROM 'object'
          AND name.declared_summary -> 'resolver' ->> 'address' IS NOT NULL
          AND name.declared_summary -> 'resolver' ->> 'chain_id' IS NOT NULL
        "#;
