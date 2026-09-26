//! Persists schema-v2 adapter output the way Interpret does for the rows Project reads: token
//! lineages, resources, name surfaces, surface bindings with their closures, and normalized
//! events with every column Interpret writes. Manifest versions, label preimages, contract
//! identity and discovery rows are left to the caller.
use anyhow::Result;
use bigname_adapters::schema_v2::BatchOutput;
use sqlx::PgPool;

pub async fn persist_output(pool: &PgPool, output: &BatchOutput) -> Result<()> {
    for lineage in &output.token_lineages {
        sqlx::query(
            "INSERT INTO token_lineages (
                 token_lineage_id, chain_id, block_hash, block_number, provenance,
                 canonicality_state
             ) VALUES ($1, $2, $3, $4, $5, $6::canonicality_state)
             ON CONFLICT (token_lineage_id) DO NOTHING",
        )
        .bind(lineage.token_lineage_id)
        .bind(&lineage.chain_id)
        .bind(&lineage.block_hash)
        .bind(lineage.block_number)
        .bind(&lineage.provenance)
        .bind(&lineage.canonicality_state)
        .execute(pool)
        .await?;
    }
    for resource in &output.resources {
        sqlx::query(
            "INSERT INTO resources (
                 resource_id, token_lineage_id, chain_id, block_hash, block_number,
                 provenance, canonicality_state
             ) VALUES ($1, $2, $3, $4, $5, $6, $7::canonicality_state)
             ON CONFLICT (resource_id) DO NOTHING",
        )
        .bind(resource.resource_id)
        .bind(resource.token_lineage_id)
        .bind(&resource.chain_id)
        .bind(&resource.block_hash)
        .bind(resource.block_number)
        .bind(&resource.provenance)
        .bind(&resource.canonicality_state)
        .execute(pool)
        .await?;
    }
    for surface in &output.name_surfaces {
        sqlx::query(
            "INSERT INTO name_surfaces (
                 logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name,
                 namehash, labelhashes, normalizer_version, visibility_state,
                 normalization_errors, deactivation_reason, deactivated_at, chain_id,
                 block_hash, block_number, provenance, canonicality_state
             ) VALUES (
                 $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
                 $17::canonicality_state
             )
             ON CONFLICT (logical_name_id) DO NOTHING",
        )
        .bind(&surface.logical_name_id)
        .bind(&surface.namespace)
        .bind(&surface.raw_name)
        .bind(&surface.raw_labels)
        .bind(&surface.dns_encoded_name)
        .bind(&surface.namehash)
        .bind(&surface.labelhashes)
        .bind(&surface.normalizer_version)
        .bind(&surface.visibility_state)
        .bind(&surface.normalization_errors)
        .bind(&surface.deactivation_reason)
        .bind(surface.deactivated_at)
        .bind(&surface.chain_id)
        .bind(&surface.block_hash)
        .bind(surface.block_number)
        .bind(&surface.provenance)
        .bind(&surface.canonicality_state)
        .execute(pool)
        .await?;
    }
    for closure in &output.binding_closures {
        sqlx::query(
            "UPDATE surface_bindings
             SET active_to = $2
             WHERE logical_name_id = $1
               AND chain_id = $3
               AND authority_arm = $4
               AND ($5::uuid IS NULL OR surface_binding_id <> $5)
               AND (
                   block_number < $6
                   OR (
                       block_number = $6
                       AND (
                           COALESCE((provenance ->> 'transaction_index')::bigint, -1),
                           COALESCE((provenance ->> 'log_index')::bigint, -1)
                       ) < ($7, $8)
                   )
               )
               AND (active_to IS NULL OR active_to > $2)",
        )
        .bind(&closure.logical_name_id)
        .bind(closure.active_to)
        .bind(&closure.chain_id)
        .bind(&closure.authority_arm)
        .bind(closure.except_surface_binding_id)
        .bind(closure.block_number)
        .bind(closure.transaction_index)
        .bind(closure.log_index)
        .execute(pool)
        .await?;
    }
    for binding in &output.surface_bindings {
        sqlx::query(
            "INSERT INTO surface_bindings (
                 surface_binding_id, logical_name_id, resource_id, binding_kind,
                 authority_arm, active_from, chain_id, block_hash, block_number,
                 provenance, canonicality_state
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::canonicality_state)",
        )
        .bind(binding.surface_binding_id)
        .bind(&binding.logical_name_id)
        .bind(binding.resource_id)
        .bind(&binding.binding_kind)
        .bind(&binding.authority_arm)
        .bind(binding.active_from)
        .bind(&binding.chain_id)
        .bind(&binding.block_hash)
        .bind(binding.block_number)
        .bind(&binding.provenance)
        .bind(&binding.canonicality_state)
        .execute(pool)
        .await?;
    }
    for event in &output.normalized_events {
        sqlx::query(
            "INSERT INTO normalized_events (
                 event_identity, namespace, logical_name_id, resource_id, event_kind,
                 source_family, manifest_version, source_manifest_id, chain_id,
                 block_number, block_hash, transaction_hash, transaction_index, log_index,
                 raw_fact_ref, derivation_kind, canonicality_state, before_state,
                 after_state, migration_correlation_ids, consumer_visibility
             ) VALUES (
                 $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
                 $17::canonicality_state, $18, $19, $20, $21
             )",
        )
        .bind(&event.event_identity)
        .bind(&event.namespace)
        .bind(&event.logical_name_id)
        .bind(event.resource_id)
        .bind(&event.event_kind)
        .bind(&event.source_family)
        .bind(event.manifest_version)
        .bind(event.source_manifest_id)
        .bind(&event.chain_id)
        .bind(event.block_number)
        .bind(&event.block_hash)
        .bind(&event.transaction_hash)
        .bind(event.transaction_index)
        .bind(event.log_index)
        .bind(&event.raw_fact_ref)
        .bind(&event.derivation_kind)
        .bind(&event.canonicality_state)
        .bind(&event.before_state)
        .bind(&event.after_state)
        .bind(&event.migration_correlation_ids)
        .bind(&event.consumer_visibility)
        .execute(pool)
        .await?;
    }
    Ok(())
}
