use sqlx::{Postgres, Transaction};

use crate::{Marker, ProjectError, Result};

/// A selected clear leaves the registration with no pointer to serve records through, so the
/// inventory build publishes no row for it. History still has to list the writes the registration
/// made while it did have a resolver, and registration-scoped history reads those ids back from
/// `record_inventory_current.provenance.attributed_event_ids`. Publish a history-only row anchored
/// on the clearing pointer: no selectors, no entries, no `resolver_address`, and
/// `provenance.record_serving = false` so every record-serving read filters it out and the routes
/// keep answering a cleared name exactly as they do today (`inventory_not_available`).
pub(super) async fn build(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target: &Marker,
) -> Result<()> {
    sqlx::query(
        r#"
        WITH cleared_pointers AS (
            SELECT latest.*
            FROM project_record_pointer_latest latest
            WHERE NOT EXISTS (
                SELECT 1 FROM project_record_pointers serving
                WHERE serving.resource_id = latest.resource_id
            )
        ),
        attribution AS (
            SELECT attributed.resource_id,
                   jsonb_agg(to_jsonb(attributed.normalized_event_id)
                             ORDER BY attributed.normalized_event_id) AS event_ids
            FROM (
                SELECT DISTINCT resource_id, normalized_event_id
                FROM project_record_history_attribution
            ) attributed
            GROUP BY attributed.resource_id
        )
        INSERT INTO project_stage_record_inventory_current (
            resource_id, record_version_boundary_key, record_version_boundary,
            selectors, unsupported_families, last_change, entries,
            support_status, unsupported_reason, provenance, chain_positions,
            canonicality_summary, manifest_version
        )
        SELECT pointer.resource_id,
               concat(
                   octet_length(pointer.logical_name_id), ':',
                   pointer.logical_name_id, ';',
                   octet_length(pointer.resource_id::text), ':',
                   pointer.resource_id::text, ';',
                   octet_length(pointer.pointer_event_id::text), ':',
                   pointer.pointer_event_id::text, ';',
                   octet_length('ResolverChanged'), ':', 'ResolverChanged', ';',
                   octet_length($1::text), ':', $1::text, ';',
                   octet_length(boundary.block_number::text), ':',
                   boundary.block_number::text, ';',
                   octet_length(boundary.block_hash), ':', boundary.block_hash, ';',
                   octet_length(to_jsonb(boundary.block_timestamp) #>> '{}'), ':',
                   to_jsonb(boundary.block_timestamp) #>> '{}', ';'
               ),
               jsonb_build_object(
                   'logical_name_id', pointer.logical_name_id,
                   'resource_id', pointer.resource_id,
                   'normalized_event_id', pointer.pointer_event_id,
                   'event_kind', 'ResolverChanged',
                   'chain_position', jsonb_build_object(
                       'chain_id', $1,
                       'block_number', boundary.block_number,
                       'block_hash', boundary.block_hash,
                       'timestamp', boundary.block_timestamp
                   )
               ),
               '[]'::jsonb,
               '[]'::jsonb,
               jsonb_build_object(
                   'normalized_event_id', pointer.pointer_event_id,
                   'event_kind', 'ResolverChanged',
                   'chain_position', jsonb_build_object(
                       'chain_id', $1,
                       'block_number', boundary.block_number,
                       'block_hash', boundary.block_hash,
                       'timestamp', boundary.block_timestamp
                   )
               ),
               '[]'::jsonb,
               'unsupported',
               'resolver_pointer_cleared',
               jsonb_build_object(
                   'chain_id', $1,
                   'logical_name_id', pointer.logical_name_id,
                   'resolver_pointer_event_id', pointer.pointer_event_id,
                   'record_serving', false,
                   'record_event_ids', '[]'::jsonb,
                   'record_link_event_ids', '[]'::jsonb,
                   'attributed_event_ids', attribution.event_ids,
                   'read_rules', '[]'::jsonb,
                   'coverage', jsonb_build_object(
                       'status', 'projected',
                       'exhaustiveness', 'not_asserted'
                   )
               ),
               jsonb_strip_nulls(jsonb_build_object(
                   'block_number', boundary.block_number,
                   'block_hash', boundary.block_hash,
                   'target_block_number', $2,
                   'target_block_hash', $3
               )),
               jsonb_build_object(
                   'state', 'canonical_lineage',
                   'target_block_number', $2,
                   'target_block_hash', $3
               ),
               pointer.pointer_manifest_version
        FROM cleared_pointers pointer
        JOIN attribution ON attribution.resource_id = pointer.resource_id
        JOIN chain_lineage boundary
          ON boundary.chain_id = $1
         AND boundary.block_number = pointer.pointer_block_number
         AND boundary.block_hash = pointer.pointer_block_hash
        ORDER BY pointer.resource_id
        "#,
    )
    .bind(chain_id)
    .bind(target.number)
    .bind(&target.hash)
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        ProjectError::database(
            "failed to build cleared-pointer record_inventory_current history rows",
            error,
        )
    })?;
    Ok(())
}
