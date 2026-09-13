use sqlx::{Postgres, Transaction};

use crate::{Marker, ProjectError, Result};

/// Serving pointers whose current resolver is a declared ENSv1 mirror resolver
/// (`docs/manifests.md` § ENSv1 mirror resolver declarations). The mirror stores no records, so
/// the ordinary node-keyed attribution finds nothing on it; `build` derives these resources'
/// inventory from the ENSv1 resolver the same name selects instead.
pub(super) async fn stage_pointers(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
) -> Result<()> {
    sqlx::query(
        r#"
        CREATE TEMP TABLE project_mirror_pointers ON COMMIT DROP AS
        SELECT pointer.*,
               resolver.support_status AS mirror_support_status,
               resolver.unsupported_reason AS mirror_unsupported_reason,
               resolver.manifest_version AS mirror_manifest_version,
               resolver.declared_summary #> '{classification,mirror}' AS mirror_classification,
               declaration_manifest.manifest_id IS NOT NULL AS mirror_namespace_matches
        FROM project_record_pointers pointer
        JOIN project_stage_resolver_current resolver
          ON resolver.chain_id = $1
         AND resolver.resolver_address = pointer.resolver_address
         AND resolver.declared_summary #>> '{classification,source_family}' = 'ens_v2_resolver_l1'
         AND resolver.declared_summary #>> '{classification,role}' = 'ensv1_mirror_resolver'
        LEFT JOIN project_manifests declaration_manifest
          ON declaration_manifest.manifest_id = (resolver.provenance ->> 'manifest_id')::bigint
         AND declaration_manifest.namespace = pointer.pointer_namespace
        WHERE pointer.pointer_source_family IN ('ens_v2_registry_l1', 'ens_v2_root_l1')
        "#,
    )
    .bind(chain_id)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to stage mirror resolver pointers", error))?;
    sqlx::query("CREATE INDEX ON project_mirror_pointers (resource_id)")
        .execute(&mut **transaction)
        .await
        .map_err(|error| {
            ProjectError::database("failed to index mirror resolver pointers", error)
        })?;
    Ok(())
}

/// One inventory row per mirror pointer, copied from the staged inventory of the ENSv1 resource
/// whose registry resolver the mirror would read: the latest ENSv1-family `ResolverChanged` for
/// the same namehash at the target. The mirror looks the name up in the ENSv1 registry and
/// forwards resolution to the resolver it finds there.
/// (upstream: .refs/ens_v2/contracts/src/resolver/ENSV1Resolver.sol:L38-L41 @ ens_v2@a971bd64)
/// (upstream: .refs/ens_v2/contracts/src/resolver/AbstractMirrorResolver.sol:L66-L74 @ ens_v2@a971bd64)
/// Only the exact node is modeled: a cleared or absent ENSv1 resolver, an ENSv1 side that is
/// itself a mirror, or an unsupported ENSv1 inventory publishes `mirrored_resolver_not_projected`
/// rather than walking to an ancestor resolver (`docs/upstream.md` § Known divergences).
pub(super) async fn build(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target: &Marker,
) -> Result<()> {
    sqlx::query(
        r#"
        WITH v1_pointers AS (
            SELECT DISTINCT ON (latest.namehash) latest.*
            FROM project_record_pointer_latest latest
            JOIN project_events event
              ON event.normalized_event_id = latest.pointer_event_id
            WHERE latest.pointer_source_family IN (
                'ens_v1_registry_l1', 'ens_v1_registrar_l1', 'ens_v1_wrapper_l1'
            )
            ORDER BY latest.namehash,
                     event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST,
                     event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
        ),
        mirrored AS (
            SELECT mirror.*,
                   v1.resource_id AS mirrored_resource_id,
                   v1.resolver_address AS mirrored_resolver_address,
                   v1.pointer_event_id AS mirrored_pointer_event_id,
                   v1.pointer_source_family AS mirrored_pointer_source_family,
                   inventory.record_version_boundary AS v1_boundary,
                   inventory.selectors AS v1_selectors,
                   inventory.unsupported_families AS v1_unsupported_families,
                   inventory.last_change AS v1_last_change,
                   inventory.entries AS v1_entries,
                   inventory.support_status AS v1_support_status,
                   inventory.unsupported_reason AS v1_unsupported_reason,
                   inventory.provenance AS v1_provenance,
                   inventory.chain_positions AS v1_chain_positions,
                   inventory.manifest_version AS v1_manifest_version,
                   inventory.resource_id IS NOT NULL
                       AND inventory.support_status = 'supported'
                       AND NOT EXISTS (
                           SELECT 1 FROM project_mirror_pointers nested
                           WHERE nested.resource_id = v1.resource_id
                       )
                       AND NOT EXISTS (
                           SELECT 1 FROM project_stage_resolver_current nested
                           WHERE nested.chain_id = $1
                             AND nested.resolver_address = v1.resolver_address
                             AND nested.declared_summary #>> '{classification,role}' =
                                 'ensv1_mirror_resolver'
                       ) AS v1_projected
            FROM project_mirror_pointers mirror
            LEFT JOIN v1_pointers v1
              ON v1.namehash = mirror.namehash
             AND v1.resolver_address IS NOT NULL
             AND v1.resolver_address NOT IN (
                 '0x0000000000000000000000000000000000000000', ''
             )
            LEFT JOIN project_stage_record_inventory_current inventory
              ON inventory.resource_id = v1.resource_id
             AND inventory.provenance ->> 'record_serving' IS DISTINCT FROM 'false'
        ),
        classified AS (
            SELECT mirrored.*,
                   mirror_support_status = 'supported'
                       AND mirror_namespace_matches
                       AND v1_projected AS supported,
                   CASE
                       WHEN mirror_support_status IS DISTINCT FROM 'supported'
                           THEN COALESCE(mirror_unsupported_reason, 'resolver_classification_missing')
                       WHEN NOT mirror_namespace_matches
                           THEN 'resolver_classification_missing'
                       WHEN NOT v1_projected
                           THEN 'mirrored_resolver_not_projected'
                   END AS unsupported_reason,
                   CASE WHEN v1_projected THEN v1_boundary ->> 'normalized_event_id' END
                       AS boundary_event_id,
                   CASE WHEN v1_projected THEN v1_boundary ->> 'event_kind' END
                       AS boundary_event_kind,
                   CASE WHEN v1_projected
                        THEN v1_boundary -> 'chain_position'
                        ELSE jsonb_strip_nulls(jsonb_build_object(
                            'chain_id', $1,
                            'block_number', boundary.block_number,
                            'block_hash', boundary.block_hash,
                            'timestamp', boundary.block_timestamp
                        ))
                   END AS boundary_position
            FROM mirrored
            LEFT JOIN chain_lineage boundary
              ON boundary.chain_id = $1
             AND boundary.block_number = mirrored.pointer_block_number
             AND boundary.block_hash = mirrored.pointer_block_hash
        )
        INSERT INTO project_stage_record_inventory_current (
            resource_id, record_version_boundary_key, record_version_boundary,
            selectors, unsupported_families, last_change, entries,
            support_status, unsupported_reason, provenance, chain_positions,
            canonicality_summary, manifest_version
        )
        SELECT resource_id,
               concat(
                   octet_length(logical_name_id), ':', logical_name_id, ';',
                   octet_length(resource_id::text), ':', resource_id::text, ';',
                   octet_length(COALESCE(boundary_event_id, '')), ':',
                   COALESCE(boundary_event_id, ''), ';',
                   octet_length(COALESCE(boundary_event_kind, '')), ':',
                   COALESCE(boundary_event_kind, ''), ';',
                   octet_length($1::text), ':', $1::text, ';',
                   octet_length(boundary_position ->> 'block_number'), ':',
                   boundary_position ->> 'block_number', ';',
                   octet_length(boundary_position ->> 'block_hash'), ':',
                   boundary_position ->> 'block_hash', ';',
                   octet_length(boundary_position ->> 'timestamp'), ':',
                   boundary_position ->> 'timestamp', ';'
               ),
               jsonb_build_object(
                   'logical_name_id', logical_name_id,
                   'resource_id', resource_id,
                   'normalized_event_id', boundary_event_id::bigint,
                   'event_kind', boundary_event_kind,
                   'chain_position', boundary_position
               ),
               CASE WHEN supported THEN v1_selectors ELSE '[]'::jsonb END,
               CASE WHEN supported THEN v1_unsupported_families
                    ELSE jsonb_build_array(jsonb_build_object(
                        'record_family', 'resolver_classification',
                        'unsupported_reason', unsupported_reason
                    )) END,
               CASE WHEN supported THEN v1_last_change
                    ELSE jsonb_build_object(
                        'normalized_event_id', pointer_event_id,
                        'event_kind', 'ResolverChanged',
                        'chain_position', boundary_position
                    ) END,
               CASE WHEN supported THEN v1_entries ELSE '[]'::jsonb END,
               CASE WHEN supported THEN 'supported' ELSE 'unsupported' END,
               unsupported_reason,
               jsonb_build_object(
                   'chain_id', $1,
                   'logical_name_id', logical_name_id,
                   'resolver_address', resolver_address,
                   'resolver_pointer_event_id', pointer_event_id,
                   'record_event_ids', CASE WHEN supported
                       THEN COALESCE(v1_provenance -> 'record_event_ids', '[]'::jsonb)
                       ELSE '[]'::jsonb END,
                   'record_link_event_ids', CASE WHEN supported
                       THEN COALESCE(v1_provenance -> 'record_link_event_ids', '[]'::jsonb)
                       ELSE '[]'::jsonb END,
                   'attributed_event_ids', CASE WHEN supported
                       THEN COALESCE(v1_provenance -> 'attributed_event_ids', '[]'::jsonb)
                       ELSE '[]'::jsonb END,
                   'read_rules', CASE WHEN supported
                       THEN COALESCE(v1_provenance -> 'read_rules', '[]'::jsonb)
                       ELSE '[]'::jsonb END,
                   'coverage', jsonb_build_object(
                       'status', 'projected',
                       'exhaustiveness', 'not_asserted'
                   ),
                   'mirror', jsonb_strip_nulls(jsonb_build_object(
                       'resolver_address', resolver_address,
                       'mirrored_source_family', 'ens_v1_resolver_l1',
                       'mirrored_registry_source_family', 'ens_v1_registry_l1',
                       'mirrored_registry_address',
                           mirror_classification ->> 'mirrored_registry_address',
                       'mirrored_resolver_address', mirrored_resolver_address,
                       'mirrored_resource_id', mirrored_resource_id,
                       'mirrored_pointer_event_id', mirrored_pointer_event_id,
                       'mirrored_pointer_source_family', mirrored_pointer_source_family,
                       'mirrored_unsupported_reason',
                           CASE WHEN NOT supported THEN v1_unsupported_reason END
                   ))
               ) || CASE WHEN supported AND v1_provenance ? 'exact_nonempty_not_found_record_keys'
                   THEN jsonb_build_object('exact_nonempty_not_found_record_keys',
                       v1_provenance -> 'exact_nonempty_not_found_record_keys')
                   ELSE '{}'::jsonb END,
               jsonb_strip_nulls(jsonb_build_object(
                   'block_number', CASE
                       WHEN supported
                        AND (v1_chain_positions ->> 'block_number')::bigint >= pointer_block_number
                           THEN v1_chain_positions -> 'block_number'
                       ELSE to_jsonb(pointer_block_number) END,
                   'block_hash', CASE
                       WHEN supported
                        AND (v1_chain_positions ->> 'block_number')::bigint >= pointer_block_number
                           THEN v1_chain_positions -> 'block_hash'
                       ELSE to_jsonb(pointer_block_hash) END,
                   'target_block_number', $2,
                   'target_block_hash', $3
               )),
               jsonb_build_object(
                   'state', 'canonical_lineage',
                   'target_block_number', $2,
                   'target_block_hash', $3
               ),
               GREATEST(
                   pointer_manifest_version,
                   COALESCE(mirror_manifest_version, 1),
                   COALESCE(CASE WHEN supported THEN v1_manifest_version END, 1)
               )
        FROM classified
        ORDER BY resource_id
        "#,
    )
    .bind(chain_id)
    .bind(target.number)
    .bind(&target.hash)
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        ProjectError::database("failed to build mirrored record_inventory_current rows", error)
    })?;
    Ok(())
}
