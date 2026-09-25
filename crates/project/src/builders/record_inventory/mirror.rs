use sqlx::{Postgres, Transaction};

use crate::{Marker, ProjectError, Result};

/// ENSv1 registry-side pointer families whose `ResolverChanged` events state the ENSv1 registry's
/// current resolver for a node.
const V1_POINTER_FAMILIES: &str =
    "'ens_v1_registry_l1', 'ens_v1_registrar_l1', 'ens_v1_wrapper_l1'";

/// Stage the pointers whose current resolver is a declared ENSv1 mirror resolver
/// (`docs/manifests.md` § ENSv1 mirror resolver declarations) and select, for each, the ENSv1
/// resolver the mirror would call.
///
/// The mirror finds the resolver with `RegistryUtils.findResolver` over the ENSv1 registry: it
/// walks the DNS-encoded name toward the root and returns the nearest node with a nonzero registry
/// resolver, the exact node first; the root node itself is never consulted. It keeps that resolver
/// only when it was found at the queried node or supports `IExtendedResolver`, and otherwise
/// answers with no resolver instead of walking on to a farther ancestor.
/// (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/resolver/ENSV1Resolver.sol:L40-L43 @ ens_v2_sepolia_20260916@366de741)
/// (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/universalResolver/libraries/LibResolution.sol:L39-L48 @ ens_v2_sepolia_20260916@366de741)
/// (upstream: .refs/ens_v1/contracts/universalResolver/RegistryUtils.sol:L25-L38 @ ens_v1@91c966f)
/// It then calls a kept resolver with the original calldata: an immediate resolver receives the
/// queried node's getter call directly, so it answers with its own storage for the queried node,
/// while an `IExtendedResolver` receives `resolve(name, data)` and answers by its own logic.
/// (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/resolver/AbstractMirrorResolver.sol:L66-L74 @ ens_v2_sepolia_20260916@366de741)
/// (upstream: .refs/ens_v1/contracts/universalResolver/ResolverCaller.sol:L66-L70 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/universalResolver/ResolverCaller.sol:L88-L96 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/universalResolver/ResolverCaller.sol:L108-L127 @ ens_v1@91c966f)
///
/// `project_mirror_substituted_pointers` therefore re-points each derivable mirrored resource at
/// the resolver selected at the queried node itself, so the ordinary node-keyed attribution
/// computes exactly the records that resolver stores for the queried node. A nearest resolver on
/// an ancestor is never derived through: the row is unsupported with
/// `ensip10_extended_resolver` or `ancestor_resolver_not_extended`.
pub(super) async fn stage_pointers(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
) -> Result<()> {
    crate::stage::mirror_evidence::create(transaction).await?;
    crate::stage::mirror_evidence::classifications(transaction, chain_id).await?;
    let pointer_sql = r#"/* project:builders.record_inventory.mirror.stage_pointers.create_mirror_pointers */
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
        "#;
    let pointer_sql =
        crate::stage::mirror_evidence::classification_sql(transaction, pointer_sql, true).await?;
    sqlx::query(&pointer_sql)
        .bind(chain_id)
        .execute(&mut **transaction)
        .await
        .map_err(|error| {
            ProjectError::database("failed to stage mirror resolver pointers", error)
        })?;
    sqlx::query("/* project:builders.record_inventory.mirror.stage_pointers.index_mirror_pointers_resource_id */ CREATE INDEX ON project_mirror_pointers (resource_id)")
        .execute(&mut **transaction)
        .await
        .map_err(|error| {
            ProjectError::database("failed to index mirror resolver pointers", error)
        })?;

    let selection = format!(
        r#"/* project:builders.record_inventory.mirror.stage_pointers.create_mirror_selection */
        CREATE TEMP TABLE project_mirror_selection ON COMMIT DROP AS
        WITH registry_state AS (
            -- The ENSv1 registry's current resolver per node: the latest canonical registry-side
            -- ResolverChanged for the node, clears included, so a cleared exact node falls through
            -- to its ancestors. The registry sets resolvers for nodes without any ENSv1 owner or
            -- surface link, so this is keyed by node, not by the event's resource or logical name
            -- (a pre-surface pointer keeps both null; docs/storage.md). The node is the name the
            -- event addresses, read in the adapters' shared order (V1_EVENT_NODE_FIELDS in
            -- crates/adapters/src/schema_v2/seam.rs): a derived child pointer keeps its NewOwner
            -- observation, whose `node` is the parent and whose `child_node` is the child.
            SELECT DISTINCT ON (lower(COALESCE(event.after_state ->> 'child_node', event.after_state ->> 'namehash', event.after_state ->> 'node')))
                   lower(COALESCE(event.after_state ->> 'child_node', event.after_state ->> 'namehash', event.after_state ->> 'node')) AS namehash,
                   event.resource_id,
                   event.namespace AS pointer_namespace,
                   event.source_family AS pointer_source_family,
                   lower(event.after_state ->> 'resolver') AS resolver_address,
                   event.manifest_version AS pointer_manifest_version,
                   event.normalized_event_id AS pointer_event_id,
                   event.block_number AS v1_block_number, event.block_hash AS v1_block_hash,
                   surface.namespace, surface.raw_name, surface.raw_labels
            FROM project_events event
            JOIN project_surfaces surface
              ON lower(surface.namehash) = lower(COALESCE(event.after_state ->> 'child_node', event.after_state ->> 'namehash', event.after_state ->> 'node'))
             AND surface.namespace = event.namespace
            WHERE event.event_kind = 'ResolverChanged'
              AND event.source_family IN ({V1_POINTER_FAMILIES})
              AND COALESCE(event.after_state ->> 'child_node', event.after_state ->> 'namehash', event.after_state ->> 'node') IS NOT NULL
            ORDER BY lower(COALESCE(event.after_state ->> 'child_node', event.after_state ->> 'namehash', event.after_state ->> 'node')),
                     event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST,
                     event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
        ),
        walk AS (
            -- Every registry node the walk consults for the queried name: the name itself and each
            -- proper ancestor below the root, deepest first.
            SELECT mirror.resource_id, queried.namespace, position - 1 AS ancestor_depth,
                   queried.raw_labels[position : cardinality(queried.raw_labels)] AS suffix
            FROM project_mirror_pointers mirror
            JOIN project_surfaces queried
              ON queried.logical_name_id = mirror.logical_name_id
            CROSS JOIN generate_series(1, cardinality(queried.raw_labels)) AS position
        ),
        candidates AS (
            SELECT walk.resource_id,
                   walk.ancestor_depth,
                   registry.resource_id AS mirrored_resource_id,
                   registry.namehash AS mirrored_node,
                   registry.raw_name AS mirrored_name,
                   registry.resolver_address AS mirrored_resolver_address,
                   registry.pointer_event_id AS mirrored_pointer_event_id,
                   registry.pointer_source_family AS mirrored_pointer_source_family,
                   registry.pointer_namespace AS mirrored_pointer_namespace,
                   registry.pointer_manifest_version AS mirrored_pointer_manifest_version,
                   registry.v1_block_number AS mirrored_block_number,
                   registry.v1_block_hash AS mirrored_block_hash
            FROM walk
            JOIN registry_state registry
              ON registry.namespace = walk.namespace
             AND registry.raw_labels = walk.suffix
            WHERE registry.resolver_address IS NOT NULL
              AND registry.resolver_address NOT IN (
                  '0x0000000000000000000000000000000000000000', ''
              )
        ),
        nearest AS (
            SELECT DISTINCT ON (resource_id) *
            FROM candidates
            ORDER BY resource_id, ancestor_depth ASC, mirrored_pointer_event_id DESC
        )
        SELECT nearest.*,
               CASE WHEN COALESCE(
                        resolver.declared_summary #> '{{classification,read_features}}',
                        '[]'::jsonb
                    ) ? 'ensip10_extended_resolver'
                    THEN 'extended_resolve' ELSE 'direct_call' END AS forwarding,
               CASE
                   WHEN resolver.resolver_address IS NULL
                       THEN 'resolver_classification_missing'
                   WHEN resolver.declared_summary #>> '{{classification,role}}' =
                        'ensv1_mirror_resolver'
                       THEN 'mirrored_resolver_is_mirror'
                   WHEN resolver.support_status IS DISTINCT FROM 'supported'
                       THEN COALESCE(
                           resolver.unsupported_reason, 'resolver_classification_missing'
                       )
                   WHEN resolver.declared_summary #>> '{{classification,source_family}}' <>
                        'ens_v1_resolver_l1'
                       THEN 'mirrored_resolver_not_ensv1'
                   WHEN declaration_manifest.manifest_id IS NULL
                       THEN 'resolver_classification_missing'
                   WHEN nearest.ancestor_depth > 0
                    AND COALESCE(
                        resolver.declared_summary #> '{{classification,read_features}}',
                        '[]'::jsonb
                    ) ? 'ensip10_extended_resolver'
                       THEN 'ensip10_extended_resolver'
                   -- The mirror keeps a resolver found above the queried node only when it is an
                   -- ENSIP-10 extended resolver and otherwise answers with no resolver; it does
                   -- not walk on to a farther ancestor.
                   WHEN nearest.ancestor_depth > 0
                       THEN 'ancestor_resolver_not_extended'
               END AS mirrored_unsupported_reason,
               COALESCE(resolver.manifest_version, 1) AS mirrored_resolver_manifest_version
        FROM nearest
        LEFT JOIN project_stage_resolver_current resolver
          ON resolver.chain_id = $1
         AND resolver.resolver_address = nearest.mirrored_resolver_address
        LEFT JOIN project_manifests declaration_manifest
          ON declaration_manifest.manifest_id = (resolver.provenance ->> 'manifest_id')::bigint
         AND declaration_manifest.namespace = nearest.mirrored_pointer_namespace
        "#
    );
    let selection =
        crate::stage::mirror_evidence::classification_sql(transaction, &selection, true).await?;
    sqlx::query(&selection)
        .bind(chain_id)
        .execute(&mut **transaction)
        .await
        .map_err(|error| {
            ProjectError::database("failed to select mirrored ENSv1 resolvers", error)
        })?;
    sqlx::query(
        r#"/* project:builders.record_inventory.mirror.stage_pointers.create_mirror_substituted_pointers */
        CREATE TEMP TABLE project_mirror_substituted_pointers ON COMMIT DROP AS
        SELECT mirror.resource_id,
               mirror.logical_name_id,
               selection.mirrored_pointer_namespace AS pointer_namespace,
               selection.mirrored_pointer_source_family AS pointer_source_family,
               mirror.namehash,
               selection.mirrored_resolver_address AS resolver_address,
               GREATEST(
                   mirror.pointer_manifest_version,
                   selection.mirrored_pointer_manifest_version
               ) AS pointer_manifest_version,
               mirror.pointer_event_id,
               CASE WHEN selection.mirrored_block_number > mirror.pointer_block_number
                    THEN selection.mirrored_block_number
                    ELSE mirror.pointer_block_number END AS pointer_block_number,
               CASE WHEN selection.mirrored_block_number > mirror.pointer_block_number
                    THEN selection.mirrored_block_hash
                    ELSE mirror.pointer_block_hash END AS pointer_block_hash
        FROM project_mirror_pointers mirror
        JOIN project_mirror_selection selection USING (resource_id)
        WHERE mirror.mirror_support_status = 'supported'
          AND mirror.mirror_namespace_matches
          AND selection.mirrored_unsupported_reason IS NULL
        "#,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        ProjectError::database("failed to substitute mirrored resolver pointers", error)
    })?;
    for statement in [
        "/* project:builders.record_inventory.mirror.stage_pointers.index_mirror_selection_resource_id */ CREATE INDEX ON project_mirror_selection (resource_id)",
        "/* project:builders.record_inventory.mirror.stage_pointers.index_mirror_substituted_pointers_resource_id */ CREATE INDEX ON project_mirror_substituted_pointers (resource_id)",
    ] {
        sqlx::query(statement)
            .execute(&mut **transaction)
            .await
            .map_err(|error| {
                ProjectError::database("failed to index mirrored resolver selection", error)
            })?;
    }
    Ok(())
}

pub(super) async fn build_inventory(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target: &Marker,
    statement: &str,
) -> Result<()> {
    let statement =
        crate::stage::mirror_evidence::classification_sql(transaction, statement, false).await?;
    sqlx::query(&statement)
        .bind(chain_id)
        .bind(target.number)
        .bind(&target.hash)
        .execute(&mut **transaction)
        .await
        .map_err(|error| {
            ProjectError::database("failed to build record_inventory_current", error)
        })?;
    Ok(())
}

/// The `provenance.mirror` object shared by derived and unsupported mirrored rows.
const MIRROR_PROVENANCE: &str = r#"
jsonb_strip_nulls(jsonb_build_object(
    'resolver_address', mirror.resolver_address,
    'mirrored_source_family', 'ens_v1_resolver_l1',
    'mirrored_registry_source_family', 'ens_v1_registry_l1',
    'mirrored_registry_address', mirror.mirror_classification ->> 'mirrored_registry_address',
    'queried_node', mirror.namehash,
    'mirrored_node', selection.mirrored_node,
    'mirrored_name', selection.mirrored_name,
    'ancestor_depth', selection.ancestor_depth,
    'forwarding', selection.forwarding,
    'mirrored_resolver_address', selection.mirrored_resolver_address,
    'mirrored_resource_id', selection.mirrored_resource_id,
    'mirrored_pointer_event_id', selection.mirrored_pointer_event_id,
    'mirrored_pointer_source_family', selection.mirrored_pointer_source_family,
    'mirrored_unsupported_reason', selection.mirrored_unsupported_reason
))
"#;

/// Finish the mirrored rows after the ordinary build: rows computed through a substituted pointer
/// take the mirror back as `provenance.resolver_address` and gain `provenance.mirror`; every other
/// mirror pointer publishes an explicit `mirrored_resolver_not_projected` row (or the mirror's own
/// classification reason) with no entries and no read rules.
pub(super) async fn build(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target: &Marker,
) -> Result<()> {
    let finish = format!(
        r#"/* project:builders.record_inventory.mirror.build.update_stage_record_inventory_current */
        UPDATE project_stage_record_inventory_current inventory
        SET provenance = inventory.provenance || jsonb_build_object(
                'resolver_address', mirror.resolver_address,
                'mirror', {MIRROR_PROVENANCE}
            ),
            unsupported_reason = CASE WHEN inventory.support_status = 'unsupported'
                THEN 'mirrored_resolver_not_projected' ELSE inventory.unsupported_reason END,
            unsupported_families = CASE WHEN inventory.support_status = 'unsupported'
                THEN (
                    SELECT COALESCE(jsonb_agg(CASE
                        WHEN family ->> 'record_family' = 'resolver_classification'
                            THEN jsonb_build_object(
                                'record_family', 'resolver_classification',
                                'unsupported_reason', 'mirrored_resolver_not_projected'
                            )
                        ELSE family END), '[]'::jsonb)
                    FROM jsonb_array_elements(inventory.unsupported_families) family
                )
                ELSE inventory.unsupported_families END,
            manifest_version = GREATEST(
                inventory.manifest_version,
                COALESCE(mirror.mirror_manifest_version, 1),
                selection.mirrored_resolver_manifest_version
            )
        FROM project_mirror_substituted_pointers substituted
        JOIN project_mirror_pointers mirror USING (resource_id)
        JOIN project_mirror_selection selection USING (resource_id)
        WHERE inventory.resource_id = substituted.resource_id
        "#
    );
    sqlx::query(&finish)
        .execute(&mut **transaction)
        .await
        .map_err(|error| {
            ProjectError::database("failed to finish derived mirrored inventory rows", error)
        })?;

    let unsupported = format!(
        r#"/* project:builders.record_inventory.mirror.build.insert_stage_record_inventory_current */
        WITH classified AS (
            SELECT mirror.*,
                   CASE
                       WHEN mirror.mirror_support_status IS DISTINCT FROM 'supported'
                           THEN COALESCE(
                               mirror.mirror_unsupported_reason, 'resolver_classification_missing'
                           )
                       WHEN NOT mirror.mirror_namespace_matches
                           THEN 'resolver_classification_missing'
                       ELSE 'mirrored_resolver_not_projected'
                   END AS unsupported_reason,
                   {MIRROR_PROVENANCE} AS mirror_provenance,
                   jsonb_strip_nulls(jsonb_build_object(
                       'chain_id', $1,
                       'block_number', boundary.block_number,
                       'block_hash', boundary.block_hash,
                       'timestamp', boundary.block_timestamp
                   )) AS boundary_position,
                   COALESCE(selection.mirrored_resolver_manifest_version, 1)
                       AS mirrored_resolver_manifest_version
            FROM project_mirror_pointers mirror
            LEFT JOIN project_mirror_selection selection USING (resource_id)
            LEFT JOIN chain_lineage boundary
              ON boundary.chain_id = $1
             AND boundary.block_number = mirror.pointer_block_number
             AND boundary.block_hash = mirror.pointer_block_hash
            WHERE NOT EXISTS (
                SELECT 1 FROM project_mirror_substituted_pointers substituted
                WHERE substituted.resource_id = mirror.resource_id
            )
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
                   '0:;', '0:;',
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
                   'normalized_event_id', NULL,
                   'event_kind', NULL,
                   'chain_position', boundary_position
               ),
               '[]'::jsonb,
               jsonb_build_array(jsonb_build_object(
                   'record_family', 'resolver_classification',
                   'unsupported_reason', unsupported_reason
               )),
               jsonb_build_object(
                   'normalized_event_id', pointer_event_id,
                   'event_kind', 'ResolverChanged',
                   'chain_position', boundary_position
               ),
               '[]'::jsonb,
               'unsupported',
               unsupported_reason,
               jsonb_build_object(
                   'chain_id', $1,
                   'logical_name_id', logical_name_id,
                   'resolver_address', resolver_address,
                   'resolver_pointer_event_id', pointer_event_id,
                   'record_event_ids', '[]'::jsonb,
                   'record_link_event_ids', '[]'::jsonb,
                   'attributed_event_ids', '[]'::jsonb,
                   'read_rules', '[]'::jsonb,
                   'coverage', jsonb_build_object(
                       'status', 'projected',
                       'exhaustiveness', 'not_asserted'
                   ),
                   'mirror', mirror_provenance
               ),
               jsonb_strip_nulls(jsonb_build_object(
                   'block_number', pointer_block_number,
                   'block_hash', pointer_block_hash,
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
                   mirrored_resolver_manifest_version
               )
        FROM classified
        ORDER BY resource_id
        "#
    );
    sqlx::query(&unsupported)
        .bind(chain_id)
        .bind(target.number)
        .bind(&target.hash)
        .execute(&mut **transaction)
        .await
        .map_err(|error| {
            ProjectError::database("failed to build unsupported mirrored inventory rows", error)
        })?;
    Ok(())
}
