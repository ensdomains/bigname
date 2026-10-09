//! One SQL work selection shared by Project consumers. Physical entry changes retain their key.
//! A deduplicated registry walk consumes observed name prefixes, so repeated registries terminate
//! by name depth and nested/rebound mounts are dependencies without a persistent graph.
//! Missing/current/former membership and divergence evidence never restrict this candidate set.
//! SourceManifestUpdated has no block position. Supported manifest sync invalidates phase hashes
//! and requires a complete Interpret/Project redo; it is not an incremental event-range trigger.
use bigname_storage::families::name::PHYSICAL_POINTER_EVENT_SQL;

pub(super) fn statement(normal_work: &str) -> String {
    format!(
        r#"/* project:families.resolution_paths.prepare */
    INSERT INTO pg_temp.bigname_resolution_path_work (logical_name_id)
    WITH RECURSIVE journal AS MATERIALIZED (
        SELECT family, key::jsonb AS key, before_image FROM project_family_undo
        WHERE chain_id=$1 AND block_number=$2
          AND family IN ('project_parent_subregistry', 'project_ens_v2_entry_owner',
            'project_wrapper_state', 'project_registry_node_state', 'project_registry_pointer',
            'project_resource_pointer', 'project_resolver_classification',
            'project_lifecycle_event', 'project_binding_candidate', 'project_name_state')
    ), path_change AS MATERIALIZED (
        SELECT 1 FROM journal WHERE family NOT IN ('project_resource_pointer', 'project_resolver_classification')
        UNION ALL
        SELECT 1 FROM journal JOIN project_resource_pointer pointer
          ON journal.family='project_resource_pointer' AND pointer.chain_id=$1
         AND pointer.resource_id=(journal.key->>1)::uuid
        WHERE pointer.pointer_position IS DISTINCT FROM journal.before_image->'pointer_position'
        UNION ALL
        SELECT 1 FROM normalized_events event WHERE event.chain_id=$1
          AND event.block_number>$4 AND event.block_number<=$2
          AND event.event_kind IN ('Upgraded','ContractDiscovered','RegistryCreated')
          AND event.source_family IN ('ens_v2_root_l1','ens_v2_registry_l1','ens_v2_migration_l1')
          AND event.consumer_visibility='activated'
          AND event.canonicality_state IN ('canonical','safe','finalized')
    ), normal AS MATERIALIZED ({normal_work}),
    changed_resolvers AS MATERIALIZED (
        SELECT journal.key->>1 AS resolver FROM journal
        JOIN project_resolver_classification current ON current.chain_id=$1
          AND journal.family='project_resolver_classification' AND current.resolver_address=journal.key->>1
        WHERE current.classification IS DISTINCT FROM journal.before_image->'classification'
           OR current.support_status IS DISTINCT FROM journal.before_image->>'support_status'
           OR current.unsupported_reason IS DISTINCT FROM journal.before_image->>'unsupported_reason'
           OR to_jsonb(current.manifest_id) IS DISTINCT FROM journal.before_image->'manifest_id'
    ), global_change AS MATERIALIZED (
        SELECT 1 FROM changed_resolvers WHERE resolver='0x322b7581ca210a69c6d0e0d7c88a7688d2789cb0'
    ), source_names AS MATERIALIZED (
        SELECT logical_name_id FROM normal WHERE EXISTS (SELECT 1 FROM path_change)
        UNION
        SELECT pointer.namespace || ':' || lower(pointer.namehash)
        FROM changed_resolvers changed JOIN project_resource_pointer pointer
          ON pointer.chain_id=$1 AND pointer.resolver_address=changed.resolver
        UNION
        SELECT pointer.namespace || ':' || lower(pointer.node)
        FROM changed_resolvers changed JOIN project_registry_pointer pointer
          ON pointer.chain_id=$1 AND pointer.resolver_address=changed.resolver
    ), source_ancestors AS MATERIALIZED (
        SELECT DISTINCT ancestor.logical_name_id FROM source_names name
        JOIN name_surfaces child ON child.logical_name_id=name.logical_name_id AND child.chain_id=$1
        JOIN name_surfaces ancestor ON ancestor.chain_id=$1 AND ancestor.namespace='ens'
          AND cardinality(ancestor.labelhashes)>=2
          AND cardinality(ancestor.labelhashes)<=cardinality(child.labelhashes)
          AND child.labelhashes[cardinality(child.labelhashes)-cardinality(ancestor.labelhashes)+1:]=ancestor.labelhashes
        WHERE EXISTS (SELECT 1 FROM source_names)
    ), changed_entries AS MATERIALIZED (
        SELECT key->>1 AS registry, key->>2 AS entry_key FROM journal WHERE family='project_ens_v2_entry_owner'
        UNION
        SELECT lower(event.raw_fact_ref->>'emitting_address'), left(lower(event.after_state->>'token_id'),58)||'00000000'
        FROM normalized_events event WHERE event.chain_id=$1 AND event.block_number>$4 AND event.block_number<=$2
          AND event.event_kind IN ('ResolverChanged','SubregistryChanged')
          AND event.source_family IN ('ens_v2_root_l1','ens_v2_registry_l1')
          AND event.consumer_visibility='activated' AND event.canonicality_state IN ('canonical','safe','finalized')
          AND event.after_state->>'token_id' ~ '^0x[0-9a-fA-F]{{64}}$'
        UNION
        SELECT lower(event.raw_fact_ref->>'emitting_address'), left(lower(event.after_state->>'token_id'),58)||'00000000'
        FROM changed_resolvers changed JOIN project_resource_pointer pointer
          ON pointer.chain_id=$1 AND pointer.resolver_address=changed.resolver
        JOIN normalized_events event ON event.event_identity=pointer.pointer_position->>'event_identity'
          AND event.chain_id=$1 AND event.source_family IN ('ens_v2_root_l1','ens_v2_registry_l1')
          AND event.after_state->>'token_id' ~ '^0x[0-9a-fA-F]{{64}}$'
        UNION
        -- Canonical retirement clears F5, but an independently mounted physical entry may
        -- still use its resolver. Last-nonzero narrows candidates; latest physical wins.
        SELECT lower(physical.emitter), left(lower(physical.token_id),58)||'00000000'
        FROM changed_resolvers changed JOIN project_resource_pointer pointer
          ON pointer.chain_id=$1 AND pointer.resolver_address IS NULL
          AND pointer.nonzero_resolver_address=changed.resolver
          AND pointer.source_family IN ('ens_v2_root_l1','ens_v2_registry_l1')
        JOIN normalized_events event ON event.event_identity=pointer.pointer_position->>'event_identity'
          AND event.chain_id=$1 AND event.resource_id=pointer.resource_id AND event.event_kind='ResolverChanged'
          AND event.source_family IN ('ens_v2_root_l1','ens_v2_registry_l1')
          AND event.consumer_visibility='activated' AND event.canonicality_state IN ('canonical','safe','finalized')
          AND event.block_number<=$2 AND NOT ({PHYSICAL_POINTER_EVENT_SQL})
        CROSS JOIN LATERAL (
            SELECT event.raw_fact_ref->>'emitting_address' AS emitter,
                   event.after_state->>'token_id' AS token_id, event.after_state->>'resolver' AS resolver
            FROM normalized_events event
            WHERE event.chain_id=$1 AND event.resource_id=pointer.resource_id AND event.event_kind='ResolverChanged'
              AND event.source_family IN ('ens_v2_root_l1','ens_v2_registry_l1')
              AND event.consumer_visibility='activated' AND event.canonicality_state IN ('canonical','safe','finalized')
              AND event.block_number<=$2 AND {PHYSICAL_POINTER_EVENT_SQL}
            ORDER BY event.block_number DESC, event.transaction_index DESC NULLS LAST,
                     event.log_index DESC NULLS LAST, event.event_identity COLLATE "C" DESC LIMIT 1
        ) physical
        WHERE physical.resolver=changed.resolver AND physical.token_id ~ '^0x[0-9a-fA-F]{{64}}$'
    ), changed_registries AS MATERIALIZED (
        SELECT lower(event.after_state->>'proxy_address') AS registry FROM normalized_events event
        WHERE event.chain_id=$1 AND event.block_number>$4 AND event.block_number<=$2
          AND event.event_kind IN ('Upgraded','ContractDiscovered')
          AND event.source_family IN ('ens_v2_registry_l1','ens_v2_migration_l1')
          AND event.consumer_visibility='activated' AND event.canonicality_state IN ('canonical','safe','finalized')
        UNION
        -- Initialization may announce a previously factory-created proxy in a later block,
        -- changing support for every observed name mounted below that registry.
        SELECT lower(event.after_state->>'registry') FROM normalized_events event
        WHERE event.chain_id=$1 AND event.block_number>$4 AND event.block_number<=$2
          AND event.event_kind='RegistryCreated' AND event.source_family='ens_v2_registry_l1'
          AND event.consumer_visibility='activated' AND event.canonicality_state IN ('canonical','safe','finalized')
          AND event.after_state->>'registry' ~ '^0x[0-9a-fA-F]{{40}}$'
        UNION
        -- An original wrapper child's owner/fuses can affect a registry now mounted at another
        -- logical parent. Read retained activated migration evidence, never discovery diagnostics.
        SELECT lower(created.after_state->>'registry')
        FROM source_ancestors ancestor
        JOIN normalized_events migration ON migration.logical_name_id=ancestor.logical_name_id
          AND migration.chain_id=$1 AND migration.event_kind='MigrationApplied'
          AND migration.block_number<=$2 AND migration.consumer_visibility='activated'
          AND migration.canonicality_state IN ('canonical','safe','finalized')
        CROSS JOIN LATERAL jsonb_array_elements(COALESCE(migration.after_state->'evidence','[]'::jsonb)) evidence
        JOIN normalized_events created ON created.event_identity=evidence->>'event_identity'
          AND evidence->>'event_kind'='RegistryCreated' AND created.chain_id=$1
          AND created.event_kind='RegistryCreated' AND created.consumer_visibility='activated'
          AND created.canonicality_state IN ('canonical','safe','finalized')
    ), logical_prefixes AS MATERIALIZED (
        SELECT DISTINCT surface.labelhashes FROM source_names name JOIN name_surfaces surface
          ON surface.logical_name_id=name.logical_name_id AND surface.chain_id=$1 AND surface.namespace='ens'
    ), candidates AS MATERIALIZED (
        SELECT surface.labelhashes FROM name_surfaces surface
        WHERE (EXISTS (SELECT 1 FROM changed_entries) OR EXISTS (SELECT 1 FROM changed_registries))
          AND surface.chain_id=$1 AND surface.namespace='ens' AND cardinality(surface.labelhashes)>0
          AND (EXISTS (SELECT 1 FROM changed_registries) OR EXISTS (
            SELECT 1 FROM unnest(surface.labelhashes) label JOIN changed_entries changed
              ON changed.entry_key=left(lower(label),58)||'00000000'))
    ), wanted AS MATERIALIZED (
        SELECT DISTINCT candidate.labelhashes[depth:] AS labelhashes
        FROM candidates candidate CROSS JOIN LATERAL generate_series(1,cardinality(candidate.labelhashes)) depth
    ), reached(labelhashes, registry) AS (
        -- The walk starts at the root registry the publication's captured manifest set admits
        -- ($5, project families/marker.rs `Composition`). With no admission it reaches nothing.
        SELECT ARRAY[]::text[], $5::text
        WHERE EXISTS (SELECT 1 FROM wanted) AND $5::text IS NOT NULL
        UNION
        SELECT next.labelhashes, pointer.subregistry
        FROM reached current JOIN wanted next ON cardinality(next.labelhashes)=cardinality(current.labelhashes)+1
          AND next.labelhashes[2:]=current.labelhashes
        JOIN project_ens_v2_entry_owner entry ON entry.chain_id=$1 AND entry.registry=current.registry
          AND entry.entry_key=left(lower(next.labelhashes[1]),58)||'00000000'
        LEFT JOIN normalized_events origin ON origin.event_identity=entry.event_identity
        CROSS JOIN LATERAL (
            SELECT event.after_state->>'subregistry' AS subregistry FROM normalized_events event
            WHERE event.chain_id=$1 AND event.resource_id=COALESCE(entry.resource_id,origin.resource_id)
              AND event.event_kind='SubregistryChanged' AND event.source_family IN ('ens_v2_root_l1','ens_v2_registry_l1')
              AND event.consumer_visibility='activated' AND event.canonicality_state IN ('canonical','safe','finalized')
              AND event.block_number<=$2 AND {PHYSICAL_POINTER_EVENT_SQL}
            ORDER BY event.block_number DESC, event.transaction_index DESC NULLS LAST,
                     event.log_index DESC NULLS LAST, event.event_identity COLLATE "C" DESC LIMIT 1
        ) pointer
        WHERE pointer.subregistry IS NOT NULL AND pointer.subregistry NOT IN ('','0x0000000000000000000000000000000000000000')
          -- A changed slot already invalidates all its descendants, including every former
          -- route below a clear/replacement. Do not traverse beyond that first changed slot.
          AND NOT EXISTS (SELECT 1 FROM changed_registries changed WHERE changed.registry=current.registry)
          AND NOT EXISTS (SELECT 1 FROM changed_entries changed WHERE changed.registry=current.registry
            AND changed.entry_key=left(lower(next.labelhashes[1]),58)||'00000000')
    ), physical_prefixes AS MATERIALIZED (
        SELECT current.labelhashes FROM reached current JOIN changed_registries changed ON changed.registry=current.registry
        UNION
        SELECT next.labelhashes FROM reached current
        JOIN wanted next ON cardinality(next.labelhashes)=cardinality(current.labelhashes)+1
          AND next.labelhashes[2:]=current.labelhashes
        JOIN changed_entries changed ON changed.registry=current.registry
          AND changed.entry_key=left(lower(next.labelhashes[1]),58)||'00000000'
    ), prefixes AS MATERIALIZED (
        SELECT labelhashes FROM logical_prefixes UNION SELECT labelhashes FROM physical_prefixes
    ), affected AS MATERIALIZED (
        SELECT child.logical_name_id FROM name_surfaces child
        WHERE (EXISTS (SELECT 1 FROM global_change) OR EXISTS (SELECT 1 FROM prefixes))
          AND child.chain_id=$1 AND child.namespace='ens' AND cardinality(child.labelhashes)>=1
          AND lower(child.labelhashes[cardinality(child.labelhashes)])='0x4f5b812789fc606be1b3b16908db13fc7a9adf7ca72641f84d75b47069d3d7f0'
          AND (EXISTS (SELECT 1 FROM global_change) OR EXISTS (
            SELECT 1 FROM prefixes prefix WHERE cardinality(prefix.labelhashes)<=cardinality(child.labelhashes)
              AND (cardinality(prefix.labelhashes)=0 OR
                child.labelhashes[cardinality(child.labelhashes)-cardinality(prefix.labelhashes)+1:]=prefix.labelhashes)))
        UNION
        SELECT summary.logical_name_id FROM project_name_summary summary
        WHERE summary.chain_id=$1 AND summary.recompose_at<=$3
    )
    SELECT DISTINCT surface.logical_name_id FROM affected
    JOIN name_surfaces surface ON surface.logical_name_id=affected.logical_name_id AND surface.chain_id=$1
    JOIN chain_lineage lineage ON lineage.chain_id=surface.chain_id AND lineage.block_hash=surface.block_hash
    WHERE EXISTS (SELECT 1 FROM affected)
      AND surface.namespace='ens' AND cardinality(surface.labelhashes)>=1
      AND surface.block_number<=$2 AND surface.visibility_state='active' AND surface.raw_name IS DISTINCT FROM ''
      AND surface.canonicality_state IN ('canonical','safe','finalized')
      AND lineage.canonicality_state IN ('canonical','safe','finalized')
    ON CONFLICT DO NOTHING"#
    )
}
