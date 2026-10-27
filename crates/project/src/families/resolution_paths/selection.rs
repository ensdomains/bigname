//! One SQL work selection shared by Project consumers. Physical entry changes retain their key.
//! A deduplicated registry walk consumes observed name prefixes, so repeated registries terminate
//! by name depth and nested/rebound mounts are dependencies without a persistent graph.
//!
//! Three steps look names up in `name_surfaces` and read two indexes Project owns there
//! (docs/storage.md, "Table ownership"). The ancestors of a touched name are found by the hash
//! of each of its suffixes, in `name_surfaces_project_path_hash_idx`. The names that hold a
//! changed entry's label, and the names under a changed prefix, are found by the hash of the
//! label's entry key, in `name_surfaces_project_entry_keys_idx`. An index only narrows the
//! surfaces read. The comparison of the label hashes themselves decides, so a hash collision
//! never changes the selection. Each lookup sits in a `LATERAL (... OFFSET 0)` subquery, which
//! the planner runs once per outer row and cannot turn into a join over every surface.
//!
//! Three cases read every observed ENS surface, because they can move every name: a changed
//! registry, a classification change of a declared ENSv1 mirror resolver (the v1 fallback),
//! and a prefix of no labels.
//! A prefix whose first label hash is null has no entry key to look up and also reads them all.
//! Missing/current/former membership and divergence evidence never restrict this candidate set.
//! SourceManifestUpdated has no block position. Supported manifest sync invalidates phase hashes
//! and requires a complete Interpret/Project redo. It is not an incremental event-range trigger.
//! A change of the ENSv2 root registry admission is not selected here: the summary and lookup
//! refreshes compose every name for it (`derived/summary.rs`, `ADMISSION_SWEEP`), and this
//! selection's refresh then rewrites nothing.
//!
//! Every touched name recomposes itself. A touched name also becomes a prefix whose whole
//! subtree recomposes, but only when its change can move other names' resolution paths.
//! A journal row is such a change only when a field that a descendant's composition reads
//! through its ancestors changed, judged against the row's before-image:
//!
//! - `project_ens_v2_entry_owner`: `expiry`, `status`, `registry_contract_instance_id`,
//!   `resource_id` or `token_id`.
//! - `project_registry_pointer`: `resolver_address`.
//! - `project_resource_pointer`: `resolver_address`, `nonzero_resolver_address`, or a move
//!   of its `namespace` or `namehash`. A move counts for the name before it and the name
//!   after it. A change of `boundary_kind` alone does not count.
//!
//! `project_resolver_classification` is already field-exact in `changed_resolvers`.
//!
//! A name whose surface is anchored in the publication's window also becomes a prefix. The
//! window is every block after the family marker's block, up to the publication's block,
//! the window the summary work list uses for new surfaces. The ENSv1 mirror lookup reads an
//! ancestor's registry pointer only through the ancestor's surface. So the names under a
//! newly surfaced name can compose to a different result even when no checked field of a
//! pointer row changed.
//!
//! A rebound name reads the original node across branches through wrapper eligibility.
//! The rebound name is mounted in a migration registry. Its composition does not read
//! the original name as an ancestor. It reads the original node through wrapper
//! eligibility instead. That read uses two inputs of the original node.
//!
//! The first is the class of its latest owner getter. The class is unknown when there is
//! no row, the owner word is unmasked, or the getter is missing or does not parse as an
//! address. It is zero when the getter is the zero address, and nonzero for any other
//! address. The stored reason does not change the class. Setting the owner to the
//! registry itself and setting it to the zero address both read as zero.
//!
//! The second input is the wrapper row of the original node: whether the row is present,
//! the name it names, whether it has a modifier, its `PARENT_CANNOT_CONTROL` fuse
//! (fuses & 65536), and the parsed stored expiry.
//!
//! A change in any of those can change the eligibility answer. So
//! `project_registry_node_state` can move other names' resolution paths when the getter
//! class changes. `project_wrapper_state` can move them when the row appears or goes away,
//! its name changes, its modifier presence changes, that fuse bit changes, or its parsed
//! expiry changes. When a wrapper row or a resource pointer moves to another name, the
//! change counts for both the old name and the new name. Every other journalled family
//! (`project_parent_subregistry`, `project_binding_candidate`, `project_name_state` and
//! `project_lifecycle_event`) moves only the name it names, never its descendants.
use bigname_storage::families::name::PHYSICAL_POINTER_EVENT_SQL;

pub(super) fn statement(normal_work: &str) -> String {
    // The `.eth` names among the surfaces `child`, and the test that `child` is `prefix` or a
    // name under it. Every arm of `affected` that reads surfaces applies the first, and the two
    // arms that follow a prefix apply the second.
    let eth_name = "child.chain_id=$1 AND child.namespace='ens' AND cardinality(child.labelhashes)>=1
          AND lower(child.labelhashes[cardinality(child.labelhashes)])='0x4f5b812789fc606be1b3b16908db13fc7a9adf7ca72641f84d75b47069d3d7f0'";
    let under_prefix = "cardinality(prefix.labelhashes)<=cardinality(child.labelhashes)
              AND (cardinality(prefix.labelhashes)=0 OR
                child.labelhashes[cardinality(child.labelhashes)-cardinality(prefix.labelhashes)+1:]=prefix.labelhashes)";
    format!(
        r#"/* project:families.resolution_paths.prepare */
    INSERT INTO pg_temp.bigname_resolution_path_work (logical_name_id)
    WITH RECURSIVE journal AS MATERIALIZED (
        SELECT family, key::jsonb AS key, before_image FROM project_family_undo
        WHERE chain_id=$1 AND block_number=$2
          AND family IN ('project_ens_v2_entry_owner', 'project_registry_pointer',
            'project_resource_pointer', 'project_resolver_classification',
            'project_registry_node_state', 'project_wrapper_state')
    ), pointer_moved AS MATERIALIZED (
        -- FORK HOT PATCH 2026-10-26: a v1 registry pointer change on a name is kept apart, so its
        -- descendants can be narrowed to the names without a nonzero pointer of their own.
        SELECT pointer.namespace || ':' || lower(pointer.node) AS logical_name_id
        FROM journal JOIN project_registry_pointer pointer
          ON journal.family='project_registry_pointer' AND pointer.chain_id=$1
         AND pointer.namespace=journal.key->>1 AND pointer.node=journal.key->>2
        WHERE journal.before_image IS NULL
           OR pointer.resolver_address IS DISTINCT FROM journal.before_image->>'resolver_address'
        UNION
        -- A resource pointer's resolver change or move on a name is the same class of change:
        -- descendants read an ancestor's resolver only through the mirror walk (first pointer wins).
        SELECT pointer.namespace || ':' || lower(pointer.namehash) AS logical_name_id
        FROM journal LEFT JOIN project_resource_pointer pointer
          ON journal.family='project_resource_pointer' AND pointer.chain_id=$1
         AND pointer.resource_id=(journal.key->>1)::uuid
        WHERE journal.family='project_resource_pointer'
          AND pointer.namespace IS NOT NULL AND pointer.namehash IS NOT NULL
          AND (journal.before_image IS NULL
           OR pointer.resolver_address IS DISTINCT FROM journal.before_image->>'resolver_address'
           OR pointer.nonzero_resolver_address IS DISTINCT FROM journal.before_image->>'nonzero_resolver_address'
           OR pointer.namespace IS DISTINCT FROM journal.before_image->>'namespace'
           OR pointer.namehash IS DISTINCT FROM journal.before_image->>'namehash')
        UNION
        SELECT (journal.before_image->>'namespace') || ':' || lower(journal.before_image->>'namehash')
        FROM journal LEFT JOIN project_resource_pointer pointer
          ON journal.family='project_resource_pointer' AND pointer.chain_id=$1
         AND pointer.resource_id=(journal.key->>1)::uuid
        WHERE journal.family='project_resource_pointer'
          AND journal.before_image->>'namespace' IS NOT NULL
          AND journal.before_image->>'namehash' IS NOT NULL
          AND (journal.before_image IS NULL
           OR pointer.resolver_address IS DISTINCT FROM journal.before_image->>'resolver_address'
           OR pointer.nonzero_resolver_address IS DISTINCT FROM journal.before_image->>'nonzero_resolver_address'
           OR pointer.namespace IS DISTINCT FROM journal.before_image->>'namespace'
           OR pointer.namehash IS DISTINCT FROM journal.before_image->>'namehash')
    ), other_moved AS MATERIALIZED (
        SELECT COALESCE(state.namespace, journal.before_image->>'namespace', journal.key->>1)
                 || ':' || lower(COALESCE(state.node, journal.before_image->>'node', journal.key->>2)) AS logical_name_id
        FROM journal LEFT JOIN project_registry_node_state state
          ON journal.family='project_registry_node_state' AND state.chain_id=$1
         AND state.namespace=journal.key->>1 AND state.node=journal.key->>2
        WHERE journal.family='project_registry_node_state'
          AND (CASE
                 WHEN journal.before_image IS NULL THEN 'unknown'
                 WHEN (journal.before_image->>'owner_word_unmasked')='true' THEN 'unknown'
                 WHEN lower(journal.before_image->>'owner_getter') IS NULL THEN 'unknown'
                 WHEN lower(journal.before_image->>'owner_getter') !~ '^0x[0-9a-f]{{40}}$' THEN 'unknown'
                 WHEN lower(journal.before_image->>'owner_getter')='0x0000000000000000000000000000000000000000' THEN 'zero'
                 ELSE 'nonzero' END)
              IS DISTINCT FROM
              (CASE
                 WHEN state.namespace IS NULL THEN 'unknown'
                 WHEN state.owner_word_unmasked IS TRUE THEN 'unknown'
                 WHEN state.owner_getter IS NULL THEN 'unknown'
                 WHEN lower(state.owner_getter) !~ '^0x[0-9a-f]{{40}}$' THEN 'unknown'
                 WHEN lower(state.owner_getter)='0x0000000000000000000000000000000000000000' THEN 'zero'
                 ELSE 'nonzero' END)
        UNION
        SELECT wrapper.logical_name_id
        FROM journal LEFT JOIN project_wrapper_state wrapper
          ON journal.family='project_wrapper_state' AND wrapper.chain_id=$1
         AND wrapper.resource_id=(journal.key->>1)::uuid
        WHERE journal.family='project_wrapper_state'
          AND wrapper.logical_name_id IS NOT NULL
          AND (journal.before_image IS NULL
           OR wrapper.resource_id IS NULL
           OR wrapper.logical_name_id IS DISTINCT FROM journal.before_image->>'logical_name_id'
           OR COALESCE(wrapper.wrapper_state_position IS NOT NULL
                AND jsonb_typeof(wrapper.wrapper_state_position)='object', false)
              IS DISTINCT FROM COALESCE(jsonb_typeof(journal.before_image->'wrapper_state_position')='object', false)
           OR COALESCE(CASE WHEN (journal.before_image->>'fuses') ~ '^-?[0-9]+$'
                             AND (journal.before_image->>'fuses')::numeric BETWEEN -9223372036854775808 AND 9223372036854775807
                            THEN ((journal.before_image->>'fuses')::bigint & 65536) <> 0 END, false)
              IS DISTINCT FROM COALESCE(wrapper.fuses IS NOT NULL AND (wrapper.fuses & 65536) <> 0, false)
           OR (CASE WHEN (journal.before_image->>'expiry_seconds') ~ '^[0-9]+$'
                     AND (journal.before_image->>'expiry_seconds')::numeric <= 18446744073709551615
                    THEN (journal.before_image->>'expiry_seconds')::numeric END)
              IS DISTINCT FROM
              (CASE WHEN wrapper.expiry_seconds::text ~ '^[0-9]+$'
                     AND wrapper.expiry_seconds <= 18446744073709551615
                    THEN wrapper.expiry_seconds END))
        UNION
        SELECT journal.before_image->>'logical_name_id'
        FROM journal LEFT JOIN project_wrapper_state wrapper
          ON journal.family='project_wrapper_state' AND wrapper.chain_id=$1
         AND wrapper.resource_id=(journal.key->>1)::uuid
        WHERE journal.family='project_wrapper_state'
          AND journal.before_image->>'logical_name_id' IS NOT NULL
          AND (journal.before_image IS NULL
           OR wrapper.resource_id IS NULL
           OR wrapper.logical_name_id IS DISTINCT FROM journal.before_image->>'logical_name_id'
           OR COALESCE(wrapper.wrapper_state_position IS NOT NULL
                AND jsonb_typeof(wrapper.wrapper_state_position)='object', false)
              IS DISTINCT FROM COALESCE(jsonb_typeof(journal.before_image->'wrapper_state_position')='object', false)
           OR COALESCE(CASE WHEN (journal.before_image->>'fuses') ~ '^-?[0-9]+$'
                             AND (journal.before_image->>'fuses')::numeric BETWEEN -9223372036854775808 AND 9223372036854775807
                            THEN ((journal.before_image->>'fuses')::bigint & 65536) <> 0 END, false)
              IS DISTINCT FROM COALESCE(wrapper.fuses IS NOT NULL AND (wrapper.fuses & 65536) <> 0, false)
           OR (CASE WHEN (journal.before_image->>'expiry_seconds') ~ '^[0-9]+$'
                     AND (journal.before_image->>'expiry_seconds')::numeric <= 18446744073709551615
                    THEN (journal.before_image->>'expiry_seconds')::numeric END)
              IS DISTINCT FROM
              (CASE WHEN wrapper.expiry_seconds::text ~ '^[0-9]+$'
                     AND wrapper.expiry_seconds <= 18446744073709551615
                    THEN wrapper.expiry_seconds END))
    ), names_whose_change_can_move_descendants AS MATERIALIZED (
        SELECT logical_name_id FROM pointer_moved
        UNION
        SELECT logical_name_id FROM other_moved
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
        -- Any ENSv1 mirror resolver an ENSv2 resolver manifest of the chain declares. The walk
        -- reads the publication's own declaration, one of these (resolution_path.rs).
        SELECT 1 FROM changed_resolvers changed
        JOIN manifest_contract_instances declared ON declared.chain_id=$1
         AND declared.declaration_kind='contract' AND declared.role='ensv1_mirror_resolver'
         AND lower(declared.declared_address)=changed.resolver
        JOIN manifest_versions manifest ON manifest.manifest_id=declared.manifest_id
         AND manifest.chain_id=declared.chain_id AND manifest.source_family='ens_v2_resolver_l1'
    ), tree_scope_event AS MATERIALIZED (
        -- PROD HOT PATCH 2026-10-27 (sel10): a rewrite of every `.eth` name needs an event of the
        -- window on the admitted root registry, on a declared ENSv1 mirror resolver, or on the
        -- `eth` node's own ENSv1 resolver. A journal diff alone (a family re-stamping a
        -- root-scope row it touched) moves no resolution path, and twice selected every `.eth`
        -- name (TYR-398, block 26286063).
        SELECT 1 FROM normalized_events event
        WHERE event.chain_id=$1 AND event.block_number>$4 AND event.block_number<=$2
          AND event.canonicality_state IN ('canonical','safe','finalized')
          AND (lower(event.raw_fact_ref->>'emitting_address')=lower($5::text)
            OR lower(event.after_state->>'proxy_address')=lower($5::text)
            OR (event.event_kind='ResolverChanged' AND event.source_family='ens_v1_registry_l1'
              AND lower(event.after_state->>'node')='0x93cdeb708b7545dc668eb9280176169d1c33cfd8ed6f04690a0bcc88a93fc4ae')
            OR EXISTS (SELECT 1 FROM manifest_contract_instances declared
                JOIN manifest_versions manifest ON manifest.manifest_id=declared.manifest_id
                 AND manifest.chain_id=declared.chain_id AND manifest.source_family='ens_v2_resolver_l1'
                WHERE declared.chain_id=$1 AND declared.declaration_kind='contract'
                  AND declared.role='ensv1_mirror_resolver'
                  AND lower(declared.declared_address) IN (
                      lower(event.raw_fact_ref->>'emitting_address'), lower(event.after_state->>'proxy_address'))))
    ), source_names AS MATERIALIZED (
        SELECT logical_name_id FROM normal
        WHERE logical_name_id IN (SELECT logical_name_id FROM names_whose_change_can_move_descendants)
        UNION
        SELECT pointer.namespace || ':' || lower(pointer.namehash)
        FROM changed_resolvers changed JOIN project_resource_pointer pointer
          ON pointer.chain_id=$1 AND pointer.resolver_address=changed.resolver
        UNION
        SELECT pointer.namespace || ':' || lower(pointer.node)
        FROM changed_resolvers changed JOIN project_registry_pointer pointer
          ON pointer.chain_id=$1 AND pointer.resolver_address=changed.resolver
    ), source_ancestors AS MATERIALIZED (
        -- An ancestor of two or more labels is the child's suffix from `depth`. The index
        -- finds the surfaces with that suffix's hash and the suffix comparison decides.
        SELECT DISTINCT probe.logical_name_id FROM source_names name
        JOIN name_surfaces child ON child.logical_name_id=name.logical_name_id AND child.chain_id=$1
        CROSS JOIN LATERAL generate_series(1,cardinality(child.labelhashes)-1) depth
        CROSS JOIN LATERAL (
            SELECT ancestor.logical_name_id FROM name_surfaces ancestor
            WHERE ancestor.chain_id=$1 AND ancestor.namespace='ens'
              AND hash_array_extended(ancestor.labelhashes,0)=hash_array_extended(child.labelhashes[depth:],0)
              AND cardinality(ancestor.labelhashes)>=2
              AND cardinality(ancestor.labelhashes)<=cardinality(child.labelhashes)
              AND child.labelhashes[cardinality(child.labelhashes)-cardinality(ancestor.labelhashes)+1:]=ancestor.labelhashes
            OFFSET 0
        ) probe
        WHERE EXISTS (SELECT 1 FROM source_names)
    ), changed_entries AS MATERIALIZED (
        SELECT journal.key->>1 AS registry, journal.key->>2 AS entry_key FROM journal
        LEFT JOIN project_ens_v2_entry_owner entry ON entry.chain_id=$1
          AND journal.family='project_ens_v2_entry_owner' AND entry.registry=journal.key->>1
          AND entry.entry_key=journal.key->>2
        WHERE journal.family='project_ens_v2_entry_owner'
          AND (journal.before_image IS NULL
           OR entry.status IS DISTINCT FROM journal.before_image->>'status'
           OR COALESCE(to_jsonb(entry.expiry), 'null'::jsonb)
                IS DISTINCT FROM COALESCE(journal.before_image->'expiry', 'null'::jsonb)
           OR entry.registry_contract_instance_id IS DISTINCT FROM journal.before_image->>'registry_contract_instance_id'
           OR COALESCE(to_jsonb(entry.resource_id), 'null'::jsonb)
                IS DISTINCT FROM COALESCE(journal.before_image->'resource_id', 'null'::jsonb)
           OR entry.token_id IS DISTINCT FROM journal.before_image->>'token_id')
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
    ), mounted_names AS MATERIALIZED (
        -- PROD HOT PATCH 2026-10-27: a changed registry other than the admitted root is reached by
        -- the walk below only through an entry whose subregistry pointer names it, and the walk
        -- stops at that entry. So the names whose current pointer names a changed registry are
        -- the only candidates such a registry needs; the window's own pointer events are already
        -- changed entries. The corpus-wide candidate set stays for a change of the root registry.
        SELECT DISTINCT surface.labelhashes FROM changed_registries changed
        JOIN project_parent_subregistry mount ON mount.chain_id=$1
          AND lower(mount.subregistry_address)=changed.registry
        JOIN name_surfaces surface ON surface.logical_name_id=mount.logical_name_id
          AND surface.chain_id=$1 AND surface.namespace='ens' AND cardinality(surface.labelhashes)>0
    ), logical_prefixes AS MATERIALIZED (
        SELECT DISTINCT surface.labelhashes FROM source_names name JOIN name_surfaces surface
          ON surface.logical_name_id=name.logical_name_id AND surface.chain_id=$1 AND surface.namespace='ens'
        UNION
        -- A surface anchored in the publication's window is a prefix too (see the module docs).
        -- The summary work list holds every such name, so its surfaces are read by key.
        SELECT DISTINCT surface.labelhashes FROM normal name JOIN name_surfaces surface
          ON surface.logical_name_id=name.logical_name_id AND surface.chain_id=$1 AND surface.namespace='ens'
        WHERE surface.block_number>$4 AND surface.block_number<=$2
    ), candidates AS MATERIALIZED (
        -- PROD HOT PATCH 2026-10-27: only a change of the root registry itself can sit below any
        -- name; every other changed registry is covered by `mounted_names`.
        SELECT surface.labelhashes FROM name_surfaces surface
        WHERE EXISTS (SELECT 1 FROM changed_registries WHERE registry=lower($5::text))
          AND surface.chain_id=$1 AND surface.namespace='ens' AND cardinality(surface.labelhashes)>0
        UNION ALL
        SELECT labelhashes FROM mounted_names
        UNION ALL
        -- With changed entries alone, a candidate holds a label of a changed entry key. The
        -- index finds the surfaces with the key's hash and the label comparison decides.
        SELECT probe.labelhashes FROM (SELECT DISTINCT entry_key FROM changed_entries) changed
        CROSS JOIN LATERAL (
            SELECT surface.labelhashes FROM name_surfaces surface
            WHERE entry_key_hashes(surface.labelhashes) @> ARRAY[hashtextextended(changed.entry_key,0)]
              AND surface.chain_id=$1 AND surface.namespace='ens' AND cardinality(surface.labelhashes)>0
              AND EXISTS (SELECT 1 FROM unnest(surface.labelhashes) label
                WHERE changed.entry_key=left(lower(label),58)||'00000000')
            OFFSET 0
        ) probe
        WHERE NOT EXISTS (SELECT 1 FROM changed_registries WHERE registry=lower($5::text))
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
    ), narrow_prefixes AS MATERIALIZED (
        -- FORK HOT PATCH 2026-10-26: prefixes that exist only because a v1 registry pointer changed
        -- on the prefix name. The ENSv1 mirror's walk stops at the first resolver it finds
        -- (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol findResolver semantics), so such a
        -- change moves only the descendants that have no nonzero pointer of their own. A prefix that
        -- any other source also names keeps its whole subtree.
        SELECT DISTINCT surface.labelhashes FROM normal name
        JOIN name_surfaces surface
          ON surface.logical_name_id=name.logical_name_id AND surface.chain_id=$1 AND surface.namespace='ens'
        WHERE name.logical_name_id IN (SELECT logical_name_id FROM pointer_moved)
        EXCEPT (
            SELECT DISTINCT surface.labelhashes FROM normal name JOIN name_surfaces surface
              ON surface.logical_name_id=name.logical_name_id AND surface.chain_id=$1 AND surface.namespace='ens'
            WHERE name.logical_name_id IN (SELECT logical_name_id FROM other_moved)
            UNION
            SELECT DISTINCT surface.labelhashes FROM source_names name JOIN name_surfaces surface
              ON surface.logical_name_id=name.logical_name_id AND surface.chain_id=$1 AND surface.namespace='ens'
            WHERE name.logical_name_id NOT IN (SELECT logical_name_id FROM normal)
            UNION
            SELECT DISTINCT surface.labelhashes FROM normal name JOIN name_surfaces surface
              ON surface.logical_name_id=name.logical_name_id AND surface.chain_id=$1 AND surface.namespace='ens'
            WHERE surface.block_number>$4 AND surface.block_number<=$2
            UNION
            SELECT labelhashes FROM physical_prefixes
        )
    ), affected AS MATERIALIZED (
        -- A fallback resolver change or a prefix of no labels affects every `.eth` name.
        SELECT child.logical_name_id FROM name_surfaces child
        WHERE (EXISTS (SELECT 1 FROM global_change)
            OR EXISTS (SELECT 1 FROM prefixes prefix WHERE cardinality(prefix.labelhashes)=0))
          AND EXISTS (SELECT 1 FROM tree_scope_event)
          AND {eth_name}
        UNION
        -- A name under a prefix holds the prefix's first label. The index finds the surfaces
        -- with the hash of that label's entry key and the suffix comparison decides.
        SELECT probe.logical_name_id FROM prefixes prefix
        CROSS JOIN LATERAL (
            SELECT child.logical_name_id FROM name_surfaces child
            WHERE entry_key_hashes(child.labelhashes)
                @> ARRAY[hashtextextended(left(lower(prefix.labelhashes[1]),58)||'00000000',0)]
              AND {eth_name}
              AND {under_prefix}
              -- FORK HOT PATCH 2026-10-26: an ancestor's v1 pointer is read only by the ENSv1 mirror
              -- walk, which runs for names below a `.eth` second-level name (families/name/batch.rs,
              -- NamePlace::BelowEthSecondLevel) and stops at the first non-cleared pointer. So under a
              -- pointer-only prefix, a second-level name never moves, and a deeper child with a nonzero
              -- pointer of its own resolves through that pointer and does not move either.
              AND NOT (
                EXISTS (SELECT 1 FROM narrow_prefixes narrow WHERE narrow.labelhashes=prefix.labelhashes)
                AND (
                  cardinality(child.labelhashes) <= 2
                  OR EXISTS (
                    SELECT 1 FROM project_registry_pointer own
                    WHERE own.chain_id=$1 AND own.namespace=child.namespace
                      AND own.node=split_part(child.logical_name_id, ':', 2)
                      AND own.resolver_address IS NOT NULL
                      AND own.resolver_address NOT IN ('', '0x0000000000000000000000000000000000000000')
                  )
                )
              )
            OFFSET 0
        ) probe
        WHERE prefix.labelhashes[1] IS NOT NULL
          -- PROD HOT PATCH 2026-10-27 (sel10): a prefix of one label is the whole `.eth` tree.
          AND (cardinality(prefix.labelhashes)>=2 OR EXISTS (SELECT 1 FROM tree_scope_event))
        UNION
        -- A first label hash that is null has no key to look up. Label hash arrays compare
        -- two nulls as equal, so such a prefix keeps the read of every surface.
        SELECT child.logical_name_id FROM name_surfaces child
        WHERE EXISTS (SELECT 1 FROM prefixes prefix
            WHERE prefix.labelhashes[1] IS NULL AND cardinality(prefix.labelhashes)>0)
          AND {eth_name}
          AND EXISTS (SELECT 1 FROM prefixes prefix
            WHERE prefix.labelhashes[1] IS NULL AND cardinality(prefix.labelhashes)>0
              AND {under_prefix})
        UNION
        SELECT summary.logical_name_id FROM project_name_summary summary
        WHERE summary.chain_id=$1 AND summary.recompose_at<=$3
        UNION
        -- Every touched name recomposes itself, whether or not its change can move
        -- other names' resolution paths.
        SELECT logical_name_id FROM normal
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
