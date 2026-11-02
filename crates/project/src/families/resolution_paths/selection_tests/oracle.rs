//! The work selection at the head of the registry pointer narrowing (TYR-380) and the exact
//! resolver-change predicates (TYR-398), before a changed registry was looked up by the entries
//! that point at it: any changed registry made every observed surface a candidate. The row and
//! scaling tests compare the current statement with this one, so the body below is a frozen copy
//! and is never edited to follow the current statement. It records which names the selection
//! chose at that point. The last change of the stack that narrows changed-registry selection
//! without changing its result removes it.
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
    // The name summary and lookup name work this statement feeds read an ancestor's registry
    // pointer only in the ENSv1 mirror walk (storage `records/mirror.rs`, `nearest`). Their
    // composition runs it only for a name below a `.eth` second-level name (storage
    // `families/name/batch.rs:527-529`, `NamePlace::BelowEthSecondLevel`), so there a name of
    // fewer than three labels never reads one. The record inventory walks at any depth, but it
    // recomposes through its own `registry_node` dependency on each node it read
    // (`lookup/inventory_work.sql`), not through this statement.
    let walks_ancestor_pointers = "cardinality(child.labelhashes)>=3";
    // The walk returns the nearest pointer that is not cleared (null, empty or the zero
    // address), as ENSv1 resolution does
    // (upstream: .refs/ens_v1/contracts/universalResolver/RegistryUtils.sol:L25-L38 @ ens_v1@91c966f).
    // A name with its own resolver never reads its ancestors'. The scalar subquery is one
    // primary-key probe per name, in both plan modes (`scaling_tests.rs`, row L).
    let has_no_own_resolver = "COALESCE((SELECT own.resolver_address FROM project_registry_pointer own
                  WHERE own.chain_id=$1 AND own.namespace=child.namespace AND own.node=child.namehash), '')
                IN ('', '0x0000000000000000000000000000000000000000')";
    // An ENSv1 resource pointer row before and after the change, at the same name. A row with
    // no before-image, or a before-image with no pointer (a version boundary alone), had no
    // family and no name.
    let v1_family = "('ens_v1_registry_l1','ens_v1_registrar_l1','ens_v1_wrapper_l1')";
    let v1_pointer_in_place = format!(
        "(pointer.source_family IN {v1_family}
            AND (journal.before_image->>'source_family' IS NULL
              OR journal.before_image->>'source_family' IN {v1_family})
            AND (journal.before_image->>'namespace' IS NULL
              OR (pointer.namespace=journal.before_image->>'namespace'
                AND pointer.namehash=journal.before_image->>'namehash')))"
    );
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
        SELECT pointer.namespace || ':' || lower(pointer.node) AS logical_name_id
        FROM journal JOIN project_registry_pointer pointer
          ON journal.family='project_registry_pointer' AND pointer.chain_id=$1
         AND pointer.namespace=journal.key->>1 AND pointer.node=journal.key->>2
        WHERE journal.before_image IS NULL
           OR pointer.resolver_address IS DISTINCT FROM journal.before_image->>'resolver_address'
    ), resource_pointer_changed AS MATERIALIZED (
        SELECT pointer.namespace || ':' || lower(pointer.namehash) AS logical_name_id,
               COALESCE({v1_pointer_in_place}, false) AS v1_in_place
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
    ), v1_pointer_moved AS MATERIALIZED (
        -- The ENSv1 resource pointer changes that count as a registry pointer change (see the
        -- module docs).
        SELECT logical_name_id FROM resource_pointer_changed WHERE v1_in_place
    ), otherwise_moved AS MATERIALIZED (
        SELECT logical_name_id FROM resource_pointer_changed WHERE NOT v1_in_place
        UNION
        SELECT (journal.before_image->>'namespace') || ':' || lower(journal.before_image->>'namehash')
        FROM journal LEFT JOIN project_resource_pointer pointer
          ON journal.family='project_resource_pointer' AND pointer.chain_id=$1
         AND pointer.resource_id=(journal.key->>1)::uuid
        WHERE journal.family='project_resource_pointer'
          AND journal.before_image->>'namespace' IS NOT NULL
          AND journal.before_image->>'namehash' IS NOT NULL
          AND NOT COALESCE({v1_pointer_in_place}, false)
          AND (journal.before_image IS NULL
           OR pointer.resolver_address IS DISTINCT FROM journal.before_image->>'resolver_address'
           OR pointer.nonzero_resolver_address IS DISTINCT FROM journal.before_image->>'nonzero_resolver_address'
           OR pointer.namespace IS DISTINCT FROM journal.before_image->>'namespace'
           OR pointer.namehash IS DISTINCT FROM journal.before_image->>'namehash')
        UNION
        SELECT COALESCE(state.namespace, journal.before_image->>'namespace', journal.key->>1)
                 || ':' || lower(COALESCE(state.node, journal.before_image->>'node', journal.key->>2))
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
        SELECT logical_name_id FROM pointer_moved UNION SELECT logical_name_id FROM v1_pointer_moved
        UNION SELECT logical_name_id FROM otherwise_moved
    ), normal AS MATERIALIZED ({normal_work}),
    changed_resolvers AS MATERIALIZED (
        -- The walks read a classification only while it is supported, and an unsupported or
        -- missing row decides the same whatever its fields (storage `name/resolution_path.rs`,
        -- `records/mirror.rs`). So a change counts only with a supported side, a deleted row
        -- included.
        SELECT journal.key->>1 AS resolver FROM journal
        LEFT JOIN project_resolver_classification current ON current.chain_id=$1
          AND journal.family='project_resolver_classification' AND current.resolver_address=journal.key->>1
        WHERE journal.family='project_resolver_classification'
          AND (current.support_status='supported'
            OR journal.before_image->>'support_status'='supported')
          AND (current.chain_id IS NULL
            OR COALESCE(current.classification, 'null'::jsonb)
              IS DISTINCT FROM COALESCE(journal.before_image->'classification', 'null'::jsonb)
            OR current.support_status IS DISTINCT FROM journal.before_image->>'support_status'
            OR current.unsupported_reason IS DISTINCT FROM journal.before_image->>'unsupported_reason'
            OR COALESCE(to_jsonb(current.manifest_id), 'null'::jsonb)
              IS DISTINCT FROM COALESCE(journal.before_image->'manifest_id', 'null'::jsonb))
    ), global_change AS MATERIALIZED (
        -- Any ENSv1 mirror resolver an ENSv2 resolver manifest of the chain declares. The walk
        -- reads the publication's own declaration, one of these (resolution_path.rs).
        SELECT 1 FROM changed_resolvers changed
        JOIN manifest_contract_instances declared ON declared.chain_id=$1
         AND declared.declaration_kind='contract' AND declared.role='ensv1_mirror_resolver'
         AND lower(declared.declared_address)=changed.resolver
        JOIN manifest_versions manifest ON manifest.manifest_id=declared.manifest_id
         AND manifest.chain_id=declared.chain_id AND manifest.source_family='ens_v2_resolver_l1'
    ), resolver_readers AS MATERIALIZED (
        SELECT pointer.namespace || ':' || lower(pointer.namehash) AS logical_name_id
        FROM changed_resolvers changed JOIN project_resource_pointer pointer
          ON pointer.chain_id=$1 AND pointer.resolver_address=changed.resolver
        UNION
        SELECT pointer.namespace || ':' || lower(pointer.node)
        FROM changed_resolvers changed JOIN project_registry_pointer pointer
          ON pointer.chain_id=$1 AND pointer.resolver_address=changed.resolver
    ), source_names AS MATERIALIZED (
        SELECT logical_name_id FROM normal
        WHERE logical_name_id IN (SELECT logical_name_id FROM names_whose_change_can_move_descendants)
        UNION
        SELECT logical_name_id FROM resolver_readers
    ), narrow_names AS MATERIALIZED (
        -- The touched names whose only change that can move other names is their registry
        -- pointer (see the module docs).
        SELECT logical_name_id FROM normal
        WHERE logical_name_id IN (SELECT logical_name_id FROM pointer_moved
            UNION SELECT logical_name_id FROM v1_pointer_moved)
        EXCEPT SELECT logical_name_id FROM otherwise_moved
        EXCEPT SELECT logical_name_id FROM resolver_readers
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
    ), logical_prefixes AS MATERIALIZED (
        -- A narrow name's surface is a full prefix only when it has no first label hash to
        -- look its descendants up by.
        SELECT DISTINCT surface.labelhashes FROM source_names name JOIN name_surfaces surface
          ON surface.logical_name_id=name.logical_name_id AND surface.chain_id=$1 AND surface.namespace='ens'
        WHERE name.logical_name_id NOT IN (SELECT logical_name_id FROM narrow_names)
           OR surface.labelhashes[1] IS NULL
        UNION
        -- A surface anchored in the publication's window is a prefix too (see the module docs).
        -- The summary work list holds every such name, so its surfaces are read by key.
        SELECT DISTINCT surface.labelhashes FROM normal name JOIN name_surfaces surface
          ON surface.logical_name_id=name.logical_name_id AND surface.chain_id=$1 AND surface.namespace='ens'
        WHERE surface.block_number>$4 AND surface.block_number<=$2
    ), candidates AS MATERIALIZED (
        -- A changed registry can sit below any name, so every surface is a candidate.
        SELECT surface.labelhashes FROM name_surfaces surface
        WHERE EXISTS (SELECT 1 FROM changed_registries)
          AND surface.chain_id=$1 AND surface.namespace='ens' AND cardinality(surface.labelhashes)>0
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
        WHERE NOT EXISTS (SELECT 1 FROM changed_registries)
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
        -- A path that any other change also names keeps its whole subtree.
        SELECT DISTINCT surface.labelhashes FROM narrow_names name JOIN name_surfaces surface
          ON surface.logical_name_id=name.logical_name_id AND surface.chain_id=$1 AND surface.namespace='ens'
        WHERE surface.labelhashes[1] IS NOT NULL
        EXCEPT SELECT labelhashes FROM prefixes
    ), affected AS MATERIALIZED (
        -- A fallback resolver change or a prefix of no labels affects every `.eth` name.
        SELECT child.logical_name_id FROM name_surfaces child
        WHERE (EXISTS (SELECT 1 FROM global_change)
            OR EXISTS (SELECT 1 FROM prefixes prefix WHERE cardinality(prefix.labelhashes)=0))
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
            OFFSET 0
        ) probe
        WHERE prefix.labelhashes[1] IS NOT NULL
        UNION
        -- Under a narrow prefix, only the names that read the prefix's registry pointer.
        SELECT probe.logical_name_id FROM narrow_prefixes prefix
        CROSS JOIN LATERAL (
            SELECT child.logical_name_id FROM name_surfaces child
            WHERE entry_key_hashes(child.labelhashes)
                @> ARRAY[hashtextextended(left(lower(prefix.labelhashes[1]),58)||'00000000',0)]
              AND {eth_name}
              AND {under_prefix}
              AND {walks_ancestor_pointers}
              AND {has_no_own_resolver}
            OFFSET 0
        ) probe
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
