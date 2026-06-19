pub(super) const CHILDREN_CURRENT_INVALIDATIONS_PREFIX: &str = r#"
WITH changed_events AS (
    SELECT ne.*, change.change_id, change.changed_at
    FROM projection_normalized_event_changes change
    JOIN normalized_events ne
      ON ne.normalized_event_id = change.normalized_event_id
    WHERE change.change_id > $1
      AND change.change_id <= $2
),
candidate_keys AS (
    SELECT
        'children_current'::TEXT AS projection,
        ne.logical_name_id AS projection_key,
        jsonb_build_object('parent_logical_name_id', ne.logical_name_id) AS key_payload,
        ne.normalized_event_id,
        ne.change_id,
        ne.changed_at
    FROM changed_events ne
    WHERE ne.event_kind IN ('SubregistryChanged', 'ParentChanged')
      AND ne.logical_name_id IS NOT NULL

    UNION ALL

    SELECT
        'children_current'::TEXT AS projection,
        parent.logical_name_id AS projection_key,
        jsonb_build_object('parent_logical_name_id', parent.logical_name_id) AS key_payload,
        ne.normalized_event_id,
        ne.change_id,
        ne.changed_at
    FROM changed_events ne
    JOIN name_surfaces parent
      ON parent.namehash = ne.after_state ->> 'parent_node'
    WHERE ne.event_kind = 'SubregistryChanged'

    UNION ALL

    SELECT
        'children_current'::TEXT AS projection,
        subregistry.logical_name_id AS projection_key,
        jsonb_build_object('parent_logical_name_id', subregistry.logical_name_id) AS key_payload,
        ne.normalized_event_id,
        ne.change_id,
        ne.changed_at
    FROM changed_events ne
    JOIN normalized_events subregistry
      ON subregistry.event_kind = 'SubregistryChanged'
     AND subregistry.derivation_kind = ne.derivation_kind
     AND subregistry.source_family IN ('ens_v2_root_l1', 'ens_v2_registry_l1')
     AND subregistry.logical_name_id IS NOT NULL
     AND subregistry.after_state ->> 'from_contract_instance_id'
         = ne.after_state ->> 'parent_contract_instance_id'
     AND subregistry.after_state ->> 'to_contract_instance_id'
         = ne.after_state ->> 'registry_contract_instance_id'
    JOIN name_surfaces parent
      ON parent.logical_name_id = subregistry.logical_name_id
     AND parent.namespace = ne.namespace
     AND parent.chain_id = ne.chain_id
     AND parent.normalized_name = ne.after_state ->> 'registry_name'
    WHERE ne.event_kind = 'ParentChanged'
      AND ne.derivation_kind = 'ens_v2_registry_resource_surface'
      AND ne.source_family IN ('ens_v2_root_l1', 'ens_v2_registry_l1')
      AND ne.after_state ->> 'parent_contract_instance_id' IS NOT NULL
      AND ne.after_state ->> 'registry_contract_instance_id' IS NOT NULL
      AND ne.after_state ->> 'registry_name' IS NOT NULL

    UNION ALL

    SELECT
        'children_current'::TEXT AS projection,
        current_child.parent_logical_name_id AS projection_key,
        jsonb_build_object(
            'parent_logical_name_id',
            current_child.parent_logical_name_id
        ) AS key_payload,
        ne.normalized_event_id,
        ne.change_id,
        ne.changed_at
    FROM changed_events ne
    JOIN name_surfaces affected_surface
      ON affected_surface.namespace = ne.namespace
     AND affected_surface.chain_id = ne.chain_id
     AND affected_surface.normalized_name = ne.after_state ->> 'registry_name'
    JOIN children_current current_child
      ON current_child.child_logical_name_id = affected_surface.logical_name_id
      OR current_child.parent_logical_name_id = affected_surface.logical_name_id
    WHERE ne.event_kind = 'ParentChanged'
      AND ne.derivation_kind = 'ens_v2_registry_resource_surface'
      AND ne.source_family IN ('ens_v2_root_l1', 'ens_v2_registry_l1')
      AND ne.after_state ->> 'registry_contract_instance_id' IS NOT NULL
      AND ne.after_state ->> 'registry_name' IS NOT NULL

    UNION ALL

    SELECT
        'children_current'::TEXT AS projection,
        parent.logical_name_id AS projection_key,
        jsonb_build_object('parent_logical_name_id', parent.logical_name_id) AS key_payload,
        ne.normalized_event_id,
        ne.change_id,
        ne.changed_at
    FROM changed_events ne
    JOIN name_surfaces child
      ON child.logical_name_id = ne.logical_name_id
    JOIN name_surfaces parent
      ON parent.namespace = child.namespace
     AND parent.chain_id = child.chain_id
     AND child.normalized_name LIKE '%.%'
     AND parent.normalized_name = substring(
         child.normalized_name FROM position('.' IN child.normalized_name) + 1
     )
    WHERE ne.event_kind IN (
        'RegistrationGranted',
        'RegistrationRenewed',
        'RegistrationReleased'
    )
)
"#;
