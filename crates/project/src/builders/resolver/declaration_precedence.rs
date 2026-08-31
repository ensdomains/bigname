pub(super) const DISCOVERY_CTES: &str = r#"
active_discovery_admissions AS (
    SELECT lower(address.address) AS resolver_address,
           origin.namespace,
           CASE origin.source_family
               WHEN 'ens_v1_registry_l1' THEN 'ens_v1_resolver_l1'
               WHEN 'ens_v1_resolver_l1' THEN 'ens_v1_resolver_l1'
               WHEN 'ens_v2_registry_l1' THEN 'ens_v2_resolver_l1'
               WHEN 'ens_v2_resolver_l1' THEN 'ens_v2_resolver_l1'
               WHEN 'basenames_base_registry' THEN 'basenames_base_resolver'
               WHEN 'basenames_base_resolver' THEN 'basenames_base_resolver'
           END AS source_family
    FROM discovery_edges edge
    JOIN contract_instance_addresses address
      ON address.contract_instance_id = edge.to_contract_instance_id
     AND address.chain_id = edge.chain_id
    LEFT JOIN project_manifests origin
      ON origin.manifest_id = edge.source_manifest_id
    WHERE edge.chain_id = $1
      AND edge.edge_kind = 'resolver'
      AND edge.canonicality_state IN ('canonical', 'safe', 'finalized')
      AND (edge.active_from_block_number IS NULL OR edge.active_from_block_number <= $2)
      AND (edge.active_to_block_number IS NULL OR edge.active_to_block_number > $2)
      AND edge.deactivated_at IS NULL
      AND (
          edge.active_from_block_hash IS NULL
          OR EXISTS (
              SELECT 1 FROM chain_lineage lineage
              WHERE lineage.chain_id = edge.chain_id
                AND lineage.block_number = edge.active_from_block_number
                AND lineage.block_hash = edge.active_from_block_hash
                AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
          )
      )
      AND (address.active_from_block_number IS NULL OR address.active_from_block_number <= $2)
      AND (address.active_to_block_number IS NULL OR address.active_to_block_number > $2)
      AND address.deactivated_at IS NULL
      AND (
          address.active_from_block_hash IS NULL
          OR EXISTS (
              SELECT 1 FROM chain_lineage lineage
              WHERE lineage.chain_id = address.chain_id
                AND lineage.block_number = address.active_from_block_number
                AND lineage.block_hash = address.active_from_block_hash
                AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
          )
      )
),
discovered AS (
    SELECT admission.resolver_address,
           admission.source_family,
           NULL::text AS classification_role,
           1 AS priority,
           NULL::bigint AS classification_manifest_id,
           NULL::text AS classification_admission_namespace,
           NULL::bigint AS classification_declaration_start_block,
           NULL::bigint AS classification_declaration_ordinality
    FROM active_discovery_admissions admission
    UNION ALL
    SELECT admission.resolver_address,
           declaration.source_family,
           declaration.classification_role,
           0 AS priority,
           declaration.manifest_id AS classification_manifest_id,
           admission.namespace AS classification_admission_namespace,
           declaration.declaration_start_block AS classification_declaration_start_block,
           declaration.classification_declaration_ordinality
    FROM active_discovery_admissions admission
    JOIN project_declared_resolver_addresses declaration
      ON declaration.namespace = admission.namespace
     AND declaration.resolver_address = admission.resolver_address
)
"#;
