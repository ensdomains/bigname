//! The SQL F3 shares with the served resolver build: the candidate classification of build.sql.
//! The active manifests come from the run's captured history (manifests.rs).

/// The served classification over the input resolvers: the candidate precedence of build.sql
/// `candidates`, the manifest join, role, support and read features of `classified` to
/// `summarized`, one row per resolver with a candidate (the manifest with the lowest id when
/// several of its family are active).
pub(super) const CLASSIFY: &str = r#"
declared AS (
    SELECT manifest.namespace, manifest.source_family,
           lower(declaration ->> 'address') AS resolver_address,
           declaration ->> 'role' AS classification_role,
           (declaration ->> 'start_block')::bigint AS declaration_start_block,
           declaration_ordinality AS classification_declaration_ordinality,
           manifest.manifest_id
    FROM manifests manifest
    CROSS JOIN LATERAL jsonb_array_elements(COALESCE(
        manifest.manifest_payload -> 'contracts', '[]'::jsonb
    )) WITH ORDINALITY declarations(declaration, declaration_ordinality)
    WHERE (manifest.source_family = 'ens_v1_resolver_l1'
           OR (manifest.source_family = 'ens_v2_resolver_l1'
               AND declaration ->> 'role' IN ('public_resolver_v2', 'ensv1_mirror_resolver')
               AND declaration ->> 'proxy_kind' = 'none'))
      AND lower(declaration ->> 'address') IN (SELECT resolver_address FROM input)
      AND (declaration ->> 'start_block' IS NULL
           OR (declaration ->> 'start_block')::bigint <= $2)
      AND (manifest.source_family <> 'ens_v2_resolver_l1' OR NOT EXISTS (
          SELECT 1 FROM jsonb_array_elements(manifest.manifest_payload -> 'contracts')
              WITH ORDINALITY later(item, ordinal)
          WHERE lower(item ->> 'address') = lower(declaration ->> 'address')
            AND COALESCE((item ->> 'start_block')::bigint, 0) <= $2
            AND (COALESCE((item ->> 'start_block')::bigint, 0), ordinal) >
                (COALESCE((declaration ->> 'start_block')::bigint, 0), declaration_ordinality)
      ))
),
admissions AS (
    SELECT lower(address.address) AS resolver_address, origin.namespace,
           CASE origin.source_family
               WHEN 'ens_v1_registry_l1' THEN 'ens_v1_resolver_l1'
               WHEN 'ens_v1_resolver_l1' THEN 'ens_v1_resolver_l1'
               WHEN 'ens_v2_registry_l1' THEN 'ens_v2_resolver_l1'
               WHEN 'ens_v2_resolver_l1' THEN 'ens_v2_resolver_l1'
               WHEN 'basenames_base_registry' THEN 'basenames_base_resolver'
               WHEN 'basenames_base_resolver' THEN 'basenames_base_resolver'
           END AS source_family
    FROM contract_instance_addresses address
    JOIN discovery_edges edge
      ON edge.to_contract_instance_id = address.contract_instance_id
     AND edge.chain_id = address.chain_id
    LEFT JOIN manifests origin ON origin.manifest_id = edge.source_manifest_id
    WHERE address.chain_id = $1
      AND lower(address.address) IN (SELECT resolver_address FROM input)
      AND edge.edge_kind = 'resolver'
      AND edge.canonicality_state IN ('canonical', 'safe', 'finalized')
      AND (edge.active_from_block_number IS NULL OR edge.active_from_block_number <= $2)
      AND (edge.active_to_block_number IS NULL OR edge.active_to_block_number > $2)
      AND edge.deactivated_at IS NULL
      AND (edge.active_from_block_hash IS NULL OR EXISTS (
          SELECT 1 FROM chain_lineage lineage
          WHERE lineage.chain_id = edge.chain_id
            AND lineage.block_number = edge.active_from_block_number
            AND lineage.block_hash = edge.active_from_block_hash
            AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')))
      AND (address.active_from_block_number IS NULL OR address.active_from_block_number <= $2)
      AND (address.active_to_block_number IS NULL OR address.active_to_block_number > $2)
      AND address.deactivated_at IS NULL
      AND (address.active_from_block_hash IS NULL OR EXISTS (
          SELECT 1 FROM chain_lineage lineage
          WHERE lineage.chain_id = address.chain_id
            AND lineage.block_number = address.active_from_block_number
            AND lineage.block_hash = address.active_from_block_hash
            AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')))
),
combined AS (
    SELECT admission.resolver_address, admission.source_family,
           NULL::text AS classification_role, 1 AS priority,
           NULL::bigint AS classification_manifest_id,
           NULL::text AS classification_admission_namespace,
           NULL::bigint AS classification_declaration_start_block,
           NULL::bigint AS classification_declaration_ordinality
    FROM admissions admission
    UNION ALL
    SELECT admission.resolver_address, declaration.source_family,
           declaration.classification_role, 0, declaration.manifest_id, admission.namespace,
           declaration.declaration_start_block,
           declaration.classification_declaration_ordinality
    FROM admissions admission
    JOIN declared declaration
      ON declaration.namespace = admission.namespace
     AND declaration.resolver_address = admission.resolver_address
    UNION ALL
    SELECT input.resolver_address, candidate ->> 'family', NULL, (candidate ->> 'priority')::int,
           NULL, NULL, NULL, NULL
    FROM input
    CROSS JOIN LATERAL jsonb_array_elements(input.item -> 'candidates') candidate
),
candidates AS (
    SELECT DISTINCT ON (combined.resolver_address) combined.*
    FROM combined
    WHERE combined.source_family IS NOT NULL
    ORDER BY combined.resolver_address, combined.priority, combined.source_family,
             combined.classification_manifest_id NULLS LAST,
             COALESCE(combined.classification_declaration_start_block, 0) DESC,
             combined.classification_declaration_ordinality DESC NULLS LAST,
             combined.classification_role
),
classified AS (
    SELECT DISTINCT ON (candidate.resolver_address)
           candidate.resolver_address, candidate.source_family,
           candidate.classification_admission_namespace,
           manifest.manifest_id, manifest.manifest_payload, manifest.manifest_event_id,
           upgrade.value AS upgrade,
           upgrade.value ->> 'implementation' AS implementation,
           COALESCE((
               SELECT declaration -> 'read_features'
               FROM jsonb_array_elements(COALESCE(
                   manifest.manifest_payload -> 'contracts', '[]'::jsonb
               )) WITH ORDINALITY declarations(declaration, declaration_ordinality)
               WHERE lower(declaration ->> 'address') = candidate.resolver_address
                 AND (declaration ->> 'start_block' IS NULL
                      OR (declaration ->> 'start_block')::bigint <= $2)
               ORDER BY COALESCE((declaration ->> 'start_block')::bigint, 0) DESC,
                        declaration_ordinality DESC
               LIMIT 1
           ), '[]'::jsonb) AS declared_read_features,
           COALESCE((
               SELECT admitted -> 'read_features'
               FROM jsonb_array_elements(COALESCE(
                   manifest.manifest_payload -> 'resolver_implementations', '[]'::jsonb
               )) WITH ORDINALITY implementations(admitted, admitted_ordinality)
               WHERE lower(admitted ->> 'address') = lower(upgrade.value ->> 'implementation')
               ORDER BY admitted_ordinality DESC
               LIMIT 1
           ), '[]'::jsonb) AS implementation_read_features,
           COALESCE(
               candidate.classification_role,
               (
                   SELECT declaration ->> 'role'
                   FROM jsonb_array_elements(COALESCE(
                       manifest.manifest_payload -> 'contracts', '[]'::jsonb
                   )) WITH ORDINALITY declarations(declaration, declaration_ordinality)
                   WHERE lower(declaration ->> 'address') = candidate.resolver_address
                     AND (declaration ->> 'start_block' IS NULL
                          OR (declaration ->> 'start_block')::bigint <= $2)
                   ORDER BY COALESCE((declaration ->> 'start_block')::bigint, 0) DESC,
                            declaration_ordinality DESC
                   LIMIT 1
               ),
               (
                   SELECT implementation ->> 'role'
                   FROM jsonb_array_elements(COALESCE(
                       manifest.manifest_payload -> 'resolver_implementations', '[]'::jsonb
                   )) WITH ORDINALITY implementations(implementation, implementation_ordinality)
                   WHERE lower(implementation ->> 'address') =
                         lower(upgrade.value ->> 'implementation')
                   ORDER BY implementation_ordinality DESC
                   LIMIT 1
               )
           ) AS classification_role,
           EXISTS (
               SELECT 1 FROM jsonb_array_elements(COALESCE(
                   manifest.manifest_payload -> 'contracts', '[]'::jsonb)) declaration
               WHERE lower(declaration ->> 'address') = candidate.resolver_address
                 AND (declaration ->> 'role' <> 'public_resolver_v2'
                      OR declaration ->> 'proxy_kind' = 'none')
                 AND (declaration ->> 'start_block' IS NULL
                      OR (declaration ->> 'start_block')::bigint <= $2)
           ) AS exact_declared,
           EXISTS (SELECT 1 FROM declared direct
                   WHERE direct.manifest_id = manifest.manifest_id
                     AND direct.resolver_address = candidate.resolver_address
                     AND direct.classification_role = 'public_resolver_v2'
                     AND direct.source_family = 'ens_v2_resolver_l1') AS direct_public_v2,
           EXISTS (SELECT 1 FROM declared direct
                   WHERE direct.manifest_id = manifest.manifest_id
                     AND direct.resolver_address = candidate.resolver_address
                     AND direct.classification_role = 'ensv1_mirror_resolver'
                     AND direct.source_family = 'ens_v2_resolver_l1') AS direct_mirror,
           EXISTS (
               SELECT 1 FROM jsonb_array_elements(COALESCE(
                   manifest.manifest_payload -> 'resolver_implementations', '[]'::jsonb
               )) implementation
               WHERE lower(implementation ->> 'address') =
                     lower(upgrade.value ->> 'implementation')
           ) AS upgraded_to_declared
    FROM candidates candidate
    JOIN input ON input.resolver_address = candidate.resolver_address
    LEFT JOIN manifests manifest
      ON (candidate.classification_manifest_id IS NOT NULL
          AND manifest.manifest_id = candidate.classification_manifest_id)
      OR (candidate.classification_manifest_id IS NULL
          AND manifest.source_family = candidate.source_family)
    LEFT JOIN LATERAL (
        SELECT input.item -> 'upgrades' -> candidate.source_family AS value
    ) upgrade ON TRUE
    ORDER BY candidate.resolver_address, manifest.manifest_id NULLS LAST
),
supported AS (
    SELECT classified.*,
           CASE WHEN manifest_id IS NULL THEN false
                WHEN source_family = 'ens_v2_resolver_l1'
                     AND classification_role = 'public_resolver_v2' THEN direct_public_v2
                WHEN source_family = 'ens_v2_resolver_l1'
                     AND classification_role = 'ensv1_mirror_resolver' THEN direct_mirror
                WHEN source_family = 'ens_v2_resolver_l1'
                    THEN upgraded_to_declared AND upgrade IS NOT NULL
                ELSE exact_declared END AS supported,
           CASE
               WHEN manifest_id IS NULL THEN 'resolver_manifest_not_active'
               WHEN source_family = 'ens_v2_resolver_l1'
                AND classification_role = 'public_resolver_v2'
                   THEN CASE WHEN NOT direct_public_v2 THEN 'resolver_not_declared' END
               WHEN source_family = 'ens_v2_resolver_l1'
                AND classification_role = 'ensv1_mirror_resolver'
                   THEN CASE WHEN NOT direct_mirror THEN 'resolver_not_declared' END
               WHEN source_family = 'ens_v2_resolver_l1' AND upgrade IS NULL
                   THEN 'resolver_implementation_unknown'
               WHEN source_family = 'ens_v2_resolver_l1' AND NOT upgraded_to_declared
                   THEN 'resolver_implementation_not_declared'
               WHEN source_family <> 'ens_v2_resolver_l1' AND NOT exact_declared
                   THEN 'resolver_not_declared'
           END AS support_reason
    FROM classified
)
SELECT jsonb_build_object(
           'resolver_address', resolver_address,
           'classification', jsonb_strip_nulls(jsonb_build_object(
               'source_family', source_family,
               'role', classification_role,
               'basis', CASE WHEN source_family = 'ens_v2_resolver_l1'
                    AND COALESCE(classification_role, '')
                        NOT IN ('public_resolver_v2', 'ensv1_mirror_resolver')
                   THEN 'erc1967_upgraded_history'
                   ELSE 'manifest_declared_address' END,
               'implementation', implementation,
               'read_features', CASE
                   WHEN NOT supported THEN '[]'::jsonb
                   WHEN source_family = 'ens_v2_resolver_l1'
                    AND COALESCE(classification_role, '')
                        NOT IN ('public_resolver_v2', 'ensv1_mirror_resolver')
                       THEN implementation_read_features
                   ELSE declared_read_features
               END,
               'mirror', CASE WHEN classification_role = 'ensv1_mirror_resolver'
                   THEN jsonb_strip_nulls(jsonb_build_object(
                       'mirrored_source_family', 'ens_v1_resolver_l1',
                       'mirrored_registry_source_family', 'ens_v1_registry_l1',
                       'mirrored_registry_address',
                           lower(manifest_payload #>> '{correlation_addresses,ens_v1_registry}')
                   )) END,
               'upgrade', upgrade
           )),
           'support_status', CASE WHEN supported THEN 'supported' ELSE 'unsupported' END,
           'unsupported_reason', support_reason,
           'manifest_id', manifest_id,
           'manifest_event_id', manifest_event_id,
           'admission_namespace', classification_admission_namespace
       )
FROM supported
"#;
