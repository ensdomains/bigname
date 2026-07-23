/// Canonical manifest/discovery interval rowset used by current watch-plan,
/// scoped-address, historical replay, and coverage reads.
///
/// The rowset deliberately carries eligibility as columns instead of applying
/// one global filter: current watch-plan reads exclude every deactivated row,
/// while historical replay and coverage retain only finitely bounded closed
/// rows. All consumers share rollout, mapped target-family, migration, and
/// interval-overlap decisions.
pub(super) const WATCHED_INTERVALS_CTES: &str = r#"
manifest_watched_intervals AS (
    SELECT
        cia.contract_instance_address_id AS source_row_id,
        mv.chain AS chain,
        mv.source_family AS source_family,
        LOWER(cia.address) AS address,
        mci.contract_instance_id AS contract_instance_id,
        CASE
            WHEN mci.declaration_kind = 'root' THEN 'manifest_root'
            ELSE 'manifest_contract'
        END::TEXT AS source,
        mv.manifest_id AS source_manifest_id,
        CASE
            WHEN manifest_range.start_block IS NULL THEN cia.active_from_block_number
            WHEN cia.active_from_block_number IS NULL THEN manifest_range.start_block
            ELSE GREATEST(manifest_range.start_block, cia.active_from_block_number)
        END AS active_from_block_number,
        cia.active_to_block_number AS active_to_block_number,
        mv.rollout_status = 'active' AS rollout_eligible,
        TRUE AS interval_eligible,
        cia.deactivated_at IS NULL AS current_eligible,
        (
            cia.deactivated_at IS NULL
            OR cia.active_to_block_number IS NOT NULL
        ) AS historical_eligible
    FROM manifest_versions mv
    JOIN manifest_contract_instances mci ON mci.manifest_id = mv.manifest_id
    LEFT JOIN LATERAL (
        SELECT (entry ->> 'start_block')::BIGINT AS start_block
        FROM jsonb_array_elements(
            CASE
                WHEN mci.declaration_kind = 'root' THEN mv.manifest_payload -> 'roots'
                ELSE mv.manifest_payload -> 'contracts'
            END
        ) entry
        WHERE (
                mci.declaration_kind = 'root'
                AND entry ->> 'name' = mci.declaration_name
            )
           OR (
                mci.declaration_kind = 'contract'
                AND entry ->> 'role' = mci.declaration_name
            )
        ORDER BY start_block NULLS LAST
        LIMIT 1
    ) manifest_range ON TRUE
    JOIN contract_instance_addresses cia
      ON cia.contract_instance_id = mci.contract_instance_id
     AND cia.chain_id = mv.chain
),
discovery_watched_intervals AS (
    SELECT
        de.discovery_edge_id AS source_row_id,
        de.chain_id AS chain,
        COALESCE(target_mv.source_family, mv.source_family) AS source_family,
        LOWER(cia.address) AS address,
        de.to_contract_instance_id AS contract_instance_id,
        'discovery_edge'::TEXT AS source,
        COALESCE(target_mv.manifest_id, de.source_manifest_id) AS source_manifest_id,
        CASE
            WHEN de.active_from_block_number IS NULL THEN cia.active_from_block_number
            WHEN cia.active_from_block_number IS NULL THEN de.active_from_block_number
            ELSE GREATEST(de.active_from_block_number, cia.active_from_block_number)
        END AS active_from_block_number,
        CASE
            WHEN de.active_to_block_number IS NULL THEN cia.active_to_block_number
            WHEN cia.active_to_block_number IS NULL THEN de.active_to_block_number
            ELSE LEAST(de.active_to_block_number, cia.active_to_block_number)
        END AS active_to_block_number,
        (
            mv.rollout_status = 'active'
            AND (
                de.edge_kind <> 'resolver'
                OR mv.source_family NOT IN (
                    'ens_v1_registry_l1',
                    'ens_v2_registry_l1',
                    'basenames_base_registry'
                )
                OR target_mv.manifest_id IS NOT NULL
            )
        ) AS rollout_eligible,
        (
            de.edge_kind <> 'migration'
            AND (
                de.active_from_block_number IS NULL
                OR cia.active_to_block_number IS NULL
                OR de.active_from_block_number <= cia.active_to_block_number
            )
            AND (
                cia.active_from_block_number IS NULL
                OR de.active_to_block_number IS NULL
                OR cia.active_from_block_number <= de.active_to_block_number
            )
        ) AS interval_eligible,
        (
            de.deactivated_at IS NULL
            AND cia.deactivated_at IS NULL
        ) AS current_eligible,
        (
            (
                de.deactivated_at IS NULL
                OR de.active_to_block_number IS NOT NULL
            )
            AND (
                cia.deactivated_at IS NULL
                OR cia.active_to_block_number IS NOT NULL
                OR de.active_to_block_number IS NOT NULL
            )
        ) AS historical_eligible
    FROM discovery_edges de
    JOIN manifest_versions mv ON mv.manifest_id = de.source_manifest_id
    LEFT JOIN manifest_versions target_mv
      ON target_mv.rollout_status = 'active'
     AND target_mv.namespace = mv.namespace
     AND target_mv.chain = de.chain_id
     AND target_mv.deployment_epoch = mv.deployment_epoch
     AND target_mv.source_family = CASE
         WHEN de.edge_kind = 'resolver' AND mv.source_family = 'ens_v1_registry_l1'
             THEN 'ens_v1_resolver_l1'
         WHEN de.edge_kind = 'resolver' AND mv.source_family = 'ens_v2_registry_l1'
             THEN 'ens_v2_resolver_l1'
         WHEN de.edge_kind = 'resolver' AND mv.source_family = 'basenames_base_registry'
             THEN 'basenames_base_resolver'
         ELSE NULL
     END
    JOIN contract_instance_addresses cia
      ON cia.contract_instance_id = de.to_contract_instance_id
     AND cia.chain_id = de.chain_id
),
watched_intervals AS (
    SELECT
        chain,
        source_family,
        address,
        contract_instance_id,
        source,
        source_manifest_id,
        active_from_block_number,
        active_to_block_number,
        rollout_eligible,
        interval_eligible,
        current_eligible,
        historical_eligible
    FROM manifest_watched_intervals
    UNION
    SELECT
        chain,
        source_family,
        address,
        contract_instance_id,
        source,
        source_manifest_id,
        active_from_block_number,
        active_to_block_number,
        rollout_eligible,
        interval_eligible,
        current_eligible,
        historical_eligible
    FROM discovery_watched_intervals
)
"#;

pub(super) const CURRENT_WATCHED_INTERVAL_PREDICATE: &str = r#"
watched.rollout_eligible
AND watched.interval_eligible
AND watched.current_eligible
"#;

pub(super) const HISTORICAL_WATCHED_INTERVAL_PREDICATE: &str = r#"
watched.rollout_eligible
AND watched.interval_eligible
AND watched.historical_eligible
"#;

pub(super) fn with_watched_intervals(query_body: &str) -> String {
    format!("WITH\n{WATCHED_INTERVALS_CTES}\n{query_body}")
}

/// Stream the two disjoint source kinds without forcing PostgreSQL to
/// materialize and sort the multi-million-row union before returning its
/// first row. The progress-aware caller restores exact `UNION` deduplication
/// in a sorted set while rows arrive.
pub(super) fn with_streaming_watched_intervals(query_body: &str) -> String {
    let streaming_ctes = WATCHED_INTERVALS_CTES.replacen(
        "\n    FROM manifest_watched_intervals\n    UNION\n",
        "\n    FROM manifest_watched_intervals\n    UNION ALL\n",
        1,
    );
    format!("WITH\n{streaming_ctes}\n{query_body}")
}
