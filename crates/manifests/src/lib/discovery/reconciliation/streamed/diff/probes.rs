//! Probe-shaped SQL for the streamed desired/stored edge diff.

use super::{STREAMED_EDGE_IS_ORPHANED_SQL, STREAMED_EXISTING_EDGE_COLUMNS_QUALIFIED};

/// These fragments jointly express exact stored-spec equality between an
/// active `discovery_edges` row (`de`) and a staged desired row (`desired`).
/// They mirror `ReconciledDiscoveryEdgeSpec` equality against the spec
/// `load_active_reconciled_discovery_edges` reconstructs.
const SPEC_IDENTITY_MATCH_SQL: &str = r#"
    desired.discovery_source = de.discovery_source
    AND desired.observation_key = de.provenance ->> 'observation_key'
    AND desired.chain_id = de.chain_id
    AND desired.edge_kind = de.edge_kind
    AND desired.from_contract_instance_id = de.from_contract_instance_id
    AND desired.to_contract_instance_id = de.to_contract_instance_id
    AND desired.source_manifest_id = COALESCE(de.source_manifest_id, -1)
    AND desired.admission = de.admission
"#;

const EXACT_SPEC_STATE_MATCH_SQL: &str = r#"
    desired.active_from_block_number IS NOT DISTINCT FROM de.active_from_block_number
    AND desired.active_from_block_hash IS NOT DISTINCT FROM de.active_from_block_hash
    AND desired.provenance_json::JSONB = (
        de.provenance - 'active_to_transaction_index' - 'active_to_log_index'
    )
"#;

/// Keep all predicates except the index's leading expression keys outside
/// this parameterized scan. `OFFSET 0` is an optimization barrier, so the
/// planner cannot push a hub-shaped endpoint predicate into the probe.
const OBSERVATION_POINT_PROBE_SQL: &str = r#"
    SELECT edge.*
    FROM discovery_edges edge
    WHERE edge.discovery_source = $1
      AND edge.provenance ->> 'observation_key'
          = desired.observation_key COLLATE "default"
    OFFSET 0
"#;

/// `assignment_starts_no_later(existing = de, desired)` in SQL.
const STARTS_NO_LATER_SQL: &str = r#"
    (
        de.active_from_block_number IS NULL
        OR (
            desired.active_from_block_number IS NOT NULL
            AND (
                de.active_from_block_number < desired.active_from_block_number
                OR (
                    de.active_from_block_number = desired.active_from_block_number
                    AND (
                        (de.provenance ->> 'transaction_index') IS NULL
                        OR (de.provenance ->> 'log_index') IS NULL
                        OR desired.active_from_transaction_index IS NULL
                        OR desired.active_from_log_index IS NULL
                        OR (
                            (de.provenance ->> 'transaction_index')::BIGINT,
                            (de.provenance ->> 'log_index')::BIGINT
                        ) <= (
                            desired.active_from_transaction_index,
                            desired.active_from_log_index
                        )
                    )
                )
            )
        )
    )
"#;

pub(super) fn deactivation_source_page_sql() -> String {
    format!(
        r#"
        WITH exact_matches AS MATERIALIZED (
            SELECT DISTINCT de.discovery_edge_id
            FROM pg_temp.reconcile_desired_edges desired
            CROSS JOIN LATERAL (
                {OBSERVATION_POINT_PROBE_SQL}
            ) de
            WHERE desired.observation_key = ANY($3::TEXT[])
              AND de.discovery_edge_id = ANY($2::BIGINT[])
              AND de.deactivated_at IS NULL
              AND {SPEC_IDENTITY_MATCH_SQL}
              AND {EXACT_SPEC_STATE_MATCH_SQL}
        )
        SELECT {STREAMED_EXISTING_EDGE_COLUMNS_QUALIFIED},
               exact.discovery_edge_id IS NULL AS deactivation_candidate
        FROM discovery_edges de
        JOIN contract_instance_addresses cia
          ON cia.contract_instance_id = de.to_contract_instance_id
         AND cia.deactivated_at IS NULL
        LEFT JOIN exact_matches exact
          ON exact.discovery_edge_id = de.discovery_edge_id
        WHERE de.discovery_source = $1
          AND de.deactivated_at IS NULL
          AND de.discovery_edge_id = ANY($2::BIGINT[])
        ORDER BY de.discovery_edge_id
        "#
    )
}

pub(super) fn insert_candidate_page_sql() -> String {
    format!(
        r#"
        INSERT INTO pg_temp.reconcile_insert_candidates (desired_row_id)
        SELECT desired.desired_row_id
        FROM pg_temp.reconcile_desired_edges desired
        WHERE desired.desired_row_id = ANY($2::BIGINT[])
          AND NOT EXISTS (
              SELECT 1
              FROM LATERAL (
                  {OBSERVATION_POINT_PROBE_SQL}
              ) de
              JOIN contract_instance_addresses cia
                ON cia.contract_instance_id = de.to_contract_instance_id
               AND cia.deactivated_at IS NULL
              WHERE de.deactivated_at IS NULL
                AND {SPEC_IDENTITY_MATCH_SQL}
                AND (
                    ({EXACT_SPEC_STATE_MATCH_SQL})
                    OR (
                        NOT {STREAMED_EDGE_IS_ORPHANED_SQL}
                        AND {STARTS_NO_LATER_SQL}
                    )
                )
          )
        "#
    )
}
