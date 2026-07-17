//! Pre-mutation set-diff queries between the staged desired edges and the
//! stored `discovery_edges` snapshot for the streamed full-source
//! reconcile, including the SQL translations of the stored-spec equality
//! and chronology comparisons.

use std::collections::HashSet;

use anyhow::{Context, Result, bail};
use sqlx::{Row, postgres::PgConnection};

use super::super::super::types::{
    EvmEventPosition, ExistingReconciledDiscoveryEdge, ObservationTerminalState,
    ReconciledDiscoveryEdgeSpec,
};
use super::super::chronology::{
    assignment_starts_no_later, compare_edge_starts, edge_starts_after_spec,
};
use super::super::existing::edge_from_row;

/// Exact stored-spec equality between an active `discovery_edges` row (`de`,
/// with `cia` as its active to-address join) and a staged desired row
/// (`desired`). This mirrors `ReconciledDiscoveryEdgeSpec` equality against
/// the spec `load_active_reconciled_discovery_edges` reconstructs:
/// `source_manifest_id` NULL loads as -1, `observation_key` and the event
/// position come from provenance, and provenance compares as jsonb minus the
/// `active_to_*` position keys — the loader round-trips stored provenance
/// through jsonb, so jsonb equality is the loader's text equality.
const STREAMED_EXACT_SPEC_MATCH_SQL: &str = r#"
    desired.discovery_source = de.discovery_source
    AND desired.observation_key = de.provenance ->> 'observation_key'
    AND desired.chain_id = de.chain_id
    AND desired.edge_kind = de.edge_kind
    AND desired.from_contract_instance_id = de.from_contract_instance_id
    AND desired.to_contract_instance_id = de.to_contract_instance_id
    AND desired.source_manifest_id = COALESCE(de.source_manifest_id, -1)
    AND desired.admission = de.admission
    AND desired.active_from_block_number IS NOT DISTINCT FROM de.active_from_block_number
    AND desired.active_from_block_hash IS NOT DISTINCT FROM de.active_from_block_hash
    AND desired.provenance_json::JSONB = (
        de.provenance - 'active_to_transaction_index' - 'active_to_log_index'
    )
"#;

const STREAMED_ACTIVE_EDGE_FROM_SQL: &str = r#"
    FROM discovery_edges de
    JOIN contract_instance_addresses cia
      ON cia.contract_instance_id = de.to_contract_instance_id
     AND cia.deactivated_at IS NULL
    WHERE de.discovery_source = $1
      AND de.deactivated_at IS NULL
"#;

const STREAMED_EXISTING_EDGE_SELECT_SQL: &str = r#"
    SELECT
        de.discovery_edge_id,
        de.provenance ->> 'observation_key' AS observation_key,
        de.chain_id,
        de.edge_kind,
        de.from_contract_instance_id,
        de.to_contract_instance_id,
        de.discovery_source,
        de.source_manifest_id,
        de.admission,
        de.active_from_block_number,
        de.active_from_block_hash,
        de.provenance,
        cia.address AS to_address,
        EXISTS (
            SELECT 1
            FROM chain_lineage rb
            WHERE rb.chain_id = de.chain_id
              AND rb.block_hash = de.active_from_block_hash
              AND rb.canonicality_state = 'orphaned'::canonicality_state
        ) AS active_from_block_is_orphaned
"#;

const STREAMED_EDGE_IS_ORPHANED_SQL: &str = r#"
    EXISTS (
        SELECT 1
        FROM chain_lineage start_block
        WHERE start_block.chain_id = de.chain_id
          AND start_block.block_hash = de.active_from_block_hash
          AND start_block.canonicality_state = 'orphaned'::canonicality_state
    )
"#;

/// `assignment_starts_no_later(existing = de, desired)` in SQL: a missing
/// existing start is "no later"; an existing start needs a desired start to
/// compare; equal blocks fall back to the block-inclusive comparison unless
/// both sides carry a full event position.
const STREAMED_STARTS_NO_LATER_SQL: &str = r#"
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

/// `starts_after(existing = de, desired)` in SQL: both block numbers must be
/// present; equal blocks only compare when both sides carry a full event
/// position.
const STREAMED_STARTS_AFTER_SQL: &str = r#"
    (
        de.active_from_block_number IS NOT NULL
        AND desired.active_from_block_number IS NOT NULL
        AND (
            de.active_from_block_number > desired.active_from_block_number
            OR (
                de.active_from_block_number = desired.active_from_block_number
                AND (de.provenance ->> 'transaction_index') IS NOT NULL
                AND (de.provenance ->> 'log_index') IS NOT NULL
                AND desired.active_from_transaction_index IS NOT NULL
                AND desired.active_from_log_index IS NOT NULL
                AND (
                    (de.provenance ->> 'transaction_index')::BIGINT,
                    (de.provenance ->> 'log_index')::BIGINT
                ) > (
                    desired.active_from_transaction_index,
                    desired.active_from_log_index
                )
            )
        )
    )
"#;

pub(super) async fn count_streamed_deactivation_candidates(
    executor: &mut PgConnection,
    discovery_source: &str,
) -> Result<usize> {
    let count = sqlx::query_scalar::<_, i64>(&format!(
        r#"
        SELECT COUNT(*)::BIGINT
        {STREAMED_ACTIVE_EDGE_FROM_SQL}
          AND NOT EXISTS (
              SELECT 1
              FROM pg_temp.reconcile_desired_edges desired
              WHERE {STREAMED_EXACT_SPEC_MATCH_SQL}
          )
        "#
    ))
    .bind(discovery_source)
    .fetch_one(executor)
    .await
    .context("failed to count streamed discovery-edge deactivation candidates")?;
    usize::try_from(count).context("streamed deactivation candidate count overflowed usize")
}

pub(super) async fn load_streamed_deactivation_candidates(
    executor: &mut PgConnection,
    discovery_source: &str,
) -> Result<Vec<ExistingReconciledDiscoveryEdge>> {
    let rows = sqlx::query(&format!(
        r#"
        {STREAMED_EXISTING_EDGE_SELECT_SQL}
        {STREAMED_ACTIVE_EDGE_FROM_SQL}
          AND NOT EXISTS (
              SELECT 1
              FROM pg_temp.reconcile_desired_edges desired
              WHERE {STREAMED_EXACT_SPEC_MATCH_SQL}
          )
        ORDER BY de.discovery_edge_id
        "#
    ))
    .bind(discovery_source)
    .fetch_all(executor)
    .await
    .context("failed to load streamed discovery-edge deactivation candidates")?;

    rows.into_iter().map(edge_from_row).collect()
}

fn desired_edge_spec_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<(i64, ReconciledDiscoveryEdgeSpec)> {
    let desired_row_id = row
        .try_get("desired_row_id")
        .context("failed to read desired_row_id")?;
    let transaction_index: Option<i64> = row
        .try_get("active_from_transaction_index")
        .context("failed to read desired active_from_transaction_index")?;
    let log_index: Option<i64> = row
        .try_get("active_from_log_index")
        .context("failed to read desired active_from_log_index")?;
    let active_from_event_position = match (transaction_index, log_index) {
        (Some(transaction_index), Some(log_index)) => Some(EvmEventPosition {
            transaction_index,
            log_index,
        }),
        (None, None) => None,
        _ => bail!("staged desired discovery edge carries a partial event position"),
    };
    Ok((
        desired_row_id,
        ReconciledDiscoveryEdgeSpec {
            observation_key: row
                .try_get("observation_key")
                .context("failed to read desired observation_key")?,
            chain: row
                .try_get("chain_id")
                .context("failed to read desired chain_id")?,
            edge_kind: row
                .try_get("edge_kind")
                .context("failed to read desired edge_kind")?,
            from_contract_instance_id: row
                .try_get("from_contract_instance_id")
                .context("failed to read desired from_contract_instance_id")?,
            to_contract_instance_id: row
                .try_get("to_contract_instance_id")
                .context("failed to read desired to_contract_instance_id")?,
            discovery_source: row
                .try_get("discovery_source")
                .context("failed to read desired discovery_source")?,
            source_manifest_id: row
                .try_get("source_manifest_id")
                .context("failed to read desired source_manifest_id")?,
            admission: row
                .try_get("admission")
                .context("failed to read desired admission")?,
            active_from_block_number: row
                .try_get("active_from_block_number")
                .context("failed to read desired active_from_block_number")?,
            active_from_block_hash: row
                .try_get("active_from_block_hash")
                .context("failed to read desired active_from_block_hash")?,
            active_from_event_position,
            provenance_json: row
                .try_get("provenance_json")
                .context("failed to read desired provenance_json")?,
        },
    ))
}

const STREAMED_DESIRED_EDGE_COLUMNS: &str = r#"
    desired.desired_row_id,
    desired.observation_key,
    desired.chain_id,
    desired.edge_kind,
    desired.from_contract_instance_id,
    desired.to_contract_instance_id,
    desired.discovery_source,
    desired.source_manifest_id,
    desired.admission,
    desired.active_from_block_number,
    desired.active_from_block_hash,
    desired.active_from_transaction_index,
    desired.active_from_log_index,
    desired.provenance_json
"#;

/// Materialize insert candidates against the pre-mutation edge snapshot:
/// desired specs with no exact active match and no non-orphaned active edge
/// materializing the same assignment at a no-later start (`current_new_
/// edges` in the in-memory chronology, before its historical exclusion,
/// which the caller applies from `collect_streamed_historical_edges`).
pub(super) async fn materialize_streamed_insert_candidates(
    executor: &mut PgConnection,
    discovery_source: &str,
) -> Result<()> {
    sqlx::query(
        r#"
        CREATE TEMP TABLE pg_temp.reconcile_insert_candidates (
            desired_row_id BIGINT PRIMARY KEY
        ) ON COMMIT DROP
        "#,
    )
    .execute(&mut *executor)
    .await
    .context("failed to create the streamed reconcile insert-candidate temp table")?;

    sqlx::query(&format!(
        r#"
        INSERT INTO pg_temp.reconcile_insert_candidates (desired_row_id)
        SELECT desired.desired_row_id
        FROM pg_temp.reconcile_desired_edges desired
        WHERE NOT EXISTS (
            SELECT 1
            FROM discovery_edges de
            JOIN contract_instance_addresses cia
              ON cia.contract_instance_id = de.to_contract_instance_id
             AND cia.deactivated_at IS NULL
            WHERE de.discovery_source = $1
              AND de.deactivated_at IS NULL
              AND {STREAMED_EXACT_SPEC_MATCH_SQL}
        )
        AND NOT EXISTS (
            SELECT 1
            FROM discovery_edges de
            JOIN contract_instance_addresses cia
              ON cia.contract_instance_id = de.to_contract_instance_id
             AND cia.deactivated_at IS NULL
            WHERE de.discovery_source = $1
              AND de.deactivated_at IS NULL
              AND de.discovery_source = desired.discovery_source
              AND de.provenance ->> 'observation_key' = desired.observation_key
              AND de.chain_id = desired.chain_id
              AND de.edge_kind = desired.edge_kind
              AND de.from_contract_instance_id = desired.from_contract_instance_id
              AND de.to_contract_instance_id = desired.to_contract_instance_id
              AND COALESCE(de.source_manifest_id, -1) = desired.source_manifest_id
              AND de.admission = desired.admission
              AND NOT {STREAMED_EDGE_IS_ORPHANED_SQL}
              AND {STREAMED_STARTS_NO_LATER_SQL}
        )
        "#
    ))
    .bind(discovery_source)
    .execute(&mut *executor)
    .await
    .context("failed to materialize streamed discovery-edge insert candidates")?;

    Ok(())
}

pub(super) async fn load_streamed_insert_candidate_page(
    executor: &mut PgConnection,
    after_row_id: i64,
    limit: i64,
) -> Result<Vec<(i64, ReconciledDiscoveryEdgeSpec)>> {
    let rows = sqlx::query(&format!(
        r#"
        SELECT {STREAMED_DESIRED_EDGE_COLUMNS}
        FROM pg_temp.reconcile_insert_candidates candidate
        JOIN pg_temp.reconcile_desired_edges desired
          ON desired.desired_row_id = candidate.desired_row_id
        WHERE candidate.desired_row_id > $1
        ORDER BY candidate.desired_row_id
        LIMIT $2
        "#
    ))
    .bind(after_row_id)
    .bind(limit)
    .fetch_all(executor)
    .await
    .context("failed to page streamed discovery-edge insert candidates")?;

    rows.iter().map(desired_edge_spec_from_row).collect()
}

/// Chronology rule 3 for the deactivation candidates: for every desired edge
/// sharing an assignment identity with a candidate at a no-later start,
/// resolve the earliest-starting active edge materializing that assignment
/// (over ALL active edges, not just candidates) and retain it.
pub(super) async fn collect_same_assignment_retained_edges(
    executor: &mut PgConnection,
    discovery_source: &str,
    candidates: &[ExistingReconciledDiscoveryEdge],
    retained_newer_edge_ids: &mut HashSet<i64>,
) -> Result<()> {
    let mut matched_desired = HashSet::<ReconciledDiscoveryEdgeSpec>::new();
    for candidate in candidates {
        let rows = sqlx::query(&format!(
            r#"
            SELECT {STREAMED_DESIRED_EDGE_COLUMNS}
            FROM pg_temp.reconcile_desired_edges desired
            WHERE desired.observation_key = $1
              AND desired.chain_id = $2
              AND desired.edge_kind = $3
              AND desired.from_contract_instance_id = $4
              AND desired.to_contract_instance_id = $5
              AND desired.discovery_source = $6
              AND desired.source_manifest_id = $7
              AND desired.admission = $8
            "#
        ))
        .bind(&candidate.spec.observation_key)
        .bind(&candidate.spec.chain)
        .bind(&candidate.spec.edge_kind)
        .bind(candidate.spec.from_contract_instance_id)
        .bind(candidate.spec.to_contract_instance_id)
        .bind(&candidate.spec.discovery_source)
        .bind(candidate.spec.source_manifest_id)
        .bind(&candidate.spec.admission)
        .fetch_all(&mut *executor)
        .await
        .context("failed to load same-assignment desired edges for a deactivation candidate")?;
        for row in &rows {
            let (_, desired) = desired_edge_spec_from_row(row)?;
            if !candidate.active_from_block_is_orphaned
                && assignment_starts_no_later(candidate, &desired)
            {
                matched_desired.insert(desired);
            }
        }
    }

    for desired in matched_desired {
        let rows = sqlx::query(&format!(
            r#"
            {STREAMED_EXISTING_EDGE_SELECT_SQL}
            {STREAMED_ACTIVE_EDGE_FROM_SQL}
              AND de.provenance ->> 'observation_key' = $2
              AND de.chain_id = $3
              AND de.edge_kind = $4
              AND de.from_contract_instance_id = $5
              AND de.to_contract_instance_id = $6
              AND COALESCE(de.source_manifest_id, -1) = $7
              AND de.admission = $8
            "#
        ))
        .bind(discovery_source)
        .bind(&desired.observation_key)
        .bind(&desired.chain)
        .bind(&desired.edge_kind)
        .bind(desired.from_contract_instance_id)
        .bind(desired.to_contract_instance_id)
        .bind(desired.source_manifest_id)
        .bind(&desired.admission)
        .fetch_all(&mut *executor)
        .await
        .context("failed to load same-assignment active edges for a desired edge")?;
        let matching_edges = rows
            .into_iter()
            .map(edge_from_row)
            .collect::<Result<Vec<_>>>()?;
        if let Some(retained) = matching_edges
            .iter()
            .filter(|edge| {
                !edge.active_from_block_is_orphaned && assignment_starts_no_later(edge, &desired)
            })
            .min_by(compare_edge_starts)
        {
            retained_newer_edge_ids.insert(retained.discovery_edge_id);
        }
    }
    Ok(())
}

/// Chronology rule 2: desired edges with a non-orphaned active successor
/// (same observation key, chain, edge kind, and from-instance, starting
/// strictly after the desired start) become closed historical epochs with
/// the successor's start as their terminal; the successor is retained.
pub(super) async fn collect_streamed_historical_edges(
    executor: &mut PgConnection,
    discovery_source: &str,
    retained_newer_edge_ids: &mut HashSet<i64>,
) -> Result<Vec<(i64, ReconciledDiscoveryEdgeSpec, ObservationTerminalState)>> {
    let rows = sqlx::query(&format!(
        r#"
        SELECT {STREAMED_DESIRED_EDGE_COLUMNS}
        FROM pg_temp.reconcile_desired_edges desired
        WHERE desired.active_from_block_number IS NOT NULL
          AND EXISTS (
              SELECT 1
              FROM discovery_edges de
              JOIN contract_instance_addresses cia
                ON cia.contract_instance_id = de.to_contract_instance_id
               AND cia.deactivated_at IS NULL
              WHERE de.discovery_source = $1
                AND de.deactivated_at IS NULL
                AND de.provenance ->> 'observation_key' = desired.observation_key
                AND de.chain_id = desired.chain_id
                AND de.edge_kind = desired.edge_kind
                AND de.from_contract_instance_id = desired.from_contract_instance_id
                AND NOT {STREAMED_EDGE_IS_ORPHANED_SQL}
                AND {STREAMED_STARTS_AFTER_SQL}
          )
        ORDER BY desired.desired_row_id
        "#
    ))
    .bind(discovery_source)
    .fetch_all(&mut *executor)
    .await
    .context("failed to load streamed historical desired discovery edges")?;
    let historical_desired = rows
        .iter()
        .map(desired_edge_spec_from_row)
        .collect::<Result<Vec<_>>>()?;

    let mut historical_edges = Vec::new();
    for (desired_row_id, desired) in historical_desired {
        let rows = sqlx::query(&format!(
            r#"
            {STREAMED_EXISTING_EDGE_SELECT_SQL}
            {STREAMED_ACTIVE_EDGE_FROM_SQL}
              AND de.provenance ->> 'observation_key' = $2
              AND de.chain_id = $3
              AND de.edge_kind = $4
              AND de.from_contract_instance_id = $5
            "#
        ))
        .bind(discovery_source)
        .bind(&desired.observation_key)
        .bind(&desired.chain)
        .bind(&desired.edge_kind)
        .bind(desired.from_contract_instance_id)
        .fetch_all(&mut *executor)
        .await
        .context("failed to load successor candidates for a historical desired edge")?;
        let successor_candidates = rows
            .into_iter()
            .map(edge_from_row)
            .collect::<Result<Vec<_>>>()?;
        let Some(successor) = successor_candidates
            .iter()
            .filter(|edge| {
                !edge.active_from_block_is_orphaned && edge_starts_after_spec(edge, &desired)
            })
            .min_by(compare_edge_starts)
        else {
            continue;
        };
        retained_newer_edge_ids.insert(successor.discovery_edge_id);
        let terminal_state = ObservationTerminalState {
            chain: successor.spec.chain.clone(),
            block_number: successor.spec.active_from_block_number,
            block_hash: successor.spec.active_from_block_hash.clone(),
            event_position: successor.spec.active_from_event_position,
        };
        historical_edges.push((desired_row_id, desired, terminal_state));
    }
    Ok(historical_edges)
}
