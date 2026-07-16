use anyhow::{Context, Result};
use sqlx::{Postgres, QueryBuilder, types::Uuid};

use crate::CONTRACT_KIND_CONTRACT;

use super::super::types::{ObservationTerminalState, ReconciledDiscoveryEdgeSpec};

#[path = "bulk/historical.rs"]
mod historical;

pub(super) use historical::reconcile_historical_discovery_edges;

const CONTRACT_INSTANCE_SEED_BATCH_SIZE: usize = 1000;
const DISCOVERY_EDGE_INSERT_BATCH_SIZE: usize = 1000;
pub(super) struct DiscoveryEdgeInsertSummary {
    pub(super) inserted_count: usize,
    pub(super) reactivated_count: usize,
}

#[derive(Clone, Debug)]
pub(super) struct PendingContractInstanceSeed {
    pub(super) contract_instance_id: Uuid,
    pub(super) chain: String,
    pub(super) address: String,
    pub(super) source_manifest_id: i64,
    pub(super) instance_provenance_json: serde_json::Value,
    pub(super) address_provenance_json: serde_json::Value,
}

pub(super) async fn insert_pending_contract_instance_seeds(
    executor: &mut sqlx::postgres::PgConnection,
    seeds: &[PendingContractInstanceSeed],
) -> Result<()> {
    if seeds.is_empty() {
        return Ok(());
    }

    for chunk in seeds.chunks(CONTRACT_INSTANCE_SEED_BATCH_SIZE) {
        let mut builder = QueryBuilder::<Postgres>::new(
            r#"
            INSERT INTO contract_instances (
                contract_instance_id,
                chain_id,
                contract_kind,
                provenance
            )
            "#,
        );
        builder.push_values(chunk, |mut row, seed| {
            row.push_bind(seed.contract_instance_id)
                .push_bind(&seed.chain)
                .push_bind(CONTRACT_KIND_CONTRACT)
                .push_bind(&seed.instance_provenance_json);
        });
        builder
            .build()
            .execute(&mut *executor)
            .await
            .context("failed to bulk insert discovered contract instances")?;
    }

    for chunk in seeds.chunks(CONTRACT_INSTANCE_SEED_BATCH_SIZE) {
        let mut builder = QueryBuilder::<Postgres>::new(
            r#"
            INSERT INTO contract_instance_addresses (
                contract_instance_id,
                chain_id,
                address,
                source_manifest_id,
                provenance
            )
            "#,
        );
        builder.push_values(chunk, |mut row, seed| {
            row.push_bind(seed.contract_instance_id)
                .push_bind(&seed.chain)
                .push_bind(&seed.address)
                .push_bind(seed.source_manifest_id)
                .push_bind(&seed.address_provenance_json);
        });
        builder.push(
            r#"
            ON CONFLICT (contract_instance_id)
            WHERE deactivated_at IS NULL
            DO NOTHING
            "#,
        );
        builder
            .build()
            .execute(&mut *executor)
            .await
            .context("failed to bulk seed discovered contract-instance addresses")?;
    }

    Ok(())
}

pub(super) async fn insert_reconciled_discovery_edges(
    executor: &mut sqlx::postgres::PgConnection,
    edges: &[&ReconciledDiscoveryEdgeSpec],
) -> Result<DiscoveryEdgeInsertSummary> {
    if edges.is_empty() {
        return Ok(DiscoveryEdgeInsertSummary {
            inserted_count: 0,
            reactivated_count: 0,
        });
    }

    let mut inserted_count = 0;
    let mut reactivated_count = 0;
    for chunk in edges.chunks(DISCOVERY_EDGE_INSERT_BATCH_SIZE) {
        let edge_values = chunk
            .iter()
            .map(|edge| {
                serde_json::from_str::<serde_json::Value>(&edge.provenance_json).with_context(
                    || {
                        format!(
                            "failed to parse reconciled discovery-edge provenance for {} {} -> {}",
                            edge.edge_kind,
                            edge.from_contract_instance_id,
                            edge.to_contract_instance_id
                        )
                    },
                )
            })
            .collect::<Result<Vec<_>>>()?;
        let mut reactivation = QueryBuilder::<Postgres>::new(
            r#"
            WITH desired_edges (
                desired_index,
                chain_id,
                edge_kind,
                from_contract_instance_id,
                to_contract_instance_id,
                discovery_source,
                source_manifest_id,
                admission,
                active_from_block_number,
                active_from_block_hash,
                provenance
            ) AS (
            "#,
        );
        reactivation.push_values(
            chunk.iter().zip(edge_values.iter()).enumerate(),
            |mut row, (desired_index, (edge, provenance))| {
                row.push_bind(desired_index as i64)
                    .push_bind(&edge.chain)
                    .push_bind(&edge.edge_kind)
                    .push_bind(edge.from_contract_instance_id)
                    .push_bind(edge.to_contract_instance_id)
                    .push_bind(&edge.discovery_source)
                    .push_bind(edge.source_manifest_id)
                    .push_bind(&edge.admission)
                    .push_bind(edge.active_from_block_number)
                    .push_bind(edge.active_from_block_hash.as_deref())
                    .push_bind(provenance);
            },
        );
        reactivation.push(
            r#"
            ), reactivation_candidates AS (
                SELECT desired.desired_index,
                       (
                           SELECT edge.discovery_edge_id
                           FROM discovery_edges edge
                           WHERE edge.chain_id = desired.chain_id
                             AND edge.edge_kind = desired.edge_kind
                             AND edge.from_contract_instance_id = desired.from_contract_instance_id
                             AND edge.to_contract_instance_id = desired.to_contract_instance_id
                             AND edge.discovery_source = desired.discovery_source
                             AND edge.source_manifest_id IS NOT DISTINCT FROM desired.source_manifest_id
                             AND edge.admission = desired.admission
                             AND edge.active_from_block_number IS NOT DISTINCT FROM desired.active_from_block_number
                             AND edge.active_from_block_hash IS NOT DISTINCT FROM desired.active_from_block_hash
                             AND (
                                 edge.provenance
                                 - 'active_to_transaction_index'
                                 - 'active_to_log_index'
                             ) = desired.provenance
                             AND edge.deactivated_at IS NOT NULL
                           ORDER BY edge.discovery_edge_id DESC
                           LIMIT 1
                           FOR UPDATE
                       ) AS discovery_edge_id
                FROM desired_edges desired
            ), reactivated AS (
                UPDATE discovery_edges edge
                SET active_to_block_number = NULL,
                    active_to_block_hash = NULL,
                    deactivated_at = NULL,
                    provenance = edge.provenance
                        - 'active_to_transaction_index'
                        - 'active_to_log_index'
                FROM reactivation_candidates candidate
                WHERE edge.discovery_edge_id = candidate.discovery_edge_id
                RETURNING candidate.desired_index
            )
            SELECT desired_index
            FROM reactivated
            ORDER BY desired_index
            "#,
        );
        let reactivated_indexes = reactivation
            .build_query_scalar::<i64>()
            .fetch_all(&mut *executor)
            .await
            .context("failed to bulk reactivate reconciled discovery edges")?;
        reactivated_count += reactivated_indexes.len();
        let reactivated_indexes = reactivated_indexes
            .into_iter()
            .collect::<std::collections::HashSet<_>>();
        let edges_to_insert = chunk
            .iter()
            .zip(edge_values.iter())
            .enumerate()
            .filter(|(desired_index, _)| !reactivated_indexes.contains(&(*desired_index as i64)))
            .map(|(_, edge)| edge)
            .collect::<Vec<_>>();
        if edges_to_insert.is_empty() {
            continue;
        }

        let mut builder = QueryBuilder::<Postgres>::new(
            r#"
            INSERT INTO discovery_edges (
                chain_id,
                edge_kind,
                from_contract_instance_id,
                to_contract_instance_id,
                discovery_source,
                source_manifest_id,
                admission,
                active_from_block_number,
                active_from_block_hash,
                provenance
            )
            "#,
        );
        builder.push_values(&edges_to_insert, |mut row, (edge, provenance)| {
            row.push_bind(&edge.chain)
                .push_bind(&edge.edge_kind)
                .push_bind(edge.from_contract_instance_id)
                .push_bind(edge.to_contract_instance_id)
                .push_bind(&edge.discovery_source)
                .push_bind(edge.source_manifest_id)
                .push_bind(&edge.admission)
                .push_bind(edge.active_from_block_number)
                .push_bind(edge.active_from_block_hash.as_deref())
                .push_bind(provenance);
        });
        builder
            .build()
            .execute(&mut *executor)
            .await
            .context("failed to bulk insert reconciled discovery edges")?;
        inserted_count += edges_to_insert.len();
    }

    Ok(DiscoveryEdgeInsertSummary {
        inserted_count,
        reactivated_count,
    })
}

/// Deactivate one reconciled discovery edge, closing its active window at the
/// terminal state when one is known. `admitted_at`-anchored `deactivated_at`
/// keeps replayed deactivations monotonic against historical block times.
pub(super) async fn deactivate_reconciled_discovery_edge(
    executor: &mut sqlx::postgres::PgConnection,
    discovery_edge_id: i64,
    terminal_state: Option<&ObservationTerminalState>,
) -> Result<bool> {
    let terminal_block_number = terminal_state.and_then(|state| state.block_number);
    let terminal_block_hash = terminal_state.and_then(|state| state.block_hash.as_deref());
    let terminal_chain = terminal_state.map(|state| state.chain.as_str());
    let materializes_terminal_position = terminal_block_number.is_some()
        && terminal_block_hash.is_some()
        && terminal_state.is_some();
    let terminal_event_position = terminal_state
        .filter(|_| materializes_terminal_position)
        .and_then(|state| state.event_position);
    let result = sqlx::query(
        r#"
        UPDATE discovery_edges
        SET active_to_block_number = COALESCE($2, active_to_block_number),
            active_to_block_hash = COALESCE($3, active_to_block_hash),
            provenance = CASE
                WHEN $5::BOOLEAN THEN (
                    provenance
                    - 'active_to_transaction_index'
                    - 'active_to_log_index'
                ) || jsonb_strip_nulls(jsonb_build_object(
                    'active_to_transaction_index', $6::BIGINT,
                    'active_to_log_index', $7::BIGINT
                ))
                ELSE provenance
            END,
            deactivated_at = COALESCE(
                (
                    SELECT GREATEST(discovery_edges.admitted_at, rb.block_timestamp)
                    FROM chain_lineage rb
                    WHERE rb.chain_id = $4
                      AND rb.block_hash = $3
                    LIMIT 1
                ),
                now()
            )
        WHERE discovery_edge_id = $1
          AND deactivated_at IS NULL
          AND (
              $2::BIGINT IS NULL
              OR active_from_block_number IS NULL
              OR active_from_block_number <= $2::BIGINT
          )
        "#,
    )
    .bind(discovery_edge_id)
    .bind(terminal_block_number)
    .bind(terminal_block_hash)
    .bind(terminal_chain)
    .bind(materializes_terminal_position)
    .bind(terminal_event_position.map(|position| position.transaction_index))
    .bind(terminal_event_position.map(|position| position.log_index))
    .execute(&mut *executor)
    .await
    .with_context(|| {
        format!("failed to deactivate reconciled discovery_edge_id {discovery_edge_id}")
    })?;
    Ok(result.rows_affected() > 0)
}
