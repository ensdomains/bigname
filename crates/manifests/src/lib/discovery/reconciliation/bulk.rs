use anyhow::{Context, Result};
use sqlx::{Postgres, QueryBuilder, postgres::PgArguments, query::QueryScalar, types::Uuid};

use crate::CONTRACT_KIND_CONTRACT;

use super::super::types::{ObservationTerminalState, ReconciledDiscoveryEdgeSpec};

const CONTRACT_INSTANCE_SEED_BATCH_SIZE: usize = 1000;
const DISCOVERY_EDGE_INSERT_BATCH_SIZE: usize = 1000;
const DISCOVERY_EDGE_IDENTITY_PREDICATE: &str = r#"
    chain_id = $1
    AND edge_kind = $2
    AND from_contract_instance_id = $3
    AND to_contract_instance_id = $4
    AND discovery_source = $5
    AND source_manifest_id IS NOT DISTINCT FROM $6
    AND admission = $7
    AND active_from_block_number IS NOT DISTINCT FROM $8
    AND active_from_block_hash IS NOT DISTINCT FROM $9
    AND (
        provenance
        - 'active_to_transaction_index'
        - 'active_to_log_index'
    ) = $10::JSONB
"#;

fn bind_discovery_edge_identity<'q>(
    query: QueryScalar<'q, Postgres, i64, PgArguments>,
    edge: &'q ReconciledDiscoveryEdgeSpec,
) -> QueryScalar<'q, Postgres, i64, PgArguments> {
    query
        .bind(&edge.chain)
        .bind(&edge.edge_kind)
        .bind(edge.from_contract_instance_id)
        .bind(edge.to_contract_instance_id)
        .bind(&edge.discovery_source)
        .bind(edge.source_manifest_id)
        .bind(&edge.admission)
        .bind(edge.active_from_block_number)
        .bind(edge.active_from_block_hash.as_deref())
        .bind(&edge.provenance_json)
}

pub(super) struct DiscoveryEdgeInsertSummary {
    pub(super) inserted_count: usize,
    pub(super) reactivated_count: usize,
}

pub(super) struct HistoricalDiscoveryEdgeSummary {
    pub(super) inserted_count: usize,
    pub(super) updated_count: usize,
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

    let mut edges_to_insert = Vec::new();
    let mut reactivated_count = 0;
    for edge in edges {
        let reactivate_sql = format!(
            r#"
            UPDATE discovery_edges
            SET active_to_block_number = NULL,
                active_to_block_hash = NULL,
                deactivated_at = NULL,
                provenance = provenance
                    - 'active_to_transaction_index'
                    - 'active_to_log_index'
            WHERE discovery_edge_id = (
                SELECT discovery_edge_id
                FROM discovery_edges
                WHERE {DISCOVERY_EDGE_IDENTITY_PREDICATE}
                  AND deactivated_at IS NOT NULL
                ORDER BY discovery_edge_id DESC
                LIMIT 1
                FOR UPDATE
            )
            RETURNING discovery_edge_id
            "#,
        );
        let reactivated =
            bind_discovery_edge_identity(sqlx::query_scalar::<_, i64>(&reactivate_sql), edge)
                .fetch_optional(&mut *executor)
                .await
                .with_context(|| {
                    format!(
                        "failed to reactivate reconciled discovery edge {} {} -> {}",
                        edge.edge_kind,
                        edge.from_contract_instance_id,
                        edge.to_contract_instance_id
                    )
                })?;
        if reactivated.is_some() {
            reactivated_count += 1;
        } else {
            edges_to_insert.push(*edge);
        }
    }

    for chunk in edges_to_insert.chunks(DISCOVERY_EDGE_INSERT_BATCH_SIZE) {
        let provenance_values = chunk
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
        builder.push_values(
            chunk.iter().zip(provenance_values.iter()),
            |mut row, (edge, provenance)| {
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
            },
        );
        builder
            .build()
            .execute(&mut *executor)
            .await
            .context("failed to bulk insert reconciled discovery edges")?;
    }

    Ok(DiscoveryEdgeInsertSummary {
        inserted_count: edges_to_insert.len(),
        reactivated_count,
    })
}

pub(super) async fn reconcile_historical_discovery_edges(
    executor: &mut sqlx::postgres::PgConnection,
    edges: &[(&ReconciledDiscoveryEdgeSpec, ObservationTerminalState)],
) -> Result<HistoricalDiscoveryEdgeSummary> {
    let mut inserted_count = 0;
    let mut updated_count = 0;
    for (edge, terminal_state) in edges {
        let terminal_transaction_index = terminal_state
            .event_position
            .map(|position| position.transaction_index);
        let terminal_log_index = terminal_state
            .event_position
            .map(|position| position.log_index);
        let close_historical_sql = format!(
            r#"
            UPDATE discovery_edges
            SET active_to_block_number = $11,
                active_to_block_hash = $12,
                deactivated_at = COALESCE(deactivated_at, now()),
                provenance = (
                    provenance
                    - 'active_to_transaction_index'
                    - 'active_to_log_index'
                ) || jsonb_strip_nulls(jsonb_build_object(
                    'active_to_transaction_index', $13::BIGINT,
                    'active_to_log_index', $14::BIGINT
                ))
            WHERE {DISCOVERY_EDGE_IDENTITY_PREDICATE}
              AND (
                  deactivated_at IS NULL
                  OR active_to_block_number IS NULL
                  OR active_to_block_number > $11
                  OR (
                      active_to_block_number = $11
                      AND $13::BIGINT IS NOT NULL
                      AND (
                          (provenance ->> 'active_to_transaction_index')::BIGINT IS NULL
                          OR (
                              (provenance ->> 'active_to_transaction_index')::BIGINT,
                              (provenance ->> 'active_to_log_index')::BIGINT
                          ) > ($13::BIGINT, $14::BIGINT)
                      )
                  )
              )
            RETURNING discovery_edge_id
            "#,
        );
        let updated =
            bind_discovery_edge_identity(sqlx::query_scalar::<_, i64>(&close_historical_sql), edge)
                .bind(terminal_state.block_number)
                .bind(terminal_state.block_hash.as_deref())
                .bind(terminal_transaction_index)
                .bind(terminal_log_index)
                .fetch_all(&mut *executor)
                .await
                .with_context(|| {
                    format!(
                        "failed to close historical reconciled discovery edge {} {} -> {}",
                        edge.edge_kind,
                        edge.from_contract_instance_id,
                        edge.to_contract_instance_id
                    )
                })?;
        updated_count += updated.len();

        let inserted = if updated.is_empty() {
            let insert_historical_sql = format!(
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
                active_to_block_number,
                active_to_block_hash,
                deactivated_at,
                provenance
            )
            SELECT
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $11, $12, now(),
                (
                    $10::JSONB
                    - 'active_to_transaction_index'
                    - 'active_to_log_index'
                ) || jsonb_strip_nulls(jsonb_build_object(
                    'active_to_transaction_index', $13::BIGINT,
                    'active_to_log_index', $14::BIGINT
                ))
            WHERE NOT EXISTS (
                SELECT 1
                FROM discovery_edges
                WHERE {DISCOVERY_EDGE_IDENTITY_PREDICATE}
            )
            RETURNING discovery_edge_id
            "#,
            );
            bind_discovery_edge_identity(sqlx::query_scalar::<_, i64>(&insert_historical_sql), edge)
                .bind(terminal_state.block_number)
                .bind(terminal_state.block_hash.as_deref())
                .bind(terminal_transaction_index)
                .bind(terminal_log_index)
                .fetch_optional(&mut *executor)
                .await
                .with_context(|| {
                    format!(
                        "failed to insert historical reconciled discovery edge {} {} -> {}",
                        edge.edge_kind,
                        edge.from_contract_instance_id,
                        edge.to_contract_instance_id
                    )
                })?
        } else {
            None
        };
        inserted_count += usize::from(inserted.is_some());

        updated_count += sqlx::query(
            r#"
            UPDATE discovery_edges
            SET active_to_block_number = $6,
                active_to_block_hash = $7,
                deactivated_at = COALESCE(deactivated_at, now()),
                provenance = (
                    provenance
                    - 'active_to_transaction_index'
                    - 'active_to_log_index'
                ) || jsonb_strip_nulls(jsonb_build_object(
                    'active_to_transaction_index', $8::BIGINT,
                    'active_to_log_index', $9::BIGINT
                ))
            WHERE chain_id = $1
              AND discovery_source = $2
              AND edge_kind = $3
              AND from_contract_instance_id = $4
              AND provenance ->> 'observation_key' = $5
              AND (
                  active_from_block_number < $6
                  OR (
                      active_from_block_number = $6
                      AND $8::BIGINT IS NOT NULL
                      AND (provenance ->> 'transaction_index')::BIGINT IS NOT NULL
                      AND (provenance ->> 'log_index')::BIGINT IS NOT NULL
                      AND (
                          (provenance ->> 'transaction_index')::BIGINT,
                          (provenance ->> 'log_index')::BIGINT
                      ) < ($8::BIGINT, $9::BIGINT)
                  )
              )
              AND (
                  active_to_block_number IS NULL
                  OR active_to_block_number > $6
                  OR (
                      active_to_block_number = $6
                      AND $8::BIGINT IS NOT NULL
                      AND (provenance ->> 'active_to_transaction_index')::BIGINT IS NOT NULL
                      AND (provenance ->> 'active_to_log_index')::BIGINT IS NOT NULL
                      AND (
                          (provenance ->> 'active_to_transaction_index')::BIGINT,
                          (provenance ->> 'active_to_log_index')::BIGINT
                      ) > ($8::BIGINT, $9::BIGINT)
                  )
              )
            "#,
        )
        .bind(&edge.chain)
        .bind(&edge.discovery_source)
        .bind(&edge.edge_kind)
        .bind(edge.from_contract_instance_id)
        .bind(&edge.observation_key)
        .bind(edge.active_from_block_number)
        .bind(edge.active_from_block_hash.as_deref())
        .bind(
            edge.active_from_event_position
                .map(|position| position.transaction_index),
        )
        .bind(
            edge.active_from_event_position
                .map(|position| position.log_index),
        )
        .execute(&mut *executor)
        .await
        .with_context(|| {
            format!(
                "failed to close discovery predecessors before {} {} -> {}",
                edge.edge_kind, edge.from_contract_instance_id, edge.to_contract_instance_id
            )
        })?
        .rows_affected() as usize;
    }

    Ok(HistoricalDiscoveryEdgeSummary {
        inserted_count,
        updated_count,
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
