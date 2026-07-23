use anyhow::{Context, Result};
use sqlx::{PgPool, Postgres, QueryBuilder};

use super::super::super::types::{ObservationTerminalState, ReconciledDiscoveryEdgeSpec};
use crate::ManifestRuntimeProgress;

const HISTORICAL_DISCOVERY_EDGE_BATCH_SIZE: usize = 1000;

pub(in crate::discovery::reconciliation) struct HistoricalDiscoveryEdgeSummary {
    pub(in crate::discovery::reconciliation) inserted_count: usize,
    pub(in crate::discovery::reconciliation) updated_count: usize,
}

pub(in crate::discovery::reconciliation) async fn reconcile_historical_discovery_edges(
    executor: &mut sqlx::postgres::PgConnection,
    edges: &[(&ReconciledDiscoveryEdgeSpec, ObservationTerminalState)],
) -> Result<HistoricalDiscoveryEdgeSummary> {
    reconcile_historical_discovery_edges_inner(executor, edges, None).await
}

pub(in crate::discovery::reconciliation) async fn reconcile_historical_discovery_edges_with_progress(
    executor: &mut sqlx::postgres::PgConnection,
    edges: &[(&ReconciledDiscoveryEdgeSpec, ObservationTerminalState)],
    progress_pool: &PgPool,
    progress: &mut dyn ManifestRuntimeProgress,
) -> Result<HistoricalDiscoveryEdgeSummary> {
    reconcile_historical_discovery_edges_inner(executor, edges, Some((progress_pool, progress)))
        .await
}

async fn reconcile_historical_discovery_edges_inner(
    executor: &mut sqlx::postgres::PgConnection,
    edges: &[(&ReconciledDiscoveryEdgeSpec, ObservationTerminalState)],
    mut progress: Option<(&PgPool, &mut dyn ManifestRuntimeProgress)>,
) -> Result<HistoricalDiscoveryEdgeSummary> {
    let mut inserted_count = 0;
    let mut updated_count = 0;

    for chunk in edges.chunks(HISTORICAL_DISCOVERY_EDGE_BATCH_SIZE) {
        let provenance_values = chunk
            .iter()
            .map(|(edge, _)| {
                serde_json::from_str::<serde_json::Value>(&edge.provenance_json).with_context(
                    || {
                        format!(
                            "failed to parse historical discovery-edge provenance for {} {} -> {}",
                            edge.edge_kind,
                            edge.from_contract_instance_id,
                            edge.to_contract_instance_id
                        )
                    },
                )
            })
            .collect::<Result<Vec<_>>>()?;

        let mut materialize = QueryBuilder::<Postgres>::new(
            r#"
            WITH desired_edges (
                chain_id,
                edge_kind,
                from_contract_instance_id,
                to_contract_instance_id,
                discovery_source,
                source_manifest_id,
                admission,
                active_from_block_number,
                active_from_block_hash,
                provenance,
                terminal_block_number,
                terminal_block_hash,
                terminal_transaction_index,
                terminal_log_index
            ) AS (
            "#,
        );
        materialize.push_values(
            chunk.iter().zip(provenance_values.iter()),
            |mut row, ((edge, terminal_state), provenance)| {
                row.push_bind(&edge.chain)
                    .push_bind(&edge.edge_kind)
                    .push_bind(edge.from_contract_instance_id)
                    .push_bind(edge.to_contract_instance_id)
                    .push_bind(&edge.discovery_source)
                    .push_bind(edge.source_manifest_id)
                    .push_bind(&edge.admission)
                    .push_bind(edge.active_from_block_number)
                    .push_bind(edge.active_from_block_hash.as_deref())
                    .push_bind(provenance)
                    .push_bind(terminal_state.block_number)
                    .push_bind(terminal_state.block_hash.as_deref())
                    .push_bind(
                        terminal_state
                            .event_position
                            .map(|position| position.transaction_index),
                    )
                    .push_bind(
                        terminal_state
                            .event_position
                            .map(|position| position.log_index),
                    );
            },
        );
        materialize.push(
            r#"
            ), closed AS (
                UPDATE discovery_edges edge
                SET active_to_block_number = desired.terminal_block_number,
                    active_to_block_hash = desired.terminal_block_hash,
                    deactivated_at = COALESCE(edge.deactivated_at, now()),
                    provenance = (
                        edge.provenance
                        - 'active_to_transaction_index'
                        - 'active_to_log_index'
                    ) || jsonb_strip_nulls(jsonb_build_object(
                        'active_to_transaction_index', desired.terminal_transaction_index,
                        'active_to_log_index', desired.terminal_log_index
                    ))
                FROM desired_edges desired
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
                  AND (
                      edge.deactivated_at IS NULL
                      OR edge.active_to_block_number IS NULL
                      OR edge.active_to_block_number > desired.terminal_block_number
                      OR (
                          edge.active_to_block_number = desired.terminal_block_number
                          AND desired.terminal_transaction_index IS NOT NULL
                          AND (
                              (edge.provenance ->> 'active_to_transaction_index')::BIGINT IS NULL
                              OR (
                                  (edge.provenance ->> 'active_to_transaction_index')::BIGINT,
                                  (edge.provenance ->> 'active_to_log_index')::BIGINT
                              ) > (
                                  desired.terminal_transaction_index,
                                  desired.terminal_log_index
                              )
                          )
                      )
                  )
                RETURNING edge.discovery_edge_id
            ), inserted AS (
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
                    desired.chain_id,
                    desired.edge_kind,
                    desired.from_contract_instance_id,
                    desired.to_contract_instance_id,
                    desired.discovery_source,
                    desired.source_manifest_id,
                    desired.admission,
                    desired.active_from_block_number,
                    desired.active_from_block_hash,
                    desired.terminal_block_number,
                    desired.terminal_block_hash,
                    now(),
                    (
                        desired.provenance
                        - 'active_to_transaction_index'
                        - 'active_to_log_index'
                    ) || jsonb_strip_nulls(jsonb_build_object(
                        'active_to_transaction_index', desired.terminal_transaction_index,
                        'active_to_log_index', desired.terminal_log_index
                    ))
                FROM desired_edges desired
                WHERE NOT EXISTS (
                    SELECT 1
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
                )
                RETURNING discovery_edge_id
            )
            SELECT
                (SELECT COUNT(*)::BIGINT FROM closed),
                (SELECT COUNT(*)::BIGINT FROM inserted)
            "#,
        );
        let (closed, inserted) = materialize
            .build_query_as::<(i64, i64)>()
            .fetch_one(&mut *executor)
            .await
            .context("failed to bulk materialize historical reconciled discovery edges")?;
        updated_count += usize::try_from(closed)
            .context("historical discovery-edge update count exceeds usize")?;
        inserted_count += usize::try_from(inserted)
            .context("historical discovery-edge insert count exceeds usize")?;

        let mut close_predecessors = QueryBuilder::<Postgres>::new(
            r#"
            WITH desired_edges (
                chain_id,
                discovery_source,
                edge_kind,
                from_contract_instance_id,
                observation_key,
                active_from_block_number,
                active_from_block_hash,
                active_from_transaction_index,
                active_from_log_index
            ) AS (
            "#,
        );
        close_predecessors.push_values(chunk, |mut row, (edge, _)| {
            row.push_bind(&edge.chain)
                .push_bind(&edge.discovery_source)
                .push_bind(&edge.edge_kind)
                .push_bind(edge.from_contract_instance_id)
                .push_bind(&edge.observation_key)
                .push_bind(edge.active_from_block_number)
                .push_bind(edge.active_from_block_hash.as_deref())
                .push_bind(
                    edge.active_from_event_position
                        .map(|position| position.transaction_index),
                )
                .push_bind(
                    edge.active_from_event_position
                        .map(|position| position.log_index),
                );
        });
        close_predecessors.push(
            r#"
            ), predecessor_terminals AS (
                SELECT DISTINCT ON (edge.discovery_edge_id)
                    edge.discovery_edge_id,
                    desired.active_from_block_number,
                    desired.active_from_block_hash,
                    desired.active_from_transaction_index,
                    desired.active_from_log_index
                FROM discovery_edges edge
                JOIN desired_edges desired
                  ON edge.chain_id = desired.chain_id
                 AND edge.discovery_source = desired.discovery_source
                 AND edge.edge_kind = desired.edge_kind
                 AND edge.from_contract_instance_id = desired.from_contract_instance_id
                 AND edge.provenance ->> 'observation_key' = desired.observation_key
                WHERE (
                    edge.active_from_block_number < desired.active_from_block_number
                    OR (
                        edge.active_from_block_number = desired.active_from_block_number
                        AND desired.active_from_transaction_index IS NOT NULL
                        AND (edge.provenance ->> 'transaction_index')::BIGINT IS NOT NULL
                        AND (edge.provenance ->> 'log_index')::BIGINT IS NOT NULL
                        AND (
                            (edge.provenance ->> 'transaction_index')::BIGINT,
                            (edge.provenance ->> 'log_index')::BIGINT
                        ) < (
                            desired.active_from_transaction_index,
                            desired.active_from_log_index
                        )
                    )
                )
                  AND (
                      edge.active_to_block_number IS NULL
                      OR edge.active_to_block_number > desired.active_from_block_number
                      OR (
                          edge.active_to_block_number = desired.active_from_block_number
                          AND desired.active_from_transaction_index IS NOT NULL
                          AND (edge.provenance ->> 'active_to_transaction_index')::BIGINT IS NOT NULL
                          AND (edge.provenance ->> 'active_to_log_index')::BIGINT IS NOT NULL
                          AND (
                              (edge.provenance ->> 'active_to_transaction_index')::BIGINT,
                              (edge.provenance ->> 'active_to_log_index')::BIGINT
                          ) > (
                              desired.active_from_transaction_index,
                              desired.active_from_log_index
                          )
                      )
                  )
                ORDER BY
                    edge.discovery_edge_id,
                    desired.active_from_block_number,
                    desired.active_from_transaction_index NULLS FIRST,
                    desired.active_from_log_index NULLS FIRST,
                    desired.active_from_block_hash
            )
            UPDATE discovery_edges edge
            SET active_to_block_number = terminal.active_from_block_number,
                active_to_block_hash = terminal.active_from_block_hash,
                deactivated_at = COALESCE(edge.deactivated_at, now()),
                provenance = (
                    edge.provenance
                    - 'active_to_transaction_index'
                    - 'active_to_log_index'
                ) || jsonb_strip_nulls(jsonb_build_object(
                    'active_to_transaction_index', terminal.active_from_transaction_index,
                    'active_to_log_index', terminal.active_from_log_index
                ))
            FROM predecessor_terminals terminal
            WHERE edge.discovery_edge_id = terminal.discovery_edge_id
            "#,
        );
        let predecessor_updates = close_predecessors
            .build()
            .execute(&mut *executor)
            .await
            .context("failed to bulk close historical discovery predecessors")?
            .rows_affected();
        updated_count += usize::try_from(predecessor_updates)
            .context("historical discovery predecessor update count exceeds usize")?;
        if let Some((progress_pool, progress)) = progress.as_mut() {
            progress.record(progress_pool).await?;
        }
    }

    Ok(HistoricalDiscoveryEdgeSummary {
        inserted_count,
        updated_count,
    })
}
