use bigname_adapters::schema_v2::seam::{LOG_INDEX_KEY, OBSERVATION_KEY, TRANSACTION_INDEX_KEY};
use bigname_adapters::schema_v2::{BatchOutput, DiscoveryEdge, DiscoveryEdgeClosure};
use sqlx::{Postgres, QueryBuilder, Transaction, types::Uuid};

use crate::{InterpretError, Result};

use super::batching::{batch_row_context, conflict_free_batches};

type ActiveAddressEpoch = (i64, Option<i64>, Option<String>, Option<i64>, Option<i64>);
pub(super) async fn write(
    transaction: &mut Transaction<'_, Postgres>,
    output: &BatchOutput,
    preserve_outside_range_closes: bool,
) -> Result<()> {
    for (start, batch) in conflict_free_batches(&output.contract_instances, |instance| {
        instance.contract_instance_id
    }) {
        let mut query = QueryBuilder::<Postgres>::new(
            "
            INSERT INTO contract_instances (
                contract_instance_id, chain_id, contract_kind, provenance
            )
            ",
        );
        query.push_values(batch, |mut row, instance| {
            row.push_bind(instance.contract_instance_id)
                .push_bind(&instance.chain_id)
                .push_bind(&instance.contract_kind)
                .push_bind(&instance.provenance);
        });
        query.push(" ON CONFLICT (contract_instance_id) DO NOTHING");
        query
            .build()
            .execute(&mut **transaction)
            .await
            .map_err(|error| {
                let context = batch_row_context(
                    start,
                    batch.iter().map(|instance| instance.contract_instance_id),
                );
                InterpretError::database(
                    format!("failed to write discovered-contract batch; {context}"),
                    error,
                )
            })?;
    }
    for address in &output.contract_addresses {
        let active: Option<ActiveAddressEpoch> = sqlx::query_as(
            "
            SELECT current.contract_instance_address_id,
                   current.active_from_block_number,
                   current.active_from_block_hash,
                   (
                       SELECT max(history.active_to_block_number)
                       FROM contract_instance_addresses history
                       WHERE history.contract_instance_id = current.contract_instance_id
                         AND history.chain_id = current.chain_id
                         AND lower(history.address) = lower(current.address)
                         AND history.contract_instance_address_id <>
                             current.contract_instance_address_id
                   ) AS prior_epoch_end,
                   (
                       SELECT max(history.active_to_block_number)
                       FROM contract_instance_addresses history
                       WHERE history.contract_instance_id = current.contract_instance_id
                         AND history.chain_id = current.chain_id
                         AND lower(history.address) = lower(current.address)
                         AND history.deactivated_at IS NOT NULL
                         AND history.provenance ->> 'source' IN
                             ('manifest_declaration', 'manifest_proxy_implementation')
                   ) AS manifest_retired_through
            FROM contract_instance_addresses current
            WHERE current.contract_instance_id = $1
              AND current.chain_id = $2
              AND lower(current.address) = lower($3)
              AND current.deactivated_at IS NULL
            ORDER BY current.admitted_at DESC
            LIMIT 1
            FOR UPDATE
            ",
        )
        .bind(address.contract_instance_id)
        .bind(&address.chain_id)
        .bind(&address.address)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|error| {
            InterpretError::database("failed to lock discovered address epoch", error)
        })?;
        if let Some((row_id, current_start, current_hash, prior_end, retired_through)) = active {
            if retired_through.is_some_and(|end| address.active_from_block_number <= end) {
                continue;
            }
            let bounded_start = bound_address_epoch(address.active_from_block_number, prior_end)?;
            let effective_start = current_start.map(|start| start.min(bounded_start));
            let effective_hash = if current_start == effective_start {
                current_hash
            } else if effective_start == Some(address.active_from_block_number) {
                Some(address.active_from_block_hash.clone())
            } else {
                None
            };
            sqlx::query(
                "
                UPDATE contract_instance_addresses
                SET active_from_block_number = $2,
                    active_from_block_hash = $3,
                    source_manifest_id = CASE
                        WHEN provenance ->> 'source' IN
                            ('manifest_declaration', 'manifest_proxy_implementation')
                        THEN source_manifest_id
                        ELSE $4
                    END,
                    provenance = CASE
                        WHEN provenance ->> 'source' IN
                            ('manifest_declaration', 'manifest_proxy_implementation')
                        THEN provenance
                        ELSE $5
                    END
                WHERE contract_instance_address_id = $1
                ",
            )
            .bind(row_id)
            .bind(effective_start)
            .bind(effective_hash)
            .bind(address.source_manifest_id)
            .bind(&address.provenance)
            .execute(&mut **transaction)
            .await
            .map_err(|error| {
                InterpretError::database("failed to backdate discovered address", error)
            })?;
            continue;
        }
        let (prior_end, manifest_retired_through): (Option<i64>, Option<i64>) = sqlx::query_as(
            "
            SELECT max(active_to_block_number),
                   max(active_to_block_number) FILTER (
                       WHERE deactivated_at IS NOT NULL
                         AND provenance ->> 'source' IN
                             ('manifest_declaration', 'manifest_proxy_implementation')
                   )
            FROM contract_instance_addresses
            WHERE contract_instance_id = $1
              AND chain_id = $2
              AND lower(address) = lower($3)
            ",
        )
        .bind(address.contract_instance_id)
        .bind(&address.chain_id)
        .bind(&address.address)
        .fetch_one(&mut **transaction)
        .await
        .map_err(|error| {
            InterpretError::database("failed to bound new discovered address epoch", error)
        })?;
        if manifest_retired_through.is_some_and(|end| address.active_from_block_number <= end) {
            continue;
        }
        let bounded_start = bound_address_epoch(address.active_from_block_number, prior_end)?;
        let bounded_hash = (bounded_start == address.active_from_block_number)
            .then_some(&address.active_from_block_hash);
        sqlx::query(
            "
            INSERT INTO contract_instance_addresses (
                contract_instance_id, chain_id, address,
                active_from_block_number, active_from_block_hash,
                source_manifest_id, provenance
            )
            SELECT $1, $2, lower($3), $4, $5, $6, $7
            WHERE NOT EXISTS (
                SELECT 1
                FROM contract_instance_addresses existing
                WHERE existing.chain_id = $2
                  AND lower(existing.address) = lower($3)
                  AND existing.deactivated_at IS NULL
            )
            ",
        )
        .bind(address.contract_instance_id)
        .bind(&address.chain_id)
        .bind(&address.address)
        .bind(bounded_start)
        .bind(bounded_hash)
        .bind(address.source_manifest_id)
        .bind(&address.provenance)
        .execute(&mut **transaction)
        .await
        .map_err(|error| InterpretError::database("failed to write discovered address", error))?;
    }

    let mut operations = output
        .discovery_edge_closures
        .iter()
        .map(Operation::Close)
        .chain(output.discovery_edges.iter().map(Operation::Open))
        .collect::<Vec<_>>();
    operations.sort_by_key(Operation::order_key);
    for operation in operations {
        match operation {
            Operation::Close(closure) => close(transaction, closure).await?,
            Operation::Open(edge) => open(transaction, edge, preserve_outside_range_closes).await?,
        }
    }
    Ok(())
}

fn bound_address_epoch(event_block: i64, prior_end: Option<i64>) -> Result<i64> {
    let Some(prior_end) = prior_end else {
        return Ok(event_block);
    };
    let floor = prior_end.checked_add(1).ok_or_else(|| {
        InterpretError::data_integrity("discovered address epoch end overflowed BIGINT")
    })?;
    Ok(event_block.max(floor))
}

enum Operation<'a> {
    Close(&'a DiscoveryEdgeClosure),
    Open(&'a DiscoveryEdge),
}

impl Operation<'_> {
    fn order_key(&self) -> (i64, i64, i64, u8) {
        match self {
            Self::Close(closure) => (
                closure.active_to_block_number,
                closure.transaction_index,
                closure.log_index,
                0,
            ),
            Self::Open(edge) => (
                edge.active_from_block_number,
                edge.provenance
                    .get(TRANSACTION_INDEX_KEY)
                    .and_then(serde_json::Value::as_i64)
                    .unwrap_or(-1),
                edge.provenance
                    .get(LOG_INDEX_KEY)
                    .and_then(serde_json::Value::as_i64)
                    .unwrap_or(-1),
                1,
            ),
        }
    }
}

async fn close(
    transaction: &mut Transaction<'_, Postgres>,
    closure: &DiscoveryEdgeClosure,
) -> Result<()> {
    let statement = format!(
        "
        UPDATE discovery_edges
        SET active_to_block_number = $5,
            active_to_block_hash = $6,
            deactivated_at = now()
        WHERE chain_id = $1
          AND from_contract_instance_id = $2
          AND edge_kind = $3
          AND provenance ->> '{OBSERVATION_KEY}' = $4
          AND canonicality_state <> 'orphaned'
          AND ($7::uuid IS NULL OR to_contract_instance_id <> $7)
          AND (
              active_from_block_number IS NULL
              OR active_from_block_number <= $5
          )
          AND (
              active_to_block_number IS NULL
              OR active_to_block_number > $5
          )
        "
    );
    sqlx::query(&statement)
        .bind(&closure.chain_id)
        .bind(closure.from_contract_instance_id)
        .bind(&closure.edge_kind)
        .bind(&closure.observation_key)
        .bind(closure.active_to_block_number)
        .bind(&closure.active_to_block_hash)
        .bind(closure.except_to_contract_instance_id)
        .execute(&mut **transaction)
        .await
        .map_err(|error| InterpretError::database("failed to close discovery edge", error))?;
    Ok(())
}

async fn open(
    transaction: &mut Transaction<'_, Postgres>,
    edge: &DiscoveryEdge,
    preserve_outside_range_closes: bool,
) -> Result<()> {
    let (transaction_index, log_index) = edge_position(edge)?;

    let existing_active_statement = format!(
        "
        SELECT EXISTS (
            SELECT 1
            FROM discovery_edges existing
            WHERE existing.chain_id = $1
              AND existing.edge_kind = $2
              AND existing.from_contract_instance_id = $3
              AND existing.to_contract_instance_id = $4
              AND existing.provenance ->> '{OBSERVATION_KEY}' = $5
              AND existing.canonicality_state <> 'orphaned'
              AND existing.deactivated_at IS NULL
              AND (
                  existing.active_from_block_number < $6
                  OR (
                      existing.active_from_block_number = $6
                      AND (
                          COALESCE(
                              (existing.provenance ->> '{TRANSACTION_INDEX_KEY}')::bigint, -1
                          ),
                          COALESCE(
                              (existing.provenance ->> '{LOG_INDEX_KEY}')::bigint, -1
                          )
                      ) <= ($7, $8)
                  )
              )
        )
        "
    );
    let existing_active: bool = sqlx::query_scalar(&existing_active_statement)
        .bind(&edge.chain_id)
        .bind(&edge.edge_kind)
        .bind(edge.from_contract_instance_id)
        .bind(edge.to_contract_instance_id)
        .bind(&edge.observation_key)
        .bind(edge.active_from_block_number)
        .bind(transaction_index)
        .bind(log_index)
        .fetch_one(&mut **transaction)
        .await
        .map_err(|error| {
            InterpretError::database("failed to find existing discovery edge epoch", error)
        })?;
    if existing_active {
        return Ok(());
    }

    let successor_statement = format!(
        "
        SELECT existing.active_from_block_number,
               existing.active_from_block_hash,
               existing.discovery_edge_id,
               existing.to_contract_instance_id
        FROM discovery_edges existing
        WHERE existing.chain_id = $1
          AND existing.edge_kind = $2
          AND existing.from_contract_instance_id = $3
          AND existing.provenance ->> '{OBSERVATION_KEY}' = $4
          AND existing.canonicality_state <> 'orphaned'
          AND existing.active_from_block_number IS NOT NULL
          AND (
              existing.active_from_block_number > $5
              OR (
                  existing.active_from_block_number = $5
                  AND (
                      COALESCE(
                          (existing.provenance ->> '{TRANSACTION_INDEX_KEY}')::bigint, -1
                      ),
                      COALESCE(
                          (existing.provenance ->> '{LOG_INDEX_KEY}')::bigint, -1
                      )
                  ) > ($6, $7)
              )
          )
        ORDER BY existing.active_from_block_number,
                 COALESCE(
                     (existing.provenance ->> '{TRANSACTION_INDEX_KEY}')::bigint, -1
                 ),
                 COALESCE((existing.provenance ->> '{LOG_INDEX_KEY}')::bigint, -1),
                 existing.discovery_edge_id
        LIMIT 1
        FOR UPDATE
        "
    );
    let successor: Option<(i64, String, i64, Uuid)> = sqlx::query_as(&successor_statement)
        .bind(&edge.chain_id)
        .bind(&edge.edge_kind)
        .bind(edge.from_contract_instance_id)
        .bind(&edge.observation_key)
        .bind(edge.active_from_block_number)
        .bind(transaction_index)
        .bind(log_index)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|error| {
            InterpretError::database("failed to find successor discovery edge", error)
        })?;

    let cap_predecessor_statement = format!(
        "
        UPDATE discovery_edges existing
        SET active_to_block_number = $5,
            active_to_block_hash = $6,
            deactivated_at = now()
        WHERE existing.chain_id = $1
          AND existing.edge_kind = $2
          AND existing.from_contract_instance_id = $3
          AND existing.provenance ->> '{OBSERVATION_KEY}' = $4
          AND existing.to_contract_instance_id <> $9
          AND existing.canonicality_state <> 'orphaned'
          AND (
              existing.active_from_block_number < $5
              OR (
                  existing.active_from_block_number = $5
                  AND (
                      COALESCE(
                          (existing.provenance ->> '{TRANSACTION_INDEX_KEY}')::bigint, -1
                      ),
                      COALESCE(
                          (existing.provenance ->> '{LOG_INDEX_KEY}')::bigint, -1
                      )
                  ) < ($7, $8)
              )
          )
          AND (
              existing.active_to_block_number IS NULL
              OR existing.active_to_block_number > $5
          )
        "
    );
    sqlx::query(&cap_predecessor_statement)
        .bind(&edge.chain_id)
        .bind(&edge.edge_kind)
        .bind(edge.from_contract_instance_id)
        .bind(&edge.observation_key)
        .bind(edge.active_from_block_number)
        .bind(&edge.active_from_block_hash)
        .bind(transaction_index)
        .bind(log_index)
        .bind(edge.to_contract_instance_id)
        .execute(&mut **transaction)
        .await
        .map_err(|error| {
            InterpretError::database("failed to cap predecessor discovery edge", error)
        })?;

    if let Some((_, _, successor_id, successor_target)) = successor.as_ref()
        && *successor_target == edge.to_contract_instance_id
    {
        sqlx::query(
            "
            UPDATE discovery_edges
            SET discovery_source = $2,
                admission_basis = $3,
                source_manifest_id = $4,
                active_from_block_number = $5,
                active_from_block_hash = $6,
                canonicality_state = $7::canonicality_state,
                provenance = $8
            WHERE discovery_edge_id = $1
            ",
        )
        .bind(successor_id)
        .bind(&edge.discovery_source)
        .bind(&edge.admission_basis)
        .bind(edge.source_manifest_id)
        .bind(edge.active_from_block_number)
        .bind(&edge.active_from_block_hash)
        .bind(&edge.canonicality_state)
        .bind(&edge.provenance)
        .execute(&mut **transaction)
        .await
        .map_err(|error| {
            InterpretError::database("failed to backdate repeated discovery edge", error)
        })?;
        return Ok(());
    }

    let successor_block = successor.as_ref().map(|value| value.0);
    let successor_hash = successor.as_ref().map(|value| value.1.as_str());
    let reopen_statement = format!(
        "
        UPDATE discovery_edges existing
        SET discovery_source = $5,
            admission_basis = $6,
            source_manifest_id = $7,
            active_to_block_number = CASE
                WHEN $15 THEN CASE
                    WHEN existing.active_to_block_number IS NULL THEN $13
                    WHEN $13::bigint IS NULL THEN existing.active_to_block_number
                    ELSE LEAST(existing.active_to_block_number, $13)
                END
                ELSE $13
            END,
            active_to_block_hash = CASE
                WHEN $15
                  AND existing.active_to_block_number IS NOT NULL
                  AND (
                      $13::bigint IS NULL
                      OR existing.active_to_block_number <= $13
                  )
                    THEN existing.active_to_block_hash
                ELSE $14
            END,
            canonicality_state = $10::canonicality_state,
            deactivated_at = CASE
                WHEN CASE
                    WHEN $15 THEN CASE
                        WHEN existing.active_to_block_number IS NULL THEN $13
                        WHEN $13::bigint IS NULL THEN existing.active_to_block_number
                        ELSE LEAST(existing.active_to_block_number, $13)
                    END
                    ELSE $13
                END IS NULL THEN NULL
                ELSE now()
            END,
            provenance = $11
        WHERE existing.chain_id = $1
          AND existing.edge_kind = $2
          AND existing.from_contract_instance_id = $3
          AND existing.to_contract_instance_id = $4
          AND existing.source_manifest_id IS NOT DISTINCT FROM $7
          AND existing.active_from_block_number = $8
          AND existing.active_from_block_hash = $9
          AND existing.provenance ->> '{OBSERVATION_KEY}' = $12
        "
    );
    let reopened = sqlx::query(&reopen_statement)
        .bind(&edge.chain_id)
        .bind(&edge.edge_kind)
        .bind(edge.from_contract_instance_id)
        .bind(edge.to_contract_instance_id)
        .bind(&edge.discovery_source)
        .bind(&edge.admission_basis)
        .bind(edge.source_manifest_id)
        .bind(edge.active_from_block_number)
        .bind(&edge.active_from_block_hash)
        .bind(&edge.canonicality_state)
        .bind(&edge.provenance)
        .bind(&edge.observation_key)
        .bind(successor_block)
        .bind(successor_hash)
        .bind(preserve_outside_range_closes)
        .execute(&mut **transaction)
        .await
        .map_err(|error| InterpretError::database("failed to reopen discovery edge", error))?;
    if reopened.rows_affected() > 0 {
        return Ok(());
    }

    sqlx::query(
        "
        INSERT INTO discovery_edges (
            chain_id, edge_kind, from_contract_instance_id,
            to_contract_instance_id, discovery_source, admission_basis,
            source_manifest_id, active_from_block_number,
            active_from_block_hash, active_to_block_number,
            active_to_block_hash, canonicality_state, deactivated_at, provenance
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $13, $14,
                $10::canonicality_state,
                CASE WHEN $13::bigint IS NULL THEN NULL ELSE now() END,
                $11)
        ",
    )
    .bind(&edge.chain_id)
    .bind(&edge.edge_kind)
    .bind(edge.from_contract_instance_id)
    .bind(edge.to_contract_instance_id)
    .bind(&edge.discovery_source)
    .bind(&edge.admission_basis)
    .bind(edge.source_manifest_id)
    .bind(edge.active_from_block_number)
    .bind(&edge.active_from_block_hash)
    .bind(&edge.canonicality_state)
    .bind(&edge.provenance)
    .bind(&edge.observation_key)
    .bind(successor_block)
    .bind(successor_hash)
    .execute(&mut **transaction)
    .await
    .map_err(|error| InterpretError::database("failed to write discovery edge", error))?;
    Ok(())
}

fn edge_position(edge: &DiscoveryEdge) -> Result<(i64, i64)> {
    let transaction_index = edge
        .provenance
        .get(TRANSACTION_INDEX_KEY)
        .and_then(serde_json::Value::as_i64);
    let log_index = edge
        .provenance
        .get(LOG_INDEX_KEY)
        .and_then(serde_json::Value::as_i64);
    match (transaction_index, log_index) {
        (Some(transaction_index), Some(log_index)) if transaction_index >= 0 && log_index >= 0 => {
            Ok((transaction_index, log_index))
        }
        _ => Err(InterpretError::data_integrity(
            "raw-log discovery edge requires non-negative transaction and log positions",
        )),
    }
}

#[cfg(test)]
mod tests;
