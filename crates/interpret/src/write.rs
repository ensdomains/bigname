mod discovery;
mod identity;
mod identity_names;
mod normalized;

use bigname_adapters::schema_v2::BatchOutput;
use sqlx::{PgPool, Postgres, Transaction};

use crate::Result;

pub(crate) async fn batch(
    pool: &PgPool,
    chain_id: &str,
    redo_range: Option<(i64, i64)>,
    output: &BatchOutput,
) -> Result<u64> {
    let preserve_outside_range_closes = redo_range.is_some();
    let mut transaction = pool.begin().await.map_err(|error| {
        crate::InterpretError::database("failed to begin interpret write transaction", error)
    })?;
    if let Some((from_block, to_block)) = redo_range {
        prepare_redo_range(&mut transaction, chain_id, from_block, to_block).await?;
    }
    identity::write(&mut transaction, output, preserve_outside_range_closes).await?;
    discovery::write(&mut transaction, output, preserve_outside_range_closes).await?;
    normalized::events(&mut transaction, &output.normalized_events).await?;
    transaction.commit().await.map_err(|error| {
        crate::InterpretError::database("failed to commit interpret write transaction", error)
    })?;
    Ok(estimate(output))
}

async fn prepare_redo_range(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    from_block: i64,
    to_block: i64,
) -> Result<()> {
    reanchor_stable_identities(transaction, chain_id, from_block, to_block).await?;
    orphan_bindings_started_in_range(transaction, chain_id, from_block, to_block).await?;
    reopen_bindings_closed_in_range(transaction, chain_id, from_block, to_block).await?;
    sqlx::query(
        "
        DELETE FROM normalized_events
        WHERE chain_id = $1
          AND block_number BETWEEN $2 AND $3
        ",
    )
    .bind(chain_id)
    .bind(from_block)
    .bind(to_block)
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        crate::InterpretError::database("failed to clear normalized-event redo range", error)
    })?;
    for table in ["resources", "token_lineages", "name_surfaces"] {
        let statement = format!(
            "UPDATE {table}
             SET canonicality_state = 'orphaned'::canonicality_state,
                 observed_at = now()
             WHERE chain_id = $1
               AND block_number BETWEEN $2 AND $3"
        );
        sqlx::query(&statement)
            .bind(chain_id)
            .bind(from_block)
            .bind(to_block)
            .execute(&mut **transaction)
            .await
            .map_err(|error| {
                crate::InterpretError::database(
                    format!("failed to orphan {table} redo range"),
                    error,
                )
            })?;
    }
    sqlx::query(
        "
        UPDATE discovery_edges
        SET canonicality_state = 'orphaned'::canonicality_state,
            deactivated_at = COALESCE(deactivated_at, now())
        WHERE chain_id = $1
          AND active_from_block_number BETWEEN $2 AND $3
        ",
    )
    .bind(chain_id)
    .bind(from_block)
    .bind(to_block)
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        crate::InterpretError::database("failed to orphan discovery-edge redo range", error)
    })?;
    sqlx::query(
        "
        UPDATE discovery_edges
        SET active_to_block_number = NULL,
            active_to_block_hash = NULL,
            deactivated_at = NULL
        WHERE chain_id = $1
          AND active_to_block_number BETWEEN $2 AND $3
        ",
    )
    .bind(chain_id)
    .bind(from_block)
    .bind(to_block)
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        crate::InterpretError::database("failed to reopen discovery edges before redo", error)
    })?;
    sqlx::query(
        "
        DELETE FROM contract_instance_addresses address
        WHERE address.chain_id = $1
          AND address.active_from_block_number BETWEEN $2 AND $3
          AND NOT EXISTS (
              SELECT 1
              FROM manifest_contract_instances declaration
              WHERE declaration.chain_id = address.chain_id
                AND declaration.contract_instance_id = address.contract_instance_id
          )
          AND NOT EXISTS (
              SELECT 1
              FROM discovery_edges edge
              WHERE edge.chain_id = address.chain_id
                AND (
                    edge.from_contract_instance_id = address.contract_instance_id
                    OR edge.to_contract_instance_id = address.contract_instance_id
                )
                AND (
                    edge.active_from_block_number IS NULL
                    OR edge.active_from_block_number < $2
                    OR edge.active_from_block_number > $3
                )
          )
        ",
    )
    .bind(chain_id)
    .bind(from_block)
    .bind(to_block)
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        crate::InterpretError::database("failed to clear discovered-address redo range", error)
    })?;
    Ok(())
}

async fn orphan_bindings_started_in_range(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    from_block: i64,
    to_block: i64,
) -> Result<()> {
    sqlx::query(
        "
        UPDATE surface_bindings
        SET canonicality_state = 'orphaned'::canonicality_state,
            observed_at = now()
        WHERE chain_id = $1
          AND block_number BETWEEN $2 AND $3
        ",
    )
    .bind(chain_id)
    .bind(from_block)
    .bind(to_block)
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        crate::InterpretError::database("failed to orphan surface_bindings redo range", error)
    })?;
    Ok(())
}

async fn reopen_bindings_closed_in_range(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    from_block: i64,
    to_block: i64,
) -> Result<()> {
    sqlx::query(
        "
        WITH closing_events AS (
            SELECT event.logical_name_id,
                   event.resource_id,
                   event.event_kind,
                   event.block_number,
                   COALESCE(event.transaction_index, -1) AS transaction_index,
                   COALESCE(event.log_index, -1) AS log_index,
                   lineage.block_timestamp + make_interval(
                       secs => COALESCE(event.log_index, 0)::double precision / 1000000.0
                   ) AS closed_at,
                   event.after_state ->> 'surface_binding_id' AS opened_binding_id
            FROM normalized_events event
            JOIN chain_lineage lineage
              ON lineage.chain_id = event.chain_id
             AND lineage.block_hash = event.block_hash
             AND lineage.block_number = event.block_number
            WHERE event.chain_id = $1
              AND event.block_number BETWEEN $2 AND $3
              AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
              AND event.event_kind IN ('SurfaceBound', 'SurfaceUnbound')
              AND event.logical_name_id IS NOT NULL
        ),
        targets AS (
            SELECT DISTINCT ON (binding.surface_binding_id)
                   binding.surface_binding_id,
                   successor.active_from AS surviving_successor_start
            FROM surface_bindings binding
            JOIN closing_events event
              ON event.logical_name_id = binding.logical_name_id
             AND binding.active_to = GREATEST(
                    event.closed_at,
                    binding.active_from + interval '1 microsecond'
                 )
             AND (
                    (
                        event.event_kind = 'SurfaceUnbound'
                        AND event.resource_id = binding.resource_id
                    )
                    OR (
                        event.event_kind = 'SurfaceBound'
                        AND (
                            event.opened_binding_id IS NULL
                            OR event.opened_binding_id <> binding.surface_binding_id::text
                        )
                    )
                 )
            LEFT JOIN LATERAL (
                SELECT candidate.active_from
                FROM surface_bindings candidate
                WHERE candidate.chain_id = binding.chain_id
                  AND candidate.logical_name_id = binding.logical_name_id
                  AND candidate.surface_binding_id <> binding.surface_binding_id
                  AND candidate.canonicality_state IN ('canonical', 'safe', 'finalized')
                  AND (
                      candidate.block_number > event.block_number
                      OR (
                          candidate.block_number = event.block_number
                          AND (
                              COALESCE(
                                  (candidate.provenance ->> 'transaction_index')::bigint,
                                  -1
                              ),
                              COALESCE(
                                  (candidate.provenance ->> 'log_index')::bigint,
                                  -1
                              )
                          ) > (event.transaction_index, event.log_index)
                      )
                  )
                ORDER BY candidate.block_number,
                         COALESCE(
                             (candidate.provenance ->> 'transaction_index')::bigint,
                             -1
                         ),
                         COALESCE(
                             (candidate.provenance ->> 'log_index')::bigint,
                             -1
                         ),
                         candidate.surface_binding_id
                LIMIT 1
            ) successor ON true
            WHERE binding.chain_id = $1
              AND (
                  binding.canonicality_state IN ('canonical', 'safe', 'finalized')
                  OR binding.block_number BETWEEN $2 AND $3
              )
            ORDER BY binding.surface_binding_id,
                     event.block_number,
                     event.transaction_index,
                     event.log_index
        )
        UPDATE surface_bindings binding
        SET active_to = target.surviving_successor_start,
            observed_at = now()
        FROM targets target
        WHERE binding.surface_binding_id = target.surface_binding_id
        ",
    )
    .bind(chain_id)
    .bind(from_block)
    .bind(to_block)
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        crate::InterpretError::database("failed to reopen identity bindings before redo", error)
    })?;
    Ok(())
}

async fn reanchor_stable_identities(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    from_block: i64,
    to_block: i64,
) -> Result<()> {
    for (table, identity_join) in [
        (
            "name_surfaces",
            "event.logical_name_id = identity.logical_name_id",
        ),
        ("resources", "event.resource_id = identity.resource_id"),
        (
            "token_lineages",
            "event.after_state ->> 'token_lineage_id' = identity.token_lineage_id::text",
        ),
    ] {
        let identity_column = match table {
            "name_surfaces" => "logical_name_id",
            "resources" => "resource_id",
            "token_lineages" => "token_lineage_id",
            _ => unreachable!("fixed stable identity table"),
        };
        let statement = format!(
            "
            WITH candidates AS (
                SELECT identity.{identity_column} AS identity_id,
                       event.block_hash,
                       event.block_number,
                       event.raw_fact_ref AS provenance,
                       lineage.canonicality_state,
                       row_number() OVER (
                           PARTITION BY identity.{identity_column}
                           ORDER BY event.block_number,
                                    event.transaction_index NULLS FIRST,
                                    event.log_index NULLS FIRST,
                                    event.normalized_event_id
                       ) AS candidate_rank
                FROM {table} identity
                JOIN normalized_events event
                  ON event.chain_id = identity.chain_id
                 AND {identity_join}
                JOIN chain_lineage lineage
                  ON lineage.chain_id = event.chain_id
                 AND lineage.block_hash = event.block_hash
                 AND lineage.block_number = event.block_number
                WHERE identity.chain_id = $1
                  AND identity.block_number BETWEEN $2 AND $3
                  AND event.block_number IS NOT NULL
                  AND event.block_number NOT BETWEEN $2 AND $3
                  AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
                  AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
            )
            UPDATE {table} identity
            SET block_hash = candidate.block_hash,
                block_number = candidate.block_number,
                provenance = candidate.provenance,
                canonicality_state = candidate.canonicality_state,
                observed_at = now()
            FROM candidates candidate
            WHERE candidate.candidate_rank = 1
              AND identity.{identity_column} = candidate.identity_id
            "
        );
        sqlx::query(&statement)
            .bind(chain_id)
            .bind(from_block)
            .bind(to_block)
            .execute(&mut **transaction)
            .await
            .map_err(|error| {
                crate::InterpretError::database(
                    format!("failed to reanchor {table} before redo"),
                    error,
                )
            })?;
    }
    Ok(())
}

fn estimate(output: &BatchOutput) -> u64 {
    let rows = output.normalized_events.len()
        + output.label_preimages.len()
        + output.name_surfaces.len()
        + output.token_lineages.len()
        + output.resources.len()
        + output.surface_bindings.len()
        + output.contract_instances.len()
        + output.contract_addresses.len()
        + output.discovery_edges.len();
    u64::try_from(rows).unwrap_or(u64::MAX).saturating_mul(512)
}
