mod batching;
mod decode_skips;
mod discovery;
mod identity;
mod identity_names;
mod migration;
mod normalized;
mod redo;

use bigname_adapters::schema_v2::BatchOutput;
use bigname_adapters::schema_v2::seam::{
    EVENT_CLOSE_TIME_SQL, LOG_INDEX_KEY, MIGRATION_APPLIED_EVENT_KIND,
    PREIMAGE_OBSERVATION_EVENT_KIND, REDO_ARM_WIDE_CLOSE_SQL, REDO_BINDING_CLOSE_CLAMP_SQL,
    REDO_CLOSED_ARM_SQL, SURFACE_BINDING_ID_KEY, SURFACE_BOUND_EVENT_KIND,
    SURFACE_UNBOUND_EVENT_KIND, TOKEN_LINEAGE_ID_KEY, TRANSACTION_INDEX_KEY,
};
use sqlx::{PgPool, Postgres, Transaction};

use crate::Result;

#[allow(clippy::too_many_arguments)]
pub(crate) async fn batch(
    pool: &PgPool,
    chain_id: &str,
    redo_range: Option<(i64, i64)>,
    prepare_redo: bool,
    complete: bool,
    expected_orphaning_epoch: i64,
    expected_lineage: &[(i64, String)],
    output: &BatchOutput,
) -> Result<u64> {
    let preserve_outside_range_closes = redo_range.is_some();
    let mut transaction = pool.begin().await.map_err(|error| {
        crate::InterpretError::database("failed to begin interpret write transaction", error)
    })?;
    revalidate_canonical_lineage(
        &mut transaction,
        chain_id,
        expected_orphaning_epoch,
        expected_lineage,
    )
    .await?;
    if let Some((from_block, to_block)) = redo_range.filter(|_| prepare_redo) {
        prepare_redo_range(&mut transaction, chain_id, from_block, to_block).await?;
    }
    decode_skips::write(&mut transaction, &output.decode_skips).await?;
    identity::write_rows(&mut transaction, output, preserve_outside_range_closes).await?;
    discovery::write(&mut transaction, output, preserve_outside_range_closes).await?;
    normalized::events(&mut transaction, &output.normalized_events).await?;
    identity::write_transitions(&mut transaction, output).await?;
    migration::write(&mut transaction, output).await?;
    if let Some((from_block, to_block)) = redo_range.filter(|_| complete) {
        reanchor_stable_identities(&mut transaction, chain_id, from_block, to_block).await?;
    }
    transaction.commit().await.map_err(|error| {
        crate::InterpretError::database("failed to commit interpret write transaction", error)
    })?;
    Ok(estimate(output))
}

async fn revalidate_canonical_lineage(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    expected_orphaning_epoch: i64,
    expected: &[(i64, String)],
) -> Result<()> {
    let orphaning_epoch: i64 = sqlx::query_scalar(
        "SELECT COALESCE(
             (SELECT lineage_orphaning_epoch FROM chain_heads WHERE chain_id = $1 FOR SHARE),
             0
         )",
    )
    .bind(chain_id)
    .fetch_one(&mut **transaction)
    .await
    .map_err(|error| {
        crate::InterpretError::database(
            "failed to revalidate interpret lineage-orphaning epoch",
            error,
        )
    })?;
    if orphaning_epoch != expected_orphaning_epoch {
        return Err(crate::InterpretError::transient(format!(
            "interpret lineage changed between input reads and write for chain {chain_id}; retry with reloaded state"
        )));
    }
    let block_numbers = expected
        .iter()
        .map(|(number, _)| *number)
        .collect::<Vec<_>>();
    let live: Vec<(i64, String)> = sqlx::query_as(
        "
        SELECT block_number, block_hash
        FROM chain_lineage
        WHERE chain_id = $1
          AND block_number = ANY($2::bigint[])
          AND canonicality_state IN ('canonical', 'safe', 'finalized')
        ORDER BY block_number, block_hash
        FOR SHARE
        ",
    )
    .bind(chain_id)
    .bind(&block_numbers)
    .fetch_all(&mut **transaction)
    .await
    .map_err(|error| {
        crate::InterpretError::database(
            "failed to revalidate interpret batch canonical lineage",
            error,
        )
    })?;
    if live != expected {
        return Err(crate::InterpretError::transient(format!(
            "interpret batch lineage changed before write for chain {chain_id}; retry with reloaded raw facts"
        )));
    }
    Ok(())
}

async fn prepare_redo_range(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    from_block: i64,
    to_block: i64,
) -> Result<()> {
    migration::clear_redo_range(transaction, chain_id, from_block, to_block).await?;
    stage_referenced_stable_identities(transaction, chain_id, from_block, to_block).await?;
    orphan_bindings_started_in_range(transaction, chain_id, from_block, to_block).await?;
    reopen_bindings_closed_in_range(transaction, chain_id, from_block, to_block).await?;
    redo::capture_resolver_evidence(transaction, chain_id, from_block, to_block).await?;
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
          AND NOT (
              address.deactivated_at IS NOT NULL
              AND address.provenance ->> 'source' IN (
                  'manifest_declaration',
                  'manifest_proxy_implementation'
              )
          )
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

async fn stage_referenced_stable_identities(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    from_block: i64,
    to_block: i64,
) -> Result<()> {
    let token_lineage_join =
        format!("event.after_state ->> '{TOKEN_LINEAGE_ID_KEY}' = identity.token_lineage_id::text");
    for (table, identity_column, identity_join) in [
        (
            "name_surfaces",
            "logical_name_id",
            "event.logical_name_id = identity.logical_name_id",
        ),
        (
            "resources",
            "resource_id",
            "event.resource_id = identity.resource_id",
        ),
        (
            "token_lineages",
            TOKEN_LINEAGE_ID_KEY,
            token_lineage_join.as_str(),
        ),
    ] {
        let statement = format!(
            "
            WITH range_anchors AS (
                SELECT DISTINCT ON (identity.{identity_column})
                       identity.{identity_column} AS identity_id,
                       event.block_hash,
                       event.block_number,
                       event.raw_fact_ref AS provenance
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
                  AND event.block_number BETWEEN $2 AND $3
                  AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
                  AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
                ORDER BY identity.{identity_column},
                         event.block_number,
                         event.transaction_index NULLS FIRST,
                         event.log_index NULLS FIRST,
                         event.normalized_event_id
            )
            UPDATE {table} identity
            SET block_hash = anchor.block_hash,
                block_number = anchor.block_number,
                provenance = anchor.provenance,
                canonicality_state = 'orphaned'::canonicality_state,
                observed_at = now()
            FROM range_anchors anchor
            WHERE identity.{identity_column} = anchor.identity_id
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
                    format!("failed to stage {table} identities for redo"),
                    error,
                )
            })?;
    }
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
    let statement = format!(
        "
        WITH closing_events AS (
            SELECT event.logical_name_id,
                   event.resource_id,
                   event.event_kind,
                   event.block_number,
                   COALESCE(event.transaction_index, -1) AS transaction_index,
                   COALESCE(event.log_index, -1) AS log_index,
                   {EVENT_CLOSE_TIME_SQL} AS closed_at,
                   COALESCE(
                       event.after_state ->> '{SURFACE_BINDING_ID_KEY}',
                       event.after_state #>> '{{successor_binding,binding_id}}'
                   ) AS opened_binding_id,
                   ({REDO_ARM_WIDE_CLOSE_SQL}) AS arm_wide_close,
                   {REDO_CLOSED_ARM_SQL} AS closed_arm
            FROM normalized_events event
            JOIN chain_lineage lineage
              ON lineage.chain_id = event.chain_id
             AND lineage.block_hash = event.block_hash
             AND lineage.block_number = event.block_number
            WHERE event.chain_id = $1
              AND event.block_number BETWEEN $2 AND $3
              AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
              AND (
                  event.event_kind IN ('{SURFACE_BOUND_EVENT_KIND}', '{SURFACE_UNBOUND_EVENT_KIND}')
                  OR ({REDO_ARM_WIDE_CLOSE_SQL})
                  OR (
                      event.event_kind = '{MIGRATION_APPLIED_EVENT_KIND}'
                      AND event.consumer_visibility = 'activated'
                  )
              )
              AND event.logical_name_id IS NOT NULL
        ),
        targets AS (
            SELECT DISTINCT ON (binding.surface_binding_id)
                   binding.surface_binding_id,
                   successor.active_from AS surviving_successor_start
            FROM surface_bindings binding
            JOIN closing_events event
              ON event.logical_name_id = binding.logical_name_id
             AND event.closed_arm = binding.authority_arm
             AND binding.active_to = {REDO_BINDING_CLOSE_CLAMP_SQL}
             AND (
                    (
                        event.event_kind = '{SURFACE_UNBOUND_EVENT_KIND}'
                        AND event.resource_id = binding.resource_id
                    )
                    OR (
                        event.arm_wide_close
                        AND event.opened_binding_id <> binding.surface_binding_id::text
                    )
                    OR (
                        event.event_kind IN ('{SURFACE_BOUND_EVENT_KIND}', '{MIGRATION_APPLIED_EVENT_KIND}')
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
                  AND candidate.authority_arm = binding.authority_arm
                  AND candidate.surface_binding_id <> binding.surface_binding_id
                  AND candidate.canonicality_state IN ('canonical', 'safe', 'finalized')
                  AND (
                      candidate.block_number > event.block_number
                      OR (
                          candidate.block_number = event.block_number
                          AND (
                              COALESCE(
                                  (candidate.provenance ->> '{TRANSACTION_INDEX_KEY}')::bigint,
                                  -1
                              ),
                              COALESCE(
                                  (candidate.provenance ->> '{LOG_INDEX_KEY}')::bigint,
                                  -1
                              )
                          ) > (event.transaction_index, event.log_index)
                      )
                  )
                ORDER BY candidate.block_number,
                         COALESCE(
                             (candidate.provenance ->> '{TRANSACTION_INDEX_KEY}')::bigint,
                             -1
                         ),
                         COALESCE(
                             (candidate.provenance ->> '{LOG_INDEX_KEY}')::bigint,
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
        "
    );
    sqlx::query(&statement)
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
    let token_lineage_join =
        format!("event.after_state ->> '{TOKEN_LINEAGE_ID_KEY}' = identity.token_lineage_id::text");
    for (table, identity_join) in [
        (
            "name_surfaces",
            "event.logical_name_id = identity.logical_name_id",
        ),
        ("resources", "event.resource_id = identity.resource_id"),
        ("token_lineages", token_lineage_join.as_str()),
    ] {
        let identity_column = match table {
            "name_surfaces" => "logical_name_id",
            "resources" => "resource_id",
            "token_lineages" => TOKEN_LINEAGE_ID_KEY,
            _ => unreachable!("fixed stable identity table"),
        };
        let candidate_filter = if table == "name_surfaces" {
            // Only an observation that re-states the surface body can anchor the surface; a
            // surviving reference of any other kind must not move the anchor or deactivated_at.
            format!("AND event.event_kind = '{PREIMAGE_OBSERVATION_EVENT_KIND}'")
        } else {
            String::new()
        };
        let deactivation_assignment = if table == "name_surfaces" {
            ",
                deactivated_at = CASE
                    WHEN identity.visibility_state = 'shadow'
                        THEN candidate.block_timestamp
                    ELSE NULL
                END"
        } else {
            ""
        };
        let statement = format!(
            "
            WITH candidates AS (
                SELECT identity.{identity_column} AS identity_id,
                       event.block_hash,
                       event.block_number,
                       event.raw_fact_ref AS provenance,
                       lineage.canonicality_state,
                       lineage.block_timestamp,
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
                  AND identity.canonicality_state = 'orphaned'
                  AND event.block_number IS NOT NULL
                  AND event.block_number NOT BETWEEN $2 AND $3
                  {candidate_filter}
                  AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
                  AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
            )
            UPDATE {table} identity
            SET block_hash = candidate.block_hash,
                block_number = candidate.block_number,
                provenance = candidate.provenance,
                canonicality_state = candidate.canonicality_state{deactivation_assignment},
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
    let rows = output.decode_skips.len()
        + output.normalized_events.len()
        + output.label_preimages.len()
        + output.name_surfaces.len()
        + output.token_lineages.len()
        + output.resources.len()
        + output.surface_bindings.len()
        + output.contract_instances.len()
        + output.contract_addresses.len()
        + output.discovery_edges.len();
    let rows = rows
        + output.migration_event_associations.len()
        + output.migration_discovery_associations.len()
        + output.migration_candidate_identity_effects.len()
        + output.migration_candidate_discovery_effects.len();
    u64::try_from(rows).unwrap_or(u64::MAX).saturating_mul(512)
}
