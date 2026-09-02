use sqlx::{Postgres, Transaction};

use crate::{ProjectError, Result};

pub(super) async fn include_retracted_roots(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    from_block: i64,
    to_block: i64,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO project_scope_expiry_names
         SELECT DISTINCT logical_name_id
         FROM project_redo_expiry_roots
         WHERE chain_id = $1 AND block_number BETWEEN $2 AND $3
         ON CONFLICT DO NOTHING",
    )
    .bind(chain_id)
    .bind(from_block)
    .bind(to_block)
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        ProjectError::database(
            "failed to include deleted path-expiry logical names in Project scope",
            error,
        )
    })?;
    Ok(())
}

pub(super) async fn include_expiring_names(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    from_block: i64,
    to_block: i64,
    target_block: i64,
) -> Result<()> {
    sqlx::query(
        r#"
        WITH affected_times AS (
            SELECT COALESCE((
                       SELECT extract(epoch FROM prior.block_timestamp)
                       FROM chain_lineage prior
                       WHERE prior.chain_id = $1
                         AND prior.block_number < $2
                         AND prior.canonicality_state IN (
                             'canonical', 'safe', 'finalized'
                         )
                       ORDER BY prior.block_number DESC
                       LIMIT 1
                   ), -1::numeric) AS prior_seconds,
                   max(extract(epoch FROM affected.block_timestamp)) AS target_seconds
            FROM chain_lineage affected
            WHERE affected.chain_id = $1
              AND affected.block_number BETWEEN $2 AND $3
              AND affected.canonicality_state IN (
                  'canonical', 'safe', 'finalized', 'orphaned'
              )
        ), expiry_candidate_events AS MATERIALIZED (
            SELECT event.logical_name_id,
                   COALESCE(event.resource_id::text, (
                       SELECT linked.resource_id::text
                       FROM normalized_events linked
                       JOIN chain_lineage linked_lineage
                         ON linked_lineage.chain_id = linked.chain_id
                        AND linked_lineage.block_hash = linked.block_hash
                        AND linked_lineage.block_number = linked.block_number
                       WHERE linked.chain_id = event.chain_id
                         AND linked.logical_name_id = event.logical_name_id
                         AND linked.block_number <= $4
                         AND linked.resource_id IS NOT NULL
                         AND linked.event_kind IN (
                             'RegistrationGranted', 'RegistrationReserved'
                         )
                         AND linked.source_family IN (
                             'ens_v2_root_l1', 'ens_v2_registry_l1'
                         )
                         AND linked.canonicality_state IN (
                             'canonical', 'safe', 'finalized'
                         )
                         AND linked_lineage.canonicality_state IN (
                             'canonical', 'safe', 'finalized'
                         )
                         AND COALESCE(
                             linked.after_state ->> 'registry_contract_instance_id',
                             linked.raw_fact_ref ->> 'emitting_address',
                             linked.after_state ->> 'registry'
                         ) = COALESCE(
                             event.after_state ->> 'registry_contract_instance_id',
                             event.raw_fact_ref ->> 'emitting_address',
                             event.after_state ->> 'registry'
                         )
                         AND linked.after_state ->> 'token_id' =
                             event.after_state ->> 'token_id'
                       ORDER BY linked.block_number DESC NULLS LAST,
                                linked.normalized_event_id DESC
                       LIMIT 1
                   ), NULLIF(CONCAT(
                       COALESCE(
                           event.after_state ->> 'registry_contract_instance_id',
                           event.raw_fact_ref ->> 'emitting_address',
                           event.after_state ->> 'registry'
                       ),
                       ':', event.after_state ->> 'token_id'
                   ), ':')) AS lifecycle_key
            FROM normalized_events event
            JOIN chain_lineage lineage
              ON lineage.chain_id = event.chain_id
             AND lineage.block_hash = event.block_hash
             AND lineage.block_number = event.block_number
            CROSS JOIN affected_times affected
            WHERE event.chain_id = $1
              AND event.block_number <= $4
              AND event.logical_name_id IS NOT NULL
              AND event.source_family IN (
                  'ens_v2_root_l1', 'ens_v2_registry_l1'
              )
              AND event.event_kind IN (
                  'RegistrationGranted', 'RegistrationReserved',
                  'RegistrationRenewed', 'RegistrationReleased', 'ExpiryChanged'
              )
              AND event.canonicality_state IN (
                  'canonical', 'safe', 'finalized'
              )
              AND lineage.canonicality_state IN (
                  'canonical', 'safe', 'finalized'
              )
              AND jsonb_typeof(event.after_state -> 'expiry') = 'number'
              AND (event.after_state ->> 'expiry')::numeric > affected.prior_seconds
              AND (event.after_state ->> 'expiry')::numeric <= affected.target_seconds
        ), candidate_lifecycles AS MATERIALIZED (
            SELECT DISTINCT event.logical_name_id, event.lifecycle_key
            FROM expiry_candidate_events event
            WHERE event.lifecycle_key IS NOT NULL
        ), registration_events AS (
            SELECT event.*, candidate.lifecycle_key
            FROM candidate_lifecycles candidate
            JOIN LATERAL (
                SELECT history.*
                FROM normalized_events history
                JOIN chain_lineage lineage
                  ON lineage.chain_id = history.chain_id
                 AND lineage.block_hash = history.block_hash
                 AND lineage.block_number = history.block_number
                WHERE history.chain_id = $1
                  AND history.block_number <= $4
                  AND history.logical_name_id = candidate.logical_name_id
                  AND history.source_family IN (
                      'ens_v2_root_l1', 'ens_v2_registry_l1'
                  )
                  AND history.event_kind IN (
                      'RegistrationGranted', 'RegistrationReserved',
                      'RegistrationRenewed', 'RegistrationReleased', 'ExpiryChanged'
                  )
                  AND history.canonicality_state IN (
                      'canonical', 'safe', 'finalized'
                  )
                  AND lineage.canonicality_state IN (
                      'canonical', 'safe', 'finalized'
                  )
                  AND COALESCE(history.resource_id::text, NULLIF(CONCAT(
                      COALESCE(
                          history.after_state ->> 'registry_contract_instance_id',
                          history.raw_fact_ref ->> 'emitting_address',
                          history.after_state ->> 'registry'
                      ),
                      ':', history.after_state ->> 'token_id'
                  ), ':')) = candidate.lifecycle_key
            ) event ON TRUE
        ), lifecycle_heads AS (
            SELECT DISTINCT ON (event.logical_name_id, event.lifecycle_key)
                   event.logical_name_id, event.lifecycle_key, event.event_kind,
                   event.after_state
            FROM registration_events event
            WHERE event.lifecycle_key IS NOT NULL
              AND event.event_kind IN (
                  'RegistrationGranted', 'RegistrationReserved',
                  'RegistrationRenewed', 'RegistrationReleased'
              )
            ORDER BY event.logical_name_id, event.lifecycle_key,
                     event.block_number DESC,
                     event.transaction_index DESC NULLS LAST,
                     event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
        ), expiry_heads AS (
            SELECT DISTINCT ON (event.logical_name_id, event.lifecycle_key)
                   event.logical_name_id, event.lifecycle_key,
                   (event.after_state ->> 'expiry')::numeric AS expiry
            FROM registration_events event
            WHERE event.lifecycle_key IS NOT NULL
              AND jsonb_typeof(event.after_state -> 'expiry') = 'number'
            ORDER BY event.logical_name_id, event.lifecycle_key,
                     event.block_number DESC,
                     event.transaction_index DESC NULLS LAST,
                     event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
        )
        INSERT INTO project_scope_expiry_names
        SELECT DISTINCT lifecycle.logical_name_id
        FROM lifecycle_heads lifecycle
        JOIN expiry_heads expiry USING (logical_name_id, lifecycle_key)
        CROSS JOIN affected_times affected
        WHERE lifecycle.event_kind IN (
                  'RegistrationGranted', 'RegistrationReserved',
                  'RegistrationRenewed'
              )
          AND ((expiry.expiry > affected.prior_seconds
                AND expiry.expiry <= affected.target_seconds)
               OR EXISTS (
                   SELECT 1
                   FROM registration_events changed
                   WHERE changed.logical_name_id = lifecycle.logical_name_id
                     AND changed.lifecycle_key = lifecycle.lifecycle_key
                     AND changed.block_number >= $2
               ))
        ON CONFLICT DO NOTHING
        "#,
    )
    .bind(chain_id)
    .bind(from_block)
    .bind(to_block)
    .bind(target_block)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to retain expiry name scope", error))?;
    Ok(())
}
